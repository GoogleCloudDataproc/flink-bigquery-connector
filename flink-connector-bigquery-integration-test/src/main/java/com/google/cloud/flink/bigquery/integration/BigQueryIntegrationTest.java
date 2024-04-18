/* FOR INTERNAL USE ONLY

* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.google.cloud.flink.bigquery.integration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.sink.BigQuerySink;
import com.google.cloud.flink.bigquery.sink.BigQuerySinkConfig;
import com.google.cloud.flink.bigquery.sink.serializer.AvroToProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProvider;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProviderImpl;
import com.google.cloud.flink.bigquery.source.BigQuerySource;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The Integration Test <b>is for internal use only</b>. It sets up a pipeline which will try to
 * read the specified BigQuery table according to the command line arguments, returning {@link
 * GenericRecord} representing the rows, perform certain operations and then log the total number of
 * records read. <br>
 * This module checks the following cases of BigQuery Table read.
 *
 * <ol>
 *   <li>Bounded Jobs: Involve reading a BigQuery Table in the <i> bounded </i> mode.<br>
 *       The arguments given in this case would be:
 *       <ul>
 *         <li>--gcp-source-project {required; project ID which contains the Source BigQuery table}
 *         <li>--bq-source-dataset {required; name of Source BigQuery dataset containing the desired
 *             table} <br>
 *         <li>--bq-source-table {required; name of Source BigQuery table to read} <br>
 *         <li>--agg-prop {required; record property to aggregate in Flink job} <br>
 *         <li>--query {optional; SQL query to fetch data from BigQuery table}
 *         <li>--gcp-dest-project {optional; project ID which contains the Destination BigQuery
 *             table}
 *         <li>--bq-dest-dataset {optional; name of Destination BigQuery dataset containing the
 *             desired table}
 *         <li>--bq-dest-table {optional; name of Destination BigQuery table to write} <br>
 *         <li>--sink-parallelism {optional; parallelism for sink job}
 *         <li>--exactly-once {optional; set flag to enable exactly once approach}
 *       </ul>
 *       The sequence of operations in the read pipeline is: <i>source > flatMap > keyBy > sum </i>
 *       <br>
 *       A counter counts the total number of records read (the number of records observed by keyBy
 *       operation) and logs this count at the end. <br>
 *       Command to run bounded tests on Dataproc Cluster is: <br>
 *       {@code gcloud dataproc jobs submit flink --id {JOB_ID} --jar= {GCS_JAR_LOCATION}
 *       --cluster={CLUSTER_NAME} --region={REGION} -- --gcp-project {GCP_PROJECT_ID} --bq-dataset
 *       {BigQuery Dataset Name} --bq-table {BigQuery Table Name} --agg-prop
 *       {PROPERTY_TO_AGGREGATE_ON} --query {QUERY} } <br>
 *   <li>Unbounded Source Job: Involve reading a BigQuery Table in the <i> unbounded </i> mode.<br>
 *       This test requires some additional arguments besides the ones mentioned in the bounded
 *       mode.
 *       <ul>
 *         <li>--ts-prop {property record for timestamp}
 *         <li>--partition-discovery-interval {optional; minutes between polling table for new data.
 *             Used in unbounded/hybrid mode} <br>
 *         <li>--mode {unbounded in this case}.
 *         <li>--expected-records {optional; The total number of records expected to be read.
 *             Default Value: 210000}.
 *         <li>--timeout {optional; Time Interval (in minutes) after which the job is terminated.
 *             Default Value: 18}.
 *       </ul>
 *       The sequence of operations in this pipeline is the same as bounded one. This job is run
 *       asynchronously. The test appends newer partitions to check the read correctness. Hence,
 *       after the job is created new partitions are added. <br>
 *       Command to run unbounded tests on Dataproc Cluster is: <br>
 *       {@code gcloud dataproc jobs submit flink --id {JOB_ID} --jar= {GCS_JAR_LOCATION}
 *       --cluster={CLUSTER_NAME} --region={REGION} --async -- --gcp-project {GCP_PROJECT_ID}
 *       --bq-dataset {BigQuery Dataset Name} --bq-table {BigQuery Table Name} --agg-prop
 *       {PROPERTY_TO_AGGREGATE_ON} --mode unbounded --ts-prop {TIMESTAMP_PROPERTY}
 *       --partition-discovery-interval {PARTITION_DISCOVERY_INTERVAL} }
 * </ol>
 */
public class BigQueryIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryIntegrationTest.class);

    private static final Long CHECKPOINT_INTERVAL = 60000L;
    private static final Integer MAX_OUT_OF_ORDER = 10;
    private static final Integer MAX_IDLENESS = 20;

    public static void main(String[] args) throws Exception {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 1) {
            LOG.error(
                    "Missing parameters!\n"
                            + "Usage: flink run <additional runtime params> <jar>"
                            + " --gcp-source-project <source gcp project id>"
                            + " --bq-source-dataset <source dataset name>"
                            + " --bq-source-table <source table name>"
                            + " --agg-prop <record property to aggregate>"
                            + " --gcp-dest-project <destination gcp project id>"
                            + " --bq-dest-dataset <destination dataset name>"
                            + " --bq-dest-table <destination table name>"
                            + " --sink-parallelism <parallelism for sink>"
                            + " --exactly-once <set for sink via 'EXACTLY ONCE' approach>"
                            + " --mode <source type>"
                            + " --query <SQL query to get data from BQ table>"
                            + " --ts-prop <timestamp property>"
                            + " --partition-discovery-interval <minutes between checking new data>");
            return;
        }
        String sourceGcpProjectName = parameterTool.getRequired("gcp-source-project");
        String query = parameterTool.get("query", "");

        if (!query.isEmpty()) {
            runQueryFlinkJob(sourceGcpProjectName, query);
            return;
        }
        String sourceDatasetName = parameterTool.getRequired("bq-source-dataset");
        String sourceTableName = parameterTool.getRequired("bq-source-table");

        // Add Sink Parameters as well. (Optional)
        String destGcpProjectName = parameterTool.get("gcp-dest-project");
        String destDatasetName = parameterTool.get("bq-dest-dataset");
        String destTableName = parameterTool.get("bq-dest-table");
        Integer sinkParallelism = parameterTool.getInt("sink-parallelism");
        boolean isExactlyOnceEnabled = parameterTool.getBoolean("exactly-once", false);

        // Ignored for bounded run and can be set for unbounded mode (not required).
        String mode = parameterTool.get("mode", "bounded");
        Long expectedNumberOfRecords = parameterTool.getLong("expected-records", 210000L);
        Integer timeoutTimePeriod = parameterTool.getInt("timeout", 18);
        Integer partitionDiscoveryInterval =
                parameterTool.getInt("partition-discovery-interval", 10);

        String recordPropertyToAggregate;
        String recordPropertyForTimestamps;
        if ((destGcpProjectName != null && !destGcpProjectName.isEmpty())
                && (destDatasetName != null && !destDatasetName.isEmpty())
                && (destTableName != null && !destTableName.isEmpty())) {
            // Sink Parameters have been provided.
            switch (mode) {
                case "bounded":
                    runBoundedFlinkJobWithSink(
                            sourceGcpProjectName,
                            sourceDatasetName,
                            sourceTableName,
                            destGcpProjectName,
                            destDatasetName,
                            destTableName,
                            isExactlyOnceEnabled,
                            sinkParallelism);
                    break;
                case "unbounded":
                    recordPropertyForTimestamps = parameterTool.getRequired("ts-prop");
                    runStreamingFlinkJobWithSink(
                            sourceGcpProjectName,
                            sourceDatasetName,
                            sourceTableName,
                            destGcpProjectName,
                            destDatasetName,
                            destTableName,
                            isExactlyOnceEnabled,
                            sinkParallelism,
                            recordPropertyForTimestamps,
                            partitionDiscoveryInterval,
                            timeoutTimePeriod);
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Allowed values for mode are bounded, unbounded or hybrid. Found "
                                    + mode);
            }
        } else {
            switch (mode) {
                case "bounded":
                    recordPropertyToAggregate = parameterTool.getRequired("agg-prop");
                    runBoundedFlinkJob(
                            sourceGcpProjectName,
                            sourceDatasetName,
                            sourceTableName,
                            recordPropertyToAggregate);
                    break;
                case "unbounded":
                    recordPropertyForTimestamps = parameterTool.getRequired("ts-prop");
                    runStreamingFlinkJob(
                            sourceGcpProjectName,
                            sourceDatasetName,
                            sourceTableName,
                            recordPropertyForTimestamps,
                            partitionDiscoveryInterval,
                            expectedNumberOfRecords,
                            timeoutTimePeriod);
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Allowed values for mode are bounded, unbounded. Found " + mode);
            }
        }
    }

    private static void runQueryFlinkJob(String projectName, String query) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(CHECKPOINT_INTERVAL);

        BigQuerySource<GenericRecord> bqSource =
                BigQuerySource.readAvrosFromQuery(query, projectName);

        env.fromSource(bqSource, WatermarkStrategy.noWatermarks(), "BigQueryQuerySource")
                .map(new Mapper())
                .print();

        env.execute("Flink BigQuery Query Integration Test");
    }

    private static void runBoundedFlinkJobWithSink(
            String sourceGcpProjectName,
            String sourceDatasetName,
            String sourceTableName,
            String destGcpProjectName,
            String destDatasetName,
            String destTableName,
            boolean exactlyOnce,
            Integer sinkParallelism)
            throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(CHECKPOINT_INTERVAL);

        BigQuerySource<GenericRecord> source =
                BigQuerySource.readAvros(
                        BigQueryReadOptions.builder()
                                .setBigQueryConnectOptions(
                                        BigQueryConnectOptions.builder()
                                                .setProjectId(sourceGcpProjectName)
                                                .setDataset(sourceDatasetName)
                                                .setTable(sourceTableName)
                                                .build())
                                .build());

        BigQueryConnectOptions sinkConnectOptions =
                BigQueryConnectOptions.builder()
                        .setProjectId(destGcpProjectName)
                        .setDataset(destDatasetName)
                        .setTable(destTableName)
                        .build();

        BigQuerySchemaProvider destSchemaProvider =
                new BigQuerySchemaProviderImpl(sinkConnectOptions);

        BigQuerySinkConfig sinkConfig;

        if (!exactlyOnce) {
            sinkConfig =
                    BigQuerySinkConfig.newBuilder()
                            .connectOptions(sinkConnectOptions)
                            .schemaProvider(destSchemaProvider)
                            .serializer(new AvroToProtoSerializer())
                            .deliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                            .build();
        } else {
            throw new IllegalArgumentException("EXACTLY ONCE is not supported yet ");
        }

        DataStreamSink boundedStreamSink =
                env.fromSource(
                                source,
                                WatermarkStrategy.noWatermarks(),
                                "BigQueryBoundedSource",
                                source.getProducedType())
                        .map(
                                new MapFunction<GenericRecord, GenericRecord>() {
                                    @Override
                                    public GenericRecord map(GenericRecord genericRecord)
                                            throws Exception {
                                        genericRecord.put(
                                                "number", (int) genericRecord.get("number") + 1);
                                        return genericRecord;
                                    }
                                })
                        .returns(
                                new GenericRecordAvroTypeInfo(
                                        sinkConfig.getSchemaProvider().getAvroSchema()))
                        .sinkTo(BigQuerySink.get(sinkConfig, env));
        if (sinkParallelism != null) {
            boundedStreamSink.setParallelism(sinkParallelism);
        }

        env.execute("Flink BigQuery Bounded Read-Write Integration Test");
    }

    private static void runStreamingFlinkJobWithSink(
            String sourceProjectName,
            String sourceDatasetName,
            String sourceTableName,
            String destProjectName,
            String destDatasetName,
            String destTableName,
            boolean exactlyOnce,
            Integer sinkParallelism,
            String recordPropertyForTimestamps,
            Integer partitionDiscoveryInterval,
            Integer timeoutTimePeriod)
            throws Exception {

        BigQuerySource<GenericRecord> source =
                BigQuerySource.streamAvros(
                        BigQueryReadOptions.builder()
                                .setBigQueryConnectOptions(
                                        BigQueryConnectOptions.builder()
                                                .setProjectId(sourceProjectName)
                                                .setDataset(sourceDatasetName)
                                                .setTable(sourceTableName)
                                                .build())
                                .setPartitionDiscoveryRefreshIntervalInMinutes(
                                        partitionDiscoveryInterval)
                                .build());

        BigQueryConnectOptions sinkConnectOptions =
                BigQueryConnectOptions.builder()
                        .setProjectId(destProjectName)
                        .setDataset(destDatasetName)
                        .setTable(destTableName)
                        .build();
        BigQuerySchemaProvider destSchemaProvider =
                new BigQuerySchemaProviderImpl(sinkConnectOptions);

        BigQuerySinkConfig sinkConfig;
        if (!exactlyOnce) {
            sinkConfig =
                    BigQuerySinkConfig.newBuilder()
                            .connectOptions(sinkConnectOptions)
                            .schemaProvider(destSchemaProvider)
                            .serializer(new AvroToProtoSerializer())
                            .deliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                            .build();
        } else {
            throw new IllegalArgumentException("EXACTLY ONCE is not supported yet ");
        }

        runJobWithSink(
                source,
                sinkConfig,
                source.getProducedType(),
                recordPropertyForTimestamps,
                timeoutTimePeriod,
                sinkParallelism);
    }

    private static void runJobWithSink(
            Source<GenericRecord, ?, ?> source,
            BigQuerySinkConfig sinkConfig,
            TypeInformation<GenericRecord> typeInfo,
            String recordPropertyForTimestamps,
            Integer timeoutTimePeriod,
            Integer sinkParallelism)
            throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(CHECKPOINT_INTERVAL);

        DataStreamSink unboundedStreamSink =
                env.fromSource(
                                source,
                                WatermarkStrategy.<GenericRecord>forBoundedOutOfOrderness(
                                                Duration.ofMinutes(MAX_OUT_OF_ORDER))
                                        .withTimestampAssigner(
                                                (event, timestamp) ->
                                                        (Long)
                                                                event.get(
                                                                        recordPropertyForTimestamps))
                                        .withIdleness(Duration.ofMinutes(MAX_IDLENESS)),
                                "BigQueryStreamingSource",
                                typeInfo)
                        .returns(
                                new GenericRecordAvroTypeInfo(
                                        sinkConfig.getSchemaProvider().getAvroSchema()))
                        .sinkTo(BigQuerySink.get(sinkConfig, env));

        if (sinkParallelism != null) {
            unboundedStreamSink.setParallelism(sinkParallelism);
        }

        String jobName = "Flink BigQuery Unbounded Read-Write Integration Test";

        CompletableFuture<Void> handle =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                env.execute(jobName);
                            } catch (Exception e) {
                                LOG.error(e.getMessage());
                            }
                        });
        try {
            handle.get(timeoutTimePeriod, TimeUnit.MINUTES);
        } catch (TimeoutException e) {
            LOG.info("Job Cancelled!");
        }
    }

    private static void runJob(
            Source<GenericRecord, ?, ?> source,
            TypeInformation<GenericRecord> typeInfo,
            String recordPropertyForTimestamps,
            Long expectedValue,
            Integer timeoutTimePeriod)
            throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(CHECKPOINT_INTERVAL);

        env.fromSource(
                        source,
                        WatermarkStrategy.<GenericRecord>forBoundedOutOfOrderness(
                                        Duration.ofMinutes(MAX_OUT_OF_ORDER))
                                .withTimestampAssigner(
                                        (event, timestamp) ->
                                                (Long) event.get(recordPropertyForTimestamps))
                                .withIdleness(Duration.ofMinutes(MAX_IDLENESS)),
                        "BigQueryStreamingSource",
                        typeInfo)
                .flatMap(
                        new FlatMapFunction<GenericRecord, Tuple2<String, Integer>>() {
                            @Override
                            public void flatMap(
                                    GenericRecord value, Collector<Tuple2<String, Integer>> out)
                                    throws Exception {
                                out.collect(Tuple2.of("commonKey", 1));
                            }
                        })
                .keyBy(mappedTuple -> mappedTuple.f0)
                .process(new CustomKeyedProcessFunction(expectedValue))
                .returns(TypeInformation.of(Long.class))
                .print();

        String jobName = "Flink BigQuery Unbounded Read Integration Test";
        CompletableFuture<Void> handle =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                env.execute(jobName);
                            } catch (Exception e) {
                                LOG.error(e.getMessage());
                            }
                        });
        try {
            handle.get(timeoutTimePeriod, TimeUnit.MINUTES);
        } catch (TimeoutException e) {
            LOG.info("Job Cancelled!");
        }
    }

    private static void runBoundedFlinkJob(
            String projectName,
            String datasetName,
            String tableName,
            String recordPropertyToAggregate)
            throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(CHECKPOINT_INTERVAL);

        BigQuerySource<GenericRecord> source =
                BigQuerySource.readAvros(
                        BigQueryReadOptions.builder()
                                .setBigQueryConnectOptions(
                                        BigQueryConnectOptions.builder()
                                                .setProjectId(projectName)
                                                .setDataset(datasetName)
                                                .setTable(tableName)
                                                .build())
                                .build());

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "BigQueryQuerySource")
                .flatMap(new FlatMapper(recordPropertyToAggregate))
                .keyBy(mappedTuple -> mappedTuple.f0)
                .sum("f1");

        env.execute("Flink BigQuery Bounded Read Integration Test");
    }

    private static void runStreamingFlinkJob(
            String projectName,
            String datasetName,
            String tableName,
            String recordPropertyForTimestamps,
            Integer partitionDiscoveryInterval,
            Long expectedNumberOfRecords,
            Integer timeoutTimePeriod)
            throws Exception {

        BigQuerySource<GenericRecord> source =
                BigQuerySource.streamAvros(
                        BigQueryReadOptions.builder()
                                .setBigQueryConnectOptions(
                                        BigQueryConnectOptions.builder()
                                                .setProjectId(projectName)
                                                .setDataset(datasetName)
                                                .setTable(tableName)
                                                .build())
                                .setPartitionDiscoveryRefreshIntervalInMinutes(
                                        partitionDiscoveryInterval)
                                .build());

        runJob(
                source,
                source.getProducedType(),
                recordPropertyForTimestamps,
                expectedNumberOfRecords,
                timeoutTimePeriod);
    }

    static class FlatMapper extends RichFlatMapFunction<GenericRecord, Tuple2<String, Integer>> {

        private final String recordPropertyToAggregate;
        private transient Counter counter;

        @Override
        public void open(Configuration config) {
            this.counter =
                    getRuntimeContext().getMetricGroup().counter("number_of_records_counter_map");
        }

        public FlatMapper(String recordPropertyToAggregate) {
            this.recordPropertyToAggregate = recordPropertyToAggregate;
        }

        @Override
        public void flatMap(GenericRecord readRecord, Collector<Tuple2<String, Integer>> out)
                throws Exception {
            this.counter.inc();
            out.collect(
                    Tuple2.of(
                            String.valueOf(
                                    (readRecord.get(recordPropertyToAggregate).toString())
                                                    .hashCode()
                                            % 1000),
                            1));
        }

        @Override
        public void close() throws Exception {
            LOG.info("Number of records read: {} ;", this.counter.getCount());
        }
    }

    static class Mapper extends RichMapFunction<GenericRecord, String> {

        private transient Counter counter;

        @Override
        public void open(Configuration config) {
            this.counter =
                    getRuntimeContext()
                            .getMetricGroup()
                            .counter("number_of_records_counter_query_map");
        }

        @Override
        public void close() throws Exception {
            LOG.info("Number of records read: {} ;", this.counter.getCount());
        }

        @Override
        public String map(GenericRecord value) throws Exception {
            this.counter.inc();
            return "[ " + value.get("HOUR") + ", " + value.get("DAY") + " ]";
        }
    }

    static class CustomKeyedProcessFunction
            extends KeyedProcessFunction<String, Tuple2<String, Integer>, Long> {

        private transient ValueState<Long> numRecords;
        private final Long expectedValue;

        CustomKeyedProcessFunction(Long expectedValue) {
            this.expectedValue = expectedValue;
        }

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Long> descriptor =
                    new ValueStateDescriptor<>("numRecords", TypeInformation.of(Long.class), 0L);
            this.numRecords = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(
                Tuple2<String, Integer> value,
                KeyedProcessFunction<String, Tuple2<String, Integer>, Long>.Context ctx,
                Collector<Long> out)
                throws Exception {
            this.numRecords.update(this.numRecords.value() + 1);
            if (this.numRecords.value() > this.expectedValue) {
                LOG.info(
                        String.format(
                                "Number of records processed (%d) exceed the expected count (%d)",
                                this.numRecords.value(), this.expectedValue));
            } else if (Objects.equals(this.numRecords.value(), this.expectedValue)) {
                LOG.info(
                        String.format(
                                "%d number of records have been processed", this.expectedValue));
            }
            out.collect(this.numRecords.value());
        }
    }
}
