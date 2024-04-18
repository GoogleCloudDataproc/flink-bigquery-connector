/*
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

package com.google.cloud.flink.bigquery.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
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
import org.apache.flink.api.connector.sink2.Sink;

/**
 * A simple BigQuery table read example with Flink's DataStream API.
 *
 * <p>The Flink pipeline will try to read the specified BigQuery table according to given the
 * command line arguments, returning {@link GenericRecord} representing the rows, and print the
 * result of specified operations or write to a BigQuery table via sink.
 *
 * <p>This example module should be used in one of the following two ways.
 *
 * <ol>
 *   <li>Specify the BQ dataset and table with an optional row restriction. Users can configure a
 *       source mode, i.e bounded, unbounded or hybrid. Bounded implies that the BQ table will be
 *       read once at the time of execution, analogous to a batch job. Unbounded source implies that
 *       the BQ table will be periodically polled for new data. Hybrid source allows defining
 *       multiple sources, and in this example, we show a combination of bounded and unbounded
 *       sources. <br>
 *       The sequence of operations in bounded and hybrid pipelines are: <i>source > flatMap > keyBy
 *       > sum > print</i> <br>
 *       The sequence of operations in the unbounded pipeline is: <i>source > sink</i> <br>
 *       Flink command line format is: <br>
 *       flink run {additional runtime params} {path to this jar}/BigQueryExample.jar <br>
 *       --gcp-source-project {required; project ID containing the source table} <br>
 *       --bq-source-dataset {required; name of dataset containing the source table} <br>
 *       --bq-source-table {required; name of table to read} <br>
 *       --gcp-sink-project {required; project ID containing the sink table} <br>
 *       --bq-sink-dataset {required; name of dataset containing the sink table} <br>
 *       --bq-sink-table {required; name of table to write to} <br>
 *       --mode {optional; source read type. Allowed values are bounded (default) or unbounded or
 *       hybrid} <br>
 *       --agg-prop {required; record property to aggregate in Flink job} <br>
 *       --ts-prop {required for unbounded/hybrid mode; property record for timestamp} <br>
 *       --oldest-partition-id {optional; oldest partition id to read. Used in unbounded/hybrid
 *       mode} <br>
 *       --restriction {optional; SQL filter applied at the BigQuery table before reading} <br>
 *       --limit {optional; maximum records to read from BigQuery table} <br>
 *       --checkpoint-interval {optional; milliseconds between state checkpoints} <br>
 *       --partition-discovery-interval {optional; minutes between polling table for new data. Used
 *       in unbounded/hybrid mode} <br>
 *       --out-of-order-tolerance {optional; out of order event tolerance in minutes. Used in
 *       unbounded/hybrid mode} <br>
 *       --max-idleness {optional; minutes to wait before marking a stream partition idle. Used in
 *       unbounded/hybrid mode} <br>
 *       --window-size {optional; window size in minutes. Used in unbounded/hybrid mode}
 *   <li>Specify SQL query to fetch data from BQ dataset. For example, "SELECT * FROM
 *       some_dataset.INFORMATION_SCHEMA.PARTITIONS". This approach can only be used as a bounded
 *       source. <br>
 *       The sequence of operations in this pipeline is: <i>source > print</i> <br>
 *       Flink command line format is: <br>
 *       flink run {additional runtime params} {path to this jar}/BigQueryExample.jar <br>
 *       --gcp-project {required; project ID which contains the BigQuery table} <br>
 *       --query {required; SQL query to fetch data from BigQuery table} <br>
 *       --limit {optional; maximum records to read from BigQuery table} <br>
 *       --checkpoint-interval {optional; time interval between state checkpoints in milliseconds}
 * </ol>
 *
 * <p>Note on row restriction: In case a restriction relies on temporal reference, something like
 * {@code "TIMESTAMP_TRUNC(ingestion_timestamp, HOUR) = '2023-06-20 19:00:00'"}, and if launching
 * the job from Flink's Rest API, a known issue is that single quotes are not supported and will
 * cause the pipeline to fail. As a workaround, using \u0027 instead of the quotes will work. For
 * example {@code "TIMESTAMP_TRUNC(ingestion_timestamp, HOUR) = \u00272023-06-20 19:00:00\u0027"}.
 */
public class BigQueryExample {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryExample.class);

    public static void main(String[] args) throws Exception {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 1) {
            LOG.error(
                    "Missing parameters!\n"
                            + "Usage: flink run <additional runtime params> <jar>"
                            + " --gcp-source-project <gcp project id for source table>"
                            + " --bq-source-dataset <dataset name for source table>"
                            + " --bq-source-table <source table name>"
                            + " --gcp-sink-project <gcp project id for sink table>"
                            + " --bq-sink-dataset <dataset name for sink table>"
                            + " --bq-sink-table <sink table name>"
                            + " --agg-prop <record property to aggregate>"
                            + " --mode <source type>"
                            + " --restriction <row filter predicate>"
                            + " --limit <limit on records returned>"
                            + " --checkpoint-interval <milliseconds between state checkpoints>"
                            + " --query <SQL query to get data from BQ table>"
                            + " --ts-prop <timestamp property>"
                            + " --oldest-partition-id <oldest partition to read>"
                            + " --partition-discovery-interval <minutes between checking new data>"
                            + " --out-of-order-tolerance <minutes to accpet out of order records>"
                            + " --max-idleness <maximum idle minutes for read stream>"
                            + " --window-size <Flink's window size in minutes>");
            return;
        }
        /**
         * we will be reading avro generic records from BigQuery, and in this case we are assuming
         * the GOOGLE_APPLICATION_CREDENTIALS env variable will be present in the execution runtime.
         * In case of needing authenticate differently, the credentials builder (part of the
         * BigQueryConnectOptions) should enable capturing the credentials from various sources.
         */
        String sourceProjectName = parameterTool.getRequired("gcp-source-project");
        String query = parameterTool.get("query", "");
        Integer recordLimit = parameterTool.getInt("limit", -1);
        Long checkpointInterval = parameterTool.getLong("checkpoint-interval", 60000L);
        if (!query.isEmpty()) {
            runQueryFlinkJob(sourceProjectName, query, recordLimit, checkpointInterval);
            return;
        }
        String sourceDatasetName = parameterTool.getRequired("bq-source-dataset");
        String sourceTableName = parameterTool.getRequired("bq-source-table");
        String rowRestriction = parameterTool.get("restriction", "").replace("\\u0027", "'");
        String recordPropertyToAggregate = parameterTool.getRequired("agg-prop");
        String mode = parameterTool.get("mode", "bounded");
        String oldestPartition = parameterTool.get("oldest-partition-id", "");
        Integer partitionDiscoveryInterval =
                parameterTool.getInt("partition-discovery-interval", 10);
        Integer maxOutOfOrder = parameterTool.getInt("out-of-order-tolerance", 10);
        Integer maxIdleness = parameterTool.getInt("max-idleness", 20);
        Integer windowSize = parameterTool.getInt("window-size", 1);

        String recordPropertyForTimestamps;
        switch (mode) {
            case "bounded":
                runBoundedFlinkJob(
                        sourceProjectName,
                        sourceDatasetName,
                        sourceTableName,
                        recordPropertyToAggregate,
                        rowRestriction,
                        recordLimit,
                        checkpointInterval);
                break;
            case "unbounded":
                String sinkProjectName = parameterTool.getRequired("gcp-sink-project");
                String sinkDatasetName = parameterTool.getRequired("bq-sink-dataset");
                String sinkTableName = parameterTool.getRequired("bq-sink-table");
                recordPropertyForTimestamps = parameterTool.getRequired("ts-prop");
                runStreamingFlinkJob(
                        sourceProjectName,
                        sourceDatasetName,
                        sourceTableName,
                        sinkProjectName,
                        sinkDatasetName,
                        sinkTableName,
                        recordPropertyToAggregate,
                        recordPropertyForTimestamps,
                        rowRestriction,
                        recordLimit,
                        checkpointInterval,
                        oldestPartition,
                        partitionDiscoveryInterval,
                        maxOutOfOrder,
                        maxIdleness);
                break;
            case "hybrid":
                recordPropertyForTimestamps = parameterTool.getRequired("ts-prop");
                runHybridFlinkJob(
                        sourceProjectName,
                        sourceDatasetName,
                        sourceTableName,
                        recordPropertyToAggregate,
                        recordPropertyForTimestamps,
                        rowRestriction,
                        checkpointInterval,
                        oldestPartition,
                        partitionDiscoveryInterval,
                        maxOutOfOrder,
                        maxIdleness,
                        windowSize);
                break;
            default:
                throw new IllegalArgumentException(
                        "Allowed values for mode are bounded, unbounded or hybrid. Found " + mode);
        }
    }

    private static void runQueryFlinkJob(
            String projectName, String query, Integer limit, Long checkpointInterval)
            throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointInterval);

        BigQuerySource<GenericRecord> bqSource =
                BigQuerySource.readAvrosFromQuery(query, projectName, limit);

        env.fromSource(bqSource, WatermarkStrategy.noWatermarks(), "BigQueryQuerySource").print();

        env.execute("Flink BigQuery Query Example");
    }

    private static void runBoundedFlinkJob(
            String projectName,
            String datasetName,
            String tableName,
            String recordPropertyToAggregate,
            String rowRestriction,
            Integer limit,
            Long checkpointInterval)
            throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointInterval);

        BigQuerySource<GenericRecord> source =
                BigQuerySource.readAvros(
                        BigQueryReadOptions.builder()
                                .setBigQueryConnectOptions(
                                        BigQueryConnectOptions.builder()
                                                .setProjectId(projectName)
                                                .setDataset(datasetName)
                                                .setTable(tableName)
                                                .build())
                                .setRowRestriction(rowRestriction)
                                .setLimit(limit)
                                .build());
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "BigQuerySource")
                .flatMap(new FlatMapper(recordPropertyToAggregate))
                .keyBy(mappedTuple -> mappedTuple.f0)
                .sum("f1")
                .print();

        env.execute("Flink BigQuery Bounded Read Example");
    }

    private static void runStreamingFlinkJob(
            String sourceProjectName,
            String sourceDatasetName,
            String sourceTableName,
            String sinkProjectName,
            String sinkDatasetName,
            String sinkTableName,
            String recordPropertyToAggregate,
            String recordPropertyForTimestamps,
            String rowRestriction,
            Integer limit,
            Long checkpointInterval,
            String oldestPartition,
            Integer partitionDiscoveryInterval,
            Integer maxOutOfOrder,
            Integer maxIdleness)
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
                                .setRowRestriction(rowRestriction)
                                .setLimit(limit)
                                .setOldestPartitionId(oldestPartition)
                                .setPartitionDiscoveryRefreshIntervalInMinutes(
                                        partitionDiscoveryInterval)
                                .build());

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointInterval);

        BigQueryConnectOptions sinkConnectOptions =
                BigQueryConnectOptions.builder()
                        .setProjectId(sinkProjectName)
                        .setDataset(sinkDatasetName)
                        .setTable(sinkTableName)
                        .build();
        BigQuerySchemaProvider schemaProvider = new BigQuerySchemaProviderImpl(sinkConnectOptions);
        BigQuerySinkConfig sinkConfig =
                BigQuerySinkConfig.newBuilder()
                        .connectOptions(sinkConnectOptions)
                        .deliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .schemaProvider(schemaProvider)
                        .serializer(new AvroToProtoSerializer())
                        .build();

        Sink<GenericRecord> sink = BigQuerySink.get(sinkConfig, env);

        env.fromSource(
                        source,
                        WatermarkStrategy.<GenericRecord>forBoundedOutOfOrderness(
                                        Duration.ofMinutes(maxOutOfOrder))
                                .withTimestampAssigner(
                                        (event, timestamp) ->
                                                (Long) event.get(recordPropertyForTimestamps))
                                .withIdleness(Duration.ofMinutes(maxIdleness)),
                        "BigQueryStreamingSource",
                        source.getProducedType())
                .keyBy(record -> record.get(recordPropertyToAggregate).hashCode() % 10000)
                // add map and returns
                .sinkTo(sink);

        env.execute("Flink BigQuery Unbounded Source And Sink Example");
    }

    private static void runHybridFlinkJob(
            String projectName,
            String datasetName,
            String tableName,
            String recordPropertyToAggregate,
            String recordPropertyForTimestamps,
            String rowRestrictionForBatch,
            Long checkpointInterval,
            String oldestPartitionForStreaming,
            Integer partitionDiscoveryInterval,
            Integer maxOutOfOrder,
            Integer maxIdleness,
            Integer windowSize)
            throws Exception {

        // we will be reading the historical batch data as the restriction shared from command line.
        BigQuerySource<GenericRecord> batchSource =
                BigQuerySource.readAvros(
                        BigQueryReadOptions.builder()
                                .setBigQueryConnectOptions(
                                        BigQueryConnectOptions.builder()
                                                .setProjectId(projectName)
                                                .setDataset(datasetName)
                                                .setTable(tableName)
                                                .build())
                                .setRowRestriction(rowRestrictionForBatch)
                                .build());

        // and then reading the new data from the streaming source, as it gets available from the
        // underlying BigQuery table.
        BigQuerySource<GenericRecord> streamingSource =
                BigQuerySource.streamAvros(
                        BigQueryReadOptions.builder()
                                .setBigQueryConnectOptions(
                                        BigQueryConnectOptions.builder()
                                                .setProjectId(projectName)
                                                .setDataset(datasetName)
                                                .setTable(tableName)
                                                .build())
                                .setOldestPartitionId(oldestPartitionForStreaming)
                                .setPartitionDiscoveryRefreshIntervalInMinutes(
                                        partitionDiscoveryInterval)
                                .build());

        // create an hybrid source with both batch and streaming flavors of the BigQuery source.
        HybridSource<GenericRecord> hybridSource =
                HybridSource.builder(batchSource).addSource(streamingSource).build();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointInterval);

        env.fromSource(
                        hybridSource,
                        WatermarkStrategy.<GenericRecord>forBoundedOutOfOrderness(
                                        Duration.ofMinutes(maxOutOfOrder))
                                .withTimestampAssigner(
                                        (event, timestamp) ->
                                                (Long) event.get(recordPropertyForTimestamps))
                                .withIdleness(Duration.ofMinutes(maxIdleness)),
                        "BigQueryHybridSource",
                        streamingSource.getProducedType())
                .flatMap(new FlatMapper(recordPropertyToAggregate))
                .keyBy(mappedTuple -> mappedTuple.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(windowSize)))
                .sum("f1")
                .print();

        env.execute("Flink BigQuery Hybrid Read Example");
    }

    static class FlatMapper implements FlatMapFunction<GenericRecord, Tuple2<String, Integer>> {

        private final String recordPropertyToAggregate;

        public FlatMapper(String recordPropertyToAggregate) {
            this.recordPropertyToAggregate = recordPropertyToAggregate;
        }

        @Override
        public void flatMap(GenericRecord record, Collector<Tuple2<String, Integer>> out)
                throws Exception {
            out.collect(Tuple2.of((String) record.get(recordPropertyToAggregate).toString(), 1));
        }
    }
}
