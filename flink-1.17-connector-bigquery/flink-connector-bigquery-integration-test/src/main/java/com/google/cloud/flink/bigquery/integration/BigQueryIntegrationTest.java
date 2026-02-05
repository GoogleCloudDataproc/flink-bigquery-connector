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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TablePipeline;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.sink.BigQuerySink;
import com.google.cloud.flink.bigquery.sink.BigQuerySinkConfig;
import com.google.cloud.flink.bigquery.sink.serializer.AvroToProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProvider;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProviderImpl;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryTableSchemaProvider;
import com.google.cloud.flink.bigquery.source.BigQuerySource;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.table.config.BigQueryReadTableConfig;
import com.google.cloud.flink.bigquery.table.config.BigQuerySinkTableConfig;
import com.google.cloud.flink.bigquery.table.config.BigQueryTableConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.concat;

/**
 * The Integration Test <b>is for internal use only</b>.
 *
 * <p>It sets up a pipeline which will try to read from the specified source, either a BigQuery
 * Table (bounded) or GCS Bucket (unbounded) and write to another BigQuery Table according to the
 * command line arguments. The read returns {@link GenericRecord} representing the rows, which are
 * then written (sink) to specified BigQuery Table.<br>
 * This module tests the following cases:
 *
 * <ol>
 *   <li>Bounded Job: Involve reading from and writing to a BigQuery Table in the <i>bounded</i>
 *       mode.<br>
 *       The arguments given in this case would be:
 *       <ul>
 *         <li>--gcp-source-project {required; project ID which contains the Source BigQuery table}
 *         <li>--bq-source-dataset {required; name of Source BigQuery dataset containing the desired
 *             table} <br>
 *         <li>--bq-source-table {required; name of Source BigQuery table to read} <br>
 *         <li>--agg-prop {required; record property to aggregate in Flink job} <br>
 *         <li>--gcp-dest-project {optional; project ID which contains the Destination BigQuery
 *             table}
 *         <li>--bq-dest-dataset {optional; name of Destination BigQuery dataset containing the
 *             desired table}
 *         <li>--bq-dest-table {optional; name of Destination BigQuery table to write} <br>
 *         <li>--sink-parallelism {optional; parallelism for sink job}
 *         <li>--exactly-once {optional; set flag to enable exactly once approach}
 *         <li>--enable-table-creation {optional; set for creating BQ table in sink}
 *         <li>--is-sql {optional; set flag to run Table API methods for read and write}
 *       </ul>
 *       The sequence of operations in the read and write pipeline is: <i>source > map > sink</i>.
 *       <br>
 *       The records read are passed to a map which increments the "number" field in the BQ table by
 *       1, and writes this modified record back to another (specified) BigQuery Table. <br>
 *       In case the <code>is-sql</code> flag is set to true, Flink's Table API's <code>
 *       .select($(*))</code> method is executed. Which is responsible for reading a source table.
 *       These read records are then pass through a <code>addOrReplaceColumns()</code> method which
 *       appends a string to the "name" field in the record. These modified records are written back
 *       to BigQuery using <code>
 *       .insertInto().execute()</code>. Overall, the execution pipeline for Table API is read >
 *       addOrReplaceColumns > sink. <br>
 *       Command to run bounded tests on Dataproc Cluster is: <br>
 *       {@code gcloud dataproc jobs submit flink --id {JOB_ID} --jar= {GCS_JAR_LOCATION}
 *       --cluster={CLUSTER_NAME} --region={REGION} -- --gcp-source-project {GCP_SOURCE_PROJECT_ID}
 *       --bq-source-dataset {BigQuery Source Dataset Name} --bq-source-table {BigQuery Source Table
 *       Name} --agg-prop {PROPERTY_TO_AGGREGATE_ON} --query {QUERY} --gcp-dest-project
 *       {GCP_DESTINATION_PROJECT_ID} --bq-dest-dataset {BigQuery Destination Dataset Name}
 *       --bq-dest-table {BigQuery Destination Table Name} --sink-parallelism {Parallelism to be
 *       followed by the sink} --exactly-once {set flag to enable exactly once approach} --is-sql
 *       {set flag to enable running Flink's Table API methods} --enable-table-creation {set flag
 *       for BQ table creation in sink}} <br>
 *   <li>Unbounded Job: an unbounded source (GCS Bucket) and writing to a BigQuery Table in the <i>
 *       unbounded </i> mode.<br>
 *       This test requires some additional arguments besides the ones mentioned in the bounded
 *       mode.
 *       <ul>
 *         <li>--gcs-source-uri {required; GCS URI of source directory to read csv files}
 *         <li>--file-discovery-interval {optional; minutes between polling the GCS bucket folder
 *             for new files. Used in unbounded/hybrid mode} <br>
 *         <li>--mode {unbounded in this case}.
 *         <li>--timeout {optional; Time Interval (in minutes) after which the job is terminated.
 *             Default Value: 18}.
 *       </ul>
 *       The sequence of operations in this pipeline is <i>source > map > sink</i>. <br>
 *       The records are read from csv files in a GCS bucket directory and are passed to a map which
 *       increments the "number" field in the BQ table by 1, and writes this modified record back to
 *       another (specified) BigQuery Table. This job is run asynchronously. The test adds newer
 *       files to the GCS bucket folder to check the read correctness. Hence, after the job is
 *       created new files are added.<br>
 *       In unbounded mode, the SQL read and write is similar as described above for bounded mode.
 *       <code>select($(*))</code> method is responsible for reading a source csv file in GCS. These
 *       read records are then pass through a flat map which appends a string to the "name" field in
 *       the record. These modified records are written back to BigQuery using <code>
 *       .insertInto().execute()</code>. Overall, the execution pipeline for Table API is read >
 *       flatmap > sink. <br>
 *       New files being read from GCS Bucket and written in similar manner to BigQuery as per the
 *       described unbounded mode test in non-sql mode.<br>
 *       Command to run unbounded tests on Dataproc Cluster is: <br>
 *       {@code gcloud dataproc jobs submit flink --id {JOB_ID} --jar= {GCS_JAR_LOCATION}
 *       --cluster={CLUSTER_NAME} --region={REGION} -- --gcp-source-project {GCP_SOURCE_PROJECT_ID}
 *       --gcs-source-uri {GCS Source URI} --agg-prop {PROPERTY_TO_AGGREGATE_ON} --query {QUERY}
 *       --gcp-dest-project {GCP_DESTINATION_PROJECT_ID} --bq-dest-dataset {BigQuery Destination
 *       Dataset Name} --bq-dest-table {BigQuery Destination Table Name} --sink-parallelism
 *       {Parallelism to be followed by the sink} --exactly-once {set flag to enable exactly once
 *       approach} --mode unbounded --file-discovery-interval {FILE_DISCOVERY_INTERVAL} --is-sql
 *       {set flag to enable running Flink's Table API methods}}
 * </ol>
 */
public class BigQueryIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryIntegrationTest.class);

    private static final Long CHECKPOINT_INTERVAL = 60000L;
    private static final RestartStrategyConfiguration RESTART_STRATEGY =
            RestartStrategies.exponentialDelayRestart(
                    Time.seconds(5), Time.minutes(10), 2.0, Time.hours(2), 0);

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
                            + " --gcs-source-uri <gcs source directory URI>"
                            + " --agg-prop <record property to aggregate>"
                            + " --gcp-dest-project <destination gcp project id>"
                            + " --bq-dest-dataset <destination dataset name>"
                            + " --bq-dest-table <destination table name>"
                            + " --sink-parallelism <parallelism for sink>"
                            + " --exactly-once <set for sink via 'EXACTLY ONCE' approach>"
                            + " --enable-table-creation <set for creating BQ table in sink>"
                            + " --mode <source type>"
                            + " --file-discovery-interval <minutes between checking new files>");
            return;
        }
        String sourceGcpProjectName = parameterTool.getRequired("gcp-source-project");

        // Add Sink Parameters as well. (Optional)
        String destGcpProjectName = parameterTool.get("gcp-dest-project");
        String destDatasetName = parameterTool.get("bq-dest-dataset");
        String destTableName = parameterTool.get("bq-dest-table");
        Integer sinkParallelism = parameterTool.getInt("sink-parallelism");
        boolean isSqlEnabled = parameterTool.getBoolean("is-sql", false);
        boolean isExactlyOnceEnabled = parameterTool.getBoolean("exactly-once", false);
        Boolean enableTableCreation = parameterTool.getBoolean("enable-table-creation", false);

        // Ignored for bounded run and can be set for unbounded mode (not required).
        String mode = parameterTool.get("mode", "bounded");
        Integer timeoutTimePeriod = parameterTool.getInt("timeout", 18);
        Integer fileDiscoveryInterval = parameterTool.getInt("file-discovery-interval", 10);

        String recordPropertyToAggregate;
        String sourceDatasetName;
        String sourceTableName;
        String gcsSourceUri;
        boolean sinkToBigQuery =
                (destGcpProjectName != null && !destGcpProjectName.isEmpty())
                        && (destDatasetName != null && !destDatasetName.isEmpty())
                        && (destTableName != null && !destTableName.isEmpty());
        if (isSqlEnabled) {
            if (sinkToBigQuery) {
                // Sink Parameters have been provided.
                switch (mode) {
                    case "bounded":
                        sourceDatasetName = parameterTool.getRequired("bq-source-dataset");
                        sourceTableName = parameterTool.getRequired("bq-source-table");
                        runBoundedSQLFlinkJob(
                                sourceGcpProjectName,
                                sourceDatasetName,
                                sourceTableName,
                                destGcpProjectName,
                                destDatasetName,
                                destTableName,
                                isExactlyOnceEnabled,
                                sinkParallelism,
                                enableTableCreation);
                        break;
                    case "unbounded":
                        gcsSourceUri = parameterTool.getRequired("gcs-source-uri");
                        runStreamingSQLFlinkJob(
                                gcsSourceUri,
                                destGcpProjectName,
                                destDatasetName,
                                destTableName,
                                isExactlyOnceEnabled,
                                fileDiscoveryInterval,
                                timeoutTimePeriod,
                                sinkParallelism);
                        break;
                    default:
                        throw new IllegalArgumentException(
                                "Allowed values for mode are bounded or unbounded. Found " + mode);
                }
            } else {
                throw new IllegalArgumentException(
                        "No example currently provided for read-only table API implementation."
                                + mode);
            }
        } else {
            if (sinkToBigQuery) {
                // Sink Parameters have been provided.
                switch (mode) {
                    case "bounded":
                        sourceDatasetName = parameterTool.getRequired("bq-source-dataset");
                        sourceTableName = parameterTool.getRequired("bq-source-table");
                        runBoundedFlinkJobWithSink(
                                sourceGcpProjectName,
                                sourceDatasetName,
                                sourceTableName,
                                destGcpProjectName,
                                destDatasetName,
                                destTableName,
                                isExactlyOnceEnabled,
                                sinkParallelism,
                                enableTableCreation);
                        break;
                    case "unbounded":
                        gcsSourceUri = parameterTool.getRequired("gcs-source-uri");
                        runStreamingFlinkJobWithSink(
                                gcsSourceUri,
                                destGcpProjectName,
                                destDatasetName,
                                destTableName,
                                isExactlyOnceEnabled,
                                sinkParallelism,
                                fileDiscoveryInterval,
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
                        sourceDatasetName = parameterTool.getRequired("bq-source-dataset");
                        sourceTableName = parameterTool.getRequired("bq-source-table");
                        recordPropertyToAggregate = parameterTool.getRequired("agg-prop");
                        runBoundedFlinkJob(
                                sourceGcpProjectName,
                                sourceDatasetName,
                                sourceTableName,
                                recordPropertyToAggregate);
                        break;
                    case "unbounded":
                        throw new IllegalArgumentException(
                                "Unbounded reads from BigQuery source is not supported");
                    default:
                        throw new IllegalArgumentException(
                                "Allowed values for mode are bounded, unbounded. Found " + mode);
                }
            }
        }
    }

    private static void runBoundedFlinkJobWithSink(
            String sourceGcpProjectName,
            String sourceDatasetName,
            String sourceTableName,
            String destGcpProjectName,
            String destDatasetName,
            String destTableName,
            boolean exactlyOnce,
            Integer sinkParallelism,
            boolean enableTableCreation)
            throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(CHECKPOINT_INTERVAL);
        env.setRestartStrategy(RESTART_STRATEGY);

        BigQueryConnectOptions sourceConnectOptions =
                BigQueryConnectOptions.builder()
                        .setProjectId(sourceGcpProjectName)
                        .setDataset(sourceDatasetName)
                        .setTable(sourceTableName)
                        .build();
        BigQuerySource<GenericRecord> source =
                BigQuerySource.readAvros(
                        BigQueryReadOptions.builder()
                                .setBigQueryConnectOptions(sourceConnectOptions)
                                .build());

        BigQueryConnectOptions sinkConnectOptions =
                BigQueryConnectOptions.builder()
                        .setProjectId(destGcpProjectName)
                        .setDataset(destDatasetName)
                        .setTable(destTableName)
                        .build();

        BigQuerySinkConfig.Builder<GenericRecord> sinkConfigBuilder =
                BigQuerySinkConfig.<GenericRecord>newBuilder()
                        .connectOptions(sinkConnectOptions)
                        .serializer(new AvroToProtoSerializer())
                        .deliveryGuarantee(
                                exactlyOnce
                                        ? DeliveryGuarantee.EXACTLY_ONCE
                                        : DeliveryGuarantee.AT_LEAST_ONCE)
                        .streamExecutionEnvironment(env);

        if (enableTableCreation) {
            sinkConfigBuilder
                    .enableTableCreation(true)
                    .partitionField("ts")
                    .partitionType(TimePartitioning.Type.DAY);
        }

        BigQuerySinkConfig sinkConfig = sinkConfigBuilder.build();

        DataStreamSink boundedStreamSink =
                env.fromSource(
                                source,
                                WatermarkStrategy.noWatermarks(),
                                "BigQueryBoundedSource",
                                source.getProducedType())
                        .map(
                                (GenericRecord genericRecord) -> {
                                    genericRecord.put(
                                            "number", (long) genericRecord.get("number") + 1);
                                    return genericRecord;
                                })
                        .returns(
                                new GenericRecordAvroTypeInfo(
                                        getAvroTableSchema(sourceConnectOptions)))
                        .sinkTo(BigQuerySink.get(sinkConfig));
        if (sinkParallelism != null) {
            boundedStreamSink.setParallelism(sinkParallelism);
        }

        env.execute("Flink BigQuery Bounded Read-Write Integration Test");
    }

    private static void runStreamingFlinkJobWithSink(
            String gcsSourceUri,
            String destProjectName,
            String destDatasetName,
            String destTableName,
            boolean exactlyOnce,
            Integer sinkParallelism,
            Integer fileDiscoveryInterval,
            Integer timeoutTimePeriod)
            throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(CHECKPOINT_INTERVAL);
        env.setRestartStrategy(RESTART_STRATEGY);

        FileSource<String> source =
                FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(gcsSourceUri))
                        .monitorContinuously(Duration.ofMinutes(fileDiscoveryInterval))
                        .build();

        BigQueryConnectOptions sinkConnectOptions =
                BigQueryConnectOptions.builder()
                        .setProjectId(destProjectName)
                        .setDataset(destDatasetName)
                        .setTable(destTableName)
                        .build();
        BigQuerySchemaProvider destSchemaProvider =
                new BigQuerySchemaProviderImpl(sinkConnectOptions);

        BigQuerySinkConfig<GenericRecord> sinkConfig =
                BigQuerySinkConfig.<GenericRecord>newBuilder()
                        .connectOptions(sinkConnectOptions)
                        .schemaProvider(destSchemaProvider)
                        .serializer(new AvroToProtoSerializer())
                        .deliveryGuarantee(
                                exactlyOnce
                                        ? DeliveryGuarantee.EXACTLY_ONCE
                                        : DeliveryGuarantee.AT_LEAST_ONCE)
                        .streamExecutionEnvironment(env)
                        .build();

        // This is a hardcoded schema that parses the source STRING DataStream to the
        // schema of the sink destination table
        String schemaString =
                "{ \"type\": \"record\", \"name\": \"CSVRecord\", "
                        + "\"fields\": ["
                        + "  {\"name\": \"unique_key\", \"type\": \"string\"},"
                        + "  {\"name\": \"name\", \"type\": \"string\"},"
                        + "  {\"name\": \"number\", \"type\": \"long\"},"
                        + "  {\"name\": \"ts\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}}"
                        + "] }";

        DataStream<String> stringStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "BigQueryStreamingSource");

        DataStreamSink unboundedStreamSink =
                stringStream
                        .map(
                                new RichMapFunction<String, GenericRecord>() {
                                    private transient org.apache.avro.Schema schema;

                                    @Override
                                    public void open(Configuration parameters) throws Exception {
                                        super.open(parameters);
                                        this.schema =
                                                new org.apache.avro.Schema.Parser()
                                                        .parse(schemaString);
                                    }

                                    // This method maps a CSV string to a GenericRecord based on the
                                    // defined hardcoded schema.
                                    // It splits the CSV string, parses the values, increments the
                                    // "number" field by 1, and populates the GenericRecord fields.
                                    @Override
                                    public GenericRecord map(String value) throws Exception {
                                        String[] csvColumns = value.split(",");
                                        GenericRecord record = new GenericData.Record(schema);
                                                                        if (csvColumns.length == 4) {
                                                                                record.put("unique_key", csvColumns[0]);
                                                                                record.put("name", csvColumns[1]);
                                                                                record.put(
                                                                                                "number",
                                                                                                Long.parseLong(csvColumns[2])
                                                                                                                + 1L);
                                                                                DateTimeFormatter formatter = DateTimeFormatter
                                                                                                .ofPattern(
                                                                                                                "yyyy-MM-dd HH:mm:ss z");
                                                                                Instant instant =
                                                                                                Instant.from(
                                                                                                                formatter.parse(csvColumns[3]));
                                                                                long timestampMicros = instant
                                                                                                .toEpochMilli() * 1000;
                                                                                record.put("ts", timestampMicros);
                                                                        } else {
                                                                                LOG.error("Invalid csv input: "
                                                                                                + value);
                                                                        }
                                        return record;
                                    }
                                })
                        .returns(
                                new GenericRecordAvroTypeInfo(
                                        sinkConfig.getSchemaProvider().getAvroSchema()))
                        .map(new FailingMapper()) // Fails on checkpoint with 20% probability
                        .returns(
                                new GenericRecordAvroTypeInfo(
                                        sinkConfig.getSchemaProvider().getAvroSchema()))
                        .sinkTo(BigQuerySink.get(sinkConfig));

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
                                                        throw new RuntimeException(e);
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

    /**
     * Bounded read and sink operation via Flink's Table API. The function is responsible for
     * reading a BigQuery table (having schema <i>name</i> <code>STRING</code>, <i>number</i> <code>
     * INTEGER</code>, <i>ts</i> <code>TIMESTAMP</code>) in bounded mode and then passing the
     * obtained records via a flatmap. The flatmap appends a string "_write_test" to the "name"
     * field and writes the modified records back to another BigQuery table.
     *
     * @param sourceGcpProjectName The GCP Project name of the source table.
     * @param sourceDatasetName Dataset name of the source table.
     * @param sourceTableName Source Table Name.
     * @param destGcpProjectName The GCP Project name of the destination table.
     * @param destDatasetName Dataset name of the destination table.
     * @param destTableName Destination Table Name.
     * @param isExactlyOnce Boolean value, True if exactly-once mode, false otherwise.
     * @param sinkParallelism Sink's parallelism.
     * @param enableTableCreation Create BQ table in sink.
     * @throws Exception in a case of error, obtaining Table Descriptor.
     */
    private static void runBoundedSQLFlinkJob(
            String sourceGcpProjectName,
            String sourceDatasetName,
            String sourceTableName,
            String destGcpProjectName,
            String destDatasetName,
            String destTableName,
            boolean isExactlyOnce,
            Integer sinkParallelism,
            boolean enableTableCreation)
            throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(CHECKPOINT_INTERVAL);
        env.setRestartStrategy(RESTART_STRATEGY);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        BigQueryConnectOptions sourceConnectOptions =
                BigQueryConnectOptions.builder()
                        .setProjectId(sourceGcpProjectName)
                        .setDataset(sourceDatasetName)
                        .setTable(sourceTableName)
                        .build();
        // Declare Read Options.
        BigQueryTableConfig readTableConfig =
                BigQueryReadTableConfig.newBuilder()
                        .project(sourceGcpProjectName)
                        .dataset(sourceDatasetName)
                        .table(sourceTableName)
                        .testMode(false)
                        .build();

        // Register the Source Table
        tEnv.createTable(
                "bigQuerySourceTable",
                BigQueryTableSchemaProvider.getTableDescriptor(readTableConfig));

        // Read the table and pass to flatmap.
        Table sourceTable =
                tEnv.from("bigQuerySourceTable")
                        .select($("*"))
                        .addOrReplaceColumns(concat($("name"), "_write_test").as("name"));

        BigQuerySinkTableConfig.Builder sinkTableConfigBuilder =
                BigQuerySinkTableConfig.newBuilder()
                        .project(destGcpProjectName)
                        .dataset(destDatasetName)
                        .table(destTableName)
                        .sinkParallelism(sinkParallelism)
                        .streamExecutionEnvironment(env)
                        .deliveryGuarantee(
                                isExactlyOnce
                                        ? DeliveryGuarantee.EXACTLY_ONCE
                                        : DeliveryGuarantee.AT_LEAST_ONCE);

        TableDescriptor descriptor;
        if (enableTableCreation) {
            sinkTableConfigBuilder
                    .enableTableCreation(true)
                    .partitionField("ts")
                    .partitionType(TimePartitioning.Type.DAY);
            org.apache.flink.table.api.Schema tableSchema =
                    getFlinkTableSchema(sourceConnectOptions);
            descriptor =
                    BigQueryTableSchemaProvider.getTableDescriptor(
                            sinkTableConfigBuilder.build(), tableSchema);
        } else {
            descriptor =
                    BigQueryTableSchemaProvider.getTableDescriptor(sinkTableConfigBuilder.build());
        }

        // Register the Sink Table
        tEnv.createTable("bigQuerySinkTable", descriptor);

        // Insert the table sourceTable to the registered sinkTable
        sourceTable.executeInsert("bigQuerySinkTable").await(30, TimeUnit.MINUTES);
    }

    /**
     * Unbounded read and sink operation via Flink's Table API. The function is responsible for
     * reading csv files from GCS bucket (having schema <i>unique_key</i> <code>STRING</code>,
     * <i>name</i> <code>STRING</code>, <i>number</i> <code>
     * INTEGER</code>, <i>ts</i> <code>STRING</code>) in unbounded mode and then passing the
     * obtained records via a flatmap. The flatmap appends a string "_write_test" to the "name"
     * field, modifies the "ts" field to a timestamp value and writes the modified records back to
     * another BigQuery table.
     *
     * @param gcsSourceUri A GCS URI in the form gs://bucket-name/folder/folder which will serve the
     *     source csv files
     * @param destGcpProjectName The GCP Project name of the destination table.
     * @param destDatasetName Dataset name of the destination table.
     * @param destTableName Destination Table Name.
     * @param isExactlyOnceEnabled Boolean value, True if exactly-once mode, false otherwise.
     * @throws Exception in a case of error, obtaining Table Descriptor.
     */
    private static void runStreamingSQLFlinkJob(
            String gcsSourceUri,
            String destGcpProjectName,
            String destDatasetName,
            String destTableName,
            Boolean isExactlyOnceEnabled,
            Integer fileDiscoveryInterval,
            Integer timeoutTimePeriod,
            Integer sinkParallelism)
            throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(CHECKPOINT_INTERVAL);
        env.setRestartStrategy(RESTART_STRATEGY);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.createTemporarySystemFunction("func", MySQLFlatMapFunction.class);

        // Register the Source Table
        tEnv.createTable(
                "bigQuerySourceTable",
                TableDescriptor.forConnector("filesystem")
                        .schema(
                                org.apache.flink.table.api.Schema.newBuilder()
                                        .column("unique_key", DataTypes.STRING())
                                        .column("name", DataTypes.STRING())
                                        .column("number", DataTypes.BIGINT())
                                        .column("ts", DataTypes.STRING())
                                        .build())
                        .format("csv")
                        .option("path", gcsSourceUri)
                        .option("csv.ignore-parse-errors", "true")
                        .option(
                                "source.monitor-interval",
                                String.valueOf(fileDiscoveryInterval) + "m")
                        .build());

        // Fetch entries in this sourceTable
        Table sourceTable =
                tEnv.from("bigQuerySourceTable")
                        .select($("*"))
                        .flatMap(
                                call(
                                        "func",
                                        Row.of($("unique_key"), $("name"), $("number"), $("ts"))))
                        .as($("unique_key"), $("name"), $("number"), $("ts"));

        // Declare Write Options.
        BigQueryTableConfig sinkTableConfig =
                BigQuerySinkTableConfig.newBuilder()
                        .project(destGcpProjectName)
                        .dataset(destDatasetName)
                        .table(destTableName)
                        .sinkParallelism(sinkParallelism)
                        .streamExecutionEnvironment(env)
                        .deliveryGuarantee(
                                isExactlyOnceEnabled
                                        ? DeliveryGuarantee.EXACTLY_ONCE
                                        : DeliveryGuarantee.AT_LEAST_ONCE)
                        .build();

        // Register the Sink Table
        tEnv.createTable(
                "bigQuerySinkTable",
                BigQueryTableSchemaProvider.getTableDescriptor(sinkTableConfig));

        // Insert the table sourceTable to the registered sinkTable
        TablePipeline pipeline = sourceTable.insertInto("bigQuerySinkTable");
        TableResult res = pipeline.execute();
        try {
            res.await(timeoutTimePeriod, TimeUnit.MINUTES);
        } catch (InterruptedException | TimeoutException e) {
            LOG.info("Job Cancelled!", e);
        }
    }

    /** Function to flatmap the Table API source Catalog Table. */
    @FunctionHint(
            input =
                    @DataTypeHint(
                            "ROW<`unique_key` STRING, `name` STRING, `number` BIGINT, `ts` STRING>"),
            output =
                    @DataTypeHint(
                            "ROW<`unique_key` STRING, `name` STRING, `number` BIGINT, `ts` TIMESTAMP(6)>"))
    public static class MySQLFlatMapFunction extends TableFunction<Row> {

        public void eval(Row row) {
            String str = (String) row.getField("name");
            String timestampString = (String) row.getField("ts");
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z");
            LocalDateTime ts = LocalDateTime.parse(timestampString, formatter);
            collect(
                    Row.of(
                            row.getField("unique_key"),
                            str + "_write_test",
                            row.getField("number"),
                            ts));
        }
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

    static class FailingMapper
            implements MapFunction<GenericRecord, GenericRecord>, CheckpointListener {

        @Override
        public GenericRecord map(GenericRecord value) {
            return value;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            int seed = new Random().nextInt(5);
            if (seed % 5 == 2) {
                throw new RuntimeException("Intentional failure in map");
            }
        }
    }

    private static Schema getAvroTableSchema(BigQueryConnectOptions connectOptions) {
        return new BigQuerySchemaProviderImpl(connectOptions).getAvroSchema();
    }

    private static org.apache.flink.table.api.Schema getFlinkTableSchema(
            BigQueryConnectOptions connectOptions) {
        Schema avroSchema = new BigQuerySchemaProviderImpl(connectOptions).getAvroSchema();
        return BigQueryTableSchemaProvider.getTableApiSchemaFromAvroSchema(avroSchema);
    }
}
