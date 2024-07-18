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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TablePipeline;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import com.google.cloud.flink.bigquery.sink.serializer.BigQueryTableSchemaProvider;
import com.google.cloud.flink.bigquery.table.config.BigQueryReadTableConfig;
import com.google.cloud.flink.bigquery.table.config.BigQuerySinkTableConfig;
import com.google.cloud.flink.bigquery.table.config.BigQueryTableConfig;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * A simple BigQuery table read and sink example with Flink's Table API.
 *
 * <p>The Flink pipeline will try to read the specified BigQuery table according to the command line
 * arguments, returning {@link GenericRecord} representing the rows, and print the result of
 * specified operations or write to a BigQuery table via sink.
 *
 * <ul>
 *   <li>Specify the BQ dataset and table with an optional row restriction. Users can configure a
 *       source mode, i.e bounded or unbounded. Bounded implies that the BQ table will be read and
 *       written once at the time of execution, analogous to a batch job.
 *   <li>Unbounded source implies that the BQ table will be periodically polled for new data which
 *       is then sink. <br>
 *       The sequence of operations in both pipelines is: <i>source > flatMap > sink</i> <br>
 *       Flink command line format is: <br>
 *       <code> flink run {additional runtime params} {path to this jar}/BigQueryTableExample.jar
 *       </code> <br>
 *       --gcp-source-project {required; project ID containing the source table} <br>
 *       --bq-source-dataset {required; name of dataset containing the source table} <br>
 *       --bq-source-table {required; name of table to read} <br>
 *       --gcp-sink-project {required; project ID containing the sink table} <br>
 *       --bq-sink-dataset {required; name of dataset containing the sink table} <br>
 *       --bq-sink-table {required; name of table to write to} <br>
 *       --mode {optional; source read type. Allowed values are bounded (default) or unbounded or
 *       hybrid} <br>
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
 * </ul>
 */
public class BigQueryTableExample {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryTableExample.class);

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
                            + " --mode <source type>"
                            + " --restriction <row filter predicate>"
                            + " --limit <limit on records returned>"
                            + " --checkpoint-interval <milliseconds between state checkpoints>"
                            + " --ts-prop <timestamp property>"
                            + " --oldest-partition-id <oldest partition to read>"
                            + " --partition-discovery-interval <minutes between checking new data>"
                            + " --out-of-order-tolerance <maximum idle minutes for read stream>"
                            + " --max-idleness <maximum idle minutes for read stream>");
            return;
        }
        /**
         * We will be reading avro generic records from BigQuery, and in this case we are assuming
         * the <i>GOOGLE_APPLICATION_CREDENTIALS</i> env variable will be present in the execution
         * runtime. In case of need to authenticate differently, the credentials builder (part of
         * the BigQueryConnectOptions) should enable capturing the credentials from various sources.
         */
        String sourceGcpProjectName = parameterTool.getRequired("gcp-source-project");
        String sourceDatasetName = parameterTool.getRequired("bq-source-dataset");
        String sourceTableName = parameterTool.getRequired("bq-source-table");
        // Read - Optional Arguments
        Integer recordLimit = parameterTool.getInt("limit", -1);
        Long checkpointInterval = parameterTool.getLong("checkpoint-interval", 60000L);
        String rowRestriction = parameterTool.get("restriction", "").replace("\\u0027", "'");
        String mode = parameterTool.get("mode", "bounded");
        String oldestPartition = parameterTool.get("oldest-partition-id", "");
        // Unbounded specific options.
        Integer partitionDiscoveryInterval =
                parameterTool.getInt("partition-discovery-interval", 10);
        Integer maxOutOfOrder = parameterTool.getInt("out-of-order-tolerance", 10);
        Integer maxIdleness = parameterTool.getInt("max-idleness", 20);
        // Sink Parameters
        String destGcpProjectName = parameterTool.getRequired("gcp-sink-project");
        String destDatasetName = parameterTool.getRequired("bq-sink-dataset");
        String destTableName = parameterTool.getRequired("bq-sink-table");
        boolean isExactlyOnce = parameterTool.getBoolean("is-exactly-once", false);

        String recordPropertyForTimestamps;
        switch (mode) {
            case "bounded":
                runBoundedSQLFlinkJob(
                        sourceGcpProjectName,
                        sourceDatasetName,
                        sourceTableName,
                        destGcpProjectName,
                        destDatasetName,
                        destTableName,
                        isExactlyOnce,
                        rowRestriction,
                        recordLimit,
                        checkpointInterval);
                break;
            case "unbounded":
                recordPropertyForTimestamps = parameterTool.getRequired("ts-prop");
                runStreamingSQLFlinkJob(
                        sourceGcpProjectName,
                        sourceDatasetName,
                        sourceTableName,
                        destGcpProjectName,
                        destDatasetName,
                        destTableName,
                        isExactlyOnce,
                        recordPropertyForTimestamps,
                        rowRestriction,
                        recordLimit,
                        checkpointInterval,
                        oldestPartition,
                        partitionDiscoveryInterval,
                        maxOutOfOrder,
                        maxIdleness);
                break;
            default:
                throw new IllegalArgumentException(
                        "Allowed values for mode are bounded or unbounded. Found " + mode);
        }
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
     * @param rowRestriction String value, filtering the rows to be read.
     * @param limit Integer value, Number of rows to limit the read result.
     * @param checkpointInterval Long value, Interval between two check points (milliseconds)
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
            String rowRestriction,
            Integer limit,
            Long checkpointInterval)
            throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointInterval);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.createTemporarySystemFunction("func", MyFlatMapFunction.class);

        // Declare Read Options.
        BigQueryTableConfig readTableConfig =
                BigQueryReadTableConfig.newBuilder()
                        .project(sourceGcpProjectName)
                        .dataset(sourceDatasetName)
                        .table(sourceTableName)
                        .limit(limit)
                        .rowRestriction(rowRestriction)
                        .testMode(false)
                        .boundedness(Boundedness.BOUNDED)
                        .build();

        // Register the Source Table
        tEnv.createTable(
                "bigQuerySourceTable",
                BigQueryTableSchemaProvider.getTableDescriptor(readTableConfig));

        // Read the table and pass to flatmap.
        Table sourceTable =
                tEnv.from("bigQuerySourceTable")
                        .select($("*"))
                        .flatMap(call("func", Row.of($("name"), $("number"), $("ts"))))
                        .as("name", "number", "ts");

        BigQueryTableConfig sinkTableConfig =
                BigQuerySinkTableConfig.newBuilder()
                        .project(destGcpProjectName)
                        .dataset(destDatasetName)
                        .table(destTableName)
                        .testMode(false)
                        .build();

        if (isExactlyOnce) {
            sinkTableConfig =
                    BigQuerySinkTableConfig.newBuilder()
                            .table(destTableName)
                            .project(destGcpProjectName)
                            .dataset(destDatasetName)
                            .testMode(false)
                            .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                            .build();
        }

        // Register the Sink Table
        tEnv.createTable(
                "bigQuerySinkTable",
                BigQueryTableSchemaProvider.getTableDescriptor(sinkTableConfig));

        // Insert the table sourceTable to the registered sinkTable
        sourceTable.executeInsert("bigQuerySinkTable");
    }

    /**
     * Unbounded read and sink operation via Flink's Table API. The function is responsible for
     * reading a BigQuery table (having schema <i>name</i> <code>STRING</code>, <i>number</i> <code>
     * INTEGER</code>, <i>ts</i> <code>TIMESTAMP</code>) in unbounded mode and then passing the
     * obtained records via a flatmap. The flatmap appends a string "_write_test" to the "name"
     * field and writes the modified records back to another BigQuery table.
     *
     * @param sourceGcpProjectName The GCP Project name of the source table.
     * @param sourceDatasetName Dataset name of the source table.
     * @param sourceTableName Source Table Name.
     * @param destGcpProjectName The GCP Project name of the destination table.
     * @param destDatasetName Dataset name of the destination table.
     * @param destTableName Destination Table Name.
     * @param isExactlyOnceEnabled Boolean value, True if exactly-once mode, false otherwise.
     * @param recordPropertyForTimestamps Required String indicating the column name along which
     *     BigQuery Table is partitioned.
     * @param rowRestriction String value, filtering the rows to be read.
     * @param limit Integer value, Number of rows to limit the read result.
     * @param checkpointInterval Long value, Interval between two check points (milliseconds).
     * @param oldestPartition Oldest partition to read.
     * @param maxOutOfOrder Maximum idle minutes for read stream.
     * @param maxIdleness Maximum idle minutes for read stream.
     * @throws Exception in a case of error, obtaining Table Descriptor.
     */
    private static void runStreamingSQLFlinkJob(
            String sourceGcpProjectName,
            String sourceDatasetName,
            String sourceTableName,
            String destGcpProjectName,
            String destDatasetName,
            String destTableName,
            boolean isExactlyOnceEnabled,
            String recordPropertyForTimestamps,
            String rowRestriction,
            Integer limit,
            Long checkpointInterval,
            String oldestPartition,
            Integer partitionDiscoveryInterval,
            Integer maxOutOfOrder,
            Integer maxIdleness)
            throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointInterval);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.createTemporarySystemFunction("func", MyFlatMapFunction.class);

        // Declare Read Options.
        BigQueryTableConfig readTableConfig =
                BigQueryReadTableConfig.newBuilder()
                        .table(sourceTableName)
                        .project(sourceGcpProjectName)
                        .dataset(sourceDatasetName)
                        .testMode(false)
                        .limit(limit)
                        .rowRestriction(rowRestriction)
                        .partitionDiscoveryInterval(partitionDiscoveryInterval)
                        .boundedness(Boundedness.CONTINUOUS_UNBOUNDED)
                        .build();

        // Register the Source Table
        tEnv.createTable(
                "bigQuerySourceTable",
                BigQueryTableSchemaProvider.getTableDescriptor(readTableConfig));
        Table sourceTable = tEnv.from("bigQuerySourceTable");

        // Fetch entries in this sourceTable
        sourceTable = sourceTable.select($("*"));

        // Declare Write Options.
        BigQueryTableConfig sinkTableConfig =
                BigQuerySinkTableConfig.newBuilder()
                        .table(destTableName)
                        .project(destGcpProjectName)
                        .dataset(destDatasetName)
                        .testMode(false)
                        .build();

        if (isExactlyOnceEnabled) {
            sinkTableConfig =
                    BigQuerySinkTableConfig.newBuilder()
                            .table(destTableName)
                            .project(destGcpProjectName)
                            .dataset(destDatasetName)
                            .testMode(false)
                            .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                            .build();
        }

        // Register the Sink Table
        tEnv.createTable(
                "bigQuerySinkTable",
                BigQueryTableSchemaProvider.getTableDescriptor(sinkTableConfig));

        // Insert the table sourceTable to the registered sinkTable
        sourceTable =
                sourceTable
                        .flatMap(call("func", Row.of($("name"), $("number"), $("ts"))))
                        .as("name", "number", "ts");

        TablePipeline pipeline = sourceTable.insertInto("bigQuerySinkTable");
        TableResult res = pipeline.execute();
    }

    /** Function to flatmap the Table API source Catalog Table. */
    @FunctionHint(
            input = @DataTypeHint("ROW<`name` STRING, `number` BIGINT, `ts` TIMESTAMP(6)>"),
            output = @DataTypeHint("ROW<`name` STRING, `number` BIGINT, `ts` TIMESTAMP(6)>"))
    public static class MyFlatMapFunction extends TableFunction<Row> {

        public void eval(Row row) {
            String str = (String) row.getField("name");
            collect(Row.of(str + "_write_test", row.getField("number"), row.getField("ts")));
        }
    }
}
