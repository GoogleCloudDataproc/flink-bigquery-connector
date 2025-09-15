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

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.ParameterTool;

import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProviderImpl;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryTableSchemaProvider;
import com.google.cloud.flink.bigquery.table.config.BigQueryReadTableConfig;
import com.google.cloud.flink.bigquery.table.config.BigQuerySinkTableConfig;
import com.google.cloud.flink.bigquery.table.config.BigQueryTableConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.table.api.Expressions.$;

/**
 * A simple BigQuery table read and sink example with Flink's Table API.
 *
 * <p>The Flink pipeline will try to read the specified BigQuery table according to the command line
 * arguments, returning {@link GenericRecord} representing the rows, and print the result of
 * specified operations or write to a BigQuery table via sink.
 *
 * <ul>
 *   <li>Specify the BQ dataset and table with an optional row restriction. Source mode is bounded.
 *       Resulting records can be written to another BQ table, with allowed delivery (write)
 *       guarantees at-least-once or exactly-once. <br>
 *       Flink command line format is: <br>
 *       <code> flink run {additional runtime params} {path to this jar}/BigQueryTableExample.jar
 *       </code> <br>
 *       --gcp-source-project {required; project ID containing the source table} <br>
 *       --bq-source-dataset {required; name of dataset containing the source table} <br>
 *       --bq-source-table {required; name of table to read} <br>
 *       --gcp-sink-project {required; project ID containing the sink table} <br>
 *       --bq-sink-dataset {required; name of dataset containing the sink table} <br>
 *       --bq-sink-table {required; name of table to write to} <br>
 *       --sink-partition-field {optional; partition field for destination table. Also enables table
 *       creation} <br>
 *       --restriction {optional; SQL filter applied at the BigQuery table before reading} <br>
 *       --limit {optional; maximum records to read from BigQuery table} <br>
 *       --checkpoint-interval {optional; milliseconds between state checkpoints} <br>
 *       --delivery-guarantee {optional; sink consistency. Allowed values are <i>at-least-once</i>
 *       (default) or <i>exactly-once</i>}
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
                            + " --sink-partition-field <sink table partition field>"
                            + " --restriction <row filter predicate>"
                            + " --limit <limit on records returned>"
                            + " --checkpoint-interval <milliseconds between state checkpoints>"
                            + " --delivery-guarantee <sink's write consistency>");
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
        // Sink Parameters
        String destGcpProjectName = parameterTool.getRequired("gcp-sink-project");
        String destDatasetName = parameterTool.getRequired("bq-sink-dataset");
        String destTableName = parameterTool.getRequired("bq-sink-table");
        String sinkPartitionField = parameterTool.get("sink-partition-field", null);
        String deliveryGuarantee = parameterTool.get("delivery-guarantee", "at-least-once");
        DeliveryGuarantee sinkMode;
        switch (deliveryGuarantee) {
            case "at-least-once":
                sinkMode = DeliveryGuarantee.AT_LEAST_ONCE;
                break;
            case "exactly-once":
                sinkMode = DeliveryGuarantee.EXACTLY_ONCE;
                break;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Allowed values for delivery-guarantee are at-least-once or exactly-once. Found %s",
                                deliveryGuarantee));
        }

        runBoundedTableAPIFlinkJob(
                sourceGcpProjectName,
                sourceDatasetName,
                sourceTableName,
                destGcpProjectName,
                destDatasetName,
                destTableName,
                sinkPartitionField,
                sinkMode,
                rowRestriction,
                recordLimit,
                checkpointInterval);
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
     * @param sinkPartitionField Sink table partitioning.
     * @param sinkMode At-least-once or exactly-once write consistency.
     * @param rowRestriction String value, filtering the rows to be read.
     * @param limit Integer value, Number of rows to limit the read result.
     * @param checkpointInterval Long value, Interval between two check points (milliseconds)
     * @throws Exception in a case of error, obtaining Table Descriptor.
     */
    private static void runBoundedTableAPIFlinkJob(
            String sourceGcpProjectName,
            String sourceDatasetName,
            String sourceTableName,
            String destGcpProjectName,
            String destDatasetName,
            String destTableName,
            String sinkPartitionField,
            DeliveryGuarantee sinkMode,
            String rowRestriction,
            Integer limit,
            Long checkpointInterval)
            throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointInterval);
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
                        .limit(limit)
                        .rowRestriction(rowRestriction)
                        .build();

        // Register the Source Table
        tEnv.createTable(
                "bigQuerySourceTable",
                BigQueryTableSchemaProvider.getTableDescriptor(readTableConfig));

        // Read the table and pass to flatmap.
        // Hardcoded source schema to extract three columns: name (string), number (int) and ts
        // (timestamp).
        Table sourceTable = tEnv.from("bigQuerySourceTable").select($("*"));

        BigQuerySinkTableConfig.Builder sinkTableConfigBuilder =
                BigQuerySinkTableConfig.newBuilder()
                        .project(destGcpProjectName)
                        .dataset(destDatasetName)
                        .table(destTableName)
                        .sinkParallelism(2)
                        .deliveryGuarantee(sinkMode)
                        .streamExecutionEnvironment(env);

        TableDescriptor descriptor;
        if (sinkPartitionField != null) {
            sinkTableConfigBuilder
                    .enableTableCreation(true)
                    .partitionField(sinkPartitionField)
                    .partitionType(TimePartitioning.Type.DAY);
            // Since the final record has same schema as the input from source, we use the same
            // table schema here. Ideally, users need to explicitly set the table schema according
            // to the record recieved by the sink.
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
        sourceTable.executeInsert("bigQuerySinkTable");
    }

    /**
     * Bounded read > join and sink operation via Flink's Table API. The function is responsible for
     * reading a BigQuery table (having schema <i>id</i> <code>STRING</code>, <i>name_left</i>
     * <code>STRING</code>) in bounded mode and then writes the modified records back to another
     * BigQuery table.
     *
     * <p>This example is for reference only, and cannot be invoked from this class's main method.
     *
     * @param sourceGcpProjectName The GCP Project name of the source table.
     * @param sourceDatasetName Dataset name of the source table.
     * @param leftSourceTableName Source Table Name (left for Join).
     * @param rightSourceTableName Source Table Name (right for Join).
     * @param destGcpProjectName The GCP Project name of the destination table.
     * @param destDatasetName Dataset name of the destination table.
     * @param destTableName Destination Table Name.
     * @param sinkMode At-least-once or exactly-once write consistency.
     * @param rowRestriction String value, filtering the rows to be read.
     * @param limit Integer value, Number of rows to limit the read result.
     * @param checkpointInterval Long value, Interval between two check points (milliseconds)
     * @throws Exception in a case of error, obtaining Table Descriptor.
     */
    public static void runBoundedJoinFlinkJob(
            String sourceGcpProjectName,
            String sourceDatasetName,
            String leftSourceTableName,
            String rightSourceTableName,
            String destGcpProjectName,
            String destDatasetName,
            String destTableName,
            DeliveryGuarantee sinkMode,
            String rowRestriction,
            Integer limit,
            Long checkpointInterval)
            throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointInterval);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Declare Read Options.
        BigQueryTableConfig readTableConfig =
                BigQueryReadTableConfig.newBuilder()
                        .table(leftSourceTableName)
                        .project(sourceGcpProjectName)
                        .dataset(sourceDatasetName)
                        .limit(limit)
                        .rowRestriction(rowRestriction)
                        .build();

        // Register the Source Table
        tEnv.createTable(
                "leftSourceTable", BigQueryTableSchemaProvider.getTableDescriptor(readTableConfig));

        readTableConfig =
                BigQueryReadTableConfig.newBuilder()
                        .table(rightSourceTableName)
                        .project(sourceGcpProjectName)
                        .dataset(sourceDatasetName)
                        .limit(limit)
                        .rowRestriction(rowRestriction)
                        .build();

        tEnv.createTable(
                "rightSourceTable",
                BigQueryTableSchemaProvider.getTableDescriptor(readTableConfig));

        // Declare Write Options.
        BigQueryTableConfig sinkTableConfig =
                BigQuerySinkTableConfig.newBuilder()
                        .table(destTableName)
                        .project(destGcpProjectName)
                        .dataset(destDatasetName)
                        .deliveryGuarantee(sinkMode)
                        .streamExecutionEnvironment(env)
                        .build();

        // Register the Sink Table
        tEnv.createTable(
                "bigQuerySinkTable",
                BigQueryTableSchemaProvider.getTableDescriptor(sinkTableConfig));

        // Join Example - Table API
        //        Table leftSourceTable = tEnv.from("leftSourceTable");
        //        Table rightSourceTable = tEnv.from("rightSourceTable");
        //        Table joinedTable =
        //                leftSourceTable
        //                        .renameColumns($("id").as("id_l"))
        //                        .join(rightSourceTable, $("id_l").isEqual($("id")))
        //                        .select($("id"), $("name_left"), $("name_right"));
        //        joinedTable.executeInsert("bigQuerySinkTable");

        // Join Example - SQL
        tEnv.executeSql(
                "insert into bigQuerySinkTable Select leftSourceTable.id AS id, "
                        + "leftSourceTable.name_left AS name_left, rightSourceTable.name_right as name_right from leftSourceTable JOIN rightSourceTable ON "
                        + "leftSourceTable.id = rightSourceTable.id;");
    }

    private static org.apache.flink.table.api.Schema getFlinkTableSchema(
            BigQueryConnectOptions connectOptions) {
        Schema avroSchema = new BigQuerySchemaProviderImpl(connectOptions).getAvroSchema();
        return BigQueryTableSchemaProvider.getTableApiSchemaFromAvroSchema(avroSchema);
    }
}
