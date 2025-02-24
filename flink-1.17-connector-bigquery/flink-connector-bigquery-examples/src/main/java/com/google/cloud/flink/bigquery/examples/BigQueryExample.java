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
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.sink.BigQuerySink;
import com.google.cloud.flink.bigquery.sink.BigQuerySinkConfig;
import com.google.cloud.flink.bigquery.sink.serializer.AvroToProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProviderImpl;
import com.google.cloud.flink.bigquery.source.BigQuerySource;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple BigQuery table read example with Flink's DataStream API.
 *
 * <p>The Flink pipeline will try to read the specified BigQuery table according to given the
 * command line arguments, returning {@link GenericRecord} representing the rows, and print the
 * result of specified operations or write to a BigQuery table via sink.
 *
 * <p>This example module should be used as mentioned:
 *
 * <ol>
 *   <li>Specify the BQ dataset and table with an optional row restriction. Source mode is bounded,
 *       implying that the BQ table will be read once at the time of execution, analogous to a batch
 *       job. The resulting records can be written to another BQ table, with allowed delivery
 *       guarantees at-least-once or exactly-once. <br>
 *       Flink command line format is: <br>
 *       flink run {additional runtime params} {path to this jar}/BigQueryExample.jar <br>
 *       --gcp-source-project {required; project ID containing the source table} <br>
 *       --bq-source-dataset {required; name of dataset containing the source table} <br>
 *       --bq-source-table {required; name of table to read} <br>
 *       --gcp-sink-project {required; project ID containing the sink table} <br>
 *       --bq-sink-dataset {required; name of dataset containing the sink table} <br>
 *       --bq-sink-table {required; name of table to write to} <br>
 *       --sink-partition-field {optional; partition field for destination table. Also enables table
 *       creation} <br>
 *       --str-prop {required; record property to modify in Flink job. Value must be string} <br>
 *       --restriction {optional; SQL filter applied at the BigQuery table before reading} <br>
 *       --limit {optional; maximum records to read from BigQuery table} <br>
 *       --checkpoint-interval {optional; milliseconds between state checkpoints} <br>
 *       --delivery-guarantee {optional; sink consistency. Allowed values are <i>at-least-once</i>
 *       (default) or <i>exactly-once</i>}
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
                            + " --sink-partition-field <sink table partition field>"
                            + " --str-prop <record property to modify (value must be string!)>"
                            + " --restriction <row filter predicate>"
                            + " --limit <limit on records returned>"
                            + " --checkpoint-interval <milliseconds between state checkpoints>"
                            + " --delivery-guarantee <sink's write consistency>");
            return;
        }
        /**
         * we will be reading avro generic records from BigQuery, and in this case we are assuming
         * the GOOGLE_APPLICATION_CREDENTIALS env variable will be present in the execution runtime.
         * In case of needing authenticate differently, the credentials builder (part of the
         * BigQueryConnectOptions) should enable capturing the credentials from various sources.
         */
        String sourceProjectName = parameterTool.getRequired("gcp-source-project");
        Integer recordLimit = parameterTool.getInt("limit", -1);
        Long checkpointInterval = parameterTool.getLong("checkpoint-interval", 60000L);
        String sourceDatasetName = parameterTool.getRequired("bq-source-dataset");
        String sourceTableName = parameterTool.getRequired("bq-source-table");
        String rowRestriction = parameterTool.get("restriction", "").replace("\\u0027", "'");
        String recordPropertyToUse = parameterTool.getRequired("str-prop");
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

        String sinkProjectName = parameterTool.getRequired("gcp-sink-project");
        String sinkDatasetName = parameterTool.getRequired("bq-sink-dataset");
        String sinkTableName = parameterTool.getRequired("bq-sink-table");
        String sinkPartitionField = parameterTool.get("sink-partition-field", null);
        runBoundedFlinkJob(
                sourceProjectName,
                sourceDatasetName,
                sourceTableName,
                sinkProjectName,
                sinkDatasetName,
                sinkTableName,
                sinkPartitionField,
                recordPropertyToUse,
                rowRestriction,
                recordLimit,
                checkpointInterval,
                sinkMode);
    }

    private static void runBoundedFlinkJob(
            String sourceProjectName,
            String sourceDatasetName,
            String sourceTableName,
            String sinkProjectName,
            String sinkDatasetName,
            String sinkTableName,
            String sinkPartitionField,
            String recordPropertyToUse,
            String rowRestriction,
            Integer limit,
            Long checkpointInterval,
            DeliveryGuarantee sinkMode)
            throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointInterval);

        BigQueryConnectOptions sourceConnectOptions =
                BigQueryConnectOptions.builder()
                        .setProjectId(sourceProjectName)
                        .setDataset(sourceDatasetName)
                        .setTable(sourceTableName)
                        .build();

        BigQuerySource<GenericRecord> source =
                BigQuerySource.readAvros(
                        BigQueryReadOptions.builder()
                                .setBigQueryConnectOptions(sourceConnectOptions)
                                .setRowRestriction(rowRestriction)
                                .setLimit(limit)
                                .build());

        BigQueryConnectOptions sinkConnectOptions =
                BigQueryConnectOptions.builder()
                        .setProjectId(sinkProjectName)
                        .setDataset(sinkDatasetName)
                        .setTable(sinkTableName)
                        .build();

        BigQuerySinkConfig.Builder<GenericRecord> sinkConfigBuilder =
                BigQuerySinkConfig.<GenericRecord>newBuilder()
                        .connectOptions(sinkConnectOptions)
                        .streamExecutionEnvironment(env)
                        .deliveryGuarantee(sinkMode)
                        .serializer(new AvroToProtoSerializer());

        if (sinkPartitionField != null) {
            sinkConfigBuilder
                    .enableTableCreation(true)
                    .partitionField(sinkPartitionField)
                    .partitionType(TimePartitioning.Type.DAY);
        }

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "BigQuerySource")
                .keyBy(record -> record.get(recordPropertyToUse).hashCode() % 10000)
                .map(
                        (GenericRecord genericRecord) -> {
                            genericRecord.put(
                                    recordPropertyToUse,
                                    genericRecord.get(recordPropertyToUse).toString()
                                            + "_modified");
                            return genericRecord;
                        })
                // Type hinting is required for Avro's GenericRecord to be serialized/deserialized
                // when flowing through the job graph.
                // Since the final record has same schema as the input from source, we use the same
                // avro schema here. Ideally, users need to explicitly set the avro schema according
                // to the record recieved by the sink.
                .returns(
                        new GenericRecordAvroTypeInfo(
                                new BigQuerySchemaProviderImpl(sourceConnectOptions)
                                        .getAvroSchema()))
                .sinkTo(BigQuerySink.get(sinkConfigBuilder.build()));

        env.execute("Flink BigQuery Bounded Read And Write Example");
    }
}
