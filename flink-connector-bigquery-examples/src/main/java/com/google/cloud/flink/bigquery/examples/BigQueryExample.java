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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.source.BigQuerySource;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * A simple BigQuery table read example with Flink's DataStream API.
 *
 * <p>The Flink pipeline will try to read the specified BigQuery table, potentially limiting the
 * element count to the specified row restriction and limit count, returning {@link GenericRecord}
 * representing the rows, to finally prints out some aggregated values given the provided payload's
 * field.
 *
 * <p>This example can run with 3 different modes, bounded/batch source, unbounded/streaming source
 * or in hybrid mode (first reads data given a restriction in batch mode and then continue reading
 * data from the provided partition in streaming mode).
 *
 * <p>Note on row restriction: In case of including a restriction with a temporal reference,
 * something like {@code "TIMESTAMP_TRUNC(ingestion_timestamp, HOUR) = '2023-06-20 19:00:00'"}, and
 * launching the job from Flink's Rest API is known the single quotes are not supported and will
 * make the pipeline fail. As a workaround for that case using \u0027 as a replacement will make it
 * work, example {@code "TIMESTAMP_TRUNC(ingestion_timestamp, HOUR) = \u00272023-06-20
 * 19:00:00\u0027"}.
 */
public class BigQueryExample {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryExample.class);

    public static void main(String[] args) throws Exception {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 1) {
            LOG.error(
                    "Missing parameters!\n"
                            + "Usage: flink run <additional runtime params> BigQuery.jar"
                            + " --gcp-project <gcp-project> --bq-dataset <dataset name>"
                            + " --bq-table <table name>"
                            + " --agg-prop <payload's property for aggregation purposes>"
                            + " --agg-prop <payload's property for event timestamp extraction>"
                            + " --restriction <single-quoted string with row predicate>"
                            + " --limit <optional: limit records returned> --query <SQL>"
                            + " --streaming <optional: sets the source in streaming mode>"
                            + " --ts-prop <optional: payload's property for timestamp extraction>"
                            + " --oldest-partition-id <optional: oldest partition id to read>");
            return;
        }
        /**
         * we will be reading avro generic records from BigQuery, and in this case we are assuming
         * the GOOGLE_APPLICATION_CREDENTIALS env variable will be present in the execution runtime.
         * In case of needing authenticate differently, the credentials builder (part of the
         * BigQueryConnectOptions) should enable capturing the credentials from various sources.
         */
        String projectName = parameterTool.getRequired("gcp-project");
        String query = parameterTool.get("query", "");
        Integer recordLimit = parameterTool.getInt("limit", -1);
        if (!query.isEmpty()) {
            runFlinkQueryJob(projectName, query, recordLimit);
        } else {
            String datasetName = parameterTool.getRequired("bq-dataset");
            String tableName = parameterTool.getRequired("bq-table");
            String rowRestriction = parameterTool.get("restriction", "").replace("\\u0027", "'");
            String recordPropertyToAggregate = parameterTool.getRequired("agg-prop");
            String recordPropertyForTimestamps = parameterTool.getRequired("ts-prop");

            Boolean streaming = parameterTool.toMap().containsKey("streaming");
            Boolean hybrid = parameterTool.toMap().containsKey("hybrid");

            if (streaming) {
                String oldestPartition = parameterTool.get("oldest-partition-id", "");
                runStreamingFlinkJob(
                        projectName,
                        datasetName,
                        tableName,
                        recordPropertyToAggregate,
                        recordPropertyForTimestamps,
                        rowRestriction,
                        recordLimit,
                        oldestPartition);
            } else if (hybrid) {
                String oldestPartition = parameterTool.getRequired("oldest-partition-id");
                runHybridFlinkJob(
                        projectName,
                        datasetName,
                        tableName,
                        recordPropertyToAggregate,
                        recordPropertyForTimestamps,
                        rowRestriction,
                        oldestPartition);
            } else {
                runFlinkJob(
                        projectName,
                        datasetName,
                        tableName,
                        recordPropertyToAggregate,
                        recordPropertyForTimestamps,
                        rowRestriction,
                        recordLimit);
            }
        }
    }

    private static void runFlinkQueryJob(String projectName, String query, Integer limit)
            throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000L);

        BigQuerySource<GenericRecord> bqSource =
                BigQuerySource.readAvrosFromQuery(query, projectName, limit);

        env.fromSource(bqSource, WatermarkStrategy.noWatermarks(), "BigQueryQuerySource").print();

        env.execute("Flink BigQuery Query Example");
    }

    private static void runJob(
            Source<GenericRecord, ?, ?> source,
            TypeInformation<GenericRecord> typeInfo,
            String sourceName,
            String jobName,
            String recordPropertyToAggregate,
            String recordPropertyForTimestamps)
            throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000L);

        env.fromSource(
                        source,
                        WatermarkStrategy.<GenericRecord>forBoundedOutOfOrderness(
                                        Duration.ofMinutes(10))
                                .withTimestampAssigner(
                                        // timestamps in BigQuery are represented at microsecond
                                        // level
                                        (event, timestamp) ->
                                                ((Long) event.get(recordPropertyForTimestamps))
                                                        / 1000)
                                .withIdleness(Duration.ofMinutes(20)),
                        sourceName,
                        typeInfo)
                .flatMap(new FlatMapper(recordPropertyToAggregate))
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .sum("f1")
                .print();

        env.execute(jobName);
    }

    private static void runFlinkJob(
            String projectName,
            String datasetName,
            String tableName,
            String recordPropertyToAggregate,
            String recordPropertyForTimestamps,
            String rowRestriction,
            Integer limit)
            throws Exception {

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
        runJob(
                source,
                source.getProducedType(),
                "BigQuerySource",
                "Flink BigQuery Bounded Read Example",
                recordPropertyToAggregate,
                recordPropertyForTimestamps);
    }

    private static void runStreamingFlinkJob(
            String projectName,
            String datasetName,
            String tableName,
            String recordPropertyToAggregate,
            String recordPropertyForTimestamps,
            String rowRestriction,
            Integer limit,
            String oldestPartition)
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
                                .setRowRestriction(rowRestriction)
                                .setLimit(limit)
                                .setOldestPartitionId(oldestPartition)
                                .build());

        runJob(
                source,
                source.getProducedType(),
                "BigQueryStreamingSource",
                "Flink BigQuery Unbounded Read Example",
                recordPropertyToAggregate,
                recordPropertyForTimestamps);
    }

    private static void runHybridFlinkJob(
            String projectName,
            String datasetName,
            String tableName,
            String recordPropertyToAggregate,
            String recordPropertyForTimestamps,
            String rowRestrictionForBatch,
            String oldestPartitionForStreaming)
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
                                .setPartitionDiscoveryRefreshIntervalInMinutes(5)
                                .build());

        // create an hybrid source with both batch and streaming flavors of the BigQuery source.
        HybridSource<GenericRecord> hybridSource =
                HybridSource.builder(batchSource).addSource(streamingSource).build();

        runJob(
                hybridSource,
                streamingSource.getProducedType(),
                "BigQueryHybridSource",
                "Flink BigQuery Hybrid Read Example",
                recordPropertyToAggregate,
                recordPropertyForTimestamps);
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
