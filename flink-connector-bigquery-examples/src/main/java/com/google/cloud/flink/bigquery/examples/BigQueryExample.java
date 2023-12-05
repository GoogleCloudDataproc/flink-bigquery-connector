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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.source.BigQuerySource;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple Flink application using DataStream API and BigQuery connector.
 *
 * <p>The Flink pipeline will try to read the specified BigQuery table according to given the
 * command line arguments, returning {@link GenericRecord} representing the rows, and finally print
 * out some aggregated values given the provided payload's field. The sequence of operations in this
 * pipeline is: <i>source > flatMap > keyBy > sum > print</i>.
 *
 * <p>This example module should be used in one of the following two ways.
 *
 * <ol>
 *   <li>Specify the BQ dataset and table with an optional row restriction. Flink command line
 *       format to execute this mode is: <br>
 *       flink run {additional runtime params} {path to this jar}/BigQueryExample.jar <br>
 *       --gcp-project {required; project ID which contains the BigQuery table} <br>
 *       --bq-dataset {required; name of BigQuery dataset containing the desired table} <br>
 *       --bq-table {required; name of BigQuery table to read} <br>
 *       --agg-prop {required; record property to aggregate in Flink job} <br>
 *       --restriction {optional; SQL filter applied at the BigQuery table before reading} <br>
 *       --limit {optional; maximum records to read from BigQuery table} <br>
 *       --checkpoint-interval {optional; time interval between state checkpoints in milliseconds}
 *   <li>Specify SQL query to fetch data from BQ dataset. For example, "SELECT * FROM
 *       some_dataset.INFORMATION_SCHEMA.PARTITIONS". Flink command line format to execute this mode
 *       is: <br>
 *       flink run {additional runtime params} {path to this jar}/BigQueryExample.jar <br>
 *       --gcp-project {required; project ID which contains the BigQuery table} <br>
 *       --query {required; SQL query to fetch data from BigQuery table} <br>
 *       --agg-prop {required; record property to aggregate in Flink job} <br>
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
                            + " --gcp-project <gcp project id>"
                            + " --bq-dataset <dataset name>"
                            + " --bq-table <table name>"
                            + " --agg-prop <record property to aggregate>"
                            + " --restriction <row filter predicate>"
                            + " --limit <limit on records returned>"
                            + " --checkpoint-interval <milliseconds between state checkpoints>"
                            + " --query <SQL query to get data from BQ table>");
            return;
        }

        String projectName = parameterTool.getRequired("gcp-project");
        String query = parameterTool.get("query", "");
        String recordPropertyToAggregate = parameterTool.getRequired("agg-prop");
        Integer recordLimit = parameterTool.getInt("limit", -1);
        Long checkpointInterval = parameterTool.getLong("checkpoint-interval", 60000L);

        if (!query.isEmpty()) {
            runFlinkQueryJob(
                    projectName, query, recordPropertyToAggregate, recordLimit, checkpointInterval);
        } else {
            String datasetName = parameterTool.getRequired("bq-dataset");
            String tableName = parameterTool.getRequired("bq-table");
            String rowRestriction = parameterTool.get("restriction", "").replace("\\u0027", "'");

            runFlinkJob(
                    projectName,
                    datasetName,
                    tableName,
                    recordPropertyToAggregate,
                    rowRestriction,
                    recordLimit,
                    checkpointInterval);
        }
    }

    private static void runFlinkQueryJob(
            String projectName,
            String query,
            String recordPropertyToAggregate,
            Integer limit,
            Long checkpointInterval)
            throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointInterval);

        /**
         * we will be reading avro generic records from BigQuery, and in this case we are assuming
         * the GOOGLE_APPLICATION_CREDENTIALS env variable will be present in the execution runtime.
         * In case of needing authenticate differently, the credentials builder (part of the
         * BigQueryConnectOptions) should enable capturing the credentials from various sources.
         */
        BigQuerySource<GenericRecord> bqSource =
                BigQuerySource.readAvrosFromQuery(query, projectName, limit);

        env.fromSource(bqSource, WatermarkStrategy.noWatermarks(), "BigQueryQuerySource")
                .flatMap(new FlatMapper(recordPropertyToAggregate))
                .keyBy(mappedTuple -> mappedTuple.f0)
                .sum("f1")
                .print();

        env.execute("Flink BigQuery query example");
    }

    private static void runFlinkJob(
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

        /**
         * we will be reading avro generic records from BigQuery, and in this case we are assuming
         * the GOOGLE_APPLICATION_CREDENTIALS env variable will be present in the execution runtime.
         * In case of needing authenticate differently, the credentials builder (part of the
         * BigQueryConnectOptions) should enable capturing the credentials from various sources.
         */
        BigQuerySource<GenericRecord> bqSource =
                BigQuerySource.readAvros(
                        BigQueryReadOptions.builder()
                                .setBigQueryConnectOptions(
                                        BigQueryConnectOptions.builder()
                                                .setProjectId(projectName)
                                                .setDataset(datasetName)
                                                .setTable(tableName)
                                                .build())
                                .setRowRestriction(rowRestriction)
                                .build(),
                        limit);

        env.fromSource(bqSource, WatermarkStrategy.noWatermarks(), "BigQuerySource")
                .flatMap(new FlatMapper(recordPropertyToAggregate))
                .keyBy(mappedTuple -> mappedTuple.f0)
                .sum("f1")
                .print();

        env.execute("Flink BigQuery example");
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
