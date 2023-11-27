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

package org.apache.flink.examples.gcp.bigquery;

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
 * <p>The Flink pipeline will try to read the specified BigQuery table, limiting the element count
 * to the specified row restriction and limit, returning {@link GenericRecord} representing the
 * rows, and finally print out some aggregated values given the provided payload's field. The
 * sequence of operations in this pipeline is: <i>source > flatMap > keyBy > max > print</i>.
 *
 * <p>Flink command line format to execute this application: <br>
 * flink run {additional runtime params} {path to this jar}/BigQueryExample.jar <br>
 * --gcp-project {required; project ID which contains the BigQuery table} <br>
 * --bq-dataset {required; name of BigQuery dataset containing the desired table} <br>
 * --bq-table {required; name of BigQuery table to read} <br>
 * --agg-prop {required; record property to aggregate in Flink job} <br>
 * --restriction {optional; SQL-like filter applied at the BigQuery table before reading} <br>
 * --limit {optional; maximum records to read from BigQuery table} <br>
 * --checkpoint-interval {optional; time interval between state checkpoints in milliseconds}
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

        if (parameterTool.getNumberOfParameters() < 4) {
            LOG.error(
                    "Missing parameters!\n"
                            + "Usage: flink run <additional runtime params> BigQuery.jar"
                            + " --gcp-project <gcp-project>"
                            + " --bq-dataset <dataset name>"
                            + " --bq-table <table name>"
                            + " --agg-prop <record property>"
                            + " --restriction <row filter predicate>"
                            + " --limit <limit records returned>"
                            + " --checkpoint-interval <milliseconds between state checkpoints>");
            return;
        }

        String projectName = parameterTool.getRequired("gcp-project");
        String datasetName = parameterTool.getRequired("bq-dataset");
        String tableName = parameterTool.getRequired("bq-table");
        String rowRestriction = parameterTool.get("restriction", "").replace("\\u0027", "'");
        Integer recordLimit = parameterTool.getInt("limit", -1);
        String recordPropertyToAggregate = parameterTool.getRequired("agg-prop");
        Long checkpointInterval = parameterTool.getLong("checkpoint-interval", 60000L);

        runFlinkJob(
                projectName,
                datasetName,
                tableName,
                recordPropertyToAggregate,
                rowRestriction,
                recordLimit,
                checkpointInterval);
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
                .max("f1")
                .print();

        env.execute("Flink BigQuery Example");
    }

    static class FlatMapper implements FlatMapFunction<GenericRecord, Tuple2<String, Integer>> {

        private final String recordPropertyToAggregate;

        public FlatMapper(String recordPropertyToAggregate) {
            this.recordPropertyToAggregate = recordPropertyToAggregate;
        }

        @Override
        public void flatMap(GenericRecord record, Collector<Tuple2<String, Integer>> out)
                throws Exception {
            out.collect(
                    Tuple2.<String, Integer>of(
                            (String) record.get(recordPropertyToAggregate).toString(), 1));
        }
    }
}
