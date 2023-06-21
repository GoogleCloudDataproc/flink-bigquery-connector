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
import org.apache.flink.connector.bigquery.common.config.BigQueryConnectOptions;
import org.apache.flink.connector.bigquery.source.BigQuerySource;
import org.apache.flink.connector.bigquery.source.config.BigQueryReadOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A simple BigQuery example.
 *
 * <p>The Flink pipeline will try to read the specified BigQuery table, limiting the element count
 * to 10 and collect the results.
 */
public class BigQueryExample {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryExample.class);

    public static void main(String[] args) throws Exception {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 3) {
            LOG.error(
                    "Missing parameters!\n"
                            + "Usage: flink run <additional runtime params> BigQuery.jar"
                            + " --gcp-project <gcp-project> --bq-dataset <dataset name>"
                            + " --bq-table <table name> --limit <# records>");
            return;
        }

        String projectName = parameterTool.getRequired("gcp-project");
        String datasetName = parameterTool.getRequired("bq-dataset");
        String tableName = parameterTool.getRequired("bq-table");
        Integer recordLimit = Integer.valueOf(parameterTool.getRequired("limit"));

        runFlinkJob(projectName, datasetName, tableName, recordLimit);
    }

    private static void runFlinkJob(
            String projectName, String datasetName, String tableName, Integer limit)
            throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000L);

        // we will be reading avro generic records from BigQuery, and in this case we are assuming
        // the
        // GOOGLE_APPLICATION_CREDENTIALS env variable will be present in the execution runtime. In
        // case
        // of needing authenticate differently, the credentials builder (part of the
        // BigQueryConnectOptions) should enable capturing the credentials from various sources.
        BigQuerySource<GenericRecord> bqSource =
                BigQuerySource.readAvros(
                        BigQueryReadOptions.builder()
                                .setBigQueryConnectOptions(
                                        BigQueryConnectOptions.builder()
                                                .setProjectId(projectName)
                                                .setDataset(datasetName)
                                                .setTable(tableName)
                                                .build())
                                .setRowRestriction(
                                        "TIMESTAMP_TRUNC(ingestion_timestamp, HOUR) = '2023-06-20 19:00:00'")
                                .build(),
                        limit);

        CollectSink sink = new CollectSink();
        sink.VALUES.clear();

        env.fromSource(bqSource, WatermarkStrategy.noWatermarks(), "BigQuerySource")
                .flatMap(new FlatMapper())
                .keyBy(t -> t.f0)
                .max("f1")
                .addSink(sink);

        env.execute("Flink BigQuery Reader");
        LOG.info("Retrieved most utilized uuid key from table: {}", CollectSink.VALUES.toString());
    }

    static class FlatMapper implements FlatMapFunction<GenericRecord, Tuple2<String, Integer>> {

        @Override
        public void flatMap(GenericRecord record, Collector<Tuple2<String, Integer>> out)
                throws Exception {
            out.collect(Tuple2.<String, Integer>of((String) record.get("uuid").toString(), 1));
        }
    }

    // Simple sink that accumulates in a static variable the read records as strings
    private static class CollectSink implements SinkFunction<Tuple2<String, Integer>> {

        public static final List<Tuple2<String, Integer>> VALUES =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(Tuple2<String, Integer> value, SinkFunction.Context context)
                throws Exception {
            VALUES.add(value);
        }
    }
}
