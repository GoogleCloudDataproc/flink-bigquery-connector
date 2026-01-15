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

package com.google.cloud.flink.bigquery.integration;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Integration test to reproduce the issue where BigQuery source subtasks get stuck in BATCH mode.
 * Includes a transformation to track record counts.
 */
public class StuckSubtasksReproTest {

    private static final Logger LOG = LoggerFactory.getLogger(StuckSubtasksReproTest.class);

    public static void main(String[] args) throws Exception {
        final MiniParameterTool params = MiniParameterTool.fromArgs(args);
        String project = params.getRequired("project");
        String dataset = params.getRequired("dataset");
        String table = params.getRequired("table");
        String rowRestriction = params.get("row-restriction", "");
        int parallelism = params.getInt("parallelism", 100);

        LOG.info("Starting StuckSubtasksReproTest with parallelism {}", parallelism);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // Create BigQuery Source Table
        String createSourceSql =
                String.format(
                        "CREATE TEMPORARY TABLE events (\n"
                                + "  `timestamp` TIMESTAMP(6),\n"
                                + "  `event_name` STRING,\n"
                                + "  `user_id` STRING,\n"
                                + "  `payload` STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = 'bigquery',\n"
                                + "  'project' = '%s',\n"
                                + "  'dataset' = '%s',\n"
                                + "  'table' = '%s'%s\n"
                                + ")",
                        project,
                        dataset,
                        table,
                        rowRestriction.isEmpty()
                                ? ""
                                : String.format(",\n  'read.row.restriction' = '%s'", rowRestriction));
        tEnv.executeSql(createSourceSql);

        // Create Blackhole Sink Table
        String createSinkSql =
                "CREATE TEMPORARY TABLE output\n"
                        + "WITH (\n"
                        + "  'connector' = 'blackhole'\n"
                        + ")\n"
                        + "LIKE events (\n"
                        + "  EXCLUDING ALL\n"
                        + ")";
        tEnv.executeSql(createSinkSql);

        // Transformation: Count records via DataStream API
        LOG.info("Setting up transformation for record counting...");
        Table sourceTable = tEnv.from("events");
        DataStream<Row> ds = tEnv.toDataStream(sourceTable);

        DataStream<Row> transformedDs =
                ds.map(
                        new MapFunction<Row, Row>() {
                            private long count = 0;

                            @Override
                            public Row map(Row value) throws Exception {
                                count++;
                                if (count % 100000 == 0) {
                                    LOG.info("Subtask processed {} records", count);
                                }
                                return value;
                            }
                        });

        // Convert back to Table and execute
        tEnv.createTemporaryView("transformed_events", transformedDs);
        LOG.info("Executing reproduction query...");
        tEnv.executeSql("INSERT INTO output SELECT * FROM transformed_events").await();

        LOG.info("Job finished successfully!");
    }

    /** MiniParameterTool implementation to avoid Flink 2.x dependency issues. */
    private static class MiniParameterTool {
        private final Map<String, String> data;

        private MiniParameterTool(Map<String, String> data) {
            this.data = data;
        }

        public static MiniParameterTool fromArgs(String[] args) {
            Map<String, String> map = new HashMap<>();
            int i = 0;
            while (i < args.length) {
                final String key = args[i];
                if (key.startsWith("--")) {
                    if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                        map.put(key.substring(2), args[i + 1]);
                        i += 2;
                    } else {
                        // flag without value
                        map.put(key.substring(2), "");
                        i += 1;
                    }
                } else {
                    i += 1;
                }
            }
            return new MiniParameterTool(map);
        }

        public String getRequired(String key) {
            String val = data.get(key);
            if (val == null) {
                throw new RuntimeException("No data for required key '" + key + "'");
            }
            return val;
        }

        public String get(String key, String defaultValue) {
            return data.getOrDefault(key, defaultValue);
        }

        public int getInt(String key, int defaultValue) {
            String val = data.get(key);
            if (val == null) {
                return defaultValue;
            }
            return Integer.parseInt(val);
        }
    }
}
