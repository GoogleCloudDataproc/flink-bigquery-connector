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
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.bigquery.common.config.BigQueryConnectOptions;
import org.apache.flink.connector.bigquery.source.BigQuerySource;
import org.apache.flink.connector.bigquery.source.config.BigQueryReadOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A simple BigQuery example.
 *
 * <p>The flink pipeline will try to read the specified BigQuery table, limiting the element count
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
                            + "Usage: flink run BigQuery.jar --gcp-project <gcp-project>"
                            + " --bq-dataset <dataset name> --bq-table <table name> ");
            return;
        }

        String projectName = parameterTool.getRequired("gcp-project");
        String datasetName = parameterTool.getRequired("bq-dataset");
        String tableName = parameterTool.getRequired("bq-table");

        runFlinkJob(projectName, datasetName, tableName);
    }

    private static void runFlinkJob(String projectName, String datasetName, String tableName)
            throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000L);

        env.fromSource(
                        BigQuerySource.readAvro()
                                .setLimit(10)
                                .setReadOptions(
                                        BigQueryReadOptions.builder()
                                                .setBigQueryConnectOptions(
                                                        BigQueryConnectOptions.builder()
                                                                .setProjectId(projectName)
                                                                .setDataset(datasetName)
                                                                .setTable(tableName)
                                                                .build())
                                                .build())
                                .build(),
                        WatermarkStrategy.noWatermarks(),
                        "BigQuerySource")
                .map(BigQueryExample::printAndReturn)
                .disableChaining()
                .addSink(new CollectSink());

        env.execute("Flink BigQuery Reader");
        LOG.info("Retrieved data from table: {}", CollectSink.VALUES.toString());
    }

    private static String printAndReturn(GenericRecord record) {
        String stringRecord = record.toString();
        LOG.info("Processed message with payload: " + stringRecord);
        return stringRecord;
    }

    // Simple sink that accumulates in a static variable the read records as strings
    private static class CollectSink implements SinkFunction<String> {

        public static final List<String> VALUES = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(String value, SinkFunction.Context context) throws Exception {
            VALUES.add(value);
        }
    }
}
