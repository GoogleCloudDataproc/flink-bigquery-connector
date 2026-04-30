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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.utils.ParameterTool;
import com.google.cloud.flink.bigquery.source.BigQuerySource;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An end-to-end example demonstrating how to read from a BigQuery Materialized View.
 *
 * <p>This example requires the following arguments:
 *
 * <ul>
 *   <li>--gcp-project {required; GCP project ID}
 *   <li>--dataset {required; name of the BigQuery dataset}
 *   <li>--view {required; name of the BigQuery Materialized View}
 * </ul>
 *
 * <p>To run this example, ensure your execution environment is authenticated with GCP.
 */
public class BigQueryMaterializedViewExample {
    private static final Logger LOG =
            LoggerFactory.getLogger(BigQueryMaterializedViewExample.class);

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        String projectName = params.get("gcp-project");
        String datasetName = params.get("dataset");
        String viewName = params.get("view");

        if (projectName == null || datasetName == null || viewName == null) {
            System.err.println(
                    "Missing required arguments. Usage: --gcp-project <project> --dataset <dataset> --view <view>");
            return;
        }

        LOG.info("Starting BigQuery Materialized View Example");
        LOG.info("Project: {}, Dataset: {}, View: {}", projectName, datasetName, viewName);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(new Configuration());

        // 1. Configure BigQuery Connection Options with Views Enabled
        BigQueryConnectOptions connectOptions =
                BigQueryConnectOptions.builder()
                        .setProjectId(projectName)
                        .setDataset(datasetName)
                        .setTable(viewName)
                        .setViewsEnabled(true) // CRITICAL: enable view support
                        .setMaterializedTableExpirationHours(1) // Optional: set temp table TTL
                        .build();

        // 2. Configure Read Options
        BigQueryReadOptions readOptions =
                BigQueryReadOptions.builder().setBigQueryConnectOptions(connectOptions).build();

        // 3. Build the BigQuery Source
        BigQuerySource<GenericRecord> source = BigQuerySource.readAvros(readOptions);

        // 4. Define the Flink Pipeline
        // Note: In a real application, you would provide the actual Avro schema of the view.
        // For this example, we use a dummy schema as the connector will resolve it from the
        // materialized table.
        String dummySchema =
                "{\"type\":\"record\",\"name\":\"Dummy\",\"fields\":[{\"name\":\"dummy\",\"type\":\"string\"}]}";
        TypeInformation<GenericRecord> typeInfo =
                new GenericRecordAvroTypeInfo(
                        new org.apache.avro.Schema.Parser().parse(dummySchema));

        DataStream<GenericRecord> stream =
                env.fromSource(
                        source, WatermarkStrategy.noWatermarks(), "BigQueryViewSource", typeInfo);

        // 5. Print the results to stdout
        stream.map((MapFunction<GenericRecord, String>) record -> "View Row: " + record.toString())
                .print();

        // 6. Execute the job
        env.execute("BigQuery Materialized View Example");
    }
}
