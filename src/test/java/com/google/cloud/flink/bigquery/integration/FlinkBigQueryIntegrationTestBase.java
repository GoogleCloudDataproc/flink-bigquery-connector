/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.flink.bigquery.integration;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.flink.bigquery.model.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkBigQueryIntegrationTestBase {

  @SuppressWarnings("unused")
  protected BigQuery bq;

  protected StreamTableEnvironment flinkTableEnv;
  protected Configuration config;

  @SuppressWarnings("static-access")
  public FlinkBigQueryIntegrationTestBase() {

    this.bq = BigQueryOptions.getDefaultInstance().getService();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    this.flinkTableEnv = StreamTableEnvironment.create(env);
    this.config = new Configuration();
    config.setGcpCredentialKeyFile(System.getenv("GOOGLE_APPLICATION_CREDENTIALS"));
    config.setProjectId(System.getenv("GOOGLE_CLOUD_PROJECT"));
    config.setDataset(System.getenv("GOOGLE_CLOUD_DATASET"));
    config.setBigQueryReadTable(System.getenv("GOOGLE_CLOUD_TABLE"));
  }
}
