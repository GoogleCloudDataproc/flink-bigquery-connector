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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.flink.bigquery.model.Configuration;

public class FlinkBigQueryIntegrationTestBase {

	private BigQuery bq;
	public static StreamTableEnvironment flinkTableEnv;
	public static Configuration config = new Configuration();

	public FlinkBigQueryIntegrationTestBase() {

		this.bq = BigQueryOptions.getDefaultInstance().getService();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		this.flinkTableEnv = StreamTableEnvironment.create(env);
		this.config = new Configuration();
		// TODO: change accordingly
		this.config.setGcpCredentialKeyFile("C:\\sridhar\\GS\\q-gcp-6750-pso-gs-flink-22-01-1231782c49d3.json");
	}
}
