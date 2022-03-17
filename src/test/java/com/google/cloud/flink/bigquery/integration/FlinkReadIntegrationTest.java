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

import static com.google.common.truth.Truth.assertThat;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.junit.Test;

import com.google.cloud.flink.bigquery.model.Configuration;

public class FlinkReadIntegrationTest extends FlinkBigQueryIntegrationTestBase {

	public FlinkReadIntegrationTest() {
		super();
		config.setGcpCredentialKeyFile(System.getenv("GOOGLE_APPLICATION_CREDENTIALS"));
		config.setProjectId("q-gcp-6750-pso-gs-flink-22-01");
		config.setDataset("wordcount_dataset");
		config.setBigQueryReadTable("wordcount_output");
	}

	private void testWordCount(TableResult tableRes) {
		assertThat(tableRes.getTableSchema()).isEqualTo(Constants.WORDCOUNT_TABLE_SCHEMA);
	}

	private void testReadFilter(TableResult tableRes) {
		assertThat(tableRes).isEqualTo("Empty Set");
	}

	@Test
	public void testReadForDifferentDataTypes() {

		Configuration config_datatype = new Configuration();
		config_datatype.setProjectId("q-gcp-6750-pso-gs-flink-22-01");
		config_datatype.setDataset("test");
		config_datatype.setBigQueryReadTable("data_types_test");
		config_datatype.setSelectedFields("string_datatype,bytes_datatype,integer_datatype,float_datatype,boolean_datatype");
		String srcQueryString = "CREATE TABLE " + config_datatype.getBigQueryReadTable()
				+ " (string_datatype STRING , bytes_datatype BYTES, integer_datatype INTEGER, float_datatype FLOAT, boolean_datatype BOOLEAN)"; // numeric_datatype NUMERIC
		flinkTableEnv.executeSql(srcQueryString + "\n" + "WITH (\n" + "  'connector' = 'bigquery',\n"
				+ "  'format' = 'arrow',\n" + "  'configOptions' = '" + config_datatype.getConfigMap() + "'\n" + ")");
		Table result = flinkTableEnv.from(config_datatype.getBigQueryReadTable());
		TableResult tableapi = result.execute();
		tableapi.print();
		assertThat(tableapi.getTableSchema()).isEqualTo(Constants.FLINK_TEST_TABLE_SCHEMA);
	}
}
