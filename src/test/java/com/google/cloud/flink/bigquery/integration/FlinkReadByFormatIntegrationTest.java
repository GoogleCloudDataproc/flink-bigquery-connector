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

import java.util.Optional;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.junit.Test;

import com.google.cloud.flink.bigquery.model.Configuration;

public class FlinkReadByFormatIntegrationTest extends FlinkReadIntegrationTest {

	Configuration config = new Configuration();

	public FlinkReadByFormatIntegrationTest() {
		super();
		config.setGcpCredentialKeyFile(System.getenv("GOOGLE_APPLICATION_CREDENTIALS"));
		config.setProjectId("bigquery-public-data");
		config.setDataset("samples");
		config.setBigQueryReadTable("shakespeare");
	}

	@Test
	public void testOutOfOrderColumns() {

		config.setSelectedFields("word_count,word");

		String srcQueryString = "CREATE TABLE " + config.getBigQueryReadTable() + " (word STRING , word_count BIGINT)";
		flinkTableEnv.executeSql(srcQueryString + "\n" + "WITH (\n" + "  'connector' = 'bigquery',\n"
				+ "  'format' = 'arrow',\n" + "  'configOptions' = '" + config.getConfigMap() + "'\n" + ")");

		Table result = flinkTableEnv.from(config.getBigQueryReadTable());

		System.out.println(result.getSchema().getFieldDataType(1));
		System.out.println(result.getResolvedSchema());

		assertThat(result.getSchema().getFieldDataType(0)).isEqualTo(Optional.of(DataTypes.STRING()));
		assertThat(result.getSchema().getFieldDataType(1)).isEqualTo(Optional.of(DataTypes.BIGINT()));
	}
}
