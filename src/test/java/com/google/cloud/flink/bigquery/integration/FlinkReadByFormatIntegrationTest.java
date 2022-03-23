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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.junit.Test;

public class FlinkReadByFormatIntegrationTest extends FlinkReadIntegrationTest {

	protected String dataFormat;

	public FlinkReadByFormatIntegrationTest() {
		super();		
	}

	@Test
	public void testOutOfOrderColumns() {

		config.setSelectedFields("word_count,word");

		String srcQueryString = "CREATE TABLE " + config.getBigQueryReadTable() + " (word STRING , word_count BIGINT)";
		flinkTableEnv.executeSql(srcQueryString + "\n" + "WITH (\n" + "  'connector' = 'bigquery',\n"
				+ "  'format' = 'arrow',\n" + "  'configOptions' = '" + config.getConfigMap() + "'\n" + ")");

		Table result = flinkTableEnv.from(config.getBigQueryReadTable());		
		assertThat(result.getResolvedSchema().getColumnDataTypes().get(0)).isEqualTo(DataTypes.STRING());
		assertThat(result.getResolvedSchema().getColumnDataTypes().get(1)).isEqualTo(DataTypes.BIGINT());
	}

	@Test
	public void testDefaultNumberOfPartitions() {

		config.setSelectedFields("word_count,word");
		String srcQueryString = "CREATE TABLE " + config.getBigQueryReadTable() + " (word STRING , word_count BIGINT)";
		flinkTableEnv.executeSql(srcQueryString + "\n" + "WITH (\n" + "  'connector' = 'bigquery',\n"
				+ "  'format' = 'arrow',\n" + "  'configOptions' = '" + config.getConfigMap() + "'\n" + ")");

		Table result = flinkTableEnv.from(config.getBigQueryReadTable());
		DataStream<Row> ds = flinkTableEnv.toDataStream(result);		
		assertThat(ds.getParallelism()).isEqualTo(1);
	}

	@Test
	public void testSelectAllColumnsFromATable() {

		config.setSelectedFields("word_count,word");

		String srcQueryString = "CREATE TABLE " + config.getBigQueryReadTable() + " (word STRING , word_count BIGINT)";
		flinkTableEnv.executeSql(srcQueryString + "\n" + "WITH (\n" + "  'connector' = 'bigquery',\n"
				+ "  'format' = 'arrow',\n" + "  'configOptions' = '" + config.getConfigMap() + "'\n" + ")");

		Table result = flinkTableEnv.from(config.getBigQueryReadTable());

		assertThat(result.getResolvedSchema().getColumnDataTypes().get(0)).isEqualTo(DataTypes.STRING());
		assertThat(result.getResolvedSchema().getColumnDataTypes().get(1)).isEqualTo(DataTypes.BIGINT());
	}
}
