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
import static org.apache.flink.table.api.Expressions.$;
import static org.junit.Assert.assertThrows;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Ignore;
import org.junit.Test;

import com.google.cloud.flink.bigquery.model.Configuration;

public class FlinkReadIntegrationTest extends FlinkBigQueryIntegrationTestBase {

	public FlinkReadIntegrationTest() {
		super();		
	}

	@SuppressWarnings("deprecation")
	private void testWordCount(TableResult tableRes) {
		assertThat(tableRes.getTableSchema()).isEqualTo(Constants.WORDCOUNT_TABLE_SCHEMA);
	}	

	@Test
	public void testReadWithOption() {

		config.setSelectedFields("word,word_count");
		String srcQueryString = "CREATE TABLE " + config.getBigQueryReadTable() + " (word STRING , word_count BIGINT)";
		flinkTableEnv.executeSql(srcQueryString + "\n" + "WITH (\n" + "  'connector' = 'bigquery',\n"
				+ "  'format' = 'arrow',\n" + "  'configOptions' = '" + config.getConfigMap() + "'\n" + ")");
		Table result = flinkTableEnv.from(config.getBigQueryReadTable());
		Table datatable = result.where($("word_count").isGreaterOrEqual(100)).select($("word"), $("word_count"));
		TableResult tableapi = datatable.execute();
		testWordCount(tableapi);

	}

	// We are passing filter in table API (Filter will work at flink level)
	@Test
	public void testReadWithFilterInTableAPI() {
		config.setSelectedFields("word,word_count");
		String srcQueryString = "CREATE TABLE " + config.getBigQueryReadTable() + " (word STRING , word_count BIGINT)";
		flinkTableEnv.executeSql(srcQueryString + "\n" + "WITH (\n" + "  'connector' = 'bigquery',\n"
				+ "  'format' = 'arrow',\n" + "  'configOptions' = '" + config.getConfigMap() + "'\n" + ")");
		Table result = flinkTableEnv.from(config.getBigQueryReadTable());
		Table datatable = result.where($("word_count").isGreaterOrEqual(500)).select($("word"), $("word_count"));
		DataStream<Row> ds = flinkTableEnv.toDataStream(datatable);
		int count = 0;
		try {
			CloseableIterator<Row> itr = ds.executeAndCollect();
			while (itr.hasNext()) {
				
				itr.next();
				count += 1;
			}
		} catch (Exception e) {
		}
		datatable.execute();		
		assertThat(count).isEqualTo(72);
	}

	// We are passing filter as an option (Filter will work at Storage API level)
	@Ignore
	@Test
	public void testReadWithFilter() {

		config.setFilter("word_count > 500 and word=\"I\"");
		config.setSelectedFields("word,word_count");
		config.setProjectId(System.getenv("GOOGLE_CLOUD_PROJECT"));
		config.setDataset(System.getenv("GOOGLE_CLOUD_DATASET"));
		config.setBigQueryReadTable(System.getenv("GOOGLE_CLOUD_TABLE"));
		String srcQueryString = "CREATE TABLE " + config.getBigQueryReadTable() + " (word STRING , word_count BIGINT)";
		flinkTableEnv.executeSql(srcQueryString + "\n" + "WITH (\n" + "  'connector' = 'bigquery',\n"
				+ "  'format' = 'arrow',\n" + "  'configOptions' = '" + config.getConfigMap() + "'\n" + ")");
		Table result = flinkTableEnv.from(config.getBigQueryReadTable());
		Table datatable = result.select($("word"), $("word_count"));
		DataStream<Row> ds = flinkTableEnv.toDataStream(datatable);
		int count = 0;
		try {
			CloseableIterator<Row> itr = ds.executeAndCollect();
			while (itr.hasNext()) {
				Row it = itr.next();
				count += 1;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		assertThat(count).isEqualTo(16);
	}

	@SuppressWarnings("deprecation")
	@Ignore
	@Test
	public void testReadForDifferentDataTypes() {

		Configuration configDatatype = new Configuration();
		configDatatype.setDataset("test");
		configDatatype.setBigQueryReadTable("data_types_test");
		configDatatype.setSelectedFields("string_datatype,bytes_datatype,integer_datatype,"
				+ "float_datatype,boolean_datatype,timestamp_datatype,"
				+ "date_datatype,datetime_datatype,geography_datatype" + "");
		String srcQueryString = "CREATE TABLE " + configDatatype.getBigQueryReadTable()
				+ " (string_datatype STRING , bytes_datatype BYTES, integer_datatype INTEGER,"
				+ " float_datatype FLOAT,boolean_datatype BOOLEAN, timestamp_datatype TIMESTAMP,"
				+ "  date_datatype DATE,datetime_datatype TIMESTAMP, geography_datatype STRING" + ")";
		flinkTableEnv.executeSql(srcQueryString + "\n" + "WITH (\n" + "  'connector' = 'bigquery',\n"
				+ "  'format' = 'arrow',\n" + "  'configOptions' = '" + configDatatype.getConfigMap() + "'\n" + ")");
		Table result = flinkTableEnv.from(configDatatype.getBigQueryReadTable());
		TableResult tableapi = result.execute();
		assertThat(tableapi.getTableSchema()).isEqualTo(Constants.FLINK_TEST_TABLE_SCHEMA);
	}

	@Test
	public void testReadCompressed() {
		config.setFilter("");
		config.setSelectedFields("word,word_count");
		config.setBqEncodedCreateReadSessionRequest("EgZCBBoCEAI");
		String table = "flink_test";
		String srcQueryString = "CREATE TABLE " + table + " (word STRING , word_count BIGINT)";
		flinkTableEnv.executeSql(srcQueryString + "\n" + "WITH (\n" + "  'connector' = 'bigquery',\n"
				+ "  'format' = 'arrow',\n" + "  'configOptions' = '" + config.getConfigMap() + "'\n" + ")");
		Table result = flinkTableEnv.from(table);
		TableResult tableapi = result.execute();
		testWordCount(tableapi);
	}

	@Test
	public void testReadCompressedWith1BackgroundThreads() {
		config.setBqEncodedCreateReadSessionRequest("EgZCBBoCEAI");
		config.setSelectedFields("word,word_count");
		config.setBqBackgroundThreadsPerStream(1);
		String table = "flink_test";
		String srcQueryString = "CREATE TABLE " + table + " (word STRING , word_count BIGINT)";
		flinkTableEnv.executeSql(srcQueryString + "\n" + "WITH (\n" + "  'connector' = 'bigquery',\n"
				+ "  'format' = 'arrow',\n" + "  'configOptions' = '" + config.getConfigMap() + "'\n" + ")");
		Table result = flinkTableEnv.from(table);
		TableResult tableapi = result.execute();
		testWordCount(tableapi);
	}

	@Test
	public void testReadCompressedWith4BackgroundThreads() {
		config.setBqEncodedCreateReadSessionRequest("EgZCBBoCEAI");
		config.setSelectedFields("word,word_count");
		config.setBqBackgroundThreadsPerStream(4);
		String srcQueryString = "CREATE TABLE " + config.getBigQueryReadTable() + " (word STRING , word_count BIGINT)";
		flinkTableEnv.executeSql(srcQueryString + "\n" + "WITH (\n" + "  'connector' = 'bigquery',\n"
				+ "  'format' = 'arrow',\n" + "  'configOptions' = '" + config.getConfigMap() + "'\n" + ")");
		Table result = flinkTableEnv.from(config.getBigQueryReadTable());
		TableResult tableapi = result.execute();
		testWordCount(tableapi);
	}

	@Ignore
	@Test(timeout = 50000) // throwing null pointer exception when use timeout
	public void testHeadDoesNotTimeoutAndOOM() {
		config.setBigQueryReadTable(Constants.LARGE_TABLE);
		config.setSelectedFields(Constants.LARGE_TABLE_FIELD);
		config.setProjectId(Constants.LARGE_TABLE_PROJECT_ID);
		config.setDataset(Constants.LARGE_TABLE_DATASET);
		// config.setParallelism(10);
		String srcQueryString = "CREATE TABLE " + config.getBigQueryReadTable() + " (is_male BOOLEAN)";
		flinkTableEnv.executeSql(srcQueryString + "\n" + "WITH (\n" + "  'connector' = 'bigquery',\n"
				+ "  'format' = 'arrow',\n" + "  'configOptions' = '" + config.getConfigMap() + "'\n" + ")");
		Table result = flinkTableEnv.from(config.getBigQueryReadTable());
		TableResult tableapi = result.execute();
		assertThat(tableapi.getResolvedSchema()).isEqualTo(Constants.LARGE_TABLE_SCHEMA);
	}

	@Test
	public void testNonExistentSchema() {		
		assertThrows("Trying to read a non existing table should throw an exception", ValidationException.class, () -> {
			Table result = flinkTableEnv.from(Constants.NON_EXISTENT_TABLE);
			result.execute();
		});
	}
}
