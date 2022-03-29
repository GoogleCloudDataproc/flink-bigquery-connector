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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Ignore;
import org.junit.Test;

public class FlinkReadIntegrationTest extends FlinkBigQueryIntegrationTestBase {
  String flinkSrcTable;

  public FlinkReadIntegrationTest() {
    super();
    flinkSrcTable = "FlinkSrcTable";
  }

  @SuppressWarnings("deprecation")
  private void testWordCount(TableResult tableRes) {
    assertThat(tableRes.getTableSchema()).isEqualTo(Constants.WORDCOUNT_TABLE_SCHEMA);
  }

  private void testReadFilter(int count) {
    assertEquals(count, 16);
  }

  @Test
  public void testReadWithOption() {
    String bigqueryReadTable = "q-gcp-6750-pso-gs-flink-22-01.wordcount_dataset.wordcount_output";
    String srcQueryString = "CREATE TABLE " + flinkSrcTable + " (word STRING , word_count BIGINT)";
    flinkTableEnv.executeSql(
        srcQueryString
            + "\n"
            + "WITH (\n"
            + "  'connector' = 'bigquery',\n"
            + "  'format' = 'arrow',\n"
            + "  'table' = '"
            + bigqueryReadTable
            + "',\n"
            + "  'selectedFields' = 'word,word_count',\n"
            + "  'credentialsFile' = '"
            + System.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            + "' \n"
            + ")");
    Table result = flinkTableEnv.from(flinkSrcTable);
    Table datatable =
        result.where($("word_count").isGreaterOrEqual(100)).select($("word"), $("word_count"));
    TableResult tableapi = datatable.execute();
    testWordCount(tableapi);
  }

  // We are passing filter in table API (Filter will work at flink level)
  @Test
  public void testReadWithFilterInTableAPI() {
    String bigqueryReadTable = "q-gcp-6750-pso-gs-flink-22-01.wordcount_dataset.wordcount_output";
    String srcQueryString = "CREATE TABLE " + flinkSrcTable + " (word STRING , word_count BIGINT)";
    flinkTableEnv.executeSql(
        srcQueryString
            + "\n"
            + "WITH (\n"
            + "  'connector' = 'bigquery',\n"
            + "  'format' = 'arrow',\n"
            + "  'table' = '"
            + bigqueryReadTable
            + "',\n"
            + "  'selectedFields' = 'word,word_count',\n"
            + "  'credentialsFile' = '"
            + System.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            + "' \n"
            + ")");
    Table result = flinkTableEnv.from(flinkSrcTable);
    Table datatable =
        result.where($("word_count").isGreaterOrEqual(500)).select($("word"), $("word_count"));
    DataStream<Row> ds = flinkTableEnv.toDataStream(datatable);
    int count = 0;
    try {
      CloseableIterator<Row> itr = ds.executeAndCollect();
      while (itr.hasNext()) {
        Row it = itr.next();
        count += 1;
      }
    } catch (Exception e) {
    }
    datatable.execute();
    assertEquals(count, 72);
  }

  // We are passing filter as an option (Filter will work at Storage API level)
  @Test
  public void testReadWithFilter() {
    String bigqueryReadTable = "q-gcp-6750-pso-gs-flink-22-01.wordcount_dataset.wordcount_output";
    String filter = "word_count > 500 and word=\"I\"";
    String srcQueryString = "CREATE TABLE " + flinkSrcTable + " (word STRING , word_count BIGINT)";
    flinkTableEnv.executeSql(
        srcQueryString
            + "\n"
            + "WITH (\n"
            + "  'connector' = 'bigquery',\n"
            + "  'format' = 'arrow',\n"
            + "  'table' = '"
            + bigqueryReadTable
            + "',\n"
            + "  'selectedFields' = 'word,word_count',\n"
            + "  'filter' = '"
            + filter
            + "',\n"
            + "  'credentialsFile' = '"
            + System.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            + "' \n"
            + ")");
    Table result = flinkTableEnv.from(flinkSrcTable);
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

  // TODO: Few data types rae not supported by Flink , custom data types research is under progress.
  @SuppressWarnings("deprecation")
  @Ignore
  @Test
  public void testReadForDifferentDataTypes() {
    String bigqueryReadTable = "q-gcp-6750-pso-gs-flink-22-01.wordcount_dataset.wordcount_output";
    String selectedFields =
        "string_datatype,bytes_datatype,integer_datatype,"
            + "float_datatype,boolean_datatype,timestamp_datatype,"
            + "date_datatype,datetime_datatype,geography_datatype"
            + "";
    String srcQueryString =
        "CREATE TABLE "
            + flinkSrcTable
            + " (string_datatype STRING , bytes_datatype BYTES, integer_datatype INTEGER,"
            + " float_datatype FLOAT,boolean_datatype BOOLEAN, timestamp_datatype TIMESTAMP,"
            + "  date_datatype DATE,datetime_datatype TIMESTAMP, geography_datatype STRING"
            + ")";
    flinkTableEnv.executeSql(
        srcQueryString
            + "\n"
            + "WITH (\n"
            + "  'connector' = 'bigquery',\n"
            + "  'format' = 'arrow',\n"
            + "  'table' = '"
            + bigqueryReadTable
            + "',\n"
            + "  'selectedFields' = '"
            + selectedFields
            + "',\n"
            + "  'credentialsFile' = '"
            + System.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            + "' \n"
            + ")");
    Table result = flinkTableEnv.from(flinkSrcTable);
    TableResult tableapi = result.execute();
    tableapi.print();
    assertThat(tableapi.getTableSchema()).isEqualTo(Constants.FLINK_TEST_TABLE_SCHEMA);
  }

  @Test
  public void testReadCompressed() {
    String bigqueryReadTable = "q-gcp-6750-pso-gs-flink-22-01.wordcount_dataset.wordcount_output";
    String srcQueryString = "CREATE TABLE " + flinkSrcTable + " (word STRING , word_count BIGINT)";
    flinkTableEnv.executeSql(
        srcQueryString
            + "\n"
            + "WITH (\n"
            + "  'connector' = 'bigquery',\n"
            + "  'format' = 'arrow',\n"
            + "  'table' = '"
            + bigqueryReadTable
            + "',\n"
            + "  'bQEncodedCreaterReadSessionRequest' = 'EgZCBBoCEAI',\n"
            + "  'selectedFields' = 'word,word_count',\n"
            + "  'credentialsFile' = '"
            + System.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            + "' \n"
            + ")");
    Table result = flinkTableEnv.from(flinkSrcTable);
    TableResult tableapi = result.execute();
    testWordCount(tableapi);
  }

  @Test
  public void testReadCompressedWith1BackgroundThreads() {
    String bigqueryReadTable = "q-gcp-6750-pso-gs-flink-22-01.wordcount_dataset.wordcount_output";
    String table = "flink_test";
    String srcQueryString = "CREATE TABLE " + table + " (word STRING , word_count BIGINT)";
    flinkTableEnv.executeSql(
        srcQueryString
            + "\n"
            + "WITH (\n"
            + "  'connector' = 'bigquery',\n"
            + "  'format' = 'arrow',\n"
            + "  'table' = '"
            + bigqueryReadTable
            + "',\n"
            + "  'selectedFields' = 'word,word_count',\n"
            + "  'bQEncodedCreaterReadSessionRequest' = 'EgZCBBoCEAI',\n"
            + "  'bQBackgroundThreadsPerStream' = '1',\n"
            + "  'credentialsFile' = '"
            + System.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            + "' \n"
            + ")");
    Table result = flinkTableEnv.from(table);
    TableResult tableapi = result.execute();
    testWordCount(tableapi);
  }

  @Test
  public void testReadCompressedWith4BackgroundThreads() {
    String bigqueryReadTable = "q-gcp-6750-pso-gs-flink-22-01.wordcount_dataset.wordcount_output";
    String srcQueryString = "CREATE TABLE " + flinkSrcTable + " (word STRING , word_count BIGINT)";
    flinkTableEnv.executeSql(
        srcQueryString
            + "\n"
            + "WITH (\n"
            + "  'connector' = 'bigquery',\n"
            + "  'format' = 'arrow',\n"
            + "  'table' = '"
            + bigqueryReadTable
            + "',\n"
            + "  'selectedFields' = 'word,word_count',\n"
            + "  'bQEncodedCreaterReadSessionRequest' = 'EgZCBBoCEAI',\n"
            + "  'bQBackgroundThreadsPerStream' = '4',\n"
            + "  'credentialsFile' = '"
            + System.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            + "' \n"
            + ")");
    Table result = flinkTableEnv.from(flinkSrcTable);
    TableResult tableapi = result.execute();
    testWordCount(tableapi);
  }

  // TODO : Code is executed in 15 sec , thought it is not working as expected with timeout.
  @Ignore
  @Test(timeout = 50000) // throwing null pointer exception when use timeout
  public void testHeadDoesNotTimeoutAndOOM() {
    String bigqueryReadTable =
        Constants.LARGE_TABLE_PROJECT_ID
            + "."
            + Constants.LARGE_TABLE_DATASET
            + "."
            + Constants.LARGE_TABLE;
    // config.setParallelism(10);
    String srcQueryString = "CREATE TABLE " + flinkSrcTable + " (is_male BOOLEAN)";
    flinkTableEnv.executeSql(
        srcQueryString
            + "\n"
            + "WITH (\n"
            + "  'connector' = 'bigquery',\n"
            + "  'format' = 'arrow',\n"
            + "  'table' = '"
            + bigqueryReadTable
            + "',\n"
            + "  'selectedFields' = '"
            + Constants.LARGE_TABLE_FIELD
            + "',\n"
            + "  'BqBackgroundThreadsPerStream' = '4',\n"
            + "  'credentialsFile' = '"
            + System.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            + "' \n"
            + ")");
    Table result = flinkTableEnv.from(flinkSrcTable);
    TableResult tableapi = result.execute();
    assertThat(tableapi.getTableSchema()).isEqualTo(Constants.LARGE_TABLE_SCHEMA);
  }

  @Test
  public void testNonExistentSchema() {
    String bigqueryReadTable = "q-gcp-6750-pso-gs-flink-22-01.wordcount_dataset.noSuchTable";
    assertThrows(
        "Trying to read a non existing table should throw an exception",
        ValidationException.class,
        () -> {
          String srcQueryString = "CREATE TABLE " + flinkSrcTable + " (test STRING)";
          flinkTableEnv.executeSql(
              srcQueryString
                  + "\n"
                  + "WITH (\n"
                  + "  'connector' = 'bigquery',\n"
                  + "  'format' = 'arrow',\n"
                  + "  'table' = '"
                  + bigqueryReadTable
                  + "',\n"
                  + "  'selectedFields' = 'test',\n"
                  + "  'credentialsFile' = '"
                  + System.getenv("GOOGLE_APPLICATION_CREDENTIALS")
                  + "' \n"
                  + ")");
          Table result = flinkTableEnv.from(flinkSrcTable);
          TableResult tableapi = result.execute();
        });
  }

  //	@Test
  //	public void testKeepingFiltersBehaviour() {
  //		Configuration config1 = new Configuration();
  //		config1.setCombinePushedDownFilters(false);
  //		config1.setFilter("word_count > 500 and word=\"I\"");
  //		config1.setProjectId("q-gcp-6750-pso-gs-flink-22-01");
  //		config1.setDataset("wordcount_dataset");
  //		config1.setBigQueryReadTable("wordcount_output");
  //		config1.setSelectedFields("word,word_count");
  //		String srcQueryString1 = "CREATE TABLE " + config1.getBigQueryReadTable()
  //				+ " (word STRING , word_count BIGINT)";
  //		flinkTableEnv.executeSql(srcQueryString1 + "\n" + "WITH (\n" + "  'connector' = 'bigquery',\n"
  //				+ "  'format' = 'arrow',\n" + "  'configOptions' = '" + config1.getConfigMap() + "'\n" +
  // ")");
  //
  //		Table result1 = flinkTableEnv.from(config1.getBigQueryReadTable());
  //		Table datatable1 = result1.select($("word"), $("word_count"));
  //		DataStream<Row> ds1 = flinkTableEnv.toDataStream(datatable1);
  //		int count1 = 0;
  //		try {
  //			CloseableIterator<Row> itr = ds1.executeAndCollect();
  //			while (itr.hasNext()) {
  //				Row it = itr.next();
  //				count1 += 1;
  //			}
  //		} catch (Exception e) {
  //			e.printStackTrace();
  //		}
  //
  //
  //		Configuration config2 = new Configuration();
  //		config2.setCombinePushedDownFilters(true);
  //		config2.setFilter("word_count > 500 and word=\"I\"");
  //		config2.setProjectId("q-gcp-6750-pso-gs-flink-22-01");
  //		config2.setDataset("wordcount_dataset");
  //		config2.setBigQueryReadTable("wordcount_output");
  //		config2.setSelectedFields("word,word_count");
  //		String srcQueryString2 = "CREATE TABLE " + config2.getBigQueryReadTable()
  //				+ " (word STRING , word_count BIGINT)";
  //		flinkTableEnv.executeSql(srcQueryString2 + "\n" + "WITH (\n" + "  'connector' = 'bigquery',\n"
  //				+ "  'format' = 'arrow',\n" + "  'configOptions' = '" + config2.getConfigMap() + "'\n" +
  // ")");
  //
  //		Table result2 = flinkTableEnv.from(config2.getBigQueryReadTable());
  //		Table datatable2 = result2.select($("word"), $("word_count"));
  //		DataStream<Row> ds2 = flinkTableEnv.toDataStream(datatable2);
  //		int count2 = 0;
  //		try {
  //			CloseableIterator<Row> itr = ds2.executeAndCollect();
  //			while (itr.hasNext()) {
  //				Row it = itr.next();
  //				count2 += 1;
  //			}
  //		} catch (Exception e) {
  //			e.printStackTrace();
  //		}
  //		assertThat(count1).isEqualTo(count2);
  //	}
}
