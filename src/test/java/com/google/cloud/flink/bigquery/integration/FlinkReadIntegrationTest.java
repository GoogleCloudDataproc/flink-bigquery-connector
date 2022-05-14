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

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Ignore;
import org.junit.Test;

public class FlinkReadIntegrationTest extends FlinkBigQueryIntegrationTestBase {
  String flinkSrcTable;
  private static final String ALL_TYPES_TABLE_NAME = "all_types";

  public FlinkReadIntegrationTest() {
    super();
    flinkSrcTable = "FlinkSrcTable";
  }

  private void testWordCount(TableResult tableRes) {
    assertThat(tableRes.getTableSchema()).isEqualTo(Constants.WORDCOUNT_TABLE_SCHEMA);
  }

  @Test
  public void testReadWithOption() {
    String bigqueryReadTable = "bigquery-public-data.samples.shakespeare";
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
  public void testReadWithFilterInTableAPI() throws Exception {
    String bigqueryReadTable = "bigquery-public-data.samples.shakespeare";
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
    int count = 0;
    TableResult tableResult = datatable.execute();
    try (CloseableIterator<Row> it = tableResult.collect()) {
      while (it.hasNext()) {
        it.next();
        count += 1;
      }
    }
    assertEquals(count, 96);
  }

  // We are passing filter as an option (Filter will work at Storage API level)
  @Test
  public void testReadWithFilter() throws Exception {
    String bigqueryReadTable = "bigquery-public-data.samples.shakespeare";
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
    int count = 0;
    TableResult tableResult = datatable.execute();
    try (CloseableIterator<Row> it = tableResult.collect()) {
      while (it.hasNext()) {
        it.next();
        count += 1;
      }
    }
    assertThat(count).isEqualTo(24);
  }

  // TODO: Few data types are not supported by Flink , custom data types research
  // is under progress.
  @Test
  public void testReadForDifferentDataTypes() {
    String bigqueryReadTable = testDataset.toString() + "." + ALL_TYPES_TABLE_NAME;
    String selectedFields =
        "numeric_datatype,string_datatype,bytes_datatype,integer_datatype,"
            + "float_datatype,boolean_datatype,timestamp_datatype,"
            + "date_datatype,datetime_datatype,geography_datatype"
            + "";
    String srcQueryString =
        "CREATE TABLE "
            + flinkSrcTable
            + " (numeric_datatype DECIMAL(38,9),"
            + "string_datatype STRING , bytes_datatype BYTES, integer_datatype BIGINT,"
            + " float_datatype DOUBLE,boolean_datatype BOOLEAN, timestamp_datatype TIMESTAMP,"
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
            + "'\n"
            + ")");
    Table result = flinkTableEnv.from(flinkSrcTable);
    TableResult tableapi = result.execute();
    assertThat(tableapi.getTableSchema()).isEqualTo(Constants.FLINK_TEST_TABLE_SCHEMA);
  }

  @Test
  public void testReadCompressed() {
    String bigqueryReadTable = "bigquery-public-data.samples.shakespeare";
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
            + "  'bqEncodedCreateReadSessionRequest' = 'EgZCBBoCEAI',\n"
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
    String bigqueryReadTable = "bigquery-public-data.samples.shakespeare";
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
            + "  'bqEncodedCreateReadSessionRequest' = 'EgZCBBoCEAI',\n"
            + "  'bqBackgroundThreadsPerStream' = '1',\n"
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
    String bigqueryReadTable = "bigquery-public-data.samples.shakespeare";
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
            + "  'bqEncodedCreateReadSessionRequest' = 'EgZCBBoCEAI',\n"
            + "  'bqBackgroundThreadsPerStream' = '4',\n"
            + "  'credentialsFile' = '"
            + System.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            + "' \n"
            + ")");
    Table result = flinkTableEnv.from(flinkSrcTable);
    TableResult tableapi = result.execute();
    testWordCount(tableapi);
  }

  // TODO : Code is executed in 15 sec , thought it is not working as expected
  // with timeout.
  @Ignore
  @Test(timeout = 50000) // throwing null pointer exception when use timeout
  public void testHeadDoesNotTimeoutAndOOM() {
    String bigqueryReadTable =
        Constants.LARGE_TABLE_PROJECT_ID
            + "."
            + Constants.LARGE_TABLE_DATASET
            + "."
            + Constants.LARGE_TABLE;

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
            + "  'bqBackgroundThreadsPerStream' = '4',\n"
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
    String bigqueryReadTable = "bigquery-public-data.samples.shakespeare";
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
          result.execute();
        });
  }
}
