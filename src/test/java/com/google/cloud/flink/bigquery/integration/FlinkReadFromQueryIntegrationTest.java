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

import static org.apache.flink.table.api.Expressions.$;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.flink.bigquery.FlinkBigQueryException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Test;

public class FlinkReadFromQueryIntegrationTest extends FlinkBigQueryIntegrationTestBase {

  private BigQuery bq;
  StreamTableEnvironment flinkTableEnv;

  public FlinkReadFromQueryIntegrationTest() {

    this.bq = BigQueryOptions.getDefaultInstance().getService();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1); // source only supports parallelism of 1
    flinkTableEnv = StreamTableEnvironment.create(env);
  }

  @Test
  public void testReadFromQuery() {
    String bigqueryReadTable = "q-gcp-6750-pso-gs-flink-22-01.wordcount_dataset.wordcount_output";
    String flinkSrcTable = "FlinkSrcTable";
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
            + "  'filter' = 'word_count > 500',\n"
            + "  'credentialsFile' = '"
            + System.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            + "' ,\n"
            + "  'selectedFields' = 'word,word_count' \n"
            + ")");
    Table result = flinkTableEnv.from(flinkSrcTable);
    Table datatable = result.select($("word"), $("word_count"));
    TableResult tableapi = datatable.execute();
    assertNotNull(tableapi);
    assertEquals(2, tableapi.getTableSchema().getFieldCount());
  }

  @Test
  public void testBadSql() {
    assertThrows(
        RuntimeException.class,
        () -> {
          String bigqueryReadTable =
              "q-gcp-6750-pso-gs-flink-22-01.wordcount_dataset.wordcount_output";
          String flinkSrcTable = "FlinkSrcTable";
          String flinkSrcTable1 = "FlinkSrcTable";
          String srcQueryString =
              "CREATE TABLE " + flinkSrcTable1 + " (word STRING , word_count BIGINT)";
          flinkTableEnv.executeSql(
              srcQueryString
                  + "\n"
                  + "WITH (\n"
                  + "  'connector' = 'bigquery',\n"
                  + "  'format' = 'arrow',\n"
                  + "  'table' = '"
                  + bigqueryReadTable
                  + "',\n"
                  + "  'filter' = 'word_count > 500',\n"
                  + "  'credentialsFile' = '"
                  + System.getenv("GOOGLE_APPLICATION_CREDENTIALS")
                  + "' ,\n"
                  + "  'selectedFields' = 'word_test' \n"
                  + ")");
          Table result = flinkTableEnv.from(flinkSrcTable1);
          Table datatable =
              result
                  .where($("word_count").isGreaterOrEqual(100))
                  .select($("word"), $("word_count"));
          TableResult tableapi = datatable.execute();
          throw new FlinkBigQueryException("Column not found");
        });
  }

  // We are passing all the configuration values and setting filter in flink and tableAPI both
  // together.
  @Test
  public void testReadFromQueryInternal() {
    String bigqueryReadTable = "q-gcp-6750-pso-gs-flink-22-01.wordcount_dataset.wordcount_output";
    String flinkSrcTable = "FlinkSrcTable";
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
            + "  'filter' = '"
            + filter
            + "',\n"
            + "  'credentialsFile' = '"
            + System.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            + "' ,\n"
            + "  'selectedFields' = 'word,word_count' \n"
            + ")");
    Table result = flinkTableEnv.from(flinkSrcTable);
    Table datatable =
        result.where($("word_count").isGreaterOrEqual(100)).select($("word"), $("word_count"));
    DataStream<Row> ds = flinkTableEnv.toDataStream(datatable);

    int count = 0;
    try {
      CloseableIterator<Row> itr = ds.executeAndCollect();
      while (itr.hasNext()) {
        itr.next();
        count += 1;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    assertEquals(count, 16);
  }
}
