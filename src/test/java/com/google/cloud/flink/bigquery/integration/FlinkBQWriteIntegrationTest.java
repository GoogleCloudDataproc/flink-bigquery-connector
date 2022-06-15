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

import com.google.cloud.ServiceOptions;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Test;

public class FlinkBQWriteIntegrationTest extends FlinkBigQueryIntegrationTestBase {
  private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();

  public FlinkBQWriteIntegrationTest() {
    super();
  }

  @Test
  public void testWriteToBigQuerySupportsOverwrite() throws Exception {
    String bigqueryReadTable = "bigquery-public-data.samples.shakespeare";
    String bigqueryWriteTable =
        PROJECT_ID + "." + testDataset.toString() + "." + "supportsOverwrite";
    boolean isExecuted = testWriteToBigQuery(bigqueryReadTable, bigqueryWriteTable);
    Thread.sleep(120 * 1000);
    int count = getCountFromBigQueryTable(bigqueryWriteTable);
    assertThat(count).isEqualTo(96);
    assertThat(isExecuted);
  }

  public int getCountFromBigQueryTable(String bigqueryWriteTable) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1); // source only supports parallelism of 1
    final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    String flinkSrcTable = "FlinkSrcTable";
    String srcQueryString = "CREATE TABLE " + flinkSrcTable + " (word STRING , word_count BIGINT)";
    tEnv.executeSql(
        srcQueryString
            + "\n"
            + "WITH (\n"
            + "  'connector' = 'bigquery',\n"
            + "  'format' = 'arrow',\n"
            + "  'table' = '"
            + bigqueryWriteTable
            + "',\n"
            + "  'selectedFields' = 'word,word_count',\n"
            + "  'credentialsFile' = '"
            + System.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            + "' \n"
            + ")");
    Table result = tEnv.from(flinkSrcTable);
    Table dataTable = result.select($("word"), $("word_count"));
    TableResult tableapi = dataTable.execute();
    int count = 0;
    try (CloseableIterator<Row> it = tableapi.collect()) {
      while (it.hasNext()) {
        it.next();
        count += 1;
      }
    }
    dataTable.execute();
    return count;
  }

  public boolean testWriteToBigQuery(String bigqueryReadTable, String bigqueryWriteTable)
      throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1); // source only supports parallelism of 1
    final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    String flinkSrcTable = "FlinkSrcTable";
    String flinkSinkTable = "FlinkSinkTable";
    String srcQueryString = "CREATE TABLE " + flinkSrcTable + " (word STRING , word_count BIGINT)";
    tEnv.executeSql(
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
    Table sourceTable = tEnv.from(flinkSrcTable);
    Table sourceResultTable =
        sourceTable.where($("word_count").isGreaterOrEqual(500)).select($("word"), $("word_count"));

    String sinkQueryString =
        "CREATE TABLE " + flinkSinkTable + " (word STRING , word_count BIGINT)";
    tEnv.executeSql(
        sinkQueryString
            + "\n"
            + "WITH (\n"
            + "  'connector' = 'bigquery',\n"
            + "  'table' = '"
            + bigqueryWriteTable
            + "',\n"
            + "  'maxParallelism' = '10'\n"
            + ")");
    TableResult sinkResult = sourceResultTable.executeInsert(flinkSinkTable, true);
    JobExecutionResult jobExecutionResult =
        sinkResult.getJobClient().get().getJobExecutionResult().get();
    return jobExecutionResult.isJobExecutionResult();
  }
}
