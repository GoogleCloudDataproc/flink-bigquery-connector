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
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.flink.bigquery.BigQueryTableSink;
import java.io.IOException;
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

  @Test
  public void testWriteToBigQuerySupportsOverwrite() throws Exception {
    String bigqueryReadTable = "bigquery-public-data.samples.shakespeare";
    String bigqueryWriteTable =
        PROJECT_ID + "." + testDataset.toString() + "." + "supportsOverwrite";
    testWriteToBigQuery(bigqueryReadTable, bigqueryWriteTable);
    Thread.sleep(120 * 1000);
    int count = getCountFromBigQueryTable(bigqueryWriteTable);
    assertThat(count).isEqualTo(96);
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
    Table datatable = result.select($("word"), $("word_count"));
    TableResult tableapi = datatable.execute();
    int count = 0;
    try (CloseableIterator<Row> it = tableapi.collect()) {
      while (it.hasNext()) {
        it.next();
        count += 1;
      }
    }
    datatable.execute();
    return count;
  }

  public void testWriteToBigQuery(String bigqueryReadTable, String bigqueryWriteTable)
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
    BigQueryTableSink tableSink = new BigQueryTableSink(sourceResultTable, bigqueryWriteTable);

    tEnv.registerTableSink(flinkSinkTable, tableSink);

    TableResult sinkResult = sourceResultTable.executeInsert(flinkSinkTable, true);
    JobExecutionResult jobExecutionResult =
        sinkResult
            .getJobClient()
            .get()
            .getJobExecutionResult(Thread.currentThread().getContextClassLoader())
            .get();
  }

  @Test
  public void testPartitionHourly() throws Exception {
    String bigqueryReadTable = "bigquery-public-data.stackoverflow.posts_questions";
    String bigqueryWriteTable =
        PROJECT_ID + "." + testDataset.toString() + "." + "hourlyPartitioned";
    String filter = "view_count=300 and owner_user_id<5000";
    testPartition("HOUR", bigqueryReadTable, bigqueryWriteTable, filter);
    Thread.sleep(120 * 1000);
    int count = bqReadTableForPartiton(bigqueryWriteTable);
    assertThat(count).isEqualTo(37);
  }

  @Test
  public void testPartitionDaily() throws Exception {
    String bigqueryReadTable = "bigquery-public-data.stackoverflow.posts_questions";
    String bigqueryWriteTable =
        PROJECT_ID + "." + testDataset.toString() + "." + "dailyPartitioned";
    String filter = "view_count=300 and owner_user_id<5000";
    testPartition("DAY", bigqueryReadTable, bigqueryWriteTable, filter);
    Thread.sleep(120 * 1000);
    int count = bqReadTableForPartiton(bigqueryWriteTable);
    assertThat(count).isEqualTo(37);
  }

  @Test
  public void testPartitionMonthly() throws Exception {
    String bigqueryReadTable = "bigquery-public-data.stackoverflow.posts_questions";
    String bigqueryWriteTable =
        PROJECT_ID + "." + testDataset.toString() + "." + "monthlyPartitioned";
    String filter = "view_count=300 and owner_user_id<5000";
    testPartition("MONTH", bigqueryReadTable, bigqueryWriteTable, filter);
    Thread.sleep(120 * 1000);
    int count = bqReadTableForPartiton(bigqueryWriteTable);
    assertThat(count).isEqualTo(37);
  }

  @Test
  public void testPartitionYearly() throws Exception {
    String bigqueryReadTable = "bigquery-public-data.stackoverflow.posts_questions";
    String bigqueryWriteTable =
        PROJECT_ID + "." + testDataset.toString() + "." + "yearlyPartitioned";
    String filter = "view_count=300 and owner_user_id<5000";
    testPartition("YEAR", bigqueryReadTable, bigqueryWriteTable, filter);
    Thread.sleep(120 * 1000);
    int count = bqReadTableForPartiton(bigqueryWriteTable);
    assertThat(count).isEqualTo(37);
  }

  public int bqReadTableForPartiton(String bigqueryReadTable) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1); // source only supports parallelism of 1
    final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    String flinkSrcTable = "FlinkSrcTable";
    String srcQueryString =
        "CREATE TABLE " + flinkSrcTable + " (creation_date TIMESTAMP,tags STRING , title STRING)";
    tEnv.executeSql(
        srcQueryString
            + "\n"
            + "WITH (\n"
            + "  'connector' = 'bigquery',\n"
            + "  'format' = 'arrow',\n"
            + "  'table' = '"
            + bigqueryReadTable
            + "',\n"
            + "  'selectedFields' = 'tags,title,creation_date',\n"
            + "  'credentialsFile' = '"
            + System.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            + "' \n"
            + ")");
    Table result = tEnv.from(flinkSrcTable);
    Table datatable = result.select($("tags"), $("title"), $("creation_date"));
    TableResult tableapi = datatable.execute();
    int count = 0;
    try (CloseableIterator<Row> it = tableapi.collect()) {
      while (it.hasNext()) {
        Row row = it.next();
        count += 1;
      }
    }
    datatable.execute();
    return count;
  }

  public void testPartition(
      String partitionType, String bigqueryReadTable, String bigqueryWriteTable, String filter)
      throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1); // source only supports parallelism of 1
    final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    String flinkSrcTable = "FlinkSrcTable";
    String flinkSinkTable = "FlinkSinkTable";
    String srcQueryString =
        "CREATE TABLE " + flinkSrcTable + " (creation_date TIMESTAMP,tags STRING , title STRING)";
    tEnv.executeSql(
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
            + "  'selectedFields' = 'tags,title,creation_date', \n"
            + "  'partitionField' = 'creation_date', \n"
            + "  'partitionType' = '"
            + partitionType
            + "', \n" // DAY is default
            + "  'credentialsFile' = '"
            + System.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            + "'\n"
            + ")");
    Table sourceTable = tEnv.from(flinkSrcTable);
    Table sourceResultTable = sourceTable.select($("tags"), $("title"), $("creation_date"));
    BigQueryTableSink tableSink = new BigQueryTableSink(sourceResultTable, bigqueryWriteTable);
    tEnv.registerTableSink(flinkSinkTable, tableSink);
    TableResult sinkResult = sourceResultTable.executeInsert(flinkSinkTable);
    JobExecutionResult jobExecutionResult =
        sinkResult
            .getJobClient()
            .get()
            .getJobExecutionResult(Thread.currentThread().getContextClassLoader())
            .get();
  }

  @Test
  public void testWriteToBigQueryPartitionedTable() throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1); // source only supports parallelism of 1
    final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    String flinkSrcTable = "FlinkSrcTable";
    String flinkSinkTable = "FlinkSinkTable";
    String bigqueryReadTable = "bigquery-public-data.stackoverflow.posts_questions";
    String bigqueryWriteTable =
        PROJECT_ID + "." + testDataset.toString() + "." + "writeToBigQueryPartitionedTable";
    String filter = "view_count=300 and owner_user_id<5000";
    String partitionType = "DAY";
    String srcQueryString =
        "CREATE TABLE " + flinkSrcTable + " (creation_date TIMESTAMP,tags STRING , title STRING)";
    tEnv.executeSql(
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
            + "  'selectedFields' = 'tags,title,creation_date', \n"
            + "  'partitionField' = 'creation_date', \n"
            + "  'partitionType' = '"
            + partitionType
            + "', \n" // DAY is default
            + "  'credentialsFile' = '"
            + System.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            + "'\n"
            + ")");
    Table sourceTable = tEnv.from(flinkSrcTable);
    Table sourceResultTable = sourceTable.select($("tags"), $("title"), $("creation_date"));
    BigQueryTableSink tableSink = new BigQueryTableSink(sourceResultTable, bigqueryWriteTable);
    tEnv.registerTableSink(flinkSinkTable, tableSink);
    TableResult sinkResult = sourceResultTable.executeInsert(flinkSinkTable);
    JobExecutionResult jobExecutionResult =
        sinkResult
            .getJobClient()
            .get()
            .getJobExecutionResult(Thread.currentThread().getContextClassLoader())
            .get();
    StandardTableDefinition tableDefinition =
        testPartitionedTableDefinition(testDataset.toString(), "writeToBigQueryPartitionedTable");
    assertThat(tableDefinition.getTimePartitioning().getField()).isEqualTo("creation_date");
    ;
  }

  public StandardTableDefinition testPartitionedTableDefinition(String dataset, String table)
      throws IOException {
    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    return bigquery.getTable(dataset, table).getDefinition();
  }
}
