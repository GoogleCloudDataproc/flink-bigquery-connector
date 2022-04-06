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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.flink.bigquery.BigQueryTableSink;
import com.google.cloud.flink.bigquery.FlinkBigQueryConfig;
import java.io.IOException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Test;

public class FlinkBQWriteIntegrationTest extends FlinkBigQueryIntegrationTestBase
    implements SupportsOverwrite {
  final FlinkBigQueryConfig.WriteMethod writeMethod = null;

  boolean overwrite = false;

  @Test
  public void testWriteToBigQueryAppendSaveMode() throws Exception {
    String bigqueryReadTable = "q-gcp-6750-pso-gs-flink-22-01.wordcount_dataset.wordcount_output";
    String bigqueryWriteTable = "q-gcp-6750-pso-gs-flink-22-01.test.sink_sample_test_sink_output6";
    testWriteToBigQuery(bigqueryReadTable, bigqueryWriteTable);
    Thread.sleep(120 * 1000);
    int count = bqReadTable(bigqueryWriteTable);
    assertThat(count).isEqualTo(72);
  }

  public int bqReadTable(String bigqueryWriteTable) throws Exception {
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

  @SuppressWarnings({"unused", "deprecation"})
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
    TableResult sinkResult = sourceResultTable.executeInsert(flinkSinkTable);
    JobExecutionResult jobExecutionResult =
        sinkResult
            .getJobClient()
            .get()
            .getJobExecutionResult(Thread.currentThread().getContextClassLoader())
            .get();
  }

  @Test
  public void testPartitioHourly() throws Exception {
    String bigqueryReadTable = "q-gcp-6750-pso-gs-flink-22-01.testDataset.posts_questions";
    String bigqueryWriteTable = "q-gcp-6750-pso-gs-flink-22-01.wordcount_dataset.sinkWriteLatest1";
    testPartition("HOUR", bigqueryReadTable, bigqueryWriteTable);
    Thread.sleep(120 * 1000); // so that writing part complete successfully
    int count = bqReadTableForPartiton(bigqueryWriteTable);
    assertThat(count).isEqualTo(10000);
  }

  @Test
  public void testPartitionDaily() throws Exception {
    String bigqueryReadTable = "q-gcp-6750-pso-gs-flink-22-01.testDataset.posts_questions";
    String bigqueryWriteTable = "q-gcp-6750-pso-gs-flink-22-01.wordcount_dataset.sinkWriteLatest2";
    testPartition("DAY", bigqueryReadTable, bigqueryWriteTable);
    Thread.sleep(120 * 1000);
    int count = bqReadTableForPartiton(bigqueryWriteTable);
    assertThat(count).isEqualTo(10000);
  }

  @Test
  public void testPartitionMonthly() throws Exception {
    String bigqueryReadTable = "q-gcp-6750-pso-gs-flink-22-01.testDataset.posts_questions";
    String bigqueryWriteTable = "q-gcp-6750-pso-gs-flink-22-01.wordcount_dataset.sinkWriteLatest3";
    testPartition("MONTH", bigqueryReadTable, bigqueryWriteTable);
    Thread.sleep(120 * 1000);
    int count = bqReadTableForPartiton(bigqueryWriteTable);
    assertThat(count).isEqualTo(10000);
  }

  @Test
  public void testPartitionYearly() throws Exception {
    String bigqueryReadTable = "q-gcp-6750-pso-gs-flink-22-01.testDataset.posts_questions";
    String bigqueryWriteTable = "q-gcp-6750-pso-gs-flink-22-01.wordcount_dataset.sinkWriteLatest6";
    testPartition("YEAR", bigqueryReadTable, bigqueryWriteTable);
    Thread.sleep(120 * 1000);
    int count = bqReadTableForPartiton(bigqueryWriteTable);
    assertThat(count).isEqualTo(10000);
  }

  @SuppressWarnings("unused")
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

  @SuppressWarnings({"deprecation", "unused"})
  public void testPartition(
      String partitionType, String bigqueryReadTable, String bigqueryWriteTable) throws Exception {
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
            + "  'selectedFields' = 'tags,title,creation_date', \n"
            + "  'partitionField' = 'creation_date', \n"
            + "  'partitionType' = '"
            + partitionType
            + "', \n" // DAY
            // is
            // default
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
  public void testWriteToBigQueryPartitionedAndClusteredTable() throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1); // source only supports parallelism of 1
    final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    String flinkSrcTable = "FlinkSrcTable";
    String flinkSinkTable = "FlinkSinkTable";
    String bigqueryReadTable = "q-gcp-6750-pso-gs-flink-22-01.testDataset.posts_questions";
    String bigqueryWriteTable = "q-gcp-6750-pso-gs-flink-22-01.wordcount_dataset.partition1";
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
            + "  'selectedFields' = 'tags,title,creation_date', \n"
            + "  'partitionField' = 'creation_date', \n"
            + "  'partitionType' = '"
            + partitionType
            + "', \n" // DAY
            // is
            // default
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
        testPartitionedTableDefinition("wordcount_dataset", "partition1");
    assertThat(tableDefinition.getTimePartitioning().getField()).isEqualTo("creation_date");
    ;
  }

  public StandardTableDefinition testPartitionedTableDefinition(String dataset, String table)
      throws IOException {
    GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
    final BigQuery bigquery =
        BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
    return bigquery.getTable(dataset, table).getDefinition();
  }

  @Test
  public void testWriteToBigQuerySimplifiedApi() throws Exception {
    String bigqueryReadTable = "q-gcp-6750-pso-gs-flink-22-01.wordcount_dataset.wordcount_output";
    String bigqueryWriteTable = "q-gcp-6750-pso-gs-flink-22-01.test.sink_sample_test_sink1234sk";
    String tempGcsBucket = "tempGcsBucket";
    testWriteToBigQuery(bigqueryReadTable, bigqueryWriteTable, tempGcsBucket);
    Thread.sleep(120 * 1000);
    int count = bqReadTable(bigqueryWriteTable);
    assertThat(count).isEqualTo(72);
  }

  @SuppressWarnings({"unused", "deprecation"})
  public void testWriteToBigQuery(
      String bigqueryReadTable, String bigqueryWriteTable, String tempGcsBucket) throws Exception {
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
            + "  'temporaryGcsBucket' = '"
            + tempGcsBucket
            + "',\n"
            + "  'selectedFields' = 'word,word_count' \n"
            + ")");
    Table sourceTable = tEnv.from(flinkSrcTable);
    Table sourceResultTable =
        sourceTable.where($("word_count").isGreaterOrEqual(500)).select($("word"), $("word_count"));
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

  @Override
  public void applyOverwrite(boolean overwrite) {

    this.overwrite = overwrite;
  }
}
