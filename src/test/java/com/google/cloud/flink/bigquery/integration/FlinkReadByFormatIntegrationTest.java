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

import java.util.Optional;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Test;

public class FlinkReadByFormatIntegrationTest extends FlinkReadIntegrationTest {

  protected String dataFormat;

  public FlinkReadByFormatIntegrationTest() {
    super();
  }

  @Test
  public void testOutOfOrderColumns() throws Exception {

    String bigqueryReadTable = "bigquery-public-data.samples.shakespeare";
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
    Table table1 = flinkTableEnv.from(flinkSrcTable);
    Table result = table1.select($("word_count"), $("word"));
    int count = 0;
    TableResult tableResult = result.execute();
    try (CloseableIterator<Row> it = tableResult.collect()) {
      while (it.hasNext()) {
        Row row = it.next();
        count += 1;
      }
    }
    assertThat(count).isEqualTo(96);
    assertThat(result.getSchema().getFieldDataType(0)).isEqualTo(Optional.of(DataTypes.BIGINT()));
    assertThat(result.getSchema().getFieldDataType(1)).isEqualTo(Optional.of(DataTypes.STRING()));
  }

  @Test
  public void testDefaultNumberOfPartitions() {
    String bigqueryReadTable = "bigquery-public-data.samples.shakespeare";
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
    DataStream<Row> ds = flinkTableEnv.toAppendStream(result, Row.class);
    assertThat(ds.getExecutionConfig().getParallelism()).isEqualTo(1);
  }

  @Test
  public void testSelectAllColumnsFromATable() {

    String bigqueryReadTable = "bigquery-public-data.samples.shakespeare";
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

    assertThat(result.getSchema().getFieldDataType(0)).isEqualTo(Optional.of(DataTypes.STRING()));
    assertThat(result.getSchema().getFieldDataType(1)).isEqualTo(Optional.of(DataTypes.BIGINT()));
  }

  @Test
  public void testViewWithDifferentColumnsForSelectAndFilter() {
    String bigqueryReadTable = "bigquery-public-data.samples.shakespeare";
    String srcQueryString = "CREATE TABLE " + flinkSrcTable + " (word STRING , word_count BIGINT)";
    String filter = "word_count > 500";
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
            + "  'selectedFields' = 'word',\n"
            + "  'credentialsFile' = '"
            + System.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            + "' \n"
            + ")");
    Table result = flinkTableEnv.from(flinkSrcTable);
    assertThat(result.getSchema().getFieldDataType(0)).isEqualTo(Optional.of(DataTypes.STRING()));
  }
}
