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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.flink.bigquery.exception.FlinkBigQueryException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

public class FlinkReadFromQueryIntegrationTest extends FlinkBigQueryIntegrationTestBase {

  StreamTableEnvironment flinkTableEnv;

  public FlinkReadFromQueryIntegrationTest() {

    BigQueryOptions.getDefaultInstance().getService();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1); // source only supports parallelism of 1
    flinkTableEnv = StreamTableEnvironment.create(env);
  }

  @Test
  public void testReadFromQuery() {
    String sql =
        "SELECT tag, COUNT(*) countVal FROM ( SELECT SPLIT(tags, \"|\") tags FROM `q-gcp-6750-pso-gs-flink-22-01.testDataset.posts_questions` a WHERE EXTRACT(YEAR FROM creation_date)>=2014 ), UNNEST(tags) tag GROUP BY 1 ORDER BY 2 DESC LIMIT 10 ";
    String flinkSrcTable = "FlinkSrcTable";
    String srcQueryString = "CREATE TABLE " + flinkSrcTable + " (tag STRING,tag_count BIGINT)";
    flinkTableEnv.executeSql(
        srcQueryString
            + "\n"
            + "WITH (\n"
            + "  'connector' = 'bigquery',\n"
            + "  'format' = 'arrow',\n"
            + "  'query' = '"
            + sql
            + "',\n"
            + "  'maxParallelism' = '10',\n"
            + "  'materializationProject' = 'q-gcp-6750-pso-gs-flink-22-01',\n"
            + "  'materializationDataset' = 'testDataset',\n"
            + "  'credentialsFile' = '"
            + System.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            + "'\n"
            + ")");
    final Table sourceTable = flinkTableEnv.from(flinkSrcTable);
    TableResult datatable = sourceTable.execute();
    assertNotNull(datatable);
    assertEquals(2, datatable.getTableSchema().getFieldCount());
  }

  @Test
  public void testBadSql() {
    String flinkSrcTable = "FlinkSrcTable";
    String sql =
        "SELECT tagging, COUNT(*) countVal FROM ( SELECT SPLIT(tags, \"|\") tagging FROM `q-gcp-6750-pso-gs-flink-22-01.testDataset.posts_questions` a WHERE EXTRACT(YEAR FROM creation_date)>=2014 ), UNNEST(tags) tag GROUP BY 1 ORDER BY 2 DESC LIMIT 10 ";
    assertThrows(
        RuntimeException.class,
        () -> {
          String srcQueryString =
              "CREATE TABLE " + flinkSrcTable + " (tag STRING,tag_count BIGINT)";
          flinkTableEnv.executeSql(
              srcQueryString
                  + "\n"
                  + "WITH (\n"
                  + "  'connector' = 'bigquery',\n"
                  + "  'format' = 'arrow',\n"
                  + "  'query' = '"
                  + sql
                  + "',\n"
                  + "  'maxParallelism' = '10',\n"
                  + "  'materializationProject' = 'q-gcp-6750-pso-gs-flink-22-01',\n"
                  + "  'materializationDataset' = 'testDataset',\n"
                  + "  'credentialsFile' = '"
                  + System.getenv("GOOGLE_APPLICATION_CREDENTIALS")
                  + "'\n"
                  + ")");
          Table result = flinkTableEnv.from(flinkSrcTable);
          result.execute();
          throw new FlinkBigQueryException("Column not found");
        });
  }

  @Test
  public void testReadFromQueryWithNewLine() {
    String sql =
        "SELECT tag, COUNT(*) countVal \n"
            + "FROM ( SELECT SPLIT(tags, \"|\") tags FROM `q-gcp-6750-pso-gs-flink-22-01.testDataset.posts_questions` a \n"
            + "WHERE EXTRACT(YEAR FROM creation_date)>=2014 ), UNNEST(tags) tag GROUP BY 1 ORDER BY 2 DESC LIMIT 10 ";

    String flinkSrcTable = "FlinkSrcTable";
    String srcQueryString = "CREATE TABLE " + flinkSrcTable + " (tag STRING,tag_count BIGINT)";
    flinkTableEnv.executeSql(
        srcQueryString
            + "\n"
            + "WITH (\n"
            + "  'connector' = 'bigquery',\n"
            + "  'format' = 'arrow',\n"
            + "  'query' = '"
            + sql
            + "',\n"
            + "  'maxParallelism' = '10',\n"
            + "  'materializationProject' = 'q-gcp-6750-pso-gs-flink-22-01',\n"
            + "  'materializationDataset' = 'testDataset',\n"
            + "  'credentialsFile' = '"
            + System.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            + "'\n"
            + ")");
    final Table sourceTable = flinkTableEnv.from(flinkSrcTable);
    TableResult datatable = sourceTable.execute();
    assertNotNull(datatable);
    assertEquals(2, datatable.getTableSchema().getFieldCount());
  }
}
