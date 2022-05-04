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

import com.google.cloud.bigquery.BigQueryOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;

public class FlinkBigQueryIntegrationTestBase {

  @ClassRule public static TestDataset testDataset = new TestDataset();

  protected String testTable;
  public static StreamTableEnvironment flinkTableEnv;

  @Before
  public void createTestTable() {
    testTable = "test_" + System.nanoTime();
  }

  public FlinkBigQueryIntegrationTestBase() {

    BigQueryOptions.getDefaultInstance().getService();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    FlinkBigQueryIntegrationTestBase.flinkTableEnv = StreamTableEnvironment.create(env);
  }

  protected static class TestDataset extends ExternalResource {

    String testDataset =
        String.format("flink_bigquery_%d_%d", System.currentTimeMillis(), System.nanoTime());

    @Override
    protected void before() throws Throwable {
      IntegrationTestUtils.createDataset(testDataset);
      IntegrationTestUtils.createTable(testDataset, Constants.ALL_TYPES_TABLE_NAME, "read");
    }

    @Override
    protected void after() {
      IntegrationTestUtils.deleteDatasetAndTables(testDataset);
    }

    @Override
    public String toString() {
      return testDataset;
    }
  }
}
