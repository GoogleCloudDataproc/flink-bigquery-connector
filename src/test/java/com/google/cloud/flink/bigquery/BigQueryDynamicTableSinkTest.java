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
package com.google.cloud.flink.bigquery;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;

import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.flink.bigquery.common.WriterCommitMessageContext;
import com.google.cloud.flink.bigquery.util.FlinkBigQueryConfig;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.DynamicTableSink.SinkRuntimeProvider;
import org.apache.flink.table.types.DataType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class BigQueryDynamicTableSinkTest {
  List<String> fieldNames = new ArrayList<String>();
  List<DataType> fieldTypes = new ArrayList<DataType>();
  FlinkBigQueryConfig bqConfig;
  BigQueryClientFactory bigQueryWriteClientFactory;
  List<String> partitionKeys = new ArrayList<String>();
  ListAccumulator<WriterCommitMessageContext> accumulator =
      new ListAccumulator<WriterCommitMessageContext>();
  BigQueryDynamicTableSink bigQueryDynamicTableSink;

  @Before
  public void setup() {
    fieldNames.add("id");
    fieldNames.add("name");
    fieldTypes.add(DataTypes.BIGINT());
    fieldTypes.add(DataTypes.STRING());
    bqConfig = Mockito.mock(FlinkBigQueryConfig.class);
    bigQueryWriteClientFactory = Mockito.mock(BigQueryClientFactory.class);

    bigQueryDynamicTableSink =
        new BigQueryDynamicTableSink(
            fieldNames,
            fieldTypes,
            bqConfig,
            bigQueryWriteClientFactory,
            partitionKeys,
            accumulator);
  }

  @Test
  public void getChangelogModeTest() {
    ChangelogMode requestedMode = ChangelogMode.insertOnly();
    ChangelogMode result = bigQueryDynamicTableSink.getChangelogMode(requestedMode);
    assertThat(result).isNotNull();
    assertThat(result.toString()).contains("INSERT");
    assertThat(result.toString()).contains("UPDATE_AFTER");
    assertThat(result.toString()).contains("DELETE");
  }

  @Test
  public void getSinkRuntimeProviderTest() {
    DynamicTableSink.Context context = mock(DynamicTableSink.Context.class);
    SinkRuntimeProvider result = bigQueryDynamicTableSink.getSinkRuntimeProvider(context);
    assertThat(result).isNotNull();
  }

  @Test
  public void asSummaryStringTest() {
    String result = bigQueryDynamicTableSink.asSummaryString();
    assertThat(result).isNotNull();
    assertThat(result).isEqualTo("BigQuery Sink");
  }
}
