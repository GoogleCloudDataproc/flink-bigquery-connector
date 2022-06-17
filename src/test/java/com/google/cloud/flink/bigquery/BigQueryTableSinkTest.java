/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.flink.bigquery;

import static com.google.common.truth.Truth.assertThat;

import java.util.Arrays;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class BigQueryTableSinkTest {
  static String bigqueryWriteTable;

  @BeforeClass
  public static void setup() {
    bigqueryWriteTable = "project.dataset.table";
  }

  @Test
  public void consumeDataStreamTest() throws JSQLParserException {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    DataStream<Row> orderA =
        env.fromCollection(
            Arrays.asList(
                Row.of(1, "John", "Test1"),
                Row.of(2, "Jacob", "Test2"),
                Row.of(3, "Sona", "Test3")));

    Table tableA = tEnv.fromDataStream(orderA, "id,name,details");
    BigQueryTableSink bigQueryTableSink = new BigQueryTableSink(tableA, bigqueryWriteTable);

    BigQueryTableSink mockBigQueryTableSink = Mockito.spy(bigQueryTableSink);
    Mockito.doNothing().when(mockBigQueryTableSink).createBigQueryTable();
    DataStreamSink<Row> sink = mockBigQueryTableSink.consumeDataStream(orderA);
    assertThat(sink).isNotNull();
    assertThat(mockBigQueryTableSink.getConsumedDataType().toString())
        .isEqualTo("ROW<`id` INT, `name` STRING, `details` STRING>");
    assertThat(mockBigQueryTableSink.getTableSchema().getFieldCount()).isEqualTo(3);
    assertThat(mockBigQueryTableSink.getTableSchema().getFieldNames()[0]).isEqualTo("id");
    assertThat(mockBigQueryTableSink.getTableSchema().getFieldNames()[1]).isEqualTo("name");
    assertThat(mockBigQueryTableSink.getTableSchema().getFieldNames()[2]).isEqualTo("details");
  }
}
