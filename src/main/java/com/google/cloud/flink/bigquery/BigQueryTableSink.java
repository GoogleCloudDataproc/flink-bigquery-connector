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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;

public class BigQueryTableSink implements AppendStreamTableSink<Row> {
  private Table srcTable;
  private String table;
  private String[] fieldNames;
  private DataType[] fieldTypes;

  public BigQueryTableSink(Table srcTable, String table) {
    this.srcTable = srcTable;
    this.table = table;
    this.fieldNames = srcTable.getSchema().getFieldNames();
    this.fieldTypes = srcTable.getSchema().getFieldDataTypes();
  }

  @Override
  public DataStreamSink<Row> consumeDataStream(DataStream<Row> dataStream) {
    BigQuerySinkFunction bigQuerySinkFunction = new BigQuerySinkFunction(srcTable, table);
    DataStreamSink<Row> sink = dataStream.addSink(bigQuerySinkFunction);
    sink.name(TableConnectorUtils.generateRuntimeName(BigQueryTableSink.class, fieldNames));
    return sink;
  }

  @Override
  public TableSink<Row> configure(String[] fieldNames, TypeInformation[] fieldTypes) {
    return null;
  }

  @Override
  public DataType getConsumedDataType() {
    return getTableSchema().toRowDataType();
  }

  @Override
  public TableSchema getTableSchema() {
    return TableSchema.builder().fields(fieldNames, fieldTypes).build();
  }
}
