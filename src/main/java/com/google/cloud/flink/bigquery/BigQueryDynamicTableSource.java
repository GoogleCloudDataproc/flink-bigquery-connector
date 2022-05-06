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

import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import java.util.ArrayList;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public final class BigQueryDynamicTableSource
    implements ScanTableSource, SupportsProjectionPushDown {

  private DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
  private DataType producedDataType;
  private ArrayList<String> readSessionStreamList;
  private BigQueryClientFactory bigQueryReadClientFactory;

  public BigQueryDynamicTableSource(
      DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
      DataType producedDataType,
      ArrayList<String> readSessionStreamList,
      BigQueryClientFactory bigQueryReadClientFactory) {

    this.bigQueryReadClientFactory = bigQueryReadClientFactory;
    this.decodingFormat = decodingFormat;
    this.producedDataType = producedDataType;
    this.readSessionStreamList = readSessionStreamList;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return decodingFormat.getChangelogMode();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
    // create runtime classes that are shipped to the cluster
    final DeserializationSchema<RowData> deserializer =
        decodingFormat.createRuntimeDecoder(runtimeProviderContext, producedDataType);
    final SourceFunction<RowData> sourceFunction =
        new BigQuerySourceFunction(deserializer, readSessionStreamList, bigQueryReadClientFactory);
    return SourceFunctionProvider.of(sourceFunction, false);
  }

  @Override
  public DynamicTableSource copy() {
    return new BigQueryDynamicTableSource(
        decodingFormat, producedDataType, readSessionStreamList, bigQueryReadClientFactory);
  }

  @Override
  public String asSummaryString() {
    return "BigQuery Table Source";
  }

  @Override
  public boolean supportsNestedProjection() {
    return false;
  }

  @Override
  public void applyProjection(int[][] projectedFields) {}
}
