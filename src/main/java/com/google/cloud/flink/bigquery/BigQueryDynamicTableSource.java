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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;

public final class BigQueryDynamicTableSource
    implements ScanTableSource,
        SupportsProjectionPushDown,
        SupportsLimitPushDown,
        SupportsPartitionPushDown,
        SupportsFilterPushDown {

  private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
  private DataType producedDataType;
  private ArrayList<String> readStreamNames;
  private BigQueryClientFactory bigQueryReadClientFactory;
  private CatalogTable catalogTable;
  private int[][] projectedFields;
  private long limit;
  private List<Map<String, String>> remainingPartitions;
  private ArrayList filters;

  public BigQueryDynamicTableSource(
      DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
      DataType producedDataType,
      ArrayList<String> readStreamNames,
      BigQueryClientFactory bigQueryReadClientFactory,
      CatalogTable catalogTable) {

    this.bigQueryReadClientFactory = bigQueryReadClientFactory;
    this.decodingFormat = decodingFormat;
    this.producedDataType = producedDataType;
    this.readStreamNames = readStreamNames;
    this.catalogTable = catalogTable;
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
        new BigQuerySourceFunction(deserializer, readStreamNames, bigQueryReadClientFactory);
    return SourceFunctionProvider.of(sourceFunction, false);
  }

  @Override
  public DynamicTableSource copy() {
    BigQueryDynamicTableSource source =
        new BigQueryDynamicTableSource(
            decodingFormat,
            producedDataType,
            readStreamNames,
            bigQueryReadClientFactory,
            catalogTable);
    source.projectedFields = projectedFields;
    source.remainingPartitions = remainingPartitions;
    source.filters = filters;
    source.limit = limit;
    return source;
  }

  @Override
  public String asSummaryString() {
    return "BigQuery Table Source";
  }

  @Override
  public Result applyFilters(List<ResolvedExpression> filters) {
    this.filters = new ArrayList<>(filters);
    return Result.of(new ArrayList<>(filters), new ArrayList<>(filters));
  }

  @Override
  public Optional<List<Map<String, String>>> listPartitions() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void applyPartitions(List<Map<String, String>> remainingPartitions) {
    if (catalogTable.getPartitionKeys() != null && catalogTable.getPartitionKeys().size() != 0) {
      this.remainingPartitions = remainingPartitions;
    } else {
      throw new UnsupportedOperationException(
          "Should not apply partitions to a non-partitioned table.");
    }
  }

  @Override
  public void applyLimit(long limit) {
    this.limit = limit;
  }

  @Override
  public boolean supportsNestedProjection() {
    return false;
  }

  @Override
  public void applyProjection(int[][] projectedFields) {
    this.projectedFields = projectedFields;
  }
}
