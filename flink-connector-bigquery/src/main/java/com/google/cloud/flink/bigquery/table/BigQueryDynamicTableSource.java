/*
 * Copyright (C) 2023 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.flink.bigquery.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.utils.BigQueryPartition;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import com.google.cloud.flink.bigquery.services.TablePartitionInfo;
import com.google.cloud.flink.bigquery.source.BigQuerySource;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.source.reader.deserializer.AvroToRowDataDeserializationSchema;
import com.google.cloud.flink.bigquery.table.restrictions.BigQueryRestriction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/** A {@link DynamicTableSource} for Google BigQuery. */
@Internal
public class BigQueryDynamicTableSource
        implements ScanTableSource,
                SupportsProjectionPushDown,
                SupportsLimitPushDown,
                SupportsFilterPushDown,
                SupportsPartitionPushDown {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryDynamicTableSource.class);

    private BigQueryReadOptions readOptions;
    private DataType producedDataType;
    private Integer limit = -1;

    public BigQueryDynamicTableSource(BigQueryReadOptions readOptions, DataType producedDataType) {
        this.readOptions = readOptions;
        this.producedDataType = producedDataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final RowType rowType = (RowType) producedDataType.getLogicalType();
        final TypeInformation<RowData> typeInfo =
                runtimeProviderContext.createTypeInformation(producedDataType);

        BigQuerySource<RowData> bqSource =
                BigQuerySource.<RowData>builder()
                        .setLimit(limit)
                        .setReadOptions(readOptions)
                        .setDeserializationSchema(
                                new AvroToRowDataDeserializationSchema(rowType, typeInfo))
                        .build();

        return SourceProvider.of(bqSource);
    }

    @Override
    public DynamicTableSource copy() {
        return new BigQueryDynamicTableSource(readOptions, producedDataType);
    }

    @Override
    public String asSummaryString() {
        return "BigQuery";
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        this.producedDataType = producedDataType;
        this.readOptions =
                this.readOptions
                        .toBuilder()
                        .setColumnNames(DataType.getFieldNames(producedDataType))
                        .build();
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = (int) limit;
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        Map<Boolean, List<Tuple3<Boolean, String, ResolvedExpression>>> translatedFilters =
                filters.stream()
                        .map(
                                expression ->
                                        Tuple2.<ResolvedExpression, Optional<String>>of(
                                                expression,
                                                BigQueryRestriction.convert(expression)))
                        .map(
                                transExp ->
                                        Tuple3.<Boolean, String, ResolvedExpression>of(
                                                transExp.f1.isPresent(),
                                                transExp.f1.orElse(""),
                                                transExp.f0))
                        .collect(
                                Collectors.groupingBy(
                                        (Tuple3<Boolean, String, ResolvedExpression> t) -> t.f0));
        String rowRestrictionByFilters =
                translatedFilters.getOrDefault(true, new ArrayList<>()).stream()
                        .map(t -> t.f1)
                        .collect(Collectors.joining(" AND "));
        String newRowRestriction = this.readOptions.getRowRestriction();
        if (!rowRestrictionByFilters.isEmpty()) {
            if (newRowRestriction.isEmpty()) {
                newRowRestriction = rowRestrictionByFilters;
            } else {
                newRowRestriction = newRowRestriction + " AND " + rowRestrictionByFilters;
            }
        }
        this.readOptions =
                this.readOptions.toBuilder().setRowRestriction(newRowRestriction).build();
        return Result.of(
                translatedFilters.getOrDefault(true, new ArrayList<>()).stream()
                        .map(t -> t.f2)
                        .collect(Collectors.toList()),
                filters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.readOptions, this.producedDataType, this.limit);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final BigQueryDynamicTableSource other = (BigQueryDynamicTableSource) obj;
        return Objects.equals(this.readOptions, other.readOptions)
                && Objects.equals(this.producedDataType, other.producedDataType)
                && Objects.equals(this.limit, other.limit);
    }

    Optional<TablePartitionInfo> retrievePartitionInfo() {
        BigQueryConnectOptions connectOptions = this.readOptions.getBigQueryConnectOptions();
        BigQueryServices.QueryDataClient dataClient =
                BigQueryServicesFactory.instance(connectOptions).queryClient();
        // store partition colum for needed later value - type translations
        return dataClient
                // get the column name that is a partition, maybe none.
                .retrievePartitionColumnInfo(
                connectOptions.getProjectId(),
                connectOptions.getDataset(),
                connectOptions.getTable());
    }

    @Override
    public Optional<List<Map<String, String>>> listPartitions() {
        BigQueryConnectOptions connectOptions = readOptions.getBigQueryConnectOptions();
        BigQueryServices.QueryDataClient dataClient =
                BigQueryServicesFactory.instance(connectOptions).queryClient();

        Optional<List<Map<String, String>>> ret =
                retrievePartitionInfo()
                        .map(
                                partitionInfo ->
                                        transformPartitionIds(
                                                connectOptions.getProjectId(),
                                                connectOptions.getDataset(),
                                                connectOptions.getTable(),
                                                partitionInfo,
                                                dataClient));

        LOG.info("Partitions with data on the BigQuery table {},", ret.toString());
        return ret;
    }

    @Override
    public void applyPartitions(List<Map<String, String>> remainingPartitions) {
        Optional<TablePartitionInfo> partitionInfo = retrievePartitionInfo();
        this.readOptions =
                this.readOptions
                        .toBuilder()
                        .setRowRestriction(
                                rebuildRestrictionsApplyingPartitions(
                                        this.readOptions.getRowRestriction(),
                                        partitionInfo,
                                        remainingPartitions))
                        .build();
        LOG.info("Partitions to be used {}.", remainingPartitions.toString());
    }

    private static List<Map<String, String>> transformPartitionIds(
            String projectId,
            String dataset,
            String table,
            TablePartitionInfo partitionInfo,
            BigQueryServices.QueryDataClient dataClient) {

        /**
         * we retrieve the existing partition ids and transform them into valid values given the
         * column data type
         */
        return BigQueryPartition.partitionValuesFromIdAndDataType(
                        dataClient.retrieveTablePartitions(projectId, dataset, table),
                        partitionInfo.getColumnType())
                .stream()
                /**
                 * for each of those valid partition values we create an map with the column name
                 * and the value
                 */
                .map(
                        pValue -> {
                            Map<String, String> partitionColAndValue = new HashMap<>();
                            partitionColAndValue.put(partitionInfo.getColumnName(), pValue);
                            return partitionColAndValue;
                        })
                .collect(Collectors.toList());
    }

    private static String rebuildRestrictionsApplyingPartitions(
            String currentRestriction,
            Optional<TablePartitionInfo> partitionInfo,
            List<Map<String, String>> remainingPartitions) {

        /**
         * given the specification, the partition restriction comes before than the filter
         * application, so we just set here the row restriction.
         */
        return currentRestriction
                + " AND "
                + remainingPartitions.stream()
                        .flatMap(map -> map.entrySet().stream())
                        .map(
                                entry ->
                                        BigQueryPartition.formatPartitionRestrictionBasedOnInfo(
                                                partitionInfo, entry.getKey(), entry.getValue()))
                        .collect(Collectors.joining(" OR "));
    }
}
