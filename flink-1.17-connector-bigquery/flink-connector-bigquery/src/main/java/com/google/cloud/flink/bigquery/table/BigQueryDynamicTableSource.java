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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import com.google.cloud.flink.bigquery.services.TablePartitionInfo;
import com.google.cloud.flink.bigquery.source.BigQuerySource;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.source.reader.deserializer.AvroToRowDataDeserializationSchema;
import com.google.cloud.flink.bigquery.table.restrictions.BigQueryRestriction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ResourceBundle;
import java.util.stream.Collectors;

/** A {@link DynamicTableSource} for Google BigQuery. */
@Internal
public class BigQueryDynamicTableSource
        implements ScanTableSource,
                SupportsProjectionPushDown,
                SupportsLimitPushDown,
                SupportsFilterPushDown {
    protected BigQueryReadOptions readOptions;
    private DataType producedDataType;

    private final RowType physicalDataType;
    private String[][] fullyQualifiedPaths;

    public BigQueryDynamicTableSource(BigQueryReadOptions readOptions, DataType physicalDataType) {
        this(
                ensureDefaultColumnNames(readOptions, physicalDataType),
                (RowType) physicalDataType.getLogicalType(),
                physicalDataType,
                defaultFlatPaths((RowType) physicalDataType.getLogicalType()));
    }

    private BigQueryDynamicTableSource(
            BigQueryReadOptions readOptions,
            RowType physicalDataType,
            DataType producedDataType,
            String[][] fullyQualifiedPaths) {
        this.readOptions = readOptions;
        this.physicalDataType = physicalDataType;
        this.producedDataType = producedDataType;
        this.fullyQualifiedPaths = fullyQualifiedPaths;
    }

    protected BigQueryDynamicTableSource(BigQueryDynamicTableSource source) {
        this(
                source.readOptions,
                source.physicalDataType,
                source.producedDataType,
                source.fullyQualifiedPaths);
    }

    private static BigQueryReadOptions ensureDefaultColumnNames(
            BigQueryReadOptions readOptions, DataType physicalDataType) {
        if (readOptions.getColumnNames().isEmpty()) {
            return readOptions
                    .toBuilder()
                    .setColumnNames(DataType.getFieldNames(physicalDataType))
                    .build();
        }
        return readOptions;
    }

    private static String[][] defaultFlatPaths(RowType rowType) {
        return rowType.getFieldNames().stream().map(f -> new String[] {f}).toArray(String[][]::new);
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

        AvroToRowDataDeserializationSchema deserializationSchema =
                new AvroToRowDataDeserializationSchema(rowType, typeInfo, fullyQualifiedPaths);

        BigQuerySource<RowData> bqSource =
                BigQuerySource.<RowData>builder()
                        .setReadOptions(readOptions)
                        .setSourceBoundedness(Boundedness.BOUNDED)
                        .setDeserializationSchema(deserializationSchema)
                        .build();

        return SourceProvider.of(bqSource);
    }

    @Override
    public DynamicTableSource copy() {
        return new BigQueryDynamicTableSource(
                readOptions, physicalDataType, producedDataType, fullyQualifiedPaths);
    }

    @Override
    public String asSummaryString() {
        ResourceBundle connectorResources = ResourceBundle.getBundle("connector");
        return connectorResources.getString("connector");
    }

    @Override
    public boolean supportsNestedProjection() {
        return true;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        this.producedDataType = producedDataType;
        // Single walk of the original DDL row tree. Produces the per-column Avro paths and
        // leaf types the deserializer will use directly — no path-walking in the deserializer.
        this.fullyQualifiedPaths = resolvePaths(projectedFields);
        this.readOptions =
                this.readOptions
                        .toBuilder()
                        .setColumnNames(toDottedPaths(this.fullyQualifiedPaths))
                        .build();
    }

    // Walk through projected fields getting fully qualified names for each column
    private String[][] resolvePaths(int[][] paths) {
        return Arrays.stream(paths)
                .map(indexPath -> toNamePath(indexPath, this.physicalDataType))
                .toArray(String[][]::new);
    }

    private static String[] toNamePath(final int[] indexPath, final RowType rowType) {
        final String[] namePath = new String[indexPath.length];
        LogicalType currentType = rowType;
        for (int i = 0; i < indexPath.length; i++) {
            final RowType row = (RowType) currentType;
            final RowType.RowField rowField = row.getFields().get(indexPath[i]);
            namePath[i] = rowField.getName();
            currentType = rowField.getType();
        }
        return namePath;
    }

    // Joins Avro field-name paths with "." to query BigQuery
    private static List<String> toDottedPaths(String[][] namePaths) {
        return Arrays.stream(namePaths)
                .map(path -> String.join(".", path))
                .collect(Collectors.toList());
    }

    @Override
    public void applyLimit(long limit) {
        this.readOptions = this.readOptions.toBuilder().setLimit((int) limit).build();
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
                // we now only add Calc filter node for filters not accepted by BQ
                translatedFilters.getOrDefault(false, new ArrayList<>()).stream()
                        .map(t -> t.f2)
                        .collect(Collectors.toList()));
    }

    @VisibleForTesting
    BigQueryReadOptions getReadOptions() {
        return this.readOptions;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                this.readOptions,
                this.producedDataType,
                this.physicalDataType,
                Arrays.deepHashCode(this.fullyQualifiedPaths));
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
                && Objects.equals(this.physicalDataType, other.physicalDataType)
                && Arrays.deepEquals(this.fullyQualifiedPaths, other.fullyQualifiedPaths);
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
}
