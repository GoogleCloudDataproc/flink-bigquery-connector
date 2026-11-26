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
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.types.DataType;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.utils.BigQueryPartitionUtils;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import com.google.cloud.flink.bigquery.services.TablePartitionInfo;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** A BigQuery source for tables with one DATE value per daily partition. */
@Internal
public class BigQueryPartitionedTableSource extends BigQueryDynamicTableSource
        implements SupportsPartitionPushDown {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryPartitionedTableSource.class);

    public BigQueryPartitionedTableSource(
            BigQueryReadOptions readOptions, DataType physicalDataType) {
        super(readOptions, physicalDataType);
    }

    private BigQueryPartitionedTableSource(BigQueryPartitionedTableSource source) {
        super(source);
    }

    @Override
    public DynamicTableSource copy() {
        return new BigQueryPartitionedTableSource(this);
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
        return BigQueryPartitionUtils.partitionValuesFromIdAndDataType(
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
         * given the specification, partition restriction comes before the filter application, so we
         * just set the row restriction here.
         */
        String partitionRestrictions =
                remainingPartitions.stream()
                        .flatMap(map -> map.entrySet().stream())
                        .map(
                                entry ->
                                        BigQueryPartitionUtils
                                                .formatPartitionRestrictionBasedOnInfo(
                                                        partitionInfo,
                                                        entry.getKey(),
                                                        entry.getValue()))
                        .collect(Collectors.joining(" OR "));

        if (partitionRestrictions.isEmpty()) {
            return currentRestriction == null ? "" : currentRestriction;
        }
        if (currentRestriction == null || currentRestriction.isEmpty()) {
            return "(" + partitionRestrictions + ")";
        }
        return currentRestriction + " AND (" + partitionRestrictions + ")";
    }
}
