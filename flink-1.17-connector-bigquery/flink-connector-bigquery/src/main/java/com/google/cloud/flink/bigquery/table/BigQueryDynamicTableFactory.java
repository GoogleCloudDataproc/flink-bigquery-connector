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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.SerializableSupplier;

import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.TablePartitionInfo;
import com.google.cloud.flink.bigquery.services.TablePartitionInfo.PartitionType;
import com.google.cloud.flink.bigquery.table.config.BigQueryConnectorOptions;
import com.google.cloud.flink.bigquery.table.config.BigQueryTableConfigurationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InterruptedIOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;

/**
 * Factory class to create configured instances of {@link BigQueryDynamicTableSource} and {@link
 * BigQueryDynamicTableSink}.
 */
@Internal
public class BigQueryDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "bigquery";

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryDynamicTableFactory.class);

    private static SerializableSupplier<BigQueryServices> testingServices = null;

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> requiredOptions = new HashSet<>();

        requiredOptions.add(BigQueryConnectorOptions.PROJECT);
        requiredOptions.add(BigQueryConnectorOptions.DATASET);
        requiredOptions.add(BigQueryConnectorOptions.TABLE);

        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> additionalOptions = new HashSet<>();

        additionalOptions.add(BigQueryConnectorOptions.LIMIT);
        additionalOptions.add(BigQueryConnectorOptions.ROW_RESTRICTION);
        additionalOptions.add(BigQueryConnectorOptions.COLUMNS_PROJECTION);
        additionalOptions.add(BigQueryConnectorOptions.MAX_STREAM_COUNT);
        additionalOptions.add(BigQueryConnectorOptions.SNAPSHOT_TIMESTAMP);
        additionalOptions.add(BigQueryConnectorOptions.CREDENTIALS_ACCESS_TOKEN);
        additionalOptions.add(BigQueryConnectorOptions.CREDENTIALS_FILE);
        additionalOptions.add(BigQueryConnectorOptions.CREDENTIALS_KEY);
        additionalOptions.add(BigQueryConnectorOptions.QUOTA_PROJECT_ID);
        additionalOptions.add(BigQueryConnectorOptions.TEST_MODE);
        additionalOptions.add(BigQueryConnectorOptions.DELIVERY_GUARANTEE);
        additionalOptions.add(BigQueryConnectorOptions.SINK_PARALLELISM);
        additionalOptions.add(BigQueryConnectorOptions.ENABLE_TABLE_CREATION);
        additionalOptions.add(BigQueryConnectorOptions.PARTITION_FIELD);
        additionalOptions.add(BigQueryConnectorOptions.PARTITION_TYPE);
        additionalOptions.add(BigQueryConnectorOptions.PARTITION_EXPIRATION_MILLIS);
        additionalOptions.add(BigQueryConnectorOptions.CLUSTERED_FIELDS);
        additionalOptions.add(BigQueryConnectorOptions.REGION);
        additionalOptions.add(BigQueryConnectorOptions.VIEWS_ENABLED);
        additionalOptions.add(BigQueryConnectorOptions.MATERIALIZATION_PROJECT);
        additionalOptions.add(BigQueryConnectorOptions.MATERIALIZATION_DATASET);
        additionalOptions.add(BigQueryConnectorOptions.MATERIALIZATION_EXPIRATION_HOURS);
        additionalOptions.add(BigQueryConnectorOptions.BILLING_PROJECT);

        return additionalOptions;
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        final Set<ConfigOption<?>> forwardOptions = new HashSet<>();

        forwardOptions.add(BigQueryConnectorOptions.PROJECT);
        forwardOptions.add(BigQueryConnectorOptions.DATASET);
        forwardOptions.add(BigQueryConnectorOptions.TABLE);
        forwardOptions.add(BigQueryConnectorOptions.LIMIT);
        forwardOptions.add(BigQueryConnectorOptions.ROW_RESTRICTION);
        forwardOptions.add(BigQueryConnectorOptions.COLUMNS_PROJECTION);
        forwardOptions.add(BigQueryConnectorOptions.MAX_STREAM_COUNT);
        forwardOptions.add(BigQueryConnectorOptions.SNAPSHOT_TIMESTAMP);
        forwardOptions.add(BigQueryConnectorOptions.CREDENTIALS_ACCESS_TOKEN);
        forwardOptions.add(BigQueryConnectorOptions.CREDENTIALS_FILE);
        forwardOptions.add(BigQueryConnectorOptions.CREDENTIALS_KEY);
        forwardOptions.add(BigQueryConnectorOptions.QUOTA_PROJECT_ID);
        forwardOptions.add(BigQueryConnectorOptions.DELIVERY_GUARANTEE);
        forwardOptions.add(BigQueryConnectorOptions.SINK_PARALLELISM);
        forwardOptions.add(BigQueryConnectorOptions.ENABLE_TABLE_CREATION);
        forwardOptions.add(BigQueryConnectorOptions.PARTITION_FIELD);
        forwardOptions.add(BigQueryConnectorOptions.PARTITION_TYPE);
        forwardOptions.add(BigQueryConnectorOptions.PARTITION_EXPIRATION_MILLIS);
        forwardOptions.add(BigQueryConnectorOptions.CLUSTERED_FIELDS);
        forwardOptions.add(BigQueryConnectorOptions.REGION);
        forwardOptions.add(BigQueryConnectorOptions.FATALIZE_SERIALIZER);
        forwardOptions.add(BigQueryConnectorOptions.VIEWS_ENABLED);
        forwardOptions.add(BigQueryConnectorOptions.MATERIALIZATION_PROJECT);
        forwardOptions.add(BigQueryConnectorOptions.MATERIALIZATION_DATASET);
        forwardOptions.add(BigQueryConnectorOptions.MATERIALIZATION_EXPIRATION_HOURS);
        forwardOptions.add(BigQueryConnectorOptions.BILLING_PROJECT);

        return forwardOptions;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        BigQueryTableConfigurationProvider configProvider =
                new BigQueryTableConfigurationProvider(helper.getOptions());
        helper.validate();

        Boolean viewsEnabled = helper.getOptions().get(BigQueryConnectorOptions.VIEWS_ENABLED);
        if (Boolean.TRUE.equals(viewsEnabled)) {
            String materializationDataset =
                    helper.getOptions().get(BigQueryConnectorOptions.MATERIALIZATION_DATASET);
            if (materializationDataset == null || materializationDataset.trim().isEmpty()) {
                throw new ValidationException(
                        String.format(
                                "Option '%s' must be set when '%s' is set to true.",
                                BigQueryConnectorOptions.MATERIALIZATION_DATASET.key(),
                                BigQueryConnectorOptions.VIEWS_ENABLED.key()));
            }
        }

        if (configProvider.isTestModeEnabled()) {
            configProvider = configProvider.withTestingServices(testingServices);
        }

        BigQueryDynamicTableSource source =
                new BigQueryDynamicTableSource(
                        configProvider.toBigQueryReadOptions(), context.getPhysicalRowDataType());
        // Flink removes partition predicates, so each listed value must cover exactly one DATE.
        RowType physicalType = (RowType) context.getPhysicalRowDataType().getLogicalType();
        List<String> partitionKeys = context.getCatalogTable().getPartitionKeys();
        Optional<TablePartitionInfo> partitionInfo;
        try {
            partitionInfo = source.retrievePartitionInfo();
        } catch (RuntimeException ex) {
            if (Thread.currentThread().isInterrupted()
                    || ExceptionUtils.findThrowable(ex, InterruptedException.class).isPresent()
                    || ExceptionUtils.findThrowable(ex, InterruptedIOException.class).isPresent()
                    || ExceptionUtils.findThrowable(ex, CancellationException.class).isPresent()) {
                throw ex;
            }
            BigQueryConnectOptions connectOptions =
                    source.getReadOptions().getBigQueryConnectOptions();
            LOG.warn(
                    "Cannot check partition push-down for table {}.{}.{}; using filter push-down instead.",
                    connectOptions.getProjectId(),
                    connectOptions.getDataset(),
                    connectOptions.getTable(),
                    ex);
            return source;
        }
        boolean exactPartitions =
                partitionInfo
                        .map(
                                info ->
                                        info.getColumnType() == StandardSQLTypeName.DATE
                                                && info.getPartitionType() == PartitionType.DAY
                                                && physicalType
                                                        .getFieldNames()
                                                        .contains(info.getColumnName())
                                                && physicalType
                                                                .getTypeAt(
                                                                        physicalType
                                                                                .getFieldNames()
                                                                                .indexOf(
                                                                                        info
                                                                                                .getColumnName()))
                                                                .getTypeRoot()
                                                        == LogicalTypeRoot.DATE
                                                && (partitionKeys.isEmpty()
                                                        || partitionKeys.equals(
                                                                Collections.singletonList(
                                                                        info.getColumnName()))))
                        .orElse(false);
        if (exactPartitions) {
            return new BigQueryPartitionedTableSource(
                    configProvider.toBigQueryReadOptions(), context.getPhysicalRowDataType());
        }
        return source;
    }

    static void setTestingServices(SerializableSupplier<BigQueryServices> testingServices) {
        BigQueryDynamicTableFactory.testingServices = testingServices;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        BigQueryTableConfigurationProvider configProvider =
                new BigQueryTableConfigurationProvider(helper.getOptions());
        helper.validate();

        if (configProvider.isTestModeEnabled()) {
            configProvider = configProvider.withTestingServices(testingServices);
        }

        return new BigQueryDynamicTableSink(
                configProvider.translateBigQueryConnectOptions(),
                configProvider.getDeliveryGuarantee(),
                context.getPhysicalRowDataType().getLogicalType(),
                configProvider.getParallelism().orElse(null),
                configProvider.enableTableCreation(),
                configProvider.getPartitionField().orElse(null),
                configProvider.getPartitionType().orElse(null),
                configProvider.getPartitionExpirationMillis().orElse(null),
                configProvider.getClusteredFields().orElse(null),
                configProvider.getRegion().orElse(null),
                configProvider.fatalizeSerializer());
    }
}
