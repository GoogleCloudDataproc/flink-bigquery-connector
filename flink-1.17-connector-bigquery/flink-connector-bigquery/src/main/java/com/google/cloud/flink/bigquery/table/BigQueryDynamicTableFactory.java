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
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.function.SerializableSupplier;

import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.table.config.BigQueryConnectorOptions;
import com.google.cloud.flink.bigquery.table.config.BigQueryTableConfigurationProvider;

import java.util.HashSet;
import java.util.Set;

/**
 * Factory class to create configured instances of {@link BigQueryDynamicTableSource} and {@link
 * BigQueryDynamicTableSink}.
 */
@Internal
public class BigQueryDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "bigquery";

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
        additionalOptions.add(BigQueryConnectorOptions.TEST_MODE);
        additionalOptions.add(BigQueryConnectorOptions.MODE);
        additionalOptions.add(BigQueryConnectorOptions.DELIVERY_GUARANTEE);
        additionalOptions.add(BigQueryConnectorOptions.PARTITION_DISCOVERY_INTERVAL);
        additionalOptions.add(BigQueryConnectorOptions.SINK_PARALLELISM);

        return additionalOptions;
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        final Set<ConfigOption<?>> forwardOptions = new HashSet<>();

        forwardOptions.add(BigQueryConnectorOptions.PROJECT);
        forwardOptions.add(BigQueryConnectorOptions.DATASET);
        forwardOptions.add(BigQueryConnectorOptions.TABLE);
        forwardOptions.add(BigQueryConnectorOptions.LIMIT);
        forwardOptions.add(BigQueryConnectorOptions.MODE);
        forwardOptions.add(BigQueryConnectorOptions.ROW_RESTRICTION);
        forwardOptions.add(BigQueryConnectorOptions.COLUMNS_PROJECTION);
        forwardOptions.add(BigQueryConnectorOptions.MAX_STREAM_COUNT);
        forwardOptions.add(BigQueryConnectorOptions.SNAPSHOT_TIMESTAMP);
        forwardOptions.add(BigQueryConnectorOptions.CREDENTIALS_ACCESS_TOKEN);
        forwardOptions.add(BigQueryConnectorOptions.CREDENTIALS_FILE);
        forwardOptions.add(BigQueryConnectorOptions.CREDENTIALS_KEY);
        forwardOptions.add(BigQueryConnectorOptions.DELIVERY_GUARANTEE);
        forwardOptions.add(BigQueryConnectorOptions.PARTITION_DISCOVERY_INTERVAL);
        forwardOptions.add(BigQueryConnectorOptions.SINK_PARALLELISM);

        return forwardOptions;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        BigQueryTableConfigurationProvider configProvider =
                new BigQueryTableConfigurationProvider(helper.getOptions());
        helper.validate();

        if (configProvider.isTestModeEnabled()) {
            configProvider = configProvider.withTestingServices(testingServices);
        }

        // Create a Source depending on the boundedness.
        return new BigQueryDynamicTableSource(
                configProvider.toBigQueryReadOptions(),
                context.getPhysicalRowDataType(),
                configProvider.isUnboundedEnabled()
                        ? Boundedness.CONTINUOUS_UNBOUNDED
                        : Boundedness.BOUNDED);
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
                configProvider.toSinkConfig(),
                context.getPhysicalRowDataType().getLogicalType(),
                configProvider.getParallelism().orElse(null));
    }
}
