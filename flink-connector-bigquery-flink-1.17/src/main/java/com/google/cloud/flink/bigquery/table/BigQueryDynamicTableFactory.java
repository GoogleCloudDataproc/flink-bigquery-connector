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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import com.google.cloud.flink.bigquery.common.utils.flink.annotations.Internal;
import com.google.cloud.flink.bigquery.common.utils.flink.core.SerializableSupplier;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.table.config.BigQueryConnectorOptions;
import com.google.cloud.flink.bigquery.table.config.BigQueryTableConfiguration;

import java.util.HashSet;
import java.util.Set;

/** Factory class to create configured instances of {@link BigQueryDynamicTableSource}. */
@Internal
public class BigQueryDynamicTableFactory implements DynamicTableSourceFactory {

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

        return forwardOptions;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        BigQueryTableConfiguration config = new BigQueryTableConfiguration(helper.getOptions());
        helper.validate();

        if (config.isTestModeEnabled()) {
            config = config.withTestingServices(testingServices);
        }

        return new BigQueryDynamicTableSource(
                config.toBigQueryReadOptions(), context.getPhysicalRowDataType());
    }

    static void setTestingServices(SerializableSupplier<BigQueryServices> testingServices) {
        BigQueryDynamicTableFactory.testingServices = testingServices;
    }
}
