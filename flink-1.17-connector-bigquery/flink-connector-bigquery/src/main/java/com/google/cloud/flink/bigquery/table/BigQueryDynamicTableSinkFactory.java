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
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.function.SerializableSupplier;

import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.table.config.BigQueryConnectorOptions;
import com.google.cloud.flink.bigquery.table.config.BigQueryTableConfiguration;

import java.util.HashSet;
import java.util.Set;

/** Factory class to create configured instances of {@link BigQueryDynamicTableSink}. */
@Internal
public class BigQueryDynamicTableSinkFactory implements DynamicTableSinkFactory {

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
        forwardOptions.add(BigQueryConnectorOptions.DELIVERY_GUARANTEE);
        forwardOptions.add(BigQueryConnectorOptions.CREDENTIALS_ACCESS_TOKEN);
        forwardOptions.add(BigQueryConnectorOptions.CREDENTIALS_FILE);
        forwardOptions.add(BigQueryConnectorOptions.CREDENTIALS_KEY);
        return forwardOptions;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        BigQueryTableConfiguration config = new BigQueryTableConfiguration(helper.getOptions());
        helper.validate();

        if (config.isTestModeEnabled()) {
            config = config.withTestingServices(testingServices);
        }

        return new BigQueryDynamicTableSink(
                config.toSinkConfig(), context.getPhysicalRowDataType());
    }

    public static void setTestingServices(SerializableSupplier<BigQueryServices> testingServices) {
        BigQueryDynamicTableSinkFactory.testingServices = testingServices;
    }
}
