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

package com.google.cloud.flink.bigquery.table.config;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.util.function.SerializableSupplier;

import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * A BigQuery Configuration class which can be used to transform to the option objects the source
 * implementation expects.
 */
@Internal
public class BigQueryTableConfigurationProvider {
    private final ReadableConfig config;
    private Optional<SerializableSupplier<BigQueryServices>> testingServices = Optional.empty();

    public BigQueryTableConfigurationProvider(ReadableConfig config) {
        this.config = config;
    }

    public BigQueryTableConfigurationProvider withTestingServices(
            SerializableSupplier<BigQueryServices> testingServices) {
        this.testingServices = Optional.of(testingServices);
        return this;
    }

    public boolean isTestModeEnabled() {
        return config.get(BigQueryConnectorOptions.TEST_MODE);
    }

    public DeliveryGuarantee getDeliveryGuarantee() {
        return config.get(BigQueryConnectorOptions.DELIVERY_GUARANTEE);
    }

    public Optional<Integer> getParallelism() {
        return Optional.ofNullable(config.get(BigQueryConnectorOptions.SINK_PARALLELISM));
    }

    public boolean enableTableCreation() {
        return config.get(BigQueryConnectorOptions.ENABLE_TABLE_CREATION);
    }

    public Optional<String> getPartitionField() {
        return Optional.ofNullable(config.get(BigQueryConnectorOptions.PARTITION_FIELD));
    }

    public Optional<TimePartitioning.Type> getPartitionType() {
        return Optional.ofNullable(config.get(BigQueryConnectorOptions.PARTITION_TYPE));
    }

    public Optional<Long> getPartitionExpirationMillis() {
        return Optional.ofNullable(
                config.get(BigQueryConnectorOptions.PARTITION_EXPIRATION_MILLIS));
    }

    public Optional<List<String>> getClusteredFields() {
        String clusteredFields = config.get(BigQueryConnectorOptions.CLUSTERED_FIELDS);
        if (clusteredFields == null) {
            return Optional.empty();
        }
        return Optional.of(Arrays.asList(clusteredFields.split(",")));
    }

    public Optional<String> getRegion() {
        return Optional.ofNullable(config.get(BigQueryConnectorOptions.REGION));
    }

    public BigQueryReadOptions toBigQueryReadOptions() {
        return BigQueryReadOptions.builder()
                .setSnapshotTimestampInMillis(
                        config.get(BigQueryConnectorOptions.SNAPSHOT_TIMESTAMP))
                .setMaxStreamCount(config.get(BigQueryConnectorOptions.MAX_STREAM_COUNT))
                .setRowRestriction(config.get(BigQueryConnectorOptions.ROW_RESTRICTION))
                .setColumnNames(
                        Optional.ofNullable(config.get(BigQueryConnectorOptions.COLUMNS_PROJECTION))
                                .map(cols -> Arrays.asList(cols.split(",")))
                                .orElse(new ArrayList<>()))
                .setBigQueryConnectOptions(translateBigQueryConnectOptions())
                .setLimit(config.get(BigQueryConnectorOptions.LIMIT))
                .build();
    }

    public BigQueryConnectOptions translateBigQueryConnectOptions() {
        return BigQueryConnectOptions.builder()
                .setProjectId(config.get(BigQueryConnectorOptions.PROJECT))
                .setDataset(config.get(BigQueryConnectorOptions.DATASET))
                .setTable(config.get(BigQueryConnectorOptions.TABLE))
                .setTestingBigQueryServices(testingServices.orElse(null))
                .setCredentialsOptions(
                        CredentialsOptions.builder()
                                .setAccessToken(
                                        config.get(
                                                BigQueryConnectorOptions.CREDENTIALS_ACCESS_TOKEN))
                                .setCredentialsFile(
                                        config.get(BigQueryConnectorOptions.CREDENTIALS_FILE))
                                .setCredentialsKey(
                                        config.get(BigQueryConnectorOptions.CREDENTIALS_KEY))
                                .build())
                .build();
    }
}
