/*
 * Copyright (C) 2024 Google Inc.
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

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.table.api.TableDescriptor;

/**
 * Configurations for a BigQuery Table API Write.
 *
 * <p>Inherits {@link BigQueryTableConfig} for general options and defines sink specific options.
 *
 * <p>Uses static inner builder to initialize new instances.
 */
public class BigQuerySinkTableConfig extends BigQueryTableConfig {

    private final DeliveryGuarantee deliveryGuarantee;
    private final Integer sinkParallelism;

    BigQuerySinkTableConfig(
            String project,
            String dataset,
            String table,
            String credentialAccessToken,
            String credentialFile,
            String credentialKey,
            boolean testMode,
            DeliveryGuarantee deliveryGuarantee,
            Integer sinkParallelism) {
        super(
                project,
                dataset,
                table,
                credentialAccessToken,
                credentialFile,
                credentialKey,
                testMode);
        this.deliveryGuarantee = deliveryGuarantee;
        this.sinkParallelism = sinkParallelism;
    }

    public static BigQuerySinkTableConfig.Builder newBuilder() {
        return new BigQuerySinkTableConfig.Builder();
    }

    /**
     * Method to update the table descriptor with {@link DeliveryGuarantee} for the sink.
     *
     * @param tableDescriptor The initial Table Descriptor
     * @return The updated {@link TableDescriptor}
     */
    @Override
    public TableDescriptor updateTableDescriptor(TableDescriptor tableDescriptor) {
        tableDescriptor = super.updateTableDescriptor(tableDescriptor);
        TableDescriptor.Builder tableDescriptorBuilder = tableDescriptor.toBuilder();
        if (this.deliveryGuarantee != null) {
            tableDescriptorBuilder.option(
                    BigQueryConnectorOptions.DELIVERY_GUARANTEE, this.deliveryGuarantee);
        }
        if (this.sinkParallelism != null) {
            tableDescriptorBuilder.option(
                    BigQueryConnectorOptions.SINK_PARALLELISM, sinkParallelism);
        }
        return tableDescriptorBuilder.build();
    }

    /** Builder for BigQuerySinkTableConfig. */
    public static class Builder extends BigQueryTableConfig.Builder {

        private DeliveryGuarantee deliveryGuarantee;
        private Integer sinkParallelism;

        @Override
        public BigQuerySinkTableConfig.Builder project(String project) {
            super.project = project;
            return this;
        }

        @Override
        public BigQuerySinkTableConfig.Builder dataset(String dataset) {
            super.dataset = dataset;
            return this;
        }

        @Override
        public BigQuerySinkTableConfig.Builder table(String table) {
            super.table = table;
            return this;
        }

        @Override
        public BigQuerySinkTableConfig.Builder credentialAccessToken(String credentialAccessToken) {
            super.credentialAccessToken = credentialAccessToken;
            return this;
        }

        @Override
        public BigQuerySinkTableConfig.Builder credentialKey(String credentialKey) {
            super.credentialKey = credentialKey;
            return this;
        }

        @Override
        public BigQuerySinkTableConfig.Builder credentialFile(String credentialFile) {
            super.credentialFile = credentialFile;
            return this;
        }

        @Override
        public BigQuerySinkTableConfig.Builder testMode(Boolean testMode) {
            super.testMode = testMode;
            return this;
        }

        /**
         * [OPTIONAL, Sink Configuration] Enum value indicating the delivery guarantee of the sink
         * job. Can be <code>DeliveryGuarantee.AT_LEAST_ONCE</code> or <code>
         * DeliveryGuarantee.EXACTLY_ONCE
         * </code><br>
         * Default: <code>DeliveryGuarantee.AT_LEAST_ONCE</code> - At-least Once Mode.
         */
        public BigQuerySinkTableConfig.Builder deliveryGuarantee(
                DeliveryGuarantee deliveryGuarantee) {
            this.deliveryGuarantee = deliveryGuarantee;
            return this;
        }

        /** [OPTIONAL, Sink Configuration] Int value indicating the parallelism of the sink job. */
        public BigQuerySinkTableConfig.Builder sinkParallelism(Integer sinkParallelism) {
            this.sinkParallelism = sinkParallelism;
            return this;
        }

        public BigQuerySinkTableConfig build() {
            return new BigQuerySinkTableConfig(
                    project,
                    dataset,
                    table,
                    credentialAccessToken,
                    credentialFile,
                    credentialKey,
                    testMode,
                    deliveryGuarantee,
                    sinkParallelism);
        }
    }
}
