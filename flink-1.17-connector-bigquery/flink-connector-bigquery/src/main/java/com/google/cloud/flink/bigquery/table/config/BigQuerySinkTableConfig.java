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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableDescriptor;

import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.flink.bigquery.sink.BigQuerySinkConfig;

import java.util.List;

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
    private final boolean enableTableCreation;
    private final String partitionField;
    private final TimePartitioning.Type partitionType;
    private final long partitionExpirationMillis;
    private final List<String> clusteredFields;
    private final String region;

    BigQuerySinkTableConfig(
            String project,
            String dataset,
            String table,
            String credentialAccessToken,
            String credentialFile,
            String credentialKey,
            boolean testMode,
            DeliveryGuarantee deliveryGuarantee,
            Integer sinkParallelism,
            boolean enableTableCreation,
            String partitionField,
            TimePartitioning.Type partitionType,
            long partitionExpirationMillis,
            List<String> clusteredFields,
            String region) {
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
        this.enableTableCreation = enableTableCreation;
        this.partitionField = partitionField;
        this.partitionType = partitionType;
        this.partitionExpirationMillis = partitionExpirationMillis;
        this.clusteredFields = clusteredFields;
        this.region = region;
    }

    public static Builder newBuilder() {
        return new Builder();
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
        private boolean enableTableCreation;
        private String partitionField;
        private TimePartitioning.Type partitionType;
        private long partitionExpirationMillis;
        private List<String> clusteredFields;
        private String region;
        private StreamExecutionEnvironment env;

        @Override
        public Builder project(String project) {
            super.project = project;
            return this;
        }

        @Override
        public Builder dataset(String dataset) {
            super.dataset = dataset;
            return this;
        }

        @Override
        public Builder table(String table) {
            super.table = table;
            return this;
        }

        @Override
        public Builder credentialAccessToken(String credentialAccessToken) {
            super.credentialAccessToken = credentialAccessToken;
            return this;
        }

        @Override
        public Builder credentialKey(String credentialKey) {
            super.credentialKey = credentialKey;
            return this;
        }

        @Override
        public Builder credentialFile(String credentialFile) {
            super.credentialFile = credentialFile;
            return this;
        }

        @Override
        public Builder testMode(Boolean testMode) {
            super.testMode = testMode;
            return this;
        }

        /**
         * [OPTIONAL, Sink Configuration] Enum value indicating the delivery guarantee of the sink.
         * Can be <code>DeliveryGuarantee.AT_LEAST_ONCE</code> or <code>
         * DeliveryGuarantee.EXACTLY_ONCE
         * </code><br>
         * Default: <code>DeliveryGuarantee.AT_LEAST_ONCE</code> - At-least Once Mode.
         *
         * @param deliveryGuarantee
         * @return Updated BigQuerySinkTableConfig builder
         */
        public Builder deliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
            this.deliveryGuarantee = deliveryGuarantee;
            return this;
        }

        /**
         * [OPTIONAL, Sink Configuration] Int value indicating the parallelism of the sink.
         *
         * @param sinkParallelism
         * @return Updated BigQuerySinkTableConfig builder
         */
        public Builder sinkParallelism(Integer sinkParallelism) {
            this.sinkParallelism = sinkParallelism;
            return this;
        }

        /**
         * [OPTIONAL, Sink Configuration] Boolean flag for automatic table creation.
         *
         * @param enableTableCreation
         * @return Updated BigQuerySinkTableConfig builder
         */
        public Builder enableTableCreation(boolean enableTableCreation) {
            this.enableTableCreation = enableTableCreation;
            return this;
        }

        /**
         * [OPTIONAL, Sink Configuration] Field for partitioning the BigQuery table. Considered if
         * table auto creation is enabled.
         *
         * @param partitionField
         * @return Updated BigQuerySinkTableConfig builder
         */
        public Builder partitionField(String partitionField) {
            this.partitionField = partitionField;
            return this;
        }

        /**
         * [OPTIONAL, Sink Configuration] Time frequency for partitioning the BigQuery table.
         * Considered if table auto creation is enabled.
         *
         * @param partitionType
         * @return Updated BigQuerySinkTableConfig builder
         */
        public Builder partitionType(TimePartitioning.Type partitionType) {
            this.partitionType = partitionType;
            return this;
        }

        /**
         * [OPTIONAL, Sink Configuration] Expiration duration of BigQuery table's partitions.
         * Considered if table auto creation is enabled.
         *
         * @param partitionExpirationMillis
         * @return Updated BigQuerySinkTableConfig builder
         */
        public Builder partitionExpirationMillis(long partitionExpirationMillis) {
            this.partitionExpirationMillis = partitionExpirationMillis;
            return this;
        }

        /**
         * [OPTIONAL, Sink Configuration] Fields for clustering the BigQuery table. Considered if
         * table auto creation is enabled.
         *
         * @param clusteredFields
         * @return Updated BigQuerySinkTableConfig builder
         */
        public Builder clusteredFields(List<String> clusteredFields) {
            this.clusteredFields = clusteredFields;
            return this;
        }

        /**
         * [OPTIONAL, Sink Configuration] GCP region for creating new BigQuery table. Considered if
         * table auto creation is enabled.
         *
         * @param region
         * @return Updated BigQuerySinkTableConfig builder
         */
        public Builder region(String region) {
            this.region = region;
            return this;
        }

        /**
         * [Required, Sink Configuration] StreamExecutionEnvironment associated with the Flink job.
         *
         * @param streamExecutionEnvironment
         * @return Updated BigQuerySinkTableConfig builder
         */
        public Builder streamExecutionEnvironment(
                StreamExecutionEnvironment streamExecutionEnvironment) {
            this.env = streamExecutionEnvironment;
            return this;
        }

        public BigQuerySinkTableConfig build() {
            if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
                BigQuerySinkConfig.validateStreamExecutionEnvironment(env);
            }
            return new BigQuerySinkTableConfig(
                    project,
                    dataset,
                    table,
                    credentialAccessToken,
                    credentialFile,
                    credentialKey,
                    testMode,
                    deliveryGuarantee,
                    sinkParallelism,
                    enableTableCreation,
                    partitionField,
                    partitionType,
                    partitionExpirationMillis,
                    clusteredFields,
                    region);
        }
    }
}
