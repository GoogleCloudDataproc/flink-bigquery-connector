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

package com.google.cloud.flink.bigquery.sink;

import org.apache.flink.connector.base.DeliveryGuarantee;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProvider;

/**
 * Configurations for a BigQuery Sink.
 *
 * <p>Uses static inner builder to initialize new instances.
 */
public class BigQuerySinkConfig {

    private int parallelism;
    private final BigQueryConnectOptions connectOptions;
    private final DeliveryGuarantee deliveryGuarantee;
    private final BigQuerySchemaProvider schemaProvider;
    private final BigQueryProtoSerializer serializer;

    public static Builder newBuilder() {
        return new Builder();
    }

    private BigQuerySinkConfig(
            int parallelism,
            BigQueryConnectOptions connectOptions,
            DeliveryGuarantee deliveryGuarantee,
            BigQuerySchemaProvider schemaProvider,
            BigQueryProtoSerializer serializer) {
        this.parallelism = parallelism;
        this.connectOptions = connectOptions;
        this.deliveryGuarantee = deliveryGuarantee;
        this.schemaProvider = schemaProvider;
        this.serializer = serializer;
    }

    public int getParallelism() {
        return parallelism;
    }

    public BigQueryConnectOptions getConnectOptions() {
        return connectOptions;
    }

    public DeliveryGuarantee getDeliveryGuarantee() {
        return deliveryGuarantee;
    }

    public BigQueryProtoSerializer getSerializer() {
        return serializer;
    }

    public BigQuerySchemaProvider getSchemaProvider() {
        return schemaProvider;
    }

    protected void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    /** Builder for BigQuerySinkConfig. */
    public static class Builder {

        private int parallelism;
        private BigQueryConnectOptions connectOptions;
        private DeliveryGuarantee deliveryGuarantee;
        private BigQuerySchemaProvider schemaProvider;
        private BigQueryProtoSerializer serializer;

        public Builder parallelism(int parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public Builder connectOptions(BigQueryConnectOptions connectOptions) {
            this.connectOptions = connectOptions;
            return this;
        }

        public Builder deliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
            this.deliveryGuarantee = deliveryGuarantee;
            return this;
        }

        public Builder schemaProvider(BigQuerySchemaProvider schemaProvider) {
            this.schemaProvider = schemaProvider;
            return this;
        }

        public Builder serializer(BigQueryProtoSerializer serializer) {
            this.serializer = serializer;
            return this;
        }

        public BigQuerySinkConfig build() {
            return new BigQuerySinkConfig(
                    parallelism, connectOptions, deliveryGuarantee, schemaProvider, serializer);
        }
    }
}
