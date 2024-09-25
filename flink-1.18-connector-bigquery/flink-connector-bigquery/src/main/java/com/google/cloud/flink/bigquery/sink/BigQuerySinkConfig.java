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

import java.util.Objects;

/**
 * Configurations for a BigQuery Sink.
 *
 * <p>Uses static inner builder to initialize new instances.
 */
public class BigQuerySinkConfig {

    private final BigQueryConnectOptions connectOptions;
    private final DeliveryGuarantee deliveryGuarantee;
    private final BigQuerySchemaProvider schemaProvider;
    private final BigQueryProtoSerializer serializer;

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                this.connectOptions, this.deliveryGuarantee, this.schemaProvider, this.serializer);
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
        BigQuerySinkConfig object = (BigQuerySinkConfig) obj;
        if (this.getConnectOptions() == object.getConnectOptions()
                && (this.getSerializer() == object.getSerializer())
                && (this.getDeliveryGuarantee() == object.getDeliveryGuarantee())) {
            BigQuerySchemaProvider thisSchemaProvider = this.getSchemaProvider();
            BigQuerySchemaProvider objSchemaProvider = object.getSchemaProvider();
            return thisSchemaProvider.getAvroSchema().equals(objSchemaProvider.getAvroSchema());
        }
        return false;
    }

    private BigQuerySinkConfig(
            BigQueryConnectOptions connectOptions,
            DeliveryGuarantee deliveryGuarantee,
            BigQuerySchemaProvider schemaProvider,
            BigQueryProtoSerializer serializer) {
        this.connectOptions = connectOptions;
        this.deliveryGuarantee = deliveryGuarantee;
        this.schemaProvider = schemaProvider;
        this.serializer = serializer;
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

    /** Builder for BigQuerySinkConfig. */
    public static class Builder {

        private BigQueryConnectOptions connectOptions;
        private DeliveryGuarantee deliveryGuarantee;
        private BigQuerySchemaProvider schemaProvider;
        private BigQueryProtoSerializer serializer;

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
                    connectOptions, deliveryGuarantee, schemaProvider, serializer);
        }
    }
}
