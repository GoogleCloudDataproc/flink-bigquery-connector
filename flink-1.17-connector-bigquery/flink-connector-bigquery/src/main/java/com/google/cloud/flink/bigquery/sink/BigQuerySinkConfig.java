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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.ExponentialDelayRestartStrategyConfiguration;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.FailureRateRestartStrategyConfiguration;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.FixedDelayRestartStrategyConfiguration;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.NoRestartStrategyConfiguration;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.types.logical.LogicalType;

import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProvider;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProviderImpl;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryTableSchemaProvider;
import com.google.cloud.flink.bigquery.sink.serializer.RowDataToProtoSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

/**
 * Configurations for a BigQuery Sink.
 *
 * <p>Uses static inner builder to initialize new instances.
 */
public class BigQuerySinkConfig {

    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySink.class);
    private static final long MILLISECONDS_PER_SECOND = 1000L;
    private static final long MILLISECONDS_PER_MINUTE = 60L * 1000L;
    private static final long MILLISECONDS_PER_HOUR = 60L * 60L * 1000L;

    private final BigQueryConnectOptions connectOptions;
    private final DeliveryGuarantee deliveryGuarantee;
    private final BigQuerySchemaProvider schemaProvider;
    private final BigQueryProtoSerializer serializer;
    private final boolean enableTableCreation;
    private final String partitionField;
    private final TimePartitioning.Type partitionType;
    private final Long partitionExpirationMillis;
    private final List<String> clusteredFields;
    private final String region;

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                connectOptions,
                deliveryGuarantee,
                schemaProvider,
                serializer,
                enableTableCreation,
                partitionField,
                partitionType,
                partitionExpirationMillis,
                clusteredFields,
                region);
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
        return (this.getConnectOptions() == object.getConnectOptions()
                && (this.getSerializer().getClass() == object.getSerializer().getClass())
                && (this.getDeliveryGuarantee() == object.getDeliveryGuarantee())
                && (this.enableTableCreation() == object.enableTableCreation())
                && (Objects.equals(this.getPartitionField(), object.getPartitionField()))
                && (this.getPartitionType() == object.getPartitionType())
                && (Objects.equals(
                        this.getPartitionExpirationMillis(), object.getPartitionExpirationMillis()))
                && (Objects.equals(this.getClusteredFields(), object.getClusteredFields()))
                && (Objects.equals(this.getRegion(), object.getRegion()))
                && (Objects.equals(this.getSchemaProvider(), object.getSchemaProvider())));
    }

    private BigQuerySinkConfig(
            BigQueryConnectOptions connectOptions,
            DeliveryGuarantee deliveryGuarantee,
            BigQuerySchemaProvider schemaProvider,
            BigQueryProtoSerializer serializer,
            boolean enableTableCreation,
            String partitionField,
            TimePartitioning.Type partitionType,
            Long partitionExpirationMillis,
            List<String> clusteredFields,
            String region) {
        this.connectOptions = connectOptions;
        this.deliveryGuarantee = deliveryGuarantee;
        this.schemaProvider = schemaProvider;
        this.serializer = serializer;
        this.enableTableCreation = enableTableCreation;
        this.partitionField = partitionField;
        this.partitionType = partitionType;
        this.partitionExpirationMillis = partitionExpirationMillis;
        this.clusteredFields = clusteredFields;
        this.region = region;
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

    public boolean enableTableCreation() {
        return enableTableCreation;
    }

    public String getPartitionField() {
        return partitionField;
    }

    public TimePartitioning.Type getPartitionType() {
        return partitionType;
    }

    public Long getPartitionExpirationMillis() {
        return partitionExpirationMillis;
    }

    public List<String> getClusteredFields() {
        return clusteredFields;
    }

    public String getRegion() {
        return region;
    }

    /** Builder for BigQuerySinkConfig. */
    public static class Builder {

        private BigQueryConnectOptions connectOptions;
        private DeliveryGuarantee deliveryGuarantee;
        private BigQuerySchemaProvider schemaProvider;
        private BigQueryProtoSerializer serializer;
        private boolean enableTableCreation;
        private String partitionField;
        private TimePartitioning.Type partitionType;
        private Long partitionExpirationMillis;
        private List<String> clusteredFields;
        private String region;
        private StreamExecutionEnvironment env;

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

        public Builder enableTableCreation(boolean enableTableCreation) {
            this.enableTableCreation = enableTableCreation;
            return this;
        }

        public Builder partitionField(String partitionField) {
            this.partitionField = partitionField;
            return this;
        }

        public Builder partitionType(TimePartitioning.Type partitionType) {
            this.partitionType = partitionType;
            return this;
        }

        public Builder partitionExpirationMillis(Long partitionExpirationMillis) {
            this.partitionExpirationMillis = partitionExpirationMillis;
            return this;
        }

        public Builder clusteredFields(List<String> clusteredFields) {
            this.clusteredFields = clusteredFields;
            return this;
        }

        public Builder region(String region) {
            this.region = region;
            return this;
        }

        public Builder streamExecutionEnvironment(
                StreamExecutionEnvironment streamExecutionEnvironment) {
            this.env = streamExecutionEnvironment;
            return this;
        }

        public BigQuerySinkConfig build() {
            if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
                validateStreamExecutionEnvironment(env);
            }
            return new BigQuerySinkConfig(
                    connectOptions,
                    deliveryGuarantee,
                    schemaProvider,
                    serializer,
                    enableTableCreation,
                    partitionField,
                    partitionType,
                    partitionExpirationMillis,
                    clusteredFields,
                    region);
        }
    }

    // DO NOT USE!
    // This method is used internally to create sink config for the Table API integration.
    // Note that the serializer is hard coded for Flink Table's RowData.
    @Internal
    public static BigQuerySinkConfig forTable(
            BigQueryConnectOptions connectOptions,
            DeliveryGuarantee deliveryGuarantee,
            LogicalType logicalType,
            boolean enableTableCreation,
            String partitionField,
            TimePartitioning.Type partitionType,
            Long partitionExpirationMillis,
            List<String> clusteredFields,
            String region) {
        return new BigQuerySinkConfig(
                connectOptions,
                deliveryGuarantee,
                new BigQuerySchemaProviderImpl(
                        BigQueryTableSchemaProvider.getAvroSchemaFromLogicalSchema(logicalType)),
                new RowDataToProtoSerializer(logicalType),
                enableTableCreation,
                partitionField,
                partitionType,
                partitionExpirationMillis,
                clusteredFields,
                region);
    }

    public static void validateStreamExecutionEnvironment(StreamExecutionEnvironment env) {
        if (env == null) {
            throw new IllegalArgumentException(
                    "Expected StreamExecutionEnvironment, found null."
                            + " Please provide the StreamExecutionEnvironment used in Flink job.");
        }
        validateRestartStrategy(env.getRestartStrategy());
    }

    private static void validateRestartStrategy(RestartStrategyConfiguration restartStrategy) {
        if (restartStrategy == null) {
            throw new IllegalArgumentException(
                    "Could not read RestartStrategyConfiguration from StreamExecutionEnvironment."
                            + " Please provide the StreamExecutionEnvironment used in Flink job and"
                            + " set a restart strategy.");
        }
        // Restart configurations are mostly being checked against Flink defaults for optimum
        // interactions with external systems, primarily BigQuery storage write APIs.
        // Keep in mind, maximum 10,000 CreateWriteStream calls are allowed by BigQuery per hour per
        // project per region.
        if (restartStrategy instanceof FixedDelayRestartStrategyConfiguration) {
            FixedDelayRestartStrategyConfiguration strategy =
                    (FixedDelayRestartStrategyConfiguration) restartStrategy;
            if (strategy.getDelayBetweenAttemptsInterval().toMilliseconds()
                            < MILLISECONDS_PER_SECOND
                    || strategy.getRestartAttempts() > 10) {
                LOG.error(
                        "Invalid FixedDelayRestartStrategyConfiguration: found restart delay {},"
                                + " milliseconds, and {} restart attempts. Should be used with"
                                + " restart delay at least 1 second, and at most 10 restart"
                                + " attempts.",
                        strategy.getDelayBetweenAttemptsInterval().toMilliseconds(),
                        strategy.getRestartAttempts());
                throw new IllegalArgumentException(
                        "Invalid restart strategy: FixedDelayRestartStrategyConfiguration should"
                                + " be used with at least restart delay 1 second, and at most 10"
                                + " restart attempts.");
            }
        } else if (restartStrategy instanceof ExponentialDelayRestartStrategyConfiguration) {
            ExponentialDelayRestartStrategyConfiguration strategy =
                    (ExponentialDelayRestartStrategyConfiguration) restartStrategy;
            if (strategy.getBackoffMultiplier() < 2.0
                    || strategy.getInitialBackoff().toMilliseconds() < MILLISECONDS_PER_SECOND
                    || strategy.getMaxBackoff().toMilliseconds() < (5L * MILLISECONDS_PER_MINUTE)
                    || strategy.getResetBackoffThreshold().toMilliseconds()
                            < MILLISECONDS_PER_HOUR) {
                LOG.error(
                        "Invalid ExponentialDelayRestartStrategyConfiguration: found backoff"
                                + " multiplier {}, initial backoff {} milliseconds, maximum backoff"
                                + " {} milliseconds, and reset threshold {} milliseconds. Should be"
                                + " used with backoff multiplier at least 2, initial backoff at"
                                + " least 1 second, maximum backoff at least 5 minutes, and reset"
                                + " threshold at least 1 hour",
                        strategy.getBackoffMultiplier(),
                        strategy.getInitialBackoff().toMilliseconds(),
                        strategy.getMaxBackoff().toMilliseconds(),
                        strategy.getResetBackoffThreshold().toMilliseconds());
                throw new IllegalArgumentException(
                        "Invalid restart strategy: ExponentialDelayRestartStrategyConfiguration"
                                + " should be used with backoff multiplier at least 2, initial"
                                + " backoff at least 1 second, maximum backoff at-least 5 minutes,"
                                + " and reset threshold at least 1 hour");
            }
        } else if (restartStrategy instanceof FailureRateRestartStrategyConfiguration) {
            FailureRateRestartStrategyConfiguration strategy =
                    (FailureRateRestartStrategyConfiguration) restartStrategy;
            double failureIntervalInMinutes =
                    ((double) strategy.getFailureInterval().toMilliseconds())
                            / MILLISECONDS_PER_MINUTE;
            double allowedFailuresPerMinute =
                    ((double) strategy.getMaxFailureRate()) / failureIntervalInMinutes;
            if (strategy.getDelayBetweenAttemptsInterval().toMilliseconds()
                            < MILLISECONDS_PER_SECOND
                    || allowedFailuresPerMinute > 1.0) {
                LOG.error(
                        "Invalid FailureRateRestartStrategyConfiguration: found restart delay {}"
                                + " milliseconds, and allowed failure rate {} per minute. Should be"
                                + " used with restart delay at least 1 second, and allowed failure"
                                + " rate at most 1 per minute.",
                        strategy.getDelayBetweenAttemptsInterval().toMilliseconds(),
                        allowedFailuresPerMinute);
                throw new IllegalArgumentException(
                        "Invalid restart strategy: FailureRateRestartStrategyConfiguration should"
                                + " be used with restart delay at least 1 second, and allowed"
                                + " failure rate at most 1 per minute.");
            }
        } else if (restartStrategy instanceof NoRestartStrategyConfiguration) {
            // This is fine!
            LOG.debug("Found NoRestartStrategyConfiguration. No validation needed.");
        } else {
            // Can be either FallbackRestartStrategyConfiguration, or a custom implementation. We
            // don't know how to verify these two possibilities, so we let them pass. If you,
            // worthy reader have a suggestion for validating these scenarios, then kindly share
            // with us at
            // https://github.com/GoogleCloudDataproc/flink-bigquery-connector/issues/new
            // Thanks!
            LOG.warn(
                    "Cannot validate RestartStrategyConfiguration in StreamExecutionEnvironment."
                            + " We recommend explicitly setting the restart strategy as one of the"
                            + " following:"
                            + " FixedDelayRestartStrategyConfiguration,"
                            + " ExponentialDelayRestartStrategyConfiguration,"
                            + " FailureRateRestartStrategyConfiguration or"
                            + " NoRestartStrategyConfiguration");
        }
    }
}
