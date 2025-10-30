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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
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

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Configurations for a BigQuery Sink.
 *
 * <p>Uses static inner builder to initialize new instances.
 *
 * @param <IN> Type of input to sink.
 */
public class BigQuerySinkConfig<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySink.class);
    private static final long MILLISECONDS_PER_SECOND = 1000L;
    private static final long MILLISECONDS_PER_MINUTE = 60L * 1000L;
    private static final long MILLISECONDS_PER_HOUR = 60L * 60L * 1000L;
    private static final String RESTART_STRATEGY_FIXED_DELAY = "fixed-delay";
    private static final String RESTART_STRATEGY_EXPONENTIAL_DELAY = "exponential-delay";
    private static final String RESTART_STRATEGY_FAILURE_RATE = "failure-rate";
    private static final String RESTART_STRATEGY_NONE = "none";

    private final BigQueryConnectOptions connectOptions;
    private final DeliveryGuarantee deliveryGuarantee;
    private final BigQuerySchemaProvider schemaProvider;
    private final BigQueryProtoSerializer<IN> serializer;
    private final boolean enableTableCreation;
    private final String partitionField;
    private final TimePartitioning.Type partitionType;
    private final Long partitionExpirationMillis;
    private final List<String> clusteredFields;
    private final String region;
    private final boolean fatalizeSerializer;

    public static <IN> Builder<IN> newBuilder() {
        return new Builder<>();
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
                region,
                fatalizeSerializer);
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
        BigQuerySinkConfig<IN> object = (BigQuerySinkConfig<IN>) obj;
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
                && (Objects.equals(this.getSchemaProvider(), object.getSchemaProvider()))
                && (this.fatalizeSerializer() == object.fatalizeSerializer()));
    }

    private BigQuerySinkConfig(
            BigQueryConnectOptions connectOptions,
            DeliveryGuarantee deliveryGuarantee,
            BigQuerySchemaProvider schemaProvider,
            BigQueryProtoSerializer<IN> serializer,
            boolean enableTableCreation,
            String partitionField,
            TimePartitioning.Type partitionType,
            Long partitionExpirationMillis,
            List<String> clusteredFields,
            String region,
            boolean fatalizeSerializer) {
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
        this.fatalizeSerializer = fatalizeSerializer;
    }

    public BigQueryConnectOptions getConnectOptions() {
        return connectOptions;
    }

    public DeliveryGuarantee getDeliveryGuarantee() {
        return deliveryGuarantee;
    }

    public BigQueryProtoSerializer<IN> getSerializer() {
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

    public boolean fatalizeSerializer() {
        return fatalizeSerializer;
    }

    /**
     * Builder for BigQuerySinkConfig.
     *
     * @param <IN> Type of input to sink.
     */
    public static class Builder<IN> {

        private BigQueryConnectOptions connectOptions;
        private DeliveryGuarantee deliveryGuarantee;
        private BigQuerySchemaProvider schemaProvider;
        private BigQueryProtoSerializer<IN> serializer;
        private boolean enableTableCreation;
        private String partitionField;
        private TimePartitioning.Type partitionType;
        private Long partitionExpirationMillis;
        private List<String> clusteredFields;
        private String region;
        private boolean fatalizeSerializer;
        private StreamExecutionEnvironment env;

        public Builder<IN> connectOptions(BigQueryConnectOptions connectOptions) {
            this.connectOptions = connectOptions;
            return this;
        }

        public Builder<IN> deliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
            this.deliveryGuarantee = deliveryGuarantee;
            return this;
        }

        public Builder<IN> schemaProvider(BigQuerySchemaProvider schemaProvider) {
            this.schemaProvider = schemaProvider;
            return this;
        }

        public Builder<IN> serializer(BigQueryProtoSerializer<IN> serializer) {
            this.serializer = serializer;
            return this;
        }

        public Builder<IN> enableTableCreation(boolean enableTableCreation) {
            this.enableTableCreation = enableTableCreation;
            return this;
        }

        public Builder<IN> partitionField(String partitionField) {
            this.partitionField = partitionField;
            return this;
        }

        public Builder<IN> partitionType(TimePartitioning.Type partitionType) {
            this.partitionType = partitionType;
            return this;
        }

        public Builder<IN> partitionExpirationMillis(Long partitionExpirationMillis) {
            this.partitionExpirationMillis = partitionExpirationMillis;
            return this;
        }

        public Builder<IN> clusteredFields(List<String> clusteredFields) {
            this.clusteredFields = clusteredFields;
            return this;
        }

        public Builder<IN> region(String region) {
            this.region = region;
            return this;
        }

        public Builder<IN> fatalizeSerializer(boolean fatalizeSerializer) {
            this.fatalizeSerializer = fatalizeSerializer;
            return this;
        }

        public Builder<IN> streamExecutionEnvironment(
                StreamExecutionEnvironment streamExecutionEnvironment) {
            this.env = streamExecutionEnvironment;
            return this;
        }

        public BigQuerySinkConfig<IN> build() {
            if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
                validateStreamExecutionEnvironment(env);
            }
            return new BigQuerySinkConfig<>(
                    connectOptions,
                    deliveryGuarantee,
                    schemaProvider,
                    serializer,
                    enableTableCreation,
                    partitionField,
                    partitionType,
                    partitionExpirationMillis,
                    clusteredFields,
                    region,
                    fatalizeSerializer);
        }
    }

    // DO NOT USE!
    // This method is used internally to create sink config for the Table API integration.
    // Note that the serializer is hard coded for Flink Table's RowData.
    @Internal
    public static BigQuerySinkConfig<RowData> forTable(
            BigQueryConnectOptions connectOptions,
            DeliveryGuarantee deliveryGuarantee,
            LogicalType logicalType,
            boolean enableTableCreation,
            String partitionField,
            TimePartitioning.Type partitionType,
            Long partitionExpirationMillis,
            List<String> clusteredFields,
            String region,
            boolean fatalizeSerializer) {
        return new BigQuerySinkConfig<>(
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
                region,
                fatalizeSerializer);
    }

    public static void validateStreamExecutionEnvironment(StreamExecutionEnvironment env) {
        if (env == null) {
            throw new IllegalArgumentException(
                    "Expected StreamExecutionEnvironment, found null."
                            + " Please provide the StreamExecutionEnvironment used in Flink job.");
        }
        validateRestartStrategy(env.getConfiguration());
    }

    private static void validateRestartStrategy(ReadableConfig config) {
        if (config == null) {
            throw new IllegalArgumentException(
                    "Could not read Configuration from StreamExecutionEnvironment."
                            + " Please provide the StreamExecutionEnvironment used in Flink job and"
                            + " set a restart strategy.");
        }

        Optional<String> maybeRestartStrategy =
                config.getOptional(RestartStrategyOptions.RESTART_STRATEGY);
        if (maybeRestartStrategy.isPresent()) {
            // Restart configurations are mostly being checked against Flink defaults for optimum
            // interactions with external systems, primarily BigQuery storage write APIs.
            // Keep in mind, maximum 10,000 CreateWriteStream calls are allowed by BigQuery per hour
            // per
            // project per region.
            String restartStrategy = maybeRestartStrategy.get();
            if (restartStrategy.equals(RESTART_STRATEGY_FIXED_DELAY)) {
                Duration delayBetweenAttemptsInterval =
                        config.get(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY);
                Integer restartAttempts =
                        config.get(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS);

                if (delayBetweenAttemptsInterval.toMillis() < MILLISECONDS_PER_SECOND
                        || restartAttempts > 10) {
                    LOG.error(
                            "Invalid fixed delay restart strategy configuration: found restart delay {},"
                                    + " milliseconds, and {} restart attempts. Should be used with"
                                    + " restart delay at least 1 second, and at most 10 restart"
                                    + " attempts.",
                            delayBetweenAttemptsInterval.toMillis(),
                            restartAttempts);
                    throw new IllegalArgumentException(
                            "Invalid restart strategy: fixed delay restart strategy configuration should"
                                    + " be used with at least restart delay 1 second, and at most 10"
                                    + " restart attempts.");
                }
            } else if (restartStrategy.equals(RESTART_STRATEGY_EXPONENTIAL_DELAY)) {
                Double backoffMultiplier =
                        config.get(
                                RestartStrategyOptions
                                        .RESTART_STRATEGY_EXPONENTIAL_DELAY_BACKOFF_MULTIPLIER);
                Duration initialBackoff =
                        config.get(
                                RestartStrategyOptions
                                        .RESTART_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF);
                Duration maxBackoff =
                        config.get(
                                RestartStrategyOptions
                                        .RESTART_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF);
                Duration resetBackoffThreshold =
                        config.get(
                                RestartStrategyOptions
                                        .RESTART_STRATEGY_EXPONENTIAL_DELAY_RESET_BACKOFF_THRESHOLD);

                if (backoffMultiplier < 2.0
                        || initialBackoff.toMillis() < MILLISECONDS_PER_SECOND
                        || maxBackoff.toMillis() < (5L * MILLISECONDS_PER_MINUTE)
                        || resetBackoffThreshold.toMillis() < MILLISECONDS_PER_HOUR) {
                    LOG.error(
                            "Invalid exponential delay restart strategy configuration: found backoff"
                                    + " multiplier {}, initial backoff {} milliseconds, maximum backoff"
                                    + " {} milliseconds, and reset threshold {} milliseconds. Should be"
                                    + " used with backoff multiplier at least 2, initial backoff at"
                                    + " least 1 second, maximum backoff at least 5 minutes, and reset"
                                    + " threshold at least 1 hour",
                            backoffMultiplier,
                            initialBackoff.toMillis(),
                            maxBackoff.toMillis(),
                            resetBackoffThreshold.toMillis());
                    throw new IllegalArgumentException(
                            "Invalid restart strategy: exponential delay restart strategy configuration"
                                    + " should be used with backoff multiplier at least 2, initial"
                                    + " backoff at least 1 second, maximum backoff at-least 5 minutes,"
                                    + " and reset threshold at least 1 hour");
                }
            } else if (restartStrategy.equals(RESTART_STRATEGY_FAILURE_RATE)) {
                Duration failureInterval =
                        config.get(
                                RestartStrategyOptions
                                        .RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL);
                Integer maxFailureRate =
                        config.get(
                                RestartStrategyOptions
                                        .RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL);
                Duration delayBetweenAttemptsInterval =
                        config.get(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY);

                double failureIntervalInMinutes =
                        ((double) failureInterval.toMillis()) / MILLISECONDS_PER_MINUTE;
                double allowedFailuresPerMinute =
                        ((double) maxFailureRate) / failureIntervalInMinutes;
                if (delayBetweenAttemptsInterval.toMillis() < MILLISECONDS_PER_SECOND
                        || allowedFailuresPerMinute > 1.0) {
                    LOG.error(
                            "Invalid failure rate restart strategy configuration: found restart delay {}"
                                    + " milliseconds, and allowed failure rate {} per minute. Should be"
                                    + " used with restart delay at least 1 second, and allowed failure"
                                    + " rate at most 1 per minute.",
                            delayBetweenAttemptsInterval.toMillis(),
                            allowedFailuresPerMinute);
                    throw new IllegalArgumentException(
                            "Invalid restart strategy: failure rate restart strategy configuration should"
                                    + " be used with restart delay at least 1 second, and allowed"
                                    + " failure rate at most 1 per minute.");
                }
            } else if (restartStrategy.equals(RESTART_STRATEGY_NONE)) {
                LOG.debug("Found no restart strategy. No validation needed.");
            } else {
                throw new IllegalStateException(
                        "Encountered unexpected restart strategy: " + restartStrategy);
            }
        } else {
            throw new IllegalArgumentException(
                    "Cannot validate RestartStrategyConfiguration in StreamExecutionEnvironment."
                            + " We recommend explicitly setting the restart strategy as one of the"
                            + " following:"
                            + " "
                            + RESTART_STRATEGY_FIXED_DELAY
                            + ","
                            + " "
                            + RESTART_STRATEGY_EXPONENTIAL_DELAY
                            + ","
                            + " "
                            + RESTART_STRATEGY_FAILURE_RATE
                            + " or"
                            + " "
                            + RESTART_STRATEGY_NONE);
        }
    }
}
