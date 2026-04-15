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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class wrapping BigQuery sinks with appropriate configurations.
 *
 * <p>With {@link DeliveryGuarantee#AT_LEAST_ONCE}, the Sink added to Flink job will be {@link
 * BigQueryDefaultSink}.
 *
 * <p>With {@link DeliveryGuarantee#EXACTLY_ONCE}, the Sink added to Flink job will be {@link
 * BigQueryExactlyOnceSink}.
 *
 * <p>Eventual data consistency at destination is also dependent on checkpointing mode. Look at
 * {@link BigQueryDefaultSink} and {@link BigQueryExactlyOnceSink} for write consistencies offered
 * across combinations of {@link CheckpointingMode} and sink's {@link DeliveryGuarantee}. It is
 * recommended that checkpointing is enabled to avoid unexpected behavior.
 */
public class BigQuerySink {

    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySink.class);

    public static <IN> Sink<IN> get(BigQuerySinkConfig<IN> sinkConfig) {
        if (sinkConfig.isCdcEnabled()) {
            validateCdcConfiguration(sinkConfig);
        }
        if (sinkConfig.getDeliveryGuarantee() == DeliveryGuarantee.AT_LEAST_ONCE) {
            return new BigQueryDefaultSink<>(sinkConfig);
        }
        if (sinkConfig.getDeliveryGuarantee() == DeliveryGuarantee.EXACTLY_ONCE) {
            return new BigQueryExactlyOnceSink<>(sinkConfig);
        }
        LOG.error(
                "BigQuery sink does not support {} delivery guarantee. Use AT_LEAST_ONCE or EXACTLY_ONCE.",
                sinkConfig.getDeliveryGuarantee());
        throw new UnsupportedOperationException(
                String.format("%s is not supported", sinkConfig.getDeliveryGuarantee()));
    }

    /**
     * Validates CDC configuration requirements.
     *
     * <p>CDC mode has the following requirements: - Must use AT_LEAST_ONCE delivery guarantee (CDC
     * uses the default stream) - The destination BigQuery table must have a PRIMARY KEY constraint
     *
     * <p>Note: Table-level requirements (PRIMARY KEY, max_staleness) are not validated here as they
     * require BigQuery API calls. They will be validated at runtime.
     *
     * @param sinkConfig The sink configuration to validate.
     * @throws IllegalArgumentException if CDC configuration is invalid.
     */
    private static void validateCdcConfiguration(BigQuerySinkConfig<?> sinkConfig) {
        if (sinkConfig.getDeliveryGuarantee() != DeliveryGuarantee.AT_LEAST_ONCE) {
            LOG.error(
                    "CDC mode requires AT_LEAST_ONCE delivery guarantee. "
                            + "BigQuery CDC uses the default stream which provides at-least-once semantics. "
                            + "Found: {}",
                    sinkConfig.getDeliveryGuarantee());
            throw new IllegalArgumentException(
                    "CDC mode requires AT_LEAST_ONCE delivery guarantee. "
                            + "BigQuery CDC uses the default stream which provides at-least-once semantics. "
                            + "Found: "
                            + sinkConfig.getDeliveryGuarantee());
        }
        if (sinkConfig.getCdcSequenceField() == null
                || sinkConfig.getCdcSequenceField().isEmpty()) {
            LOG.warn(
                    "CDC mode enabled without write.cdc-sequence-field. "
                            + "Records will be written without _change_sequence_number, so ordering "
                            + "between multiple changes to the same primary key may be nondeterministic.");
        }
        if (sinkConfig.enableTableCreation()) {
            if (sinkConfig.getCdcPrimaryKeyColumns() == null
                    || sinkConfig.getCdcPrimaryKeyColumns().isEmpty()) {
                throw new IllegalArgumentException(
                        "CDC table auto-creation requires write.cdc-primary-key-columns to be set.");
            }
            if (StringUtils.isNullOrWhitespaceOnly(sinkConfig.getCdcMaxStaleness())) {
                LOG.warn(
                        "CDC table auto-creation enabled without write.cdc-max-staleness. "
                                + "The table will still be created, but max_staleness will remain unset. "
                                + "If needed, run ALTER TABLE ... SET OPTIONS (max_staleness = INTERVAL ...)"
                                + " after creation.");
            }
        }
        LOG.info(
                "CDC mode enabled. Ensure the destination BigQuery table has a PRIMARY KEY "
                        + "constraint configured for CDC to work properly. "
                        + "If table auto-creation is enabled, configure primary keys with "
                        + "write.cdc-primary-key-columns. max_staleness is optional, but if omitted "
                        + "or not persisted by BigQuery during create, it may need to be set later "
                        + "with ALTER TABLE ... SET OPTIONS.");
    }
}
