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

package com.google.cloud.flink.bigquery.source.reader.deserializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import com.google.cloud.flink.bigquery.common.exceptions.BigQueryConnectorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * A schema bridge for de-serializing the BigQuery's return types ({@code GenericRecord} or {@link
 * ArrowRecord}) into a flink managed instance.
 *
 * @param <IN> The input type to de-serialize.
 * @param <OUT> The output record type for to sink for downstream processing.
 */
@PublicEvolving
public interface BigQueryDeserializationSchema<IN, OUT>
        extends Serializable, ResultTypeQueryable<OUT> {

    Logger LOG = LoggerFactory.getLogger(BigQueryDeserializationSchema.class);

    /**
     * De-serializes the IN type record.
     *
     * @param record The BSON document to de-serialize.
     * @return The de-serialized message as an object (null if the message cannot be de-serialized).
     * @throws BigQueryConnectorException In case of problems while de-serializing.
     */
    OUT deserialize(IN record) throws BigQueryConnectorException;

    /**
     * De-serializes the IN type record.
     *
     * <p>Can output multiple records through the {@link Collector}. Note that number and size of
     * the produced records should be relatively small. Depending on the source implementation
     * records can be buffered in memory or collecting records might delay emitting checkpoint
     * barrier.
     *
     * @param record The IN document to de-serialize.
     * @param out The collector to put the resulting messages.
     */
    default void deserialize(IN record, Collector<OUT> out) throws BigQueryConnectorException {
        OUT deserialize = deserialize(record);
        try {
            if (deserialize != null) {
                out.collect(deserialize);
            }
        } catch (RuntimeException e) {
            LOG.error(
                    String.format(
                            "Failed to forward the deserialized record %s to the next operator.%nError %s%nCause %s",
                            deserialize, e.getMessage(), e.getCause()));
            throw new BigQueryConnectorException(
                    "Failed to forward the deserialized record to the next operator.");
        }
    }
}
