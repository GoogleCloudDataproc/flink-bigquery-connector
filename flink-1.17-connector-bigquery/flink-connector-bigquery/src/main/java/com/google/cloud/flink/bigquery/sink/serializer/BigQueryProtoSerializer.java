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

package com.google.cloud.flink.bigquery.sink.serializer;

import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.protobuf.ByteString;
import org.apache.avro.Schema;

import java.io.Serializable;

/**
 * Base class for defining a Flink record to BigQuery proto serializer.
 *
 * <p>One BigQueryProtoSerializer should correspond to a single BigQuery table.
 *
 * @param <IN> Type of records to be written to BigQuery.
 */
public abstract class BigQueryProtoSerializer<IN> implements Serializable {

    /**
     * Convert Flink record to proto ByteString compatible with BigQuery table.
     *
     * @param record Record to serialize.
     * @return ByteString.
     * @throws BigQuerySerializationException If serialization failed.
     */
    public abstract ByteString serialize(IN record) throws BigQuerySerializationException;

    /**
     * Initializes the serializer with a BigQuery table schema. This will be called once for every
     * serializer instance before its first serialize call.
     *
     * @param schemaProvider BigQuery table's schema information.
     */
    public void init(BigQuerySchemaProvider schemaProvider) {}

    /**
     * Derives Avro {@link Schema} describing the data record. This is primarily used by the sink to
     * infer schema for creating new destination BigQuery table if one doesn't already exist.
     *
     * @param record Record to check for schema
     * @return Schema.
     */
    public abstract Schema getAvroSchema(IN record);

    /**
     * Initializes the serializer for CDC mode with an augmented schema that includes CDC
     * pseudocolumns. This will be called once for every serializer instance before its first
     * serialize call when CDC is enabled.
     *
     * <p>The default implementation calls {@link #init(BigQuerySchemaProvider)}. Subclasses should
     * override this method to store the CDC-aware descriptor separately.
     *
     * @param schemaProvider BigQuery table's schema information augmented with CDC columns.
     */
    public void initForCdc(BigQuerySchemaProvider schemaProvider) {
        init(schemaProvider);
    }

    /**
     * Convert Flink record to proto ByteString with CDC metadata included.
     *
     * <p>The default implementation calls {@link #serialize(Object)}. Subclasses that support CDC
     * should override this method to include the CDC pseudocolumns in the serialized output.
     *
     * @param record Record to serialize.
     * @param changeType The CDC change type ("UPSERT" or "DELETE").
     * @param changeSequenceNumber The sequence number for ordering (hexadecimal string).
     * @return ByteString with CDC metadata included.
     * @throws BigQuerySerializationException If serialization failed.
     */
    public ByteString serializeWithCdc(IN record, String changeType, String changeSequenceNumber)
            throws BigQuerySerializationException {
        // Default implementation ignores CDC fields for backward compatibility.
        // Subclasses should override to include CDC pseudocolumns.
        return serialize(record);
    }

    /**
     * Extracts a sequence number from the record for CDC ordering.
     *
     * <p>The sequence number is used by BigQuery to determine the order of changes for records with
     * the same primary key. Higher sequence numbers take precedence.
     *
     * <p>The default implementation returns "0". Subclasses should override this method to extract
     * a meaningful sequence number from the record.
     *
     * @param record Record to extract sequence number from.
     * @param sequenceField The name of the field containing the sequence value, or null.
     * @return Hexadecimal string representation of the sequence number.
     */
    public String extractSequenceNumber(IN record, String sequenceField) {
        return "0";
    }
}
