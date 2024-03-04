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

import java.io.Serializable;

/**
 * Interface for defining a Flink record to BigQuery proto serializer.
 *
 * <p>One BigQueryProtoSerializer should correspond to a single BigQuery table.
 *
 * @param <IN> Type of records to be written to BigQuery.
 */
public interface BigQueryProtoSerializer<IN> extends Serializable {

    /**
     * Convert Flink record to proto ByteString compatible with BigQuery table.
     *
     * @param record Record to serialize.
     * @return ByteString.
     * @throws BigQuerySerializationException If serialization failed.
     */
    public ByteString serialize(IN record) throws BigQuerySerializationException;

    /**
     * Initializes the serializer with a BigQuery table schema. This should be called once for every
     * serializer instance before its first serialize call.
     *
     * @param schemaProvider BigQuery table's schema information.
     */
    public void init(BigQuerySchemaProvider schemaProvider);
}
