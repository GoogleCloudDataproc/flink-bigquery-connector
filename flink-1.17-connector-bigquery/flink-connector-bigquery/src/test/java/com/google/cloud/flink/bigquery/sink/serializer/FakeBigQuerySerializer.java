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

/** Mock serializer for Sink unit tests. */
public class FakeBigQuerySerializer extends BigQueryProtoSerializer {

    private static final FakeBigQuerySerializer EMPTY_SERIALIZER =
            new FakeBigQuerySerializer(null, false);
    private static final FakeBigQuerySerializer ERRING_SERIALIZER =
            new FakeBigQuerySerializer(null, true);

    private final ByteString serializeResult;
    private final boolean throwException;

    public FakeBigQuerySerializer(ByteString serializeResponse) {
        this(serializeResponse, false);
    }

    public static FakeBigQuerySerializer getEmptySerializer() {
        return EMPTY_SERIALIZER;
    }

    public static FakeBigQuerySerializer getErringSerializer() {
        return ERRING_SERIALIZER;
    }

    public FakeBigQuerySerializer(ByteString serializeResponse, boolean throwException) {
        this.serializeResult = serializeResponse;
        this.throwException = throwException;
    }

    @Override
    public ByteString serialize(Object record) throws BigQuerySerializationException {
        if (throwException) {
            throw new BigQuerySerializationException("Fake error for testing");
        }
        return serializeResult;
    }

    @Override
    public void init(BigQuerySchemaProvider schemaProvider) {}
}
