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

import com.google.protobuf.ByteString;
import org.junit.Test;

import static org.junit.Assert.assertThrows;

/** Tests for {@link BigQueryProtoSerializer}. */
public class BigQueryProtoSerializerTest {

    private final BigQueryProtoSerializer<String> serializer =
            new BigQueryProtoSerializer<String>() {
                @Override
                public ByteString serialize(String record) {
                    return ByteString.copyFromUtf8(record);
                }

                @Override
                public org.apache.avro.Schema getAvroSchema(String record) {
                    return null;
                }
            };

    @Test
    public void extractSequenceNumber_withoutOverride_shouldThrow() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> serializer.extractSequenceNumber("record", "sequence_field"));
    }

    @Test
    public void serializeWithCdc_withoutOverride_shouldThrow() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> serializer.serializeWithCdc("record", "UPSERT", "0000000000000001"));
    }
}
