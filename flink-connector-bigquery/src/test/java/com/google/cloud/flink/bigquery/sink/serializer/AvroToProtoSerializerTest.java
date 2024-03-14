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

package com.google.cloud.flink.bigquery.sink.serializer;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import static com.google.common.truth.Truth.assertThat;

/** Tests for {@link AvroToProtoSerializer}. */
public class AvroToProtoSerializerTest {

    @Test
    public void testPrimitiveTypesConversion() {
        Schema schema = AvroToProtoSerializerTestUtils.testPrimitiveTypesConversion().getSchema();
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testPrimitiveTypesConversion().getDescriptor();

        byte[] byteArray = "Any String you want".getBytes();
        String recordSchemaString =
                "{\"type\":\"record\",\"name\":\"required_record_field\",\"doc\":\"Translated Avro Schema for required_record_field\",\"fields\":[{\"name\":\"species\",\"type\":\"string\"}]}}";
        Schema recordSchema = Schema.parse(recordSchemaString);
        GenericRecord genericRecord =
                new GenericRecordBuilder(recordSchema).set("species", "hello").build();

        GenericRecord record =
                new GenericRecordBuilder(schema)
                        .set("number", -7099548873856657385L)
                        .set("price", 0.5616495161359795)
                        .set("species", "icukpigbcvtpfntnbmhy")
                        .set("flighted", true)
                        .set("sound", ByteBuffer.wrap(byteArray))
                        .set("required_record_field", genericRecord)
                        .build();

        DynamicMessage message =
                AvroToProtoSerializer.getDynamicMessageFromGenericRecord(record, descriptor);
        assertThat(message.getField(descriptor.findFieldByNumber(1)))
                .isEqualTo(-7099548873856657385L);
        assertThat(message.getField(descriptor.findFieldByNumber(2))).isEqualTo(0.5616495161359795);
        assertThat(message.getField(descriptor.findFieldByNumber(3)))
                .isEqualTo("icukpigbcvtpfntnbmhy");
        assertThat(message.getField(descriptor.findFieldByNumber(4))).isEqualTo(true);
        assertThat(message.getField(descriptor.findFieldByNumber(5)))
                .isEqualTo(ByteString.copyFrom(byteArray));

        FieldDescriptor fieldDescriptor = descriptor.findFieldByNumber(6);
        message = (DynamicMessage) message.getField(fieldDescriptor);
        assertThat(
                        message.getField(
                                descriptor
                                        .findNestedTypeByName(
                                                fieldDescriptor.toProto().getTypeName())
                                        .findFieldByNumber(1)))
                .isEqualTo("hello");
    }

    @Test
    public void testLogicalTypesConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testLogicalTypesConversion().getDescriptor();
        Schema schema = AvroToProtoSerializerTestUtils.testLogicalTypesConversion().getSchema();

        GenericRecord record =
                new GenericRecordBuilder(schema)
                        .set("timestamp", null)
                        .set(
                                "numeric_field",
                                ByteBuffer.wrap(
                                        new BigDecimal(123456.7891011)
                                                .unscaledValue()
                                                .toByteArray()))
                        .set(
                                "bignumeric_field",
                                ByteBuffer.wrap(
                                        new BigDecimal(123456.7891011)
                                                .unscaledValue()
                                                .toByteArray()))
                        .set("geography", "POINT(12, 13)")
                        .set("Json", "{\"name\": \"John\", \"surname\": \"Doe\"}")
                        .build();

        DynamicMessage message =
                AvroToProtoSerializer.getDynamicMessageFromGenericRecord(record, descriptor);
    }

    @Test
    public void testAllPrimitiveSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testAllPrimitiveSchemaConversion().getDescriptor();
        Schema schema =
                AvroToProtoSerializerTestUtils.testAllPrimitiveSchemaConversion().getSchema();
    }

    @Test
    public void testAllLogicalSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testAllLogicalSchemaConversion().getDescriptor();
        Schema schema = AvroToProtoSerializerTestUtils.testAllLogicalSchemaConversion().getSchema();
    }

    @Test
    public void testAllUnionLogicalSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testAllUnionLogicalSchemaConversion()
                        .getDescriptor();
        Schema schema =
                AvroToProtoSerializerTestUtils.testAllUnionLogicalSchemaConversion().getSchema();
    }

    @Test
    public void testAllUnionPrimitiveSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testAllUnionPrimitiveSchemaConversion()
                        .getDescriptor();
        Schema schema =
                AvroToProtoSerializerTestUtils.testAllUnionPrimitiveSchemaConversion().getSchema();
    }

    @Test
    public void testUnionInRecordSchemaConversation() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testUnionInRecordSchemaConversation()
                        .getDescriptor();
        Schema schema =
                AvroToProtoSerializerTestUtils.testUnionInRecordSchemaConversation().getSchema();
    }

    @Test
    public void testRecordOfLogicalTypeSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testRecordOfLogicalTypeSchemaConversion()
                        .getDescriptor();
        Schema schema =
                AvroToProtoSerializerTestUtils.testRecordOfLogicalTypeSchemaConversion()
                        .getSchema();
    }

    @Test
    public void testDefaultValueSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testDefaultValueSchemaConversion().getDescriptor();
        Schema schema =
                AvroToProtoSerializerTestUtils.testDefaultValueSchemaConversion().getSchema();
    }

    @Test
    public void testRecordOfRecordSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testRecordOfRecordSchemaConversion().getDescriptor();
        Schema schema =
                AvroToProtoSerializerTestUtils.testRecordOfRecordSchemaConversion().getSchema();
    }

    @Test
    public void testMapOfUnionTypeSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testMapOfUnionTypeSchemaConversion().getDescriptor();
        Schema schema =
                AvroToProtoSerializerTestUtils.testMapOfUnionTypeSchemaConversion().getSchema();
    }

    @Test
    public void testMapOfArraySchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testMapOfArraySchemaConversion().getDescriptor();
        Schema schema = AvroToProtoSerializerTestUtils.testMapOfArraySchemaConversion().getSchema();
    }

    @Test
    public void testMapInRecordSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testMapInRecordSchemaConversion().getDescriptor();
        Schema schema =
                AvroToProtoSerializerTestUtils.testMapInRecordSchemaConversion().getSchema();
    }

    @Test
    public void testMapOfMapSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testMapOfMapSchemaConversion().getDescriptor();
        Schema schema = AvroToProtoSerializerTestUtils.testMapOfMapSchemaConversion().getSchema();
    }

    @Test
    public void testMapOfRecordSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testMapOfRecordSchemaConversion().getDescriptor();
        Schema schema =
                AvroToProtoSerializerTestUtils.testMapOfRecordSchemaConversion().getSchema();
    }

    @Test
    public void testRecordOfArraySchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testRecordOfArraySchemaConversion().getDescriptor();
        Schema schema =
                AvroToProtoSerializerTestUtils.testRecordOfArraySchemaConversion().getSchema();
    }

    @Test
    public void testArrayOfRecordSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testArrayOfRecordSchemaConversion().getDescriptor();
        Schema schema =
                AvroToProtoSerializerTestUtils.testArrayOfRecordSchemaConversion().getSchema();
    }

    @Test
    public void testUnionOfRecordSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testUnionOfRecordSchemaConversion().getDescriptor();
        Schema schema =
                AvroToProtoSerializerTestUtils.testUnionOfRecordSchemaConversion().getSchema();
    }

    @Test
    public void testSpecialSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testSpecialSchemaConversion().getDescriptor();
        Schema schema = AvroToProtoSerializerTestUtils.testSpecialSchemaConversion().getSchema();
    }

    @Test
    public void testAllPrimitiveSingleUnionSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testAllPrimitiveSingleUnionSchemaConversion()
                        .getDescriptor();
        Schema schema =
                AvroToProtoSerializerTestUtils.testAllPrimitiveSingleUnionSchemaConversion()
                        .getSchema();
    }

    @Test
    public void testRecordOfUnionFieldSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testRecordOfUnionFieldSchemaConversion()
                        .getDescriptor();
        Schema schema =
                AvroToProtoSerializerTestUtils.testRecordOfUnionFieldSchemaConversion().getSchema();
    }

    @Test
    public void testArrayAndRequiredTypesConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testArrayAndRequiredTypesConversion()
                        .getDescriptor();
        Schema schema =
                AvroToProtoSerializerTestUtils.testArrayAndRequiredTypesConversion().getSchema();
    }
}
