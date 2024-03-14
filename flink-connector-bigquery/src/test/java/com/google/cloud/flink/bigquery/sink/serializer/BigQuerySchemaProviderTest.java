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

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import org.apache.avro.Schema;
import org.junit.Test;

import static com.google.cloud.flink.bigquery.sink.serializer.AvroToProtoSerializerTestUtils.getAvroSchemaFromFieldString;
import static com.google.cloud.flink.bigquery.sink.serializer.AvroToProtoSerializerTestUtils.getRecord;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

/** Tests for {@link BigQuerySchemaProvider}. */
public class BigQuerySchemaProviderTest {
    @Test
    public void testPrimitiveTypesConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testPrimitiveTypesConversion().getDescriptor();

        assertThat(descriptor.findFieldByNumber(1).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("number")
                                .setNumber(1)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(2).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_DOUBLE)
                                .setName("price")
                                .setNumber(2)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(3).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("species")
                                .setNumber(3)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(4).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BOOL)
                                .setName("flighted")
                                .setNumber(4)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(5).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BYTES)
                                .setName("sound")
                                .setNumber(5)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(6).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setName("required_record_field")
                                .setNumber(6)
                                .setTypeName(
                                        descriptor.findFieldByNumber(6).toProto().getTypeName())
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.getNestedTypes()).hasSize(1);

        assertThat(
                        descriptor
                                .findNestedTypeByName(
                                        descriptor.findFieldByNumber(6).toProto().getTypeName())
                                .findFieldByNumber(1)
                                .toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("species")
                                .setNumber(1)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());
    }

    @Test
    public void testLogicalTypesConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testLogicalTypesConversion().getDescriptor();

        assertThat(descriptor.findFieldByNumber(1).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("timestamp")
                                .setNumber(1)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(2).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BYTES)
                                .setName("numeric_field")
                                .setNumber(2)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(3).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BYTES)
                                .setName("bignumeric_field")
                                .setNumber(3)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(4).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("geography")
                                .setNumber(4)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(5).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("json")
                                .setNumber(5)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());
    }

    private void assertPrimitive(Descriptor descriptor) {
        assertThat(descriptor.findFieldByNumber(1).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("name")
                                .setNumber(1)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(2).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("number")
                                .setNumber(2)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(3).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("quantity")
                                .setNumber(3)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(4).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BYTES)
                                .setName("fixed_field")
                                .setNumber(4)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        // TODO: This is different than beam
        assertThat(descriptor.findFieldByNumber(5).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_FLOAT)
                                .setName("price")
                                .setNumber(5)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(6).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_DOUBLE)
                                .setName("double_field")
                                .setNumber(6)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(7).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BOOL)
                                .setName("boolean_field")
                                .setNumber(7)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(8).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("enum_field")
                                .setNumber(8)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(9).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BYTES)
                                .setName("byte_field")
                                .setNumber(9)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());
    }

    @Test
    public void testAllPrimitiveSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testAllPrimitiveSchemaConversion().getDescriptor();
        assertPrimitive(descriptor);
    }

    @Test
    public void testAllLogicalSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testAllLogicalSchemaConversion().getDescriptor();

        assertThat(descriptor.findFieldByNumber(1).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("ts_micros")
                                .setNumber(1)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(2).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("ts_millis")
                                .setNumber(2)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(3).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("time_micros")
                                .setNumber(3)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(4).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("time_millis")
                                .setNumber(4)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(5).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("lts_micros")
                                .setNumber(5)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(6).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("lts_millis")
                                .setNumber(6)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(7).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT32)
                                .setName("date")
                                .setNumber(7)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(8).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BYTES)
                                .setName("decimal")
                                .setNumber(8)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(9).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("uuid")
                                .setNumber(9)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(10).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("geography")
                                .setNumber(10)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());
    }

    @Test
    public void testAllUnionLogicalSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testAllUnionLogicalSchemaConversion()
                        .getDescriptor();

        assertThat(descriptor.findFieldByNumber(1).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("ts_micros")
                                .setNumber(1)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(2).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("ts_millis")
                                .setNumber(2)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(3).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("time_micros")
                                .setNumber(3)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(4).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("time_millis")
                                .setNumber(4)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(5).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("lts_micros")
                                .setNumber(5)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(6).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("lts_millis")
                                .setNumber(6)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(7).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT32)
                                .setName("date")
                                .setNumber(7)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(8).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BYTES)
                                .setName("decimal")
                                .setNumber(8)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(9).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("uuid")
                                .setNumber(9)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(10).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("geography")
                                .setNumber(10)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());
    }

    @Test
    public void testAllUnionPrimitiveSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testAllUnionPrimitiveSchemaConversion()
                        .getDescriptor();

        assertThat(descriptor.findFieldByNumber(1).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("name")
                                .setNumber(1)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(2).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("number")
                                .setNumber(2)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(3).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("quantity")
                                .setNumber(3)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(4).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BYTES)
                                .setName("fixed_field")
                                .setNumber(4)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        // TODO: This is different than beam
        assertThat(descriptor.findFieldByNumber(5).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_FLOAT)
                                .setName("price")
                                .setNumber(5)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(6).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_DOUBLE)
                                .setName("double_field")
                                .setNumber(6)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(7).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BOOL)
                                .setName("boolean_field")
                                .setNumber(7)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(8).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("enum_field")
                                .setNumber(8)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(9).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BYTES)
                                .setName("byte_field")
                                .setNumber(9)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());
    }

    @Test
    public void testUnionInRecordSchemaConversation() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testUnionInRecordSchemaConversation()
                        .getDescriptor();

        FieldDescriptorProto fieldDescriptorProto = descriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptorProto.getName()).isEqualTo("record_with_union");
        assertThat(fieldDescriptorProto.getNumber()).isEqualTo(1);
        assertThat(fieldDescriptorProto.getLabel())
                .isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        assertThat(fieldDescriptorProto.getType())
                .isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(fieldDescriptorProto.hasTypeName()).isTrue();

        Descriptor nestedDescriptor =
                descriptor.findNestedTypeByName(fieldDescriptorProto.getTypeName());
        FieldDescriptor fieldDescriptor = nestedDescriptor.findFieldByNumber(1);
        assertThat(fieldDescriptor.isOptional()).isTrue();
        assertThat(fieldDescriptor.getType()).isEqualTo(FieldDescriptor.Type.BOOL);
        assertThat(fieldDescriptor.getName()).isEqualTo("union_in_record");
        assertThat(fieldDescriptor.hasDefaultValue()).isTrue();
        assertThat(fieldDescriptor.getDefaultValue()).isEqualTo(true);
    }

    @Test
    public void testRecordOfLogicalTypeSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testRecordOfLogicalTypeSchemaConversion()
                        .getDescriptor();

        FieldDescriptorProto fieldDescriptorProto = descriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptorProto.getName()).isEqualTo("record_of_logical_type");
        assertThat(fieldDescriptorProto.getNumber()).isEqualTo(1);
        assertThat(fieldDescriptorProto.getLabel())
                .isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        assertThat(fieldDescriptorProto.getType())
                .isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(fieldDescriptorProto.hasTypeName()).isTrue();
        descriptor = descriptor.findNestedTypeByName(fieldDescriptorProto.getTypeName());

        assertThat(descriptor.findFieldByNumber(1).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("ts_micros")
                                .setNumber(1)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(2).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("ts_millis")
                                .setNumber(2)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(3).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("time_micros")
                                .setNumber(3)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(4).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("time_millis")
                                .setNumber(4)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(5).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("lts_micros")
                                .setNumber(5)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(6).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("lts_millis")
                                .setNumber(6)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(7).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT32)
                                .setName("date")
                                .setNumber(7)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(8).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BYTES)
                                .setName("decimal")
                                .setNumber(8)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(9).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("uuid")
                                .setNumber(9)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(10).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("geography")
                                .setNumber(10)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());
    }

    @Test
    public void testMultipleDatatypeUnionSchemaConversion() {
        String fieldString =
                " \"fields\": [\n"
                        + "{\"name\": \"multiple_type_union\","
                        + " \"type\":[\"null\", \"string\", \"int\"]"
                        + "}"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new BigQuerySchemaProvider(avroSchema));
        assertThat(exception)
                .hasMessageThat()
                .contains("Multiple non-null union types are not supported.");
    }

    @Test
    public void testDefaultValueSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testDefaultValueSchemaConversion().getDescriptor();

        FieldDescriptorProto fieldDescriptorProto = descriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptorProto.getName()).isEqualTo("long_with_default");
        assertThat(fieldDescriptorProto.getNumber()).isEqualTo(1);
        assertThat(fieldDescriptorProto.getLabel())
                .isEqualTo(FieldDescriptorProto.Label.LABEL_OPTIONAL);
        assertThat(fieldDescriptorProto.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_INT64);
        assertThat(fieldDescriptorProto.hasDefaultValue()).isTrue();
        assertThat(fieldDescriptorProto.getDefaultValue()).isEqualTo("100");
    }

    @Test
    public void testRecordOfRecordSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testRecordOfRecordSchemaConversion().getDescriptor();

        FieldDescriptorProto field = descriptor.findFieldByNumber(1).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("record_in_record");
        assertThat(field.getNumber()).isEqualTo(1);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        assertThat(field.hasTypeName()).isTrue();
        Descriptor nestedDescriptor = descriptor.findNestedTypeByName(field.getTypeName());

        field = nestedDescriptor.findFieldByNumber(1).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("record_field");
        assertThat(field.getNumber()).isEqualTo(1);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        assertThat(field.hasTypeName()).isTrue();

        nestedDescriptor = nestedDescriptor.findNestedTypeByName(field.getTypeName());
        field = nestedDescriptor.findFieldByNumber(1).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_INT64);
        assertThat(field.getName()).isEqualTo("value");
        assertThat(field.getNumber()).isEqualTo(1);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);

        field = nestedDescriptor.findFieldByNumber(2).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_STRING);
        assertThat(field.getName()).isEqualTo("another_value");
        assertThat(field.getNumber()).isEqualTo(2);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
    }

    @Test
    public void testMapOfUnionTypeSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testMapOfUnionTypeSchemaConversion().getDescriptor();

        FieldDescriptorProto field = descriptor.findFieldByNumber(1).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("map_of_union");
        assertThat(field.getNumber()).isEqualTo(1);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REPEATED);
        assertThat(field.hasTypeName()).isTrue();
        Descriptor nestedDescriptor = descriptor.findNestedTypeByName(field.getTypeName());
        FieldDescriptorProto fieldDescriptor = nestedDescriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptor.getName()).isEqualTo("key");
        assertThat(fieldDescriptor.getNumber()).isEqualTo(1);
        assertThat(fieldDescriptor.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_STRING);
        assertThat(fieldDescriptor.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        fieldDescriptor = nestedDescriptor.findFieldByNumber(2).toProto();
        assertThat(fieldDescriptor.getName()).isEqualTo("value");
        assertThat(fieldDescriptor.getNumber()).isEqualTo(2);
        assertThat(fieldDescriptor.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_FLOAT);
        assertThat(fieldDescriptor.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }

    @Test
    public void testMapOfArraySchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testMapOfArraySchemaConversion().getDescriptor();

        FieldDescriptorProto field = descriptor.findFieldByNumber(1).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("map_of_array");
        assertThat(field.getNumber()).isEqualTo(1);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REPEATED);
        assertThat(field.hasTypeName()).isTrue();
        Descriptor nestedDescriptor = descriptor.findNestedTypeByName(field.getTypeName());
        FieldDescriptorProto fieldDescriptor = nestedDescriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptor.getName()).isEqualTo("key");
        assertThat(fieldDescriptor.getNumber()).isEqualTo(1);
        assertThat(fieldDescriptor.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_STRING);
        assertThat(fieldDescriptor.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        fieldDescriptor = nestedDescriptor.findFieldByNumber(2).toProto();
        assertThat(fieldDescriptor.getName()).isEqualTo("value");
        assertThat(fieldDescriptor.getNumber()).isEqualTo(2);
        assertThat(fieldDescriptor.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_INT64);
        assertThat(fieldDescriptor.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REPEATED);
    }

    @Test
    public void testMapInRecordSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testMapInRecordSchemaConversion().getDescriptor();

        FieldDescriptorProto field = descriptor.findFieldByNumber(1).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("record_with_map");
        assertThat(field.getNumber()).isEqualTo(1);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        assertThat(field.hasTypeName()).isTrue();

        Descriptor nestedDescriptor = descriptor.findNestedTypeByName(field.getTypeName());
        FieldDescriptorProto fieldDescriptorProto = nestedDescriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptorProto.getName()).isEqualTo("map_in_record");
        assertThat(fieldDescriptorProto.getNumber()).isEqualTo(1);
        assertThat(fieldDescriptorProto.getType())
                .isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(fieldDescriptorProto.getLabel())
                .isEqualTo(FieldDescriptorProto.Label.LABEL_REPEATED);
        assertThat(fieldDescriptorProto.hasTypeName()).isTrue();

        nestedDescriptor =
                nestedDescriptor.findNestedTypeByName(fieldDescriptorProto.getTypeName());
        fieldDescriptorProto = nestedDescriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptorProto.getName()).isEqualTo("key");
        assertThat(fieldDescriptorProto.getNumber()).isEqualTo(1);
        assertThat(fieldDescriptorProto.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_STRING);
        assertThat(fieldDescriptorProto.getLabel())
                .isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        fieldDescriptorProto = nestedDescriptor.findFieldByNumber(2).toProto();
        assertThat(fieldDescriptorProto.getName()).isEqualTo("value");
        assertThat(fieldDescriptorProto.getNumber()).isEqualTo(2);
        assertThat(fieldDescriptorProto.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_INT64);
        assertThat(fieldDescriptorProto.getLabel())
                .isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
    }

    @Test
    public void testArrayOfUnionValueSchemaConversion() {
        String fieldString =
                " \"fields\": [\n"
                        + "{\"name\": \"array_with_union\", \"type\": "
                        + "{\"type\": \"array\", \"items\":  [\"long\", \"null\"]}}"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new BigQuerySchemaProvider(avroSchema));
        assertThat(exception).hasMessageThat().contains("Array cannot have a NULLABLE element");
    }

    @Test
    public void testNestedArraysSchemaConversion() {
        String fieldString =
                " \"fields\": [\n"
                        + "{\"name\": \"nested_arrays\", \"type\":{\"type\": \"array\", \"items\": "
                        + "{\"name\": \"array_inside\", \"type\": \"array\", \"items\": \"long\"}"
                        + "}}"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);

        IllegalStateException exception =
                assertThrows(
                        IllegalStateException.class, () -> new BigQuerySchemaProvider(avroSchema));
        assertThat(exception).hasMessageThat().contains("Nested arrays not supported by BigQuery.");
    }

    @Test
    public void testMapOfMapSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testMapOfMapSchemaConversion().getDescriptor();

        FieldDescriptorProto field = descriptor.findFieldByNumber(1).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("map_of_map");
        assertThat(field.getNumber()).isEqualTo(1);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REPEATED);
        assertThat(field.hasTypeName()).isTrue();
        Descriptor nestedDescriptor = descriptor.findNestedTypeByName(field.getTypeName());
        FieldDescriptorProto fieldDescriptor = nestedDescriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptor.getName()).isEqualTo("key");
        assertThat(fieldDescriptor.getNumber()).isEqualTo(1);
        assertThat(fieldDescriptor.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_STRING);
        assertThat(fieldDescriptor.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        fieldDescriptor = nestedDescriptor.findFieldByNumber(2).toProto();
        assertThat(fieldDescriptor.getName()).isEqualTo("value");
        assertThat(fieldDescriptor.getNumber()).isEqualTo(2);
        assertThat(fieldDescriptor.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(fieldDescriptor.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REPEATED);
        assertThat(fieldDescriptor.hasTypeName()).isTrue();
        assertThat(nestedDescriptor.findNestedTypeByName(fieldDescriptor.getTypeName()).toProto())
                .isEqualTo(
                        DescriptorProto.newBuilder()
                                .setName(fieldDescriptor.getTypeName())
                                .addField(
                                        FieldDescriptorProto.newBuilder()
                                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                                .setName("key")
                                                .setNumber(1)
                                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                                .build())
                                .addField(
                                        FieldDescriptorProto.newBuilder()
                                                .setType(FieldDescriptorProto.Type.TYPE_BYTES)
                                                .setName("value")
                                                .setNumber(2)
                                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                                .build())
                                .build());
    }

    @Test
    public void testMapOfRecordSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testMapOfRecordSchemaConversion().getDescriptor();

        FieldDescriptorProto field = descriptor.findFieldByNumber(1).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("map_of_records");
        assertThat(field.getNumber()).isEqualTo(1);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REPEATED);
        assertThat(field.hasTypeName()).isTrue();
        Descriptor nestedDescriptor = descriptor.findNestedTypeByName(field.getTypeName());
        FieldDescriptorProto fieldDescriptor = nestedDescriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptor.getName()).isEqualTo("key");
        assertThat(fieldDescriptor.getNumber()).isEqualTo(1);
        assertThat(fieldDescriptor.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_STRING);
        assertThat(fieldDescriptor.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        fieldDescriptor = nestedDescriptor.findFieldByNumber(2).toProto();
        assertThat(fieldDescriptor.getName()).isEqualTo("value");
        assertThat(fieldDescriptor.getNumber()).isEqualTo(2);
        assertThat(fieldDescriptor.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(fieldDescriptor.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        assertThat(fieldDescriptor.hasTypeName()).isTrue();
        assertThat(nestedDescriptor.findNestedTypeByName(fieldDescriptor.getTypeName()).toProto())
                .isEqualTo(
                        DescriptorProto.newBuilder()
                                .setName(fieldDescriptor.getTypeName())
                                .addField(
                                        FieldDescriptorProto.newBuilder()
                                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                                .setName("value")
                                                .setNumber(1)
                                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                                .build())
                                .addField(
                                        FieldDescriptorProto.newBuilder()
                                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                                .setName("another_value")
                                                .setNumber(2)
                                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                                .build())
                                .build());
    }

    @Test
    public void testRecordOfArraySchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testRecordOfArraySchemaConversion().getDescriptor();

        FieldDescriptorProto field = descriptor.findFieldByNumber(1).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("record_with_array");
        assertThat(field.getNumber()).isEqualTo(1);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        assertThat(field.hasTypeName()).isTrue();
        assertThat(descriptor.findNestedTypeByName(field.getTypeName()).toProto())
                .isEqualTo(
                        DescriptorProto.newBuilder()
                                .setName(field.getTypeName())
                                .addField(
                                        FieldDescriptorProto.newBuilder()
                                                .setType(FieldDescriptorProto.Type.TYPE_BOOL)
                                                .setName("array_in_record")
                                                .setNumber(1)
                                                .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
                                                .build())
                                .build());
    }

    @Test
    public void testArrayOfRecordSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testArrayOfRecordSchemaConversion().getDescriptor();

        FieldDescriptorProto field = descriptor.findFieldByNumber(1).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("array_of_records");
        assertThat(field.getNumber()).isEqualTo(1);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REPEATED);
        assertThat(field.hasTypeName()).isTrue();
        assertThat(descriptor.findNestedTypeByName(field.getTypeName()).toProto())
                .isEqualTo(
                        DescriptorProto.newBuilder()
                                .setName(field.getTypeName())
                                .addField(
                                        FieldDescriptorProto.newBuilder()
                                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                                .setName("value")
                                                .setNumber(1)
                                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                                .build())
                                .addField(
                                        FieldDescriptorProto.newBuilder()
                                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                                .setName("another_value")
                                                .setNumber(2)
                                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                                .build())
                                .build());
    }

    @Test
    public void testUnionOfRecordSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testUnionOfRecordSchemaConversion().getDescriptor();

        FieldDescriptorProto field = descriptor.findFieldByNumber(1).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("record_field_union");
        assertThat(field.getNumber()).isEqualTo(1);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_OPTIONAL);
        assertThat(field.hasTypeName()).isTrue();
        assertThat(descriptor.findNestedTypeByName(field.getTypeName()).toProto())
                .isEqualTo(
                        DescriptorProto.newBuilder()
                                .setName(field.getTypeName())
                                .addField(
                                        FieldDescriptorProto.newBuilder()
                                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                                .setName("value")
                                                .setNumber(1)
                                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                                .build())
                                .addField(
                                        FieldDescriptorProto.newBuilder()
                                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                                .setName("another_value")
                                                .setNumber(2)
                                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                                .build())
                                .build());
    }

    @Test
    public void testSpecialSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testSpecialSchemaConversion().getDescriptor();

        FieldDescriptorProto field = descriptor.findFieldByNumber(1).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("record_field");
        assertThat(field.getNumber()).isEqualTo(1);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        assertThat(field.hasTypeName()).isTrue();
        assertThat(descriptor.findNestedTypeByName(field.getTypeName()).toProto())
                .isEqualTo(
                        DescriptorProto.newBuilder()
                                .setName(field.getTypeName())
                                .addField(
                                        FieldDescriptorProto.newBuilder()
                                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                                .setName("value")
                                                .setNumber(1)
                                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                                .build())
                                .addField(
                                        FieldDescriptorProto.newBuilder()
                                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                                .setName("another_value")
                                                .setNumber(2)
                                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                                .build())
                                .build());

        field = descriptor.findFieldByNumber(2).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("map_field");
        assertThat(field.getNumber()).isEqualTo(2);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REPEATED);
        assertThat(field.hasTypeName()).isTrue();
        assertThat(descriptor.findNestedTypeByName(field.getTypeName()).toProto())
                .isEqualTo(
                        DescriptorProto.newBuilder()
                                .setName(field.getTypeName())
                                .addField(
                                        FieldDescriptorProto.newBuilder()
                                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                                .setName("key")
                                                .setNumber(1)
                                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                                .build())
                                .addField(
                                        FieldDescriptorProto.newBuilder()
                                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                                .setName("value")
                                                .setNumber(2)
                                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                                .build())
                                .build());

        field = descriptor.findFieldByNumber(3).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_FLOAT);
        assertThat(field.getName()).isEqualTo("array_field");
        assertThat(field.getNumber()).isEqualTo(3);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REPEATED);
    }

    @Test
    public void testAllPrimitiveSingleUnionSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testAllPrimitiveSingleUnionSchemaConversion()
                        .getDescriptor();
        assertPrimitive(descriptor);
    }

    @Test
    public void testRecordOfUnionFieldSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testRecordOfUnionFieldSchemaConversion()
                        .getDescriptor();

        FieldDescriptorProto fieldDescriptorProto = descriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptorProto.getName()).isEqualTo("record_with_union");
        assertThat(fieldDescriptorProto.getNumber()).isEqualTo(1);
        assertThat(fieldDescriptorProto.getLabel())
                .isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        assertThat(fieldDescriptorProto.getType())
                .isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(fieldDescriptorProto.hasTypeName()).isTrue();

        Descriptor nestedDescriptor =
                descriptor.findNestedTypeByName(fieldDescriptorProto.getTypeName());
        FieldDescriptor fieldDescriptor = nestedDescriptor.findFieldByNumber(1);
        assertThat(fieldDescriptor.isOptional()).isTrue();
        assertThat(fieldDescriptor.getType()).isEqualTo(FieldDescriptor.Type.BOOL);
        assertThat(fieldDescriptor.getName()).isEqualTo("union_in_record");
        assertThat(fieldDescriptor.hasDefaultValue()).isTrue();
        assertThat(fieldDescriptor.getDefaultValue()).isEqualTo(true);
    }

    @Test
    public void testArrayAndRequiredTypesConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestUtils.testArrayAndRequiredTypesConversion()
                        .getDescriptor();

        FieldDescriptorProto fieldDescriptorProto = descriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptorProto.getType())
                .isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(fieldDescriptorProto.getName()).isEqualTo("optional_record_field");
        assertThat(fieldDescriptorProto.getLabel())
                .isEqualTo(FieldDescriptorProto.Label.LABEL_OPTIONAL);
        assertThat(fieldDescriptorProto.hasTypeName()).isTrue();

        Descriptor nestedDescriptor =
                descriptor.findNestedTypeByName(fieldDescriptorProto.getTypeName());
        fieldDescriptorProto = nestedDescriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptorProto.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_INT32);
        assertThat(fieldDescriptorProto.getName()).isEqualTo("date");
        assertThat(fieldDescriptorProto.getLabel())
                .isEqualTo(FieldDescriptorProto.Label.LABEL_REPEATED);

        fieldDescriptorProto = nestedDescriptor.findFieldByNumber(2).toProto();
        assertThat(fieldDescriptorProto.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_INT64);
        assertThat(fieldDescriptorProto.getName()).isEqualTo("timestamp");
        assertThat(fieldDescriptorProto.getLabel())
                .isEqualTo(FieldDescriptorProto.Label.LABEL_OPTIONAL);

        fieldDescriptorProto = nestedDescriptor.findFieldByNumber(3).toProto();
        assertThat(fieldDescriptorProto.getType())
                .isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(fieldDescriptorProto.getName()).isEqualTo("datetime_record");
        assertThat(fieldDescriptorProto.getLabel())
                .isEqualTo(FieldDescriptorProto.Label.LABEL_REPEATED);
        assertThat(fieldDescriptorProto.hasTypeName()).isTrue();

        nestedDescriptor =
                nestedDescriptor.findNestedTypeByName(fieldDescriptorProto.getTypeName());
        fieldDescriptorProto = nestedDescriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptorProto.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_STRING);
        assertThat(fieldDescriptorProto.getName()).isEqualTo("datetime");
        assertThat(fieldDescriptorProto.getLabel())
                .isEqualTo(FieldDescriptorProto.Label.LABEL_REPEATED);

        fieldDescriptorProto = nestedDescriptor.findFieldByNumber(2).toProto();
        assertThat(fieldDescriptorProto.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_STRING);
        assertThat(fieldDescriptorProto.getName()).isEqualTo("time");
        assertThat(fieldDescriptorProto.getLabel())
                .isEqualTo(FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }

    // -------- Tests to Check Unsupported Features ------------
    public static void assertExpectedUnsupportedException(
            String fieldString, String expectedError) {
        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> new BigQuerySchemaProvider(avroSchema));
        assertThat(exception).hasMessageThat().contains(expectedError);
    }

    @Test
    public void testArrayOfMapSchemaConversion() {
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"array_of_map\", \"type\": {\"type\": \"array\", \"items\": {\"type\": \"map\", \"values\": \"bytes\"}}}\n"
                        + " ]\n";
        assertExpectedUnsupportedException(fieldString, "Array of Type MAP not supported yet.");
    }

    @Test
    public void testArrayOfUnionOfMapSchemaConversion() {
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"array_of_map_union\", \"type\": [\"null\", {\"type\": \"array\", \"items\": {\"type\": \"map\", \"values\": \"bytes\"}}]}\n"
                        + " ]\n";
        assertExpectedUnsupportedException(
                fieldString, "MAP/ARRAYS in UNION types are not supported");
    }

    @Test
    public void testUnionOfArrayOfRecordSchemaConversion() {
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"array_of_records_union\", \"type\": [\"null\", {\"type\": \"array\", \"items\": "
                        + getRecord("inside_record_union")
                        + "}]}\n"
                        + " ]\n";

        assertExpectedUnsupportedException(
                fieldString, "MAP/ARRAYS in UNION types are not supported");
    }

    @Test
    public void testUnionOfArraySchemaConversion() {
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"array_field_union\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"float\"}]}\n"
                        + " ]\n";

        assertExpectedUnsupportedException(
                fieldString, "MAP/ARRAYS in UNION types are not supported");
    }

    @Test
    public void testUnionOfMapSchemaConversion() {
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"map_field_union\", \"type\": [\"null\", {\"type\": \"map\", \"values\": \"long\"}]}\n"
                        + " ]\n";

        assertExpectedUnsupportedException(
                fieldString, "MAP/ARRAYS in UNION types are not supported");
    }
}
