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

import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import org.apache.avro.Schema;
import org.junit.Test;

import static com.google.cloud.flink.bigquery.sink.serializer.AvroToProtoSerializerTestSchemas.getAvroSchemaFromFieldString;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

/** Tests for {@link BigQuerySchemaProvider}. */
public class BigQuerySchemaProviderTest {

    // ------------------ Test Primitive Data Types (Nullable and Required) ------------------
    @Test
    public void testPrimitiveTypesConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestSchemas.getSchemaWithRequiredPrimitiveTypes()
                        .getDescriptor();
        assertPrimitive(descriptor, false);
    }

    @Test
    public void testRemainingPrimitiveSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestSchemas.getSchemaWithRemainingPrimitiveTypes()
                        .getDescriptor();
        assertRemainingPrimitive(descriptor, false);
    }

    @Test
    public void testNullablePrimitiveTypesConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestSchemas.getSchemaWithNullablePrimitiveTypes()
                        .getDescriptor();
        assertPrimitive(descriptor, true);
    }

    @Test
    public void testUnionOfRemainingPrimitiveSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestSchemas.getSchemaWithUnionOfRemainingPrimitiveTypes()
                        .getDescriptor();
        assertRemainingPrimitive(descriptor, true);
    }

    // --------------- Test Logical Data Types (Nullable and Required) ---------------
    @Test
    public void testLogicalTypesConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestSchemas.getSchemaWithRequiredLogicalTypes()
                        .getDescriptor();
        assertLogical(descriptor, false);
    }

    @Test
    public void testRemainingLogicalSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestSchemas.getSchemaWithRemainingLogicalTypes()
                        .getDescriptor();
        assertRemainingLogical(descriptor, false);
    }

    @Test
    public void testNullableLogicalTypesConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestSchemas.getSchemaWithNullableLogicalTypes()
                        .getDescriptor();
        assertLogical(descriptor, true);
    }

    @Test
    public void testUnionOfRemainingLogicalSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestSchemas.getSchemaWithUnionOfLogicalTypes().getDescriptor();
        assertRemainingLogical(descriptor, true);
    }

    // -------------------- Tests for UNION, RECORD Types.--------- ----------------
    @Test
    public void testUnionInRecordSchemaConversation() {
        Descriptor descriptor =
                AvroToProtoSerializerTestSchemas.getSchemaWithRecordOfUnionTypes().getDescriptor();

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
                AvroToProtoSerializerTestSchemas.getSchemaWithRecordOfLogicalTypes()
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
                        () -> new BigQuerySchemaProviderImpl(avroSchema));
        assertThat(exception)
                .hasMessageThat()
                .contains("Multiple non-null union types are not supported.");
    }

    @Test
    public void testDefaultValueSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestSchemas.getSchemaWithDefaultValue().getDescriptor();

        FieldDescriptorProto fieldDescriptorProto = descriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptorProto.getName()).isEqualTo("long_with_default");
        assertThat(fieldDescriptorProto.getNumber()).isEqualTo(1);
        assertThat(fieldDescriptorProto.getLabel())
                .isEqualTo(FieldDescriptorProto.Label.LABEL_OPTIONAL);
        assertThat(fieldDescriptorProto.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_INT64);
        assertThat(fieldDescriptorProto.hasDefaultValue()).isTrue();
        assertThat(fieldDescriptorProto.getDefaultValue()).isEqualTo("100");
    }

    private void assertPrimitive(Descriptor descriptor, Boolean isNullable) {
        FieldDescriptorProto.Label label =
                isNullable
                        ? FieldDescriptorProto.Label.LABEL_OPTIONAL
                        : FieldDescriptorProto.Label.LABEL_REQUIRED;
        assertThat(descriptor.findFieldByNumber(1).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("number")
                                .setNumber(1)
                                .setLabel(label)
                                .build());

        assertThat(descriptor.findFieldByNumber(2).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_DOUBLE)
                                .setName("price")
                                .setNumber(2)
                                .setLabel(label)
                                .build());

        assertThat(descriptor.findFieldByNumber(3).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("species")
                                .setNumber(3)
                                .setLabel(label)
                                .build());

        assertThat(descriptor.findFieldByNumber(4).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BOOL)
                                .setName("flighted")
                                .setNumber(4)
                                .setLabel(label)
                                .build());

        assertThat(descriptor.findFieldByNumber(5).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BYTES)
                                .setName("sound")
                                .setNumber(5)
                                .setLabel(label)
                                .build());

        assertThat(descriptor.findFieldByNumber(6).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setName("required_record_field")
                                .setNumber(6)
                                .setTypeName(
                                        descriptor.findFieldByNumber(6).toProto().getTypeName())
                                .setLabel(label)
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
                                .setLabel(label)
                                .build());
    }

    private void assertRemainingPrimitive(Descriptor descriptor, Boolean isNullable) {

        FieldDescriptorProto.Label label =
                isNullable
                        ? FieldDescriptorProto.Label.LABEL_OPTIONAL
                        : FieldDescriptorProto.Label.LABEL_REQUIRED;

        assertThat(descriptor.findFieldByNumber(1).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT32)
                                .setName("quantity")
                                .setNumber(1)
                                .setLabel(label)
                                .build());

        assertThat(descriptor.findFieldByNumber(2).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BYTES)
                                .setName("fixed_field")
                                .setNumber(2)
                                .setLabel(label)
                                .build());

        // TODO: This is different than beam
        assertThat(descriptor.findFieldByNumber(3).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_FLOAT)
                                .setName("float_field")
                                .setNumber(3)
                                .setLabel(label)
                                .build());

        assertThat(descriptor.findFieldByNumber(4).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("enum_field")
                                .setNumber(4)
                                .setLabel(label)
                                .build());
    }

    private void assertLogical(Descriptor descriptor, boolean isNullable) {

        FieldDescriptorProto.Label label =
                isNullable
                        ? FieldDescriptorProto.Label.LABEL_OPTIONAL
                        : FieldDescriptorProto.Label.LABEL_REQUIRED;

        assertThat(descriptor.findFieldByNumber(1).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("timestamp")
                                .setNumber(1)
                                .setLabel(label)
                                .build());

        assertThat(descriptor.findFieldByNumber(2).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("time")
                                .setNumber(2)
                                .setLabel(label)
                                .build());
        assertThat(descriptor.findFieldByNumber(3).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("datetime")
                                .setNumber(3)
                                .setLabel(label)
                                .build());
        assertThat(descriptor.findFieldByNumber(4).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT32)
                                .setName("date")
                                .setNumber(4)
                                .setLabel(label)
                                .build());
        assertThat(descriptor.findFieldByNumber(5).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BYTES)
                                .setName("numeric_field")
                                .setNumber(5)
                                .setLabel(label)
                                .build());

        assertThat(descriptor.findFieldByNumber(6).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BYTES)
                                .setName("bignumeric_field")
                                .setNumber(6)
                                .setLabel(label)
                                .build());
        assertThat(descriptor.findFieldByNumber(7).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("geography")
                                .setNumber(7)
                                .setLabel(label)
                                .build());
        assertThat(descriptor.findFieldByNumber(8).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("json")
                                .setNumber(8)
                                .setLabel(label)
                                .build());
    }

    private void assertRemainingLogical(Descriptor descriptor, Boolean isNullable) {
        FieldDescriptorProto.Label label =
                isNullable
                        ? FieldDescriptorProto.Label.LABEL_OPTIONAL
                        : FieldDescriptorProto.Label.LABEL_REQUIRED;

        assertThat(descriptor.findFieldByNumber(1).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("ts_millis")
                                .setNumber(1)
                                .setLabel(label)
                                .build());

        assertThat(descriptor.findFieldByNumber(2).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("time_millis")
                                .setNumber(2)
                                .setLabel(label)
                                .build());

        assertThat(descriptor.findFieldByNumber(3).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("lts_millis")
                                .setNumber(3)
                                .setLabel(label)
                                .build());

        assertThat(descriptor.findFieldByNumber(4).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("uuid")
                                .setNumber(4)
                                .setLabel(label)
                                .build());
    }
}
