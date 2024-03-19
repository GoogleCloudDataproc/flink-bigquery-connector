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

import com.google.protobuf.DescriptorProtos;
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

    // ------------ Test Schemas with Record of Different Types -----------
    @Test
    public void testRecordOfArraySchemaConversation() {
        Descriptor descriptor =
                AvroToProtoSerializerTestSchemas.getSchemaWithRecordOfArray().getDescriptor();
        FieldDescriptorProto field = descriptor.findFieldByNumber(1).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("record_with_array");
        assertThat(field.getNumber()).isEqualTo(1);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        assertThat(field.hasTypeName()).isTrue();
        assertThat(descriptor.findNestedTypeByName(field.getTypeName()).toProto())
                .isEqualTo(
                        DescriptorProtos.DescriptorProto.newBuilder()
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
    public void testRecordOfUnionSchemaConversation() {
        Descriptor descriptor =
                AvroToProtoSerializerTestSchemas.getSchemaWithRecordOfUnionType().getDescriptor();

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
    public void testRecordOfMapSchemaConversation() {
        Descriptor descriptor =
                AvroToProtoSerializerTestSchemas.getSchemaWithRecordOfMap().getDescriptor();
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
    public void testRecordOfRecordSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestSchemas.getSchemaWithRecordOfRecord()
                        .getDescriptor();

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
    public void testRecordOfPrimitiveTypeSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestSchemas.getSchemaWithRecordOfPrimitiveTypes()
                        .getDescriptor();

        FieldDescriptorProto fieldDescriptorProto = descriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptorProto.getName()).isEqualTo("record_of_primitive_types");
        assertThat(fieldDescriptorProto.getNumber()).isEqualTo(1);
        assertThat(fieldDescriptorProto.getLabel())
                .isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        assertThat(fieldDescriptorProto.getType())
                .isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(fieldDescriptorProto.hasTypeName()).isTrue();
        descriptor = descriptor.findNestedTypeByName(fieldDescriptorProto.getTypeName());

        assertPrimitive(descriptor, false);
    }

    @Test
    public void testRecordOfRemainingPrimitiveTypeSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestSchemas.getSchemaWithRecordOfRemainingPrimitiveTypes()
                        .getDescriptor();

        FieldDescriptorProto fieldDescriptorProto = descriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptorProto.getName()).isEqualTo("record_of_remaining_primitive_types");
        assertThat(fieldDescriptorProto.getNumber()).isEqualTo(1);
        assertThat(fieldDescriptorProto.getLabel())
                .isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        assertThat(fieldDescriptorProto.getType())
                .isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(fieldDescriptorProto.hasTypeName()).isTrue();
        descriptor = descriptor.findNestedTypeByName(fieldDescriptorProto.getTypeName());

        assertRemainingPrimitive(descriptor, false);
    }

    @Test
    public void testRecordOfLogicalTypeSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestSchemas.getSchemaWithRecordOfLogicalTypes()
                        .getDescriptor();

        FieldDescriptorProto fieldDescriptorProto = descriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptorProto.getName()).isEqualTo("record_of_logical_types");
        assertThat(fieldDescriptorProto.getNumber()).isEqualTo(1);
        assertThat(fieldDescriptorProto.getLabel())
                .isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        assertThat(fieldDescriptorProto.getType())
                .isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(fieldDescriptorProto.hasTypeName()).isTrue();
        descriptor = descriptor.findNestedTypeByName(fieldDescriptorProto.getTypeName());

        assertLogical(descriptor, false);
    }

    @Test
    public void testRecordOfRemainingLogicalTypeSchemaConversion() {
        Descriptor descriptor =
                AvroToProtoSerializerTestSchemas.getSchemaWithRecordOfRemainingLogicalTypes()
                        .getDescriptor();

        FieldDescriptorProto fieldDescriptorProto = descriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptorProto.getName()).isEqualTo("record_of_remaining_logical_types");
        assertThat(fieldDescriptorProto.getNumber()).isEqualTo(1);
        assertThat(fieldDescriptorProto.getLabel())
                .isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        assertThat(fieldDescriptorProto.getType())
                .isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(fieldDescriptorProto.hasTypeName()).isTrue();
        descriptor = descriptor.findNestedTypeByName(fieldDescriptorProto.getTypeName());

        assertRemainingLogical(descriptor, false);
    }

    // ------------Test Schemas with MAP of Different Types --------------

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

    //    assertExpectedUnsupportedException(
    //                fieldString, "MAP/ARRAYS in UNION types are not supported");

    //    assertExpectedUnsupportedException(
    //                fieldString, "MAP/ARRAYS in UNION types are not supported");
}
