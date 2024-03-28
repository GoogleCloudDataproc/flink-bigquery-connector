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
import org.assertj.core.api.Assertions;
import org.junit.Test;

import static com.google.cloud.flink.bigquery.sink.serializer.TestBigQuerySchemas.getAvroSchemaFromFieldString;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

/** Tests for {@link BigQuerySchemaProvider}. */
public class BigQuerySchemaProviderTest {

    // ------------------ Test Primitive Data Types (Nullable and Required) ------------------
    @Test
    public void testPrimitiveTypesConversion() {
        Descriptor descriptor =
                TestBigQuerySchemas.getSchemaWithRequiredPrimitiveTypes()
                        .getDescriptor();
        assertPrimitive(descriptor, FieldDescriptorProto.Label.LABEL_REQUIRED);
    }

    @Test
    public void testRemainingPrimitiveSchemaConversion() {
        Descriptor descriptor =
                TestBigQuerySchemas.getSchemaWithRemainingPrimitiveTypes()
                        .getDescriptor();
        assertRemainingPrimitive(descriptor, FieldDescriptorProto.Label.LABEL_REQUIRED);
    }

    @Test
    public void testNullablePrimitiveTypesConversion() {
        Descriptor descriptor =
                TestBigQuerySchemas.getSchemaWithNullablePrimitiveTypes()
                        .getDescriptor();
        assertPrimitive(descriptor, FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }

    @Test
    public void testUnionOfRemainingPrimitiveSchemaConversion() {
        Descriptor descriptor =
                TestBigQuerySchemas.getSchemaWithUnionOfRemainingPrimitiveTypes()
                        .getDescriptor();
        assertRemainingPrimitive(descriptor, FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }

    // --------------- Test Logical Data Types (Nullable and Required) ---------------
    @Test
    public void testLogicalTypesConversion() {
        Descriptor descriptor =
                TestBigQuerySchemas.getSchemaWithRequiredLogicalTypes()
                        .getDescriptor();
        assertLogical(descriptor, FieldDescriptorProto.Label.LABEL_REQUIRED);
    }

    @Test
    public void testRemainingLogicalSchemaConversion() {
        Descriptor descriptor =
                TestBigQuerySchemas.getSchemaWithRemainingLogicalTypes()
                        .getDescriptor();
        assertRemainingLogical(descriptor, FieldDescriptorProto.Label.LABEL_REQUIRED);
    }

    @Test
    public void testNullableLogicalTypesConversion() {
        Descriptor descriptor =
                TestBigQuerySchemas.getSchemaWithNullableLogicalTypes()
                        .getDescriptor();
        assertLogical(descriptor, FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }

    @Test
    public void testUnionOfRemainingLogicalSchemaConversion() {
        Descriptor descriptor =
                TestBigQuerySchemas.getSchemaWithUnionOfLogicalTypes().getDescriptor();
        assertRemainingLogical(descriptor, FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }

    // ------------ Test Schemas with Record of Different Types -----------
    @Test
    public void testRecordOfArraySchemaConversation() {
        Descriptor descriptor =
                TestBigQuerySchemas.getSchemaWithRecordOfArray().getDescriptor();
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
                TestBigQuerySchemas.getSchemaWithRecordOfUnionType().getDescriptor();

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
                TestBigQuerySchemas.getSchemaWithRecordOfMap().getDescriptor();
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
                TestBigQuerySchemas.getSchemaWithRecordOfRecord().getDescriptor();

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
                TestBigQuerySchemas.getSchemaWithRecordOfPrimitiveTypes()
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

        assertPrimitive(descriptor, FieldDescriptorProto.Label.LABEL_REQUIRED);
    }

    @Test
    public void testRecordOfRemainingPrimitiveTypeSchemaConversion() {
        Descriptor descriptor =
                TestBigQuerySchemas.getSchemaWithRecordOfRemainingPrimitiveTypes()
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

        assertRemainingPrimitive(descriptor, FieldDescriptorProto.Label.LABEL_REQUIRED);
    }

    @Test
    public void testRecordOfLogicalTypeSchemaConversion() {
        Descriptor descriptor =
                TestBigQuerySchemas.getSchemaWithRecordOfLogicalTypes()
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

        assertLogical(descriptor, FieldDescriptorProto.Label.LABEL_REQUIRED);
    }

    @Test
    public void testRecordOfRemainingLogicalTypeSchemaConversion() {
        Descriptor descriptor =
                TestBigQuerySchemas.getSchemaWithRecordOfRemainingLogicalTypes()
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

        assertRemainingLogical(descriptor, FieldDescriptorProto.Label.LABEL_REQUIRED);
    }

    // ------------Test Schemas with MAP of Different Types --------------
    @Test
    public void testMapOfArraySchemaConversion() {
        Descriptor descriptor =
                TestBigQuerySchemas.getSchemaWithMapOfArray().getDescriptor();
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
    public void testMapOfUnionSchemaConversion() {
        Descriptor descriptor =
                TestBigQuerySchemas.getSchemaWithMapOfUnionType().getDescriptor();
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
    public void testMapOfMapSchemaConversion() {
        Descriptor descriptor =
                TestBigQuerySchemas.getSchemaWithMapOfMap().getDescriptor();
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
                        DescriptorProtos.DescriptorProto.newBuilder()
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
                TestBigQuerySchemas.getSchemaWithMapOfRecord().getDescriptor();
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
                        DescriptorProtos.DescriptorProto.newBuilder()
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
    public void testMapSchemaConversion() {
        Descriptor descriptor =
                TestBigQuerySchemas.getSchemaWithMapType().getDescriptor();

        FieldDescriptorProto field = descriptor.findFieldByNumber(1).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("map_field");
        assertThat(field.getNumber()).isEqualTo(1);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REPEATED);
        assertThat(field.hasTypeName()).isTrue();
        assertThat(descriptor.findNestedTypeByName(field.getTypeName()).toProto())
                .isEqualTo(
                        DescriptorProtos.DescriptorProto.newBuilder()
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
    }

    // ------------Test Schemas with ARRAY of Different Types -------------
    @Test
    public void testArrayOfArraySchemaConversion() {
        String fieldString = TestBigQuerySchemas.getSchemaWithArrayOfArray();
        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);

        IllegalStateException exception =
                assertThrows(
                        IllegalStateException.class,
                        () -> new BigQuerySchemaProviderImpl(avroSchema));
        assertThat(exception).hasMessageThat().contains("Nested arrays not supported by BigQuery.");
    }

    @Test
    public void testArrayOfUnionSchemaConversion() {
        String fieldString = TestBigQuerySchemas.getSchemaWithArrayOfUnionValue();
        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new BigQuerySchemaProviderImpl(avroSchema));
        Assertions.assertThat(exception)
                .hasMessageContaining("Array cannot have a NULLABLE element");
    }

    @Test
    public void testArrayOfUnionOfMapSchemaConversion() {
        String fieldString = TestBigQuerySchemas.getSchemaWithArrayOfUnionOfMap();
        assertExpectedUnsupportedException(
                fieldString, "MAP/ARRAYS in UNION types are not supported");
    }

    @Test
    public void testArrayOfMapSchemaConversion() {
        String fieldString = TestBigQuerySchemas.getSchemaWithArrayOfMap();
        assertExpectedUnsupportedException(fieldString, "Array of Type MAP not supported yet.");
    }

    @Test
    public void testArrayOfRecordSchemaConversion() {
        Descriptor descriptor =
                TestBigQuerySchemas.getSchemaWithArrayOfRecord().getDescriptor();
        FieldDescriptorProto field = descriptor.findFieldByNumber(1).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("array_of_records");
        assertThat(field.getNumber()).isEqualTo(1);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REPEATED);
        assertThat(field.hasTypeName()).isTrue();
        assertThat(descriptor.findNestedTypeByName(field.getTypeName()).toProto())
                .isEqualTo(
                        DescriptorProtos.DescriptorProto.newBuilder()
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
    public void testArraysOfPrimitiveTypesSchemaConversion() {
        Descriptor descriptor =
                TestBigQuerySchemas.getSchemaWithArraysOfPrimitiveTypes()
                        .getDescriptor();
        assertPrimitive(descriptor, FieldDescriptorProto.Label.LABEL_REPEATED);
    }

    @Test
    public void testArraysOfRemainingPrimitiveTypesSchemaConversion() {
        Descriptor descriptor =
                TestBigQuerySchemas.getSchemaWithArraysOfRemainingPrimitiveTypes()
                        .getDescriptor();
        assertRemainingPrimitive(descriptor, FieldDescriptorProto.Label.LABEL_REPEATED);
    }

    @Test
    public void testArraysOfLogicalTypesSchemaConversion() {
        Descriptor descriptor =
                TestBigQuerySchemas.getSchemaWithArraysOfLogicalTypes()
                        .getDescriptor();
        assertLogical(descriptor, FieldDescriptorProto.Label.LABEL_REPEATED);
    }

    @Test
    public void testArraysOfRemainingLogicalTypesSchemaConversion() {
        Descriptor descriptor =
                TestBigQuerySchemas.getSchemaWithArraysOfRemainingLogicalTypes()
                        .getDescriptor();
        assertRemainingLogical(descriptor, FieldDescriptorProto.Label.LABEL_REPEATED);
    }

    // ------------Test Schemas with UNION of Different Types (Excluding Primitive and Logical)
    @Test
    public void testUnionOfArraySchemaConversion() {
        String fieldString = TestBigQuerySchemas.getSchemaWithUnionOfArray();
        assertExpectedUnsupportedException(
                fieldString, "MAP/ARRAYS in UNION types are not supported");
    }

    @Test
    public void testUnionOfArrayOfRecordSchemaConversion() {
        String fieldString = TestBigQuerySchemas.getSchemaWithUnionOfArrayOfRecord();
        assertExpectedUnsupportedException(
                fieldString, "MAP/ARRAYS in UNION types are not supported");
    }

    @Test
    public void testUnionOfMapSchemaConversion() {
        String fieldString = TestBigQuerySchemas.getSchemaWithUnionOfMap();
        assertExpectedUnsupportedException(
                fieldString, "MAP/ARRAYS in UNION types are not supported");
    }

    @Test
    public void testUnionOfRecordSchemaConversion() {
        Descriptor descriptor =
                TestBigQuerySchemas.getSchemaWithUnionOfRecord().getDescriptor();
        FieldDescriptorProto field = descriptor.findFieldByNumber(1).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("record_field_union");
        assertThat(field.getNumber()).isEqualTo(1);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_OPTIONAL);
        assertThat(field.hasTypeName()).isTrue();
        assertThat(descriptor.findNestedTypeByName(field.getTypeName()).toProto())
                .isEqualTo(
                        DescriptorProtos.DescriptorProto.newBuilder()
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
    public void testUnionOfSinglePrimitiveType() {
        Descriptor descriptor =
                TestBigQuerySchemas.getSchemaWithAllPrimitiveSingleUnion()
                        .getDescriptor();
        assertPrimitive(descriptor, FieldDescriptorProto.Label.LABEL_REQUIRED);
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
                TestBigQuerySchemas.getSchemaWithDefaultValue().getDescriptor();

        FieldDescriptorProto fieldDescriptorProto = descriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptorProto.getName()).isEqualTo("long_with_default");
        assertThat(fieldDescriptorProto.getNumber()).isEqualTo(1);
        assertThat(fieldDescriptorProto.getLabel())
                .isEqualTo(FieldDescriptorProto.Label.LABEL_OPTIONAL);
        assertThat(fieldDescriptorProto.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_INT64);
        assertThat(fieldDescriptorProto.hasDefaultValue()).isTrue();
        assertThat(fieldDescriptorProto.getDefaultValue()).isEqualTo("100");
    }

    private static void assertExpectedUnsupportedException(
            String fieldString, String expectedError) {
        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> new BigQuerySchemaProviderImpl(avroSchema));
        assertThat(exception).hasMessageThat().contains(expectedError);
    }

    private void assertPrimitive(Descriptor descriptor, FieldDescriptorProto.Label label) {
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

    private void assertRemainingPrimitive(Descriptor descriptor, FieldDescriptorProto.Label label) {

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

    private void assertLogical(Descriptor descriptor, FieldDescriptorProto.Label label) {
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

    private void assertRemainingLogical(Descriptor descriptor, FieldDescriptorProto.Label label) {

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
