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

import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.google.cloud.flink.bigquery.sink.serializer.AvroToProtoSerializer.getDynamicMessageFromGenericRecord;
import static com.google.cloud.flink.bigquery.sink.serializer.TestBigQuerySchemas.getAvroSchemaFromFieldString;
import static com.google.cloud.flink.bigquery.sink.serializer.TestBigQuerySchemas.getRecordSchema;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

/** Tests for {@link AvroToProtoSerializer}. */
public class AvroToProtoSerializerTest {

    @Test
    public void testPrimitiveTypesConversion() {
        BigQuerySchemaProvider bigQuerySchemaprovider =
                TestBigQuerySchemas.getSchemaWithRequiredPrimitiveTypes();
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaprovider);
        Descriptor descriptor = serializer.getDescriptor();
        Schema avroSchema = bigQuerySchemaprovider.getAvroSchema();

        byte[] byteArray = "Any String you want".getBytes();
        String recordSchemaString =
                "{\"type\":\"record\",\"name\":\"required_record_field\",\"doc\":\"Translated Avro Schema for required_record_field\",\"fields\":[{\"name\":\"species\",\"type\":\"string\"}]}}";
        Schema recordSchema = new Schema.Parser().parse(recordSchemaString);
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("number", -7099548873856657385L)
                        .set("price", 0.5616495161359795)
                        .set("species", "String")
                        .set("flighted", true)
                        .set("sound", ByteBuffer.wrap(byteArray))
                        .set(
                                "required_record_field",
                                new GenericRecordBuilder(recordSchema)
                                        .set("species", "hello")
                                        .build())
                        .build();

        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);
        assertThat(message.getField(descriptor.findFieldByNumber(1)))
                .isEqualTo(-7099548873856657385L);
        assertThat(message.getField(descriptor.findFieldByNumber(2))).isEqualTo(0.5616495161359795);
        assertThat(message.getField(descriptor.findFieldByNumber(3))).isEqualTo("String");
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
    public void testRemainingPrimitiveTypesConversion() {
        BigQuerySchemaProvider bigQuerySchemaprovider =
                TestBigQuerySchemas.getSchemaWithRemainingPrimitiveTypes();
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaprovider);
        Descriptor descriptor = serializer.getDescriptor();
        Schema avroSchema = bigQuerySchemaprovider.getAvroSchema();

        byte[] byteArray = "Any String you want".getBytes();
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("quantity", 1234)
                        .set("fixed_field", new GenericData.Fixed(avroSchema, byteArray))
                        .set("float_field", Float.parseFloat("12345.6789"))
                        .set("enum_field", "C")
                        .build();
        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);
        assertThat(message.getField(descriptor.findFieldByNumber(1))).isEqualTo(1234);
        assertThat(message.getField(descriptor.findFieldByNumber(2)))
                .isEqualTo(ByteString.copyFrom(byteArray));
        assertThat(message.getField(descriptor.findFieldByNumber(3))).isEqualTo(12345.6789f);
        assertThat(message.getField(descriptor.findFieldByNumber(4))).isEqualTo("C");
    }

    @Test
    public void testNullablePrimitiveTypesConversion() throws BigQuerySerializationException {
        BigQuerySchemaProvider bigQuerySchemaprovider =
                TestBigQuerySchemas.getSchemaWithNullablePrimitiveTypes();
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaprovider);
        Schema avroSchema = bigQuerySchemaprovider.getAvroSchema();

        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("number", null)
                        .set("price", null)
                        .set("species", null)
                        .set("flighted", null)
                        .set("sound", null)
                        .set("required_record_field", null)
                        .build();

        ByteString byteString = serializer.serialize(record);
        assertThat(byteString.toStringUtf8()).isEqualTo("");
    }

    @Test
    public void testUnionOfRemainingPrimitiveConversion() throws BigQuerySerializationException {
        BigQuerySchemaProvider bigQuerySchemaprovider =
                TestBigQuerySchemas.getSchemaWithUnionOfRemainingPrimitiveTypes();
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaprovider);
        Schema avroSchema = bigQuerySchemaprovider.getAvroSchema();

        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("quantity", null)
                        .set("fixed_field", null)
                        .set("float_field", null)
                        .set("enum_field", null)
                        .build();
        ByteString byteString = serializer.serialize(record);
        assertThat(byteString.toStringUtf8()).isEqualTo("");
    }

    // ------------ Test Schemas with Record of Different Types -----------
    @Test
    public void testRecordOfArrayConversation() {
        BigQuerySchemaProvider bigQuerySchemaprovider =
                TestBigQuerySchemas.getSchemaWithRecordOfArray();
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaprovider);
        Descriptor descriptor = serializer.getDescriptor();
        Schema avroSchema = bigQuerySchemaprovider.getAvroSchema();
        List<Boolean> arrayList = Arrays.asList(false, true, false);
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set(
                                "record_with_array",
                                new GenericRecordBuilder(
                                                avroSchema.getField("record_with_array").schema())
                                        .set("array_in_record", arrayList)
                                        .build())
                        .build();

        FieldDescriptor fieldDescriptor = descriptor.findFieldByNumber(1);
        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);
        message = (DynamicMessage) message.getField(fieldDescriptor);
        assertThat(
                        message.getField(
                                descriptor
                                        .findNestedTypeByName(
                                                fieldDescriptor.toProto().getTypeName())
                                        .findFieldByNumber(1)))
                .isEqualTo(arrayList);
    }

    @Test
    public void testRecordOfUnionSchemaConversation() {
        BigQuerySchemaProvider bigQuerySchemaprovider =
                TestBigQuerySchemas.getSchemaWithRecordOfUnionType();
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaprovider);
        Descriptor descriptor = serializer.getDescriptor();
        Schema avroSchema = bigQuerySchemaprovider.getAvroSchema();

        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set(
                                "record_with_union",
                                new GenericRecordBuilder(
                                                avroSchema.getField("record_with_union").schema())
                                        .set("union_in_record", null)
                                        .build())
                        .build();

        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);
        FieldDescriptor fieldDescriptor = descriptor.findFieldByNumber(1);
        ByteString byteString = ((DynamicMessage) message.getField(fieldDescriptor)).toByteString();
        assertThat(byteString.toStringUtf8()).isEqualTo("");
    }

    @Test
    public void testRecordOfRecordConversion() {
        BigQuerySchemaProvider bigQuerySchemaprovider =
                TestBigQuerySchemas.getSchemaWithRecordOfRecord();
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaprovider);
        Schema avroSchema = bigQuerySchemaprovider.getAvroSchema();
        Descriptor descriptor = bigQuerySchemaprovider.getDescriptor();
        GenericRecord innerRecord =
                new GenericRecordBuilder(
                                avroSchema
                                        .getField("record_in_record")
                                        .schema()
                                        .getField("record_field")
                                        .schema())
                        .set("value", 7267611125055979836L)
                        .set("another_value", "yllgqpemxjnpsoaqlwlgbqjkywxnavntf")
                        .build();
        innerRecord =
                new GenericRecordBuilder(avroSchema.getField("record_in_record").schema())
                        .set("record_field", innerRecord)
                        .build();
        GenericRecord record =
                new GenericRecordBuilder(avroSchema).set("record_in_record", innerRecord).build();
        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);
        message = (DynamicMessage) message.getField(descriptor.findFieldByNumber(1));
        descriptor =
                descriptor.findNestedTypeByName(
                        descriptor.findFieldByNumber(1).toProto().getTypeName());
        message = (DynamicMessage) message.getField(descriptor.findFieldByNumber(1));
        descriptor =
                descriptor.findNestedTypeByName(
                        descriptor.findFieldByNumber(1).toProto().getTypeName());
        assertThat(message.getField(descriptor.findFieldByNumber(1)))
                .isEqualTo(7267611125055979836L);
        assertThat(message.getField(descriptor.findFieldByNumber(2)))
                .isEqualTo("yllgqpemxjnpsoaqlwlgbqjkywxnavntf");
    }

    @Test
    public void testRecordOfPrimitiveTypeConversion() {
        BigQuerySchemaProvider bigQuerySchemaprovider =
                TestBigQuerySchemas.getSchemaWithRecordOfPrimitiveTypes();
        Schema avroSchema = bigQuerySchemaprovider.getAvroSchema();

        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaprovider);
        Descriptor descriptor = serializer.getDescriptor();
        Schema innerRecordSchema = avroSchema.getField("record_of_primitive_types").schema();

        byte[] byteArray = "Any String you want".getBytes();
        String recordSchemaString =
                "{\"type\":\"record\",\"name\":\"required_record_field\",\"doc\":\"Translated Avro Schema for required_record_field\",\"fields\":[{\"name\":\"species\",\"type\":\"string\"}]}}";
        Schema recordSchema = new Schema.Parser().parse(recordSchemaString);
        GenericRecord innerRecord =
                new GenericRecordBuilder(innerRecordSchema)
                        .set("number", -7099548873856657385L)
                        .set("price", 0.5616495161359795)
                        .set("species", "String")
                        .set("flighted", true)
                        .set("sound", ByteBuffer.wrap(byteArray))
                        .set(
                                "required_record_field",
                                new GenericRecordBuilder(recordSchema)
                                        .set("species", "hello")
                                        .build())
                        .build();
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("record_of_primitive_types", innerRecord)
                        .build();

        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);
        message = (DynamicMessage) message.getField(descriptor.findFieldByNumber(1));
        descriptor =
                descriptor.findNestedTypeByName(
                        descriptor.findFieldByNumber(1).toProto().getTypeName());

        assertThat(message.getField(descriptor.findFieldByNumber(1)))
                .isEqualTo(-7099548873856657385L);
        assertThat(message.getField(descriptor.findFieldByNumber(2))).isEqualTo(0.5616495161359795);
        assertThat(message.getField(descriptor.findFieldByNumber(3))).isEqualTo("String");
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
    public void testRecordOfRemainingPrimitiveTypeConversion() {
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRecordOfRemainingPrimitiveTypes();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        Descriptor descriptor = serializer.getDescriptor();
        Schema innerRecordSchema =
                avroSchema.getField("record_of_remaining_primitive_types").schema();

        byte[] byteArray = "Any String you want".getBytes();
        GenericRecord innerRecord =
                new GenericRecordBuilder(innerRecordSchema)
                        .set("quantity", 1234)
                        .set("fixed_field", new GenericData.Fixed(avroSchema, byteArray))
                        .set("float_field", Float.parseFloat("12345.6789"))
                        .set("enum_field", "C")
                        .build();
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("record_of_remaining_primitive_types", innerRecord)
                        .build();

        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);
        message = (DynamicMessage) message.getField(descriptor.findFieldByNumber(1));
        descriptor =
                descriptor.findNestedTypeByName(
                        descriptor.findFieldByNumber(1).toProto().getTypeName());
        assertThat(message.getField(descriptor.findFieldByNumber(1))).isEqualTo(1234);
        assertThat(message.getField(descriptor.findFieldByNumber(2)))
                .isEqualTo(ByteString.copyFrom(byteArray));
        assertThat(message.getField(descriptor.findFieldByNumber(3))).isEqualTo(12345.6789f);
        assertThat(message.getField(descriptor.findFieldByNumber(4))).isEqualTo("C");
    }

    // ------------Test Schemas with ARRAY of Different Types -------------
    @Test
    public void testArrayOfUnionConversion() {
        String notNullString =
                " \"fields\": [\n"
                        + "{\"name\": \"array_with_union\", \"type\": {\"type\": \"array\", \"items\":  \"long\"}} ]";
        Schema notNullSchema = getAvroSchemaFromFieldString(notNullString);
        BigQuerySchemaProvider bigQuerySchemaProvider =
                new BigQuerySchemaProviderImpl(notNullSchema);
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);

        // 1. check UNION with NULL
        String fieldString = TestBigQuerySchemas.getSchemaWithArrayOfUnionValue();
        Schema nullSchema = getAvroSchemaFromFieldString(fieldString);
        GenericRecord record =
                new GenericRecordBuilder(nullSchema)
                        .set("array_with_union", Arrays.asList(1234567L, null))
                        .build();
        BigQuerySerializationException exception =
                assertThrows(
                        BigQuerySerializationException.class, () -> serializer.serialize(record));
        Assertions.assertThat(exception)
                .hasMessageContaining("Array cannot have NULLABLE datatype");

        // 2. Check union of NOT NULL-multiple types.
        fieldString = TestBigQuerySchemas.getSchemaWithArrayOfMultipleValues();
        nullSchema = getAvroSchemaFromFieldString(fieldString);
        GenericRecord anotherRecord =
                new GenericRecordBuilder(nullSchema)
                        .set("array_with_union", Arrays.asList(1234567L, 12345))
                        .build();
        exception =
                assertThrows(
                        BigQuerySerializationException.class,
                        () -> serializer.serialize(anotherRecord));
        Assertions.assertThat(exception)
                .hasMessageContaining("ARRAY cannot have multiple datatypes in BigQuery.");
    }

    @Test
    public void testArrayOfNullConversion() {
        String fieldString = TestBigQuerySchemas.getSchemaWithArrayOfNullValue();
        String notNullString =
                " \"fields\": [\n"
                        + "{\"name\": \"array_with_null\", \"type\": {\"type\": \"array\", \"items\":  \"long\"}} ]";
        Schema notNullSchema = getAvroSchemaFromFieldString(notNullString);
        BigQuerySchemaProvider bigQuerySchemaProvider =
                new BigQuerySchemaProviderImpl(notNullSchema);
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);

        Schema nullSchema = getAvroSchemaFromFieldString(fieldString);
        GenericRecord record =
                new GenericRecordBuilder(nullSchema)
                        .set("array_with_null", Arrays.asList(1234567L, null))
                        .build();
        BigQuerySerializationException exception =
                assertThrows(
                        BigQuerySerializationException.class, () -> serializer.serialize(record));
        Assertions.assertThat(exception)
                .hasMessageContaining("Array cannot have NULLABLE datatype");
    }

    @Test
    public void testArrayOfRecordConversion() {
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithArrayOfRecord();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        Descriptor descriptor = serializer.getDescriptor();
        Schema innerRecordSchema = new Schema.Parser().parse(getRecordSchema("inside_record"));
        GenericRecord innerRecord =
                new GenericRecordBuilder(innerRecordSchema)
                        .set("value", 8034881802526489441L)
                        .set("another_value", "fefmmuyoosmglqtnwfxahgoxqpyhc")
                        .build();
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("array_of_records", Arrays.asList(innerRecord, innerRecord))
                        .build();
        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);
        List<DynamicMessage> arrayResult =
                (List<DynamicMessage>) message.getField(descriptor.findFieldByNumber(1));
        assertThat(arrayResult).hasSize(2);
        // the descriptor for elements inside the array.
        descriptor =
                descriptor.findNestedTypeByName(
                        descriptor.findFieldByNumber(1).toProto().getTypeName());
        message = arrayResult.get(0);
        assertThat(message.getField(descriptor.findFieldByNumber(1)))
                .isEqualTo(8034881802526489441L);
        assertThat(message.getField(descriptor.findFieldByNumber(2)))
                .isEqualTo("fefmmuyoosmglqtnwfxahgoxqpyhc");

        message = arrayResult.get(1);
        assertThat(message.getField(descriptor.findFieldByNumber(1)))
                .isEqualTo(8034881802526489441L);
        assertThat(message.getField(descriptor.findFieldByNumber(2)))
                .isEqualTo("fefmmuyoosmglqtnwfxahgoxqpyhc");
    }

    @Test
    public void testArraysOfPrimitiveTypesConversion() {
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithArraysOfPrimitiveTypes();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        Descriptor descriptor = serializer.getDescriptor();
        ByteBuffer byteArray = ByteBuffer.wrap("Hello".getBytes());
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("number", Collections.singletonList(-250555967807021764L))
                        .set("price", Arrays.asList(0.34593866360929726, 0.35197578762609993))
                        .set(
                                "species",
                                Arrays.asList(
                                        "nsguocxfjqaufhsunahvxmcpivutfqv",
                                        "q",
                                        "pldvejbqmfyosgxmbmqjsafjbcfqwhiagbckmti"))
                        .set("flighted", Arrays.asList(false, false, false, true))
                        .set(
                                "sound",
                                Arrays.asList(
                                        byteArray, byteArray, byteArray, byteArray, byteArray))
                        .set(
                                "required_record_field",
                                Collections.singletonList(
                                        new GenericRecordBuilder(
                                                        avroSchema
                                                                .getField("required_record_field")
                                                                .schema()
                                                                .getElementType())
                                                .set(
                                                        "species",
                                                        Arrays.asList("a", "b", "c", "d", "e", "f"))
                                                .build()))
                        .build();
        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);
        List<Object> arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(1));
        assertThat(arrayResult).hasSize(1);
        assertThat(arrayResult.get(0)).isEqualTo(-250555967807021764L);

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(2));
        assertThat(arrayResult).hasSize(2);
        assertThat(arrayResult.get(0)).isEqualTo(0.34593866360929726);
        assertThat(arrayResult.get(1)).isEqualTo(0.35197578762609993);

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(3));
        assertThat(arrayResult).hasSize(3);
        assertThat(arrayResult.get(0)).isEqualTo("nsguocxfjqaufhsunahvxmcpivutfqv");
        assertThat(arrayResult.get(1)).isEqualTo("q");
        assertThat(arrayResult.get(2)).isEqualTo("pldvejbqmfyosgxmbmqjsafjbcfqwhiagbckmti");

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(4));
        assertThat(arrayResult).hasSize(4);
        assertThat(arrayResult.get(0)).isEqualTo(false);
        assertThat(arrayResult.get(1)).isEqualTo(false);
        assertThat(arrayResult.get(2)).isEqualTo(false);
        assertThat(arrayResult.get(3)).isEqualTo(true);

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(5));
        assertThat(arrayResult).hasSize(5);
        // Not checking the rest since they are the same.
        assertThat(arrayResult.get(0)).isEqualTo(ByteString.copyFrom("Hello".getBytes()));
        // obtaining the record field inside the array.
        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(6));
        assertThat(arrayResult).hasSize(1);
        // Since this is a record field, getting the descriptor for inside the record
        descriptor =
                descriptor.findNestedTypeByName(
                        descriptor.findFieldByNumber(6).toProto().getTypeName());
        message = (DynamicMessage) arrayResult.get(0);
        // The given is a record containing an array, so obtaining the array inside the record.
        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(1));
        assertThat(arrayResult).hasSize(6);

        assertThat(arrayResult.get(0)).isEqualTo("a");
        assertThat(arrayResult.get(1)).isEqualTo("b");
        assertThat(arrayResult.get(2)).isEqualTo("c");
        assertThat(arrayResult.get(3)).isEqualTo("d");
        assertThat(arrayResult.get(4)).isEqualTo("e");
        assertThat(arrayResult.get(5)).isEqualTo("f");
    }

    @Test
    public void testArraysOfRemainingPrimitiveTypesConversion() {
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithArraysOfRemainingPrimitiveTypes();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        Descriptor descriptor = serializer.getDescriptor();
        byte[] byteArray =
                ByteBuffer.allocate(40)
                        .putInt(-77)
                        .putInt(-55)
                        .putInt(60)
                        .putInt(-113)
                        .putInt(120)
                        .putInt(-13)
                        .putInt(-69)
                        .putInt(61)
                        .putInt(108)
                        .putInt(41)
                        .array();
        GenericFixed fixed =
                new GenericData.Fixed(avroSchema.getField("fixed_field").schema(), byteArray);
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("quantity", Collections.singletonList(89767285))
                        .set("fixed_field", Collections.singletonList(fixed))
                        .set(
                                "float_field",
                                Arrays.asList(0.26904225f, 0.558431f, 0.2269839f, 0.70421267f))
                        .set("enum_field", Arrays.asList("A", "C", "A"))
                        .build();
        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);
        List<Object> arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(1));
        assertThat(arrayResult).hasSize(1);
        assertThat(arrayResult.get(0)).isEqualTo(89767285);

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(2));
        assertThat(arrayResult).hasSize(1);
        assertThat(arrayResult.get(0)).isEqualTo(ByteString.copyFrom(byteArray));

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(3));
        assertThat(arrayResult).hasSize(4);
        assertThat(arrayResult.get(0)).isEqualTo(0.26904225f);
        assertThat(arrayResult.get(1)).isEqualTo(0.558431f);
        assertThat(arrayResult.get(2)).isEqualTo(0.2269839f);
        assertThat(arrayResult.get(3)).isEqualTo(0.70421267f);

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(4));
        assertThat(arrayResult).hasSize(3);
        assertThat(arrayResult.get(0)).isEqualTo("A");
        assertThat(arrayResult.get(1)).isEqualTo("C");
        assertThat(arrayResult.get(2)).isEqualTo("A");
    }

    // ------------Test Schemas with UNION of Different Types (Excluding Primitive and Logical)
    @Test
    public void testUnionOfArrayConversion() throws BigQuerySerializationException {
        String fieldString = TestBigQuerySchemas.getSchemaWithUnionOfArray();
        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        GenericRecord record =
                new GenericRecordBuilder(avroSchema).set("array_field_union", null).build();

        String nonNullFieldString =
                "\"fields\": [\n"
                        + "   {\"name\": \"array_field_union\", \"type\": {\"type\": \"array\", \"items\": \"float\"}}\n"
                        + " ]";
        Schema nonNullSchema = getAvroSchemaFromFieldString(nonNullFieldString);
        BigQuerySchemaProvider bigQuerySchemaProvider =
                new BigQuerySchemaProviderImpl(nonNullSchema);
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        ByteString byteString = serializer.serialize(record);
        assertThat(byteString.toStringUtf8()).isEqualTo("");
    }

    @Test
    public void testUnionOfArrayOfRecordConversion() throws BigQuerySerializationException {
        String fieldString = TestBigQuerySchemas.getSchemaWithUnionOfArrayOfRecord();
        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        GenericRecord record =
                new GenericRecordBuilder(avroSchema).set("array_of_records_union", null).build();

        String nonNullFieldString =
                "\"fields\": [\n"
                        + "   {\"name\": \"array_of_records_union\", \"type\": {\"type\": \"array\", \"items\": {\"name\": \"inside_record_union\", \"type\": \"record\", \"fields\": [{\"name\": \"value\", \"type\": \"long\"},{\"name\": \"another_value\",\"type\": \"string\"}]}}}\n"
                        + " ]";
        Schema nonNullSchema = getAvroSchemaFromFieldString(nonNullFieldString);
        BigQuerySchemaProvider bigQuerySchemaProvider =
                new BigQuerySchemaProviderImpl(nonNullSchema);
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        ByteString byteString = serializer.serialize(record);
        assertThat(byteString.toStringUtf8()).isEqualTo("");
    }

    @Test
    public void testNullInsertionInRequiredField() {
        String recordSchemaString =
                "\"fields\":[{\"name\": \"record_field_union\", \"type\":"
                        + getRecordSchema("inner_record")
                        + " }]";
        Schema recordSchema = getAvroSchemaFromFieldString(recordSchemaString);
        // For descriptor with RECORD of MODE Required.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                new BigQuerySchemaProviderImpl(recordSchema);
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);

        Schema nullableRecordSchema =
                TestBigQuerySchemas.getSchemaWithUnionOfRecord().getAvroSchema();
        // Form a Null record.
        GenericRecord record =
                new GenericRecordBuilder(nullableRecordSchema)
                        .set("record_field_union", null)
                        .build();
        // Try to serialize.
        BigQuerySerializationException exception =
                assertThrows(
                        BigQuerySerializationException.class, () -> serializer.serialize(record));
        Assertions.assertThat(exception)
                .hasMessageContaining(
                        "Received null value for non-nullable field record_field_union");
    }

    @Test
    public void testUnionOfRecordConversion() throws BigQuerySerializationException {
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithUnionOfRecord();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        Descriptor descriptor = serializer.getDescriptor();
        // Record Is Null
        GenericRecord record =
                new GenericRecordBuilder(avroSchema).set("record_field_union", null).build();
        ByteString byteString = serializer.serialize(record);
        assertThat(byteString.toStringUtf8()).isEqualTo("");

        // Record is not null
        record =
                new GenericRecordBuilder(avroSchema)
                        .set(
                                "record_field_union",
                                new GenericRecordBuilder(
                                                getAvroSchemaFromFieldString(
                                                        "\"fields\":[{\"name\":\"value\",\"type\":\"long\"},{\"name\":\"another_value\",\"type\":\"string\"}]"))
                                        .set("value", 12345678910L)
                                        .set("another_value", "hello")
                                        .build())
                        .build();
        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);
        // obtain "inner_record"
        message = (DynamicMessage) message.getField(descriptor.findFieldByNumber(1));
        // update the descriptor to point to the "inner_record" now.
        descriptor =
                descriptor.findNestedTypeByName(
                        descriptor.findFieldByNumber(1).toProto().getTypeName());
        assertThat(message.getField(descriptor.findFieldByNumber(1))).isEqualTo(12345678910L);
        assertThat(message.getField(descriptor.findFieldByNumber(2))).isEqualTo("hello");
    }

    @Test
    public void testUnionOfSinglePrimitiveType() {
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithAllPrimitiveSingleUnion();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        Descriptor descriptor = serializer.getDescriptor();

        byte[] byteArray = "Any String you want".getBytes();
        String recordSchemaString =
                "{\"type\":\"record\",\"name\":\"required_record_field\",\"doc\":\"Translated Avro Schema for required_record_field\",\"fields\":[{\"name\":\"species\",\"type\":\"string\"}]}}";
        Schema recordSchema = new Schema.Parser().parse(recordSchemaString);
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("number", -7099548873856657385L)
                        .set("price", 0.5616495161359795)
                        .set("species", "String")
                        .set("flighted", true)
                        .set("sound", ByteBuffer.wrap(byteArray))
                        .set(
                                "required_record_field",
                                new GenericRecordBuilder(recordSchema)
                                        .set("species", "hello")
                                        .build())
                        .build();
        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);
        assertThat(message.getField(descriptor.findFieldByNumber(1)))
                .isEqualTo(-7099548873856657385L);
        assertThat(message.getField(descriptor.findFieldByNumber(2))).isEqualTo(0.5616495161359795);
        assertThat(message.getField(descriptor.findFieldByNumber(3))).isEqualTo("String");
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
}
