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
import org.apache.commons.lang3.ArrayUtils;
import org.assertj.core.api.Assertions;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static com.google.cloud.flink.bigquery.sink.serializer.AvroToProtoSerializer.getDynamicMessageFromGenericRecord;
import static com.google.cloud.flink.bigquery.sink.serializer.TestBigQuerySchemas.getAvroSchemaFromFieldString;
import static com.google.cloud.flink.bigquery.sink.serializer.TestBigQuerySchemas.getRecordSchema;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

/** Tests for {@link AvroToProtoSerializer}. */
public class AvroToProtoSerializerTest {

    // ---------- Test the Helper Functions --------------
    @Test
    public void testConvertJson() {
        Object value = "{\"name\": \"test_value\", \"value\": \"value\"}";
        String convertedValue = AvroToProtoSerializer.convertJson(value);
        assertThat(convertedValue).isEqualTo(value.toString());
    }

    @Test
    public void testErrorConvertJson() {
        Object value = "invalid_json_string";
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> AvroToProtoSerializer.convertJson(value));
        Assertions.assertThat(exception).hasMessageContaining(" is not in valid JSON Format.");

        Object invalidType = 123456;
        exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> AvroToProtoSerializer.convertJson(invalidType));
        Assertions.assertThat(exception)
                .hasMessageContaining("Expecting a value as String/Utf8 type (json format)");
    }

    @Test
    public void testConvertGeography() {
        Object value = "POINT(-121 41)";
        String convertedValue = AvroToProtoSerializer.convertGeography(value);
        assertThat(convertedValue).isEqualTo(value.toString());
    }

    @Test
    public void testErrorConvertGeography() {
        Object value = 5678;
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> AvroToProtoSerializer.convertGeography(value));
        Assertions.assertThat(exception)
                .hasMessageContaining(
                        "Expecting a value as String/Utf8 type (geography_wkt or geojson format).");
    }

    @Test
    public void testConvertUUID() {
        Object value = UUID.randomUUID();
        String convertedValue = AvroToProtoSerializer.convertUUID(value);
        assertThat(convertedValue).isEqualTo(value.toString());
    }

    @Test
    public void testConvertUUIDString() {
        Object value = UUID.randomUUID().toString();
        String convertedValue = AvroToProtoSerializer.convertUUID(value);
        assertThat(convertedValue).isEqualTo(value.toString());
    }

    @Test
    public void testErrorConvertUUID() {
        Object value = 5678;
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> AvroToProtoSerializer.convertUUID(value));
        Assertions.assertThat(exception).hasMessageContaining("Expecting a value as String type.");

        Object invalidUuidValue = "This is not a valid UUID String";
        exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> AvroToProtoSerializer.convertUUID(invalidUuidValue));
        Assertions.assertThat(exception).hasMessageContaining("Invalid UUID string: ");
    }

    @Test
    public void testErrorConvertBigNumeric() {
        Object value = "A random string";
        assertThrows(
                ClassCastException.class, () -> AvroToProtoSerializer.convertBigDecimal(value));

        BigDecimal bigDecimal =
                new BigDecimal(
                        "5789604461865805559771174925043439539266.349923328202820554419728792395656481996700");
        // Form byte array in big-endian order.
        Object byteBuffer = ByteBuffer.wrap(bigDecimal.unscaledValue().toByteArray());
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> AvroToProtoSerializer.convertBigDecimal(byteBuffer));
        Assertions.assertThat(exception).hasMessageContaining("BigDecimal overflow:");
    }

    @Test
    public void testConvertTimeStringType() {
        Object value = "00:22:59.700440";
        String convertedValue = AvroToProtoSerializer.convertTime(value, true);
        assertThat(convertedValue).isEqualTo(value.toString());
    }

    @Test
    public void testConvertTimeLongType() {
        Object value = 36708529123L;
        String convertedValue = AvroToProtoSerializer.convertTime(value, true);
        assertThat(convertedValue).isEqualTo("10:11:48.529123");
    }

    @Test
    public void testConvertTimeLongMillisType() {
        Object value = 36708529;
        String convertedValue = AvroToProtoSerializer.convertTime(value, false);
        assertThat(convertedValue).isEqualTo("10:11:48.529");
    }

    @Test
    public void testOutOfBoundsTimeValue() {
        Object invalidValueLong = 86400000000L;
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> AvroToProtoSerializer.convertTime(invalidValueLong, true));
        Assertions.assertThat(exception)
                .hasMessageContaining("Time passed should be between 00:00 and 23:59:59.999999");

        Object invalidValueInt = 86400000;
        exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> AvroToProtoSerializer.convertTime(invalidValueInt, false));
        Assertions.assertThat(exception)
                .hasMessageContaining("Time passed should be between 00:00 and 23:59:59.999999");
    }

    @Test
    public void testTimeMismatchErrorType() {

        Object invalidValueInt = 1234567;
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> AvroToProtoSerializer.convertTime(invalidValueInt, true));
        Assertions.assertThat(exception)
                .hasMessageContaining("Expecting a value as LONG type for Time(micros)");

        Object invalidValueLong = 123456789L;
        exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> AvroToProtoSerializer.convertTime(invalidValueLong, false));
        Assertions.assertThat(exception)
                .hasMessageContaining("Expecting a value as INTEGER type for Time(millis)");

        Object floatValue = 1234.56;
        exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> AvroToProtoSerializer.convertTime(floatValue, true));
        Assertions.assertThat(exception)
                .hasMessageContaining("Expecting a value as LONG type for Time(micros)");
    }

    @Test
    public void testConvertTimestampMillis() {
        Object value = 1710270870L;
        Long convertedValue =
                AvroToProtoSerializer.convertTimestamp(value, false, "Timestamp (millis)");
        assertThat(convertedValue).isEqualTo(1710270870000L);
    }

    @Test
    public void testConvertTimestampMillisTimeInstant() {
        Object value = Instant.ofEpochMilli(1710270870123L);
        Long convertedValue =
                AvroToProtoSerializer.convertTimestamp(value, false, "Timestamp (millis)");
        assertThat(convertedValue).isEqualTo(1710270870123000L);
    }

    @Test
    public void testConvertTimestampMicrosTimeInstant() {
        Object value = Instant.parse("2024-03-13T01:01:16.579501+00:00");
        Long convertedValue =
                AvroToProtoSerializer.convertTimestamp(value, true, "Timestamp (micros)");
        assertThat(convertedValue).isEqualTo(1710291676579000L);
    }

    @Test
    public void testConvertTimestampMicros() {
        Object value = 1710270870123L;
        Long convertedValue =
                AvroToProtoSerializer.convertTimestamp(value, true, "Timestamp (micros)");
        assertThat(convertedValue).isEqualTo(1710270870123L);
    }

    @Test
    public void testConvertErrorTimestamp() {
        Object invalidValue = "not_a_timestamp";
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                AvroToProtoSerializer.convertTimestamp(
                                        invalidValue, true, "Timestamp (micros)"));
        Assertions.assertThat(exception).hasMessageContaining("Expecting a value as Long type");

        // Maximum Timestamp + 1
        Object invalidLongValue = 253402300800000000L;
        exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                AvroToProtoSerializer.convertTimestamp(
                                        invalidLongValue, true, "Timestamp (micros)"));
        Assertions.assertThat(exception)
                .hasMessageContaining(
                        "Should be a long value indicating microseconds since Epoch "
                                + "(1970-01-01 00:00:00.000000+00:00) between 0001-01-01T00:00:00Z "
                                + "and 9999-12-31T23:59:59.999999000Z");

        // Minimum Timestamp - 1
        Object anotherInvalidLongValue = -62135596800000001L;
        exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                AvroToProtoSerializer.convertTimestamp(
                                        anotherInvalidLongValue, true, "Timestamp (micros)"));
        Assertions.assertThat(exception)
                .hasMessageContaining(
                        "Should be a long value indicating microseconds since Epoch "
                                + "(1970-01-01 00:00:00.000000+00:00) between 0001-01-01T00:00:00Z "
                                + "and 9999-12-31T23:59:59.999999000Z");
    }

    @Test
    public void testConvertDateInstant() {
        Object value = Instant.parse("2024-03-13T00:00:00.000000+00:00");
        Integer convertedValue = AvroToProtoSerializer.convertDate(value);
        assertThat(convertedValue).isEqualTo(19795);
    }

    @Test
    public void testConvertDate() {
        Object value = 19794;
        Integer convertedValue = AvroToProtoSerializer.convertDate(value);
        assertThat(convertedValue).isEqualTo(19794);
    }

    @Test
    public void testConvertErrorDate() {
        Object invalidValue = 29328967;
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> AvroToProtoSerializer.convertDate(invalidValue));
        Assertions.assertThat(exception)
                .hasMessageContaining(
                        "Should be a Integer value indicating days since Epoch (1970-01-01 00:00:00)");

        Object floatValue = 1234.56;
        assertThrows(
                IllegalArgumentException.class,
                () -> AvroToProtoSerializer.convertDate(floatValue));
    }

    @Test
    public void testConvertDateTimeStringType() {
        Object value = "2024-01-01 00:22:59.700440";
        String convertedValue = AvroToProtoSerializer.convertDateTime(value, true);
        assertThat(convertedValue).isEqualTo(value.toString());
    }

    @Test
    public void testConvertDateTimeLongType() {
        Object value = 1710290574347000L;
        String convertedValue = AvroToProtoSerializer.convertDateTime(value, true);
        assertThat(convertedValue).isEqualTo("2024-03-13T00:42:54.347");
    }

    @Test
    public void testConvertErrorTimeDateLongType() {
        Object value = 1424413213878251952L;
        assertThrows(
                IllegalArgumentException.class,
                () -> AvroToProtoSerializer.convertDateTime(value, true));
        Object floatValue = 1234.56;
        assertThrows(
                IllegalArgumentException.class,
                () -> AvroToProtoSerializer.convertDateTime(floatValue, true));
    }
    // --------------------------------------------------------------------------------------------------

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

    // --------------- Test Logical Data Types (Nullable and Required) ---------------
    @Test
    public void testLogicalTypesConversion() {
        BigQuerySchemaProvider bigQuerySchemaprovider =
                TestBigQuerySchemas.getSchemaWithRequiredLogicalTypes();
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaprovider);
        Descriptor descriptor = serializer.getDescriptor();
        Schema avroSchema = bigQuerySchemaprovider.getAvroSchema();

        BigDecimal bigDecimal = new BigDecimal("123456.7891011");
        byte[] bytes = bigDecimal.unscaledValue().toByteArray();
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("timestamp", DateTime.parse("2024-03-20T12:50:50.269+05:30"))
                        .set("time", 50546554456L)
                        .set("datetime", 1710943144787424L)
                        .set("date", 19802)
                        .set("numeric_field", ByteBuffer.wrap(bytes))
                        .set("bignumeric_field", ByteBuffer.wrap(bytes))
                        .set("geography", "GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (3 4, 5 6))")
                        .set("Json", "{\"FirstName\": \"John\", \"LastName\": \"Doe\"}")
                        .build();

        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);
        assertThat(message.getField(descriptor.findFieldByNumber(1))).isEqualTo(1710919250269000L);
        assertThat(message.getField(descriptor.findFieldByNumber(2))).isEqualTo("14:02:26.554456");
        assertThat(message.getField(descriptor.findFieldByNumber(3)))
                .isEqualTo("2024-03-20T13:59:04.787424");
        assertThat(message.getField(descriptor.findFieldByNumber(4))).isEqualTo(19802);
        // TODO: Check the ByteString.
        assertThat(message.getField(descriptor.findFieldByNumber(7)))
                .isEqualTo("GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (3 4, 5 6))");
        assertThat(message.getField(descriptor.findFieldByNumber(8)))
                .isEqualTo("{\"FirstName\": \"John\", \"LastName\": \"Doe\"}");
    }

    @Test
    public void testRemainingLogicalConversion() {
        BigQuerySchemaProvider bigQuerySchemaprovider =
                TestBigQuerySchemas.getSchemaWithRemainingLogicalTypes();
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaprovider);
        Descriptor descriptor = serializer.getDescriptor();
        Schema avroSchema = bigQuerySchemaprovider.getAvroSchema();

        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("ts_millis", DateTime.parse("2024-03-20T12:50:50.269+05:30"))
                        .set("time_millis", 45745727)
                        .set("lts_millis", 1710938587462L)
                        .set("uuid", UUID.fromString("8e25e7e5-0dc5-4292-b59b-3665b0ab8280"))
                        .build();

        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);
        assertThat(message.getField(descriptor.findFieldByNumber(1))).isEqualTo(1710919250269000L);
        assertThat(message.getField(descriptor.findFieldByNumber(2))).isEqualTo("12:42:25.727");
        assertThat(message.getField(descriptor.findFieldByNumber(3)))
                .isEqualTo("2024-03-20T12:43:07.462");
        assertThat(message.getField(descriptor.findFieldByNumber(4)))
                .isEqualTo("8e25e7e5-0dc5-4292-b59b-3665b0ab8280");
    }

    @Test
    public void testNullableLogicalTypesConversion() throws BigQuerySerializationException {
        BigQuerySchemaProvider bigQuerySchemaprovider =
                TestBigQuerySchemas.getSchemaWithNullableLogicalTypes();
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaprovider);
        Schema avroSchema = bigQuerySchemaprovider.getAvroSchema();

        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("timestamp", null)
                        .set("time", null)
                        .set("datetime", null)
                        .set("date", null)
                        .set("numeric_field", null)
                        .set("bignumeric_field", null)
                        .set("geography", null)
                        .set("Json", null)
                        .build();
        ByteString byteString = serializer.serialize(record);
        assertThat(byteString.toStringUtf8()).isEqualTo("");
    }

    @Test
    public void testUnionOfRemainingLogicalConversion() throws BigQuerySerializationException {
        BigQuerySchemaProvider bigQuerySchemaprovider =
                TestBigQuerySchemas.getSchemaWithUnionOfLogicalTypes();
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaprovider);
        Schema avroSchema = bigQuerySchemaprovider.getAvroSchema();

        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("ts_millis", null)
                        .set("time_millis", null)
                        .set("lts_millis", null)
                        .set("uuid", null)
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

    @Test
    public void testRecordOfLogicalTypeConversion() {
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRecordOfLogicalTypes();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        Descriptor descriptor = serializer.getDescriptor();
        Schema innerRecordSchema = avroSchema.getField("record_of_logical_types").schema();

        BigDecimal bigDecimal = new BigDecimal("123456.7891011");
        byte[] bytes = bigDecimal.unscaledValue().toByteArray();
        GenericRecord innerRecord =
                new GenericRecordBuilder(innerRecordSchema)
                        .set("timestamp", DateTime.parse("2024-03-20T12:50:50.269+05:30"))
                        .set("time", 50546554456L)
                        .set("datetime", 1710943144787424L)
                        .set("date", 19802)
                        .set("numeric_field", ByteBuffer.wrap(bytes))
                        .set("bignumeric_field", ByteBuffer.wrap(bytes))
                        .set("geography", "GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (3 4, 5 6))")
                        .set("Json", "{\"FirstName\": \"John\", \"LastName\": \"Doe\"}")
                        .build();
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("record_of_logical_types", innerRecord)
                        .build();
        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);
        message = (DynamicMessage) message.getField(descriptor.findFieldByNumber(1));
        descriptor =
                descriptor.findNestedTypeByName(
                        descriptor.findFieldByNumber(1).toProto().getTypeName());
        assertThat(message.getField(descriptor.findFieldByNumber(1))).isEqualTo(1710919250269000L);
        assertThat(message.getField(descriptor.findFieldByNumber(2))).isEqualTo("14:02:26.554456");
        assertThat(message.getField(descriptor.findFieldByNumber(3)))
                .isEqualTo("2024-03-20T13:59:04.787424");
        assertThat(message.getField(descriptor.findFieldByNumber(4))).isEqualTo(19802);
        // TODO: Check the ByteString.
        assertThat(message.getField(descriptor.findFieldByNumber(7)))
                .isEqualTo("GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (3 4, 5 6))");
        assertThat(message.getField(descriptor.findFieldByNumber(8)))
                .isEqualTo("{\"FirstName\": \"John\", \"LastName\": \"Doe\"}");
    }

    @Test
    public void testRecordOfRemainingLogicalTypeConversion() {
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRecordOfRemainingLogicalTypes();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        Descriptor descriptor = serializer.getDescriptor();
        Schema innerRecordSchema =
                avroSchema.getField("record_of_remaining_logical_types").schema();
        GenericRecord innerRecord =
                new GenericRecordBuilder(innerRecordSchema)
                        .set("ts_millis", DateTime.parse("2024-03-20T12:50:50.269+05:30"))
                        .set("time_millis", 45745727)
                        .set("lts_millis", 1710938587462L)
                        .set("uuid", UUID.fromString("8e25e7e5-0dc5-4292-b59b-3665b0ab8280"))
                        .build();

        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("record_of_remaining_logical_types", innerRecord)
                        .build();
        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);
        message = (DynamicMessage) message.getField(descriptor.findFieldByNumber(1));
        descriptor =
                descriptor.findNestedTypeByName(
                        descriptor.findFieldByNumber(1).toProto().getTypeName());
        assertThat(message.getField(descriptor.findFieldByNumber(1))).isEqualTo(1710919250269000L);
        assertThat(message.getField(descriptor.findFieldByNumber(2))).isEqualTo("12:42:25.727");
        assertThat(message.getField(descriptor.findFieldByNumber(3)))
                .isEqualTo("2024-03-20T12:43:07.462");
        assertThat(message.getField(descriptor.findFieldByNumber(4)))
                .isEqualTo("8e25e7e5-0dc5-4292-b59b-3665b0ab8280");
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

    @Test
    public void testArraysOfLogicalTypesConversion() {
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithArraysOfLogicalTypes();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        Descriptor descriptor = serializer.getDescriptor();
        BigDecimal bigDecimal = new BigDecimal("123456.7891011");
        byte[] bytes = bigDecimal.unscaledValue().toByteArray();
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set(
                                "timestamp",
                                Collections.singletonList(
                                        DateTime.parse("2024-03-20T12:50:50.269+05:30")))
                        .set("time", Arrays.asList(50546554456L, 50546554456L))
                        .set(
                                "datetime",
                                Arrays.asList(
                                        1710943144787424L, 1710943144787424L, 1710943144787424L))
                        .set("date", Arrays.asList(19802, 19802, 19802, 19802))
                        .set("numeric_field", Collections.singletonList(ByteBuffer.wrap(bytes)))
                        .set("bignumeric_field", Collections.singletonList(ByteBuffer.wrap(bytes)))
                        .set(
                                "geography",
                                Collections.singletonList(
                                        "GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (3 4, 5 6))"))
                        .set(
                                "Json",
                                Arrays.asList(
                                        "{\"FirstName\" : \"John\", \"LastName\": \"Doe\"}",
                                        "{\"FirstName\" : \"Jane\", \"LastName\": \"Doe\"}"))
                        .build();
        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);

        List<Object> arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(1));
        assertThat(arrayResult).hasSize(1);
        assertThat(arrayResult.get(0)).isEqualTo(1710919250269000L);

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(2));
        assertThat(arrayResult).hasSize(2);
        assertThat(arrayResult.get(0)).isEqualTo("14:02:26.554456");

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(3));
        assertThat(arrayResult).hasSize(3);
        assertThat(arrayResult.get(0)).isEqualTo("2024-03-20T13:59:04.787424");

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(4));
        assertThat(arrayResult).hasSize(4);
        assertThat(arrayResult.get(0)).isEqualTo(19802);

        ArrayUtils.reverse(bytes);
        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(5));
        assertThat(arrayResult).hasSize(1);
        assertThat(arrayResult.get(0)).isEqualTo(ByteString.copyFrom(bytes));

        ArrayUtils.reverse(bytes);
        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(6));
        assertThat(arrayResult).hasSize(1);
        assertThat(arrayResult.get(0)).isEqualTo(ByteString.copyFrom(bytes));

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(7));
        assertThat(arrayResult).hasSize(1);
        assertThat(arrayResult.get(0))
                .isEqualTo("GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (3 4, 5 6))");

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(8));
        assertThat(arrayResult).hasSize(2);
        assertThat(arrayResult.get(0))
                .isEqualTo("{\"FirstName\" : \"John\", \"LastName\": \"Doe\"}");
        assertThat(arrayResult.get(1))
                .isEqualTo("{\"FirstName\" : \"Jane\", \"LastName\": \"Doe\"}");
    }

    @Test
    public void testArraysOfRemainingLogicalTypesConversion() {
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithArraysOfRemainingLogicalTypes();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        Descriptor descriptor = serializer.getDescriptor();
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("time_millis", Arrays.asList(45745727, 45745727, 45745727))
                        .set("lts_millis", Arrays.asList(1710938587462L, 1710938587462L))
                        .set(
                                "ts_millis",
                                Collections.singletonList(
                                        DateTime.parse("2024-03-20T12:50:50.269+05:30")))
                        .set(
                                "uuid",
                                Collections.singletonList(
                                        UUID.fromString("8e25e7e5-0dc5-4292-b59b-3665b0ab8280")))
                        .build();
        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);

        List<Object> arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(2));
        assertThat(arrayResult).hasSize(3);
        assertThat(arrayResult.get(0)).isEqualTo("12:42:25.727");

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(1));
        assertThat(arrayResult).hasSize(1);
        assertThat(arrayResult.get(0)).isEqualTo(1710919250269000L);

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(3));
        assertThat(arrayResult).hasSize(2);
        assertThat(arrayResult.get(0)).isEqualTo("2024-03-20T12:43:07.462");

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(4));
        assertThat(arrayResult).hasSize(1);
        assertThat(arrayResult.get(0)).isEqualTo("8e25e7e5-0dc5-4292-b59b-3665b0ab8280");
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
