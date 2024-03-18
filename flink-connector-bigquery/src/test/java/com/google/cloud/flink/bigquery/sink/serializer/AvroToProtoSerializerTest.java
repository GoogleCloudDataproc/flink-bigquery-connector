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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.assertj.core.api.Assertions;
import org.joda.time.Instant;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.UUID;

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
        Object value = 2494194728L;
        String convertedValue = AvroToProtoSerializer.convertTime(value, true);
        assertThat(convertedValue).isEqualTo("00:37:10.681");
    }

    @Test
    public void testConvertErrorTimeLongType() {
        Object invalidValueLong = 142441321387821952L;
        assertThrows(
                IllegalArgumentException.class,
                () -> AvroToProtoSerializer.convertTime(invalidValueLong, true));
        Object floatValue = 1234.56;
        assertThrows(
                UnsupportedOperationException.class,
                () -> AvroToProtoSerializer.convertTime(floatValue, true));
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

        Object invalidLongValue = 1123456789101112131L;
        exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                AvroToProtoSerializer.convertTimestamp(
                                        invalidLongValue, true, "Timestamp (micros)"));
        Assertions.assertThat(exception)
                .hasMessageContaining(
                        "Should be a long value indicating microseconds since Epoch (1970-01-01 00:00:00) between 0001-01-01 00:00:00.0 and 9999-12-31 23:59:59.999999");
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
        Object value = 142441321937062776L;
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
                UnsupportedOperationException.class,
                () -> AvroToProtoSerializer.convertDateTime(floatValue, true));
    }
    // --------------------------------------------------------------------------------------------------

    @Test
    public void testPrimitiveTypesConversion() {
        BigQueryAvroToProtoSerializerTestResult bigQueryAvroToProtoSerializerTestResult =
                AvroToProtoSerializerTestSchemas.testPrimitiveTypesConversion();
        Schema schema = bigQueryAvroToProtoSerializerTestResult.getSchema();
        Descriptor descriptor = bigQueryAvroToProtoSerializerTestResult.getDescriptor();

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
                        .set("species", "String")
                        .set("flighted", true)
                        .set("sound", ByteBuffer.wrap(byteArray))
                        .set("required_record_field", genericRecord)
                        .build();

        DynamicMessage message =
                AvroToProtoSerializer.getDynamicMessageFromGenericRecord(record, descriptor);
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
    public void testLogicalTypesConversion() {
        BigQueryAvroToProtoSerializerTestResult bigQueryAvroToProtoSerializerTestResult =
                AvroToProtoSerializerTestSchemas.testLogicalTypesConversion();
        Schema schema = bigQueryAvroToProtoSerializerTestResult.getSchema();
        Descriptor descriptor = bigQueryAvroToProtoSerializerTestResult.getDescriptor();

        BigDecimal bigDecimal = new BigDecimal("123456.7891011");
        byte[] bytes = bigDecimal.unscaledValue().toByteArray();
        GenericRecord record =
                new GenericRecordBuilder(schema)
                        .set("timestamp", null)
                        .set("numeric_field", ByteBuffer.wrap(bytes))
                        .set("bignumeric_field", ByteBuffer.wrap(bytes))
                        .set("geography", "GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (3 4, 5 6))")
                        .set("Json", "{\"FirstName\": \"John\", \"LastName\": \"Doe\"}")
                        .build();

        DynamicMessage message =
                AvroToProtoSerializer.getDynamicMessageFromGenericRecord(record, descriptor);
        assertThat(message.getField(descriptor.findFieldByNumber(1))).isEqualTo(0);
        assertThat(message.getField(descriptor.findFieldByNumber(4)))
                .isEqualTo("GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (3 4, 5 6))");
        assertThat(message.getField(descriptor.findFieldByNumber(5)))
                .isEqualTo("{\"FirstName\": \"John\", \"LastName\": \"Doe\"}");
    }

    @Test
    public void testAllPrimitiveSchemaConversion() {
        BigQueryAvroToProtoSerializerTestResult bigQueryAvroToProtoSerializerTestResult =
                AvroToProtoSerializerTestSchemas.testAllPrimitiveSchemaConversion();
        Schema schema = bigQueryAvroToProtoSerializerTestResult.getSchema();
        Descriptor descriptor = bigQueryAvroToProtoSerializerTestResult.getDescriptor();
        byte[] byteArray = "Any String you want".getBytes();
        GenericRecord record =
                new GenericRecordBuilder(schema)
                        .set("name", "abcd")
                        .set("number", 1234L)
                        .set("quantity", 12345)
                        .set("fixed_field", new GenericData.Fixed(schema, byteArray))
                        .set("price", Float.parseFloat("12345.6789"))
                        .set("double_field", Double.parseDouble("1234.4567"))
                        .set("boolean_field", false)
                        .set("enum_field", "C")
                        .set("byte_field", ByteBuffer.wrap(byteArray))
                        .build();
        DynamicMessage message =
                AvroToProtoSerializer.getDynamicMessageFromGenericRecord(record, descriptor);
        assertThat(message.getField(descriptor.findFieldByNumber(1))).isEqualTo("abcd");
        assertThat(message.getField(descriptor.findFieldByNumber(2))).isEqualTo(1234L);
        assertThat(message.getField(descriptor.findFieldByNumber(3))).isEqualTo(12345L);
        assertThat(message.getField(descriptor.findFieldByNumber(4)))
                .isEqualTo(ByteString.copyFrom(byteArray));
        assertThat(message.getField(descriptor.findFieldByNumber(5))).isEqualTo(12345.6789f);
        assertThat(message.getField(descriptor.findFieldByNumber(6))).isEqualTo(1234.4567d);
        assertThat(message.getField(descriptor.findFieldByNumber(7))).isEqualTo(false);
        assertThat(message.getField(descriptor.findFieldByNumber(8))).isEqualTo("C");
        assertThat(message.getField(descriptor.findFieldByNumber(9)))
                .isEqualTo(ByteString.copyFrom(byteArray));
    }
}
