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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/** Tests for {@link AvroToProtoSerializer}. */
public class AvroToProtoSerializerTest {

    // ---------- Test the Helper Functions --------------
    @Test
    public void testConvertJson() {
        Object value = "{\"name\": \"test_value\", \"value\": \"value\"}";
        assertEquals(value.toString(), AvroToProtoSerializer.convertJson(value));
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
        assertEquals(value.toString(), AvroToProtoSerializer.convertGeography(value));
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
        assertEquals(value.toString(), AvroToProtoSerializer.convertUUID(value));
    }

    @Test
    public void testConvertUUIDString() {
        Object value = UUID.randomUUID().toString();
        assertEquals(value.toString(), AvroToProtoSerializer.convertUUID(value));
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
        String bigNumericSchemaString =
                "{\"type\":\"record\","
                        + "\"name\":\"root\","
                        + "\"namespace\":\"com.google.cloud.flink.bigquery\","
                        + "\"doc\":\"Translated Avro Schema for root\","
                        + "\"fields\":"
                        + "[{"
                        + "\"name\":\"bignumeric_field\",\"type\":"
                        + "{\"type\":\"bytes\","
                        + "\"logicalType\":\"decimal\","
                        + "\"precision\":77,"
                        + "\"scale\":38}"
                        + "}]"
                        + "}\n";
        Schema bigNumericSchema =
                new Schema.Parser()
                        .parse(bigNumericSchemaString)
                        .getField("bignumeric_field")
                        .schema();
        Object value = "A random string";
        assertThrows(
                IllegalArgumentException.class,
                () -> AvroToProtoSerializer.convertBigDecimal(value, bigNumericSchema));

        BigDecimal bigDecimal =
                new BigDecimal(
                        "5789604461865805559771174925043439539266.349923328202820554419728792395656481996700");
        // Form byte array in big-endian order.
        Object byteBuffer = ByteBuffer.wrap(bigDecimal.unscaledValue().toByteArray());
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                AvroToProtoSerializer.convertBigDecimal(
                                        byteBuffer, bigNumericSchema));
        Assertions.assertThat(exception).hasMessageContaining("ByteString overflow:");
    }

    @Test
    public void testErrorConvertNumeric() {
        String numericSchemaString =
                "{\"type\":\"record\","
                        + "\"name\":\"root\","
                        + "\"namespace\":\"com.google.cloud.flink.bigquery\","
                        + "\"doc\":\"Translated Avro Schema for root\","
                        + "\"fields\":"
                        + "[{"
                        + "\"name\":\"numeric_field\",\"type\":"
                        + "{\"type\":\"bytes\","
                        + "\"logicalType\":\"decimal\","
                        + "\"precision\":38,"
                        + "\"scale\":9,"
                        + "\"isNumeric\": true}"
                        + "}]"
                        + "}\n";
        Schema numericSchema =
                new Schema.Parser().parse(numericSchemaString).getField("numeric_field").schema();
        Object value = "A random string";
        assertThrows(
                IllegalArgumentException.class,
                () -> AvroToProtoSerializer.convertBigDecimal(value, numericSchema));

        BigDecimal bigDecimal = new BigDecimal("57896349923328446186583439539266.349923328");
        // Form byte array in big-endian order.
        Object byteBuffer = ByteBuffer.wrap(bigDecimal.unscaledValue().toByteArray());
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> AvroToProtoSerializer.convertBigDecimal(byteBuffer, numericSchema));
        Assertions.assertThat(exception).hasMessageContaining("ByteString overflow:");
    }

    @Test
    public void testConvertTimeStringType() {
        Object value = "00:22:59.700440";
        assertEquals(value.toString(), AvroToProtoSerializer.convertTime(value, true));
    }

    @Test
    public void testConvertTimeLongType() {
        Object value = 36708529123L;
        assertEquals("10:11:48.529123", AvroToProtoSerializer.convertTime(value, true));
    }

    @Test
    public void testConvertTimeLongMillisType() {
        Object value = 36708529;
        assertEquals("10:11:48.529", AvroToProtoSerializer.convertTime(value, false));
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
        assertThat(1710270870000L)
                .isEqualTo(
                        AvroToProtoSerializer.convertTimestamp(value, false, "Timestamp (millis)"));
    }

    @Test
    public void testConvertTimestampMillisTimeInstant() {
        Object value = Instant.ofEpochMilli(1710270870123L);
        assertThat(1710270870123000L)
                .isEqualTo(
                        AvroToProtoSerializer.convertTimestamp(value, false, "Timestamp (millis)"));
    }

    @Test
    public void testConvertTimestampMicrosTimeInstant() {
        Object value = Instant.parse("2024-03-13T01:01:16.579501+00:00");
        assertThat(AvroToProtoSerializer.convertTimestamp(value, true, "Timestamp (micros)"))
                .isEqualTo(1710291676579000L);
    }

    @Test
    public void testConvertTimestampMicros() {
        Object value = 1710270870123L;
        assertThat(AvroToProtoSerializer.convertTimestamp(value, true, "Timestamp (micros)"))
                .isEqualTo(1710270870123L);
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
        assertThat(19795).isEqualTo(AvroToProtoSerializer.convertDate(value));
    }

    @Test
    public void testConvertDate() {
        Object value = 19794;
        assertThat(19794).isEqualTo(AvroToProtoSerializer.convertDate(value));
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
        assertEquals(value.toString(), AvroToProtoSerializer.convertDateTime(value, true));
    }

    @Test
    public void testConvertDateTimeLongType() {
        Object value = 1710290574347000L;
        assertEquals("2024-03-13T00:42:54.347", AvroToProtoSerializer.convertDateTime(value, true));
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

    /**
     * Test to check <code>getDynamicMessageFromGenericRecord()</code> for Primitive types supported
     * by BigQuery.
     *
     * <ul>
     *   <li>BQ Type - Converted Avro Type
     *   <li>"INTEGER" - LONG
     *   <li>"FLOAT" - FLOAT
     *   <li>"STRING" - STRING
     *   <li>"BOOLEAN" - BOOLEAN
     *   <li>"BYTES" - BYTES
     * </ul>
     */
    @Test
    public void testAllBigQuerySupportedPrimitiveTypesConversionToDynamicMessageCorrectly() {
        // Obtaining the Schema Provider and the Avro-Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRequiredPrimitiveTypes();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        byte[] byteArray = "Any String you want".getBytes();
        Schema recordSchema = avroSchema.getField("required_record_field").schema();
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

        // Form the Dynamic Message.
        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);

        // Check for the desired results.
        assertEquals(-7099548873856657385L, message.getField(descriptor.findFieldByNumber(1)));
        assertEquals(0.5616495161359795, message.getField(descriptor.findFieldByNumber(2)));
        assertEquals("String", message.getField(descriptor.findFieldByNumber(3)));
        assertEquals(true, message.getField(descriptor.findFieldByNumber(4)));
        assertEquals(
                ByteString.copyFrom(byteArray), message.getField(descriptor.findFieldByNumber(5)));
        FieldDescriptor fieldDescriptor = descriptor.findFieldByNumber(6);
        message = (DynamicMessage) message.getField(fieldDescriptor);
        assertEquals(
                "hello",
                message.getField(
                        descriptor
                                .findNestedTypeByName(fieldDescriptor.toProto().getTypeName())
                                .findFieldByNumber(1)));
    }

    /**
     * Test to check <code>getDynamicMessageFromGenericRecord()</code> for Primitive types supported
     * by BigQuery. However <code>null</code> value is passed to the record field to test for error.
     */
    @Test
    public void testAllBigQuerySupportedPrimitiveTypesConversionToDynamicMessageIncorrectly() {
        // Obtaining the Schema Provider and the Avro-Record.
        // -- Non-nullable Schema for descriptor
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRequiredPrimitiveTypes();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        // -- Nullable Schema for descriptor
        Schema avroSchema =
                TestBigQuerySchemas.getSchemaWithNullablePrimitiveTypes().getAvroSchema();
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("number", null)
                        .set("price", null)
                        .set("species", null)
                        .set("flighted", null)
                        .set("sound", null)
                        .set("required_record_field", null)
                        .build();

        // Check for the desired results.
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> getDynamicMessageFromGenericRecord(record, descriptor));
        Assertions.assertThat(exception)
                .hasMessageContaining("Received null value for non-nullable field");
    }

    /**
     * Test to check <code>getDynamicMessageFromGenericRecord()</code> for Primitive types supported
     * by Avro but not offered by BigQuery.
     *
     * <ul>
     *   <li>DOUBLE
     *   <li>ENUM
     *   <li>FIXED
     *   <li>INT
     * </ul>
     */
    @Test
    public void testAllRemainingAvroSupportedPrimitiveTypesConversionToDynamicMessageCorrectly() {
        // Obtaining the Schema Provider and the Avro-Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRemainingPrimitiveTypes();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        byte[] byteArray = "Any String you want".getBytes();
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("quantity", 1234)
                        .set("fixed_field", new GenericData.Fixed(avroSchema, byteArray))
                        .set("float_field", Float.parseFloat("12345.6789"))
                        .set("enum_field", "C")
                        .build();

        // Form the Dynamic Message.
        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);

        // Check for the desired results.
        assertEquals(1234, message.getField(descriptor.findFieldByNumber(1)));
        assertEquals(
                ByteString.copyFrom(byteArray), message.getField(descriptor.findFieldByNumber(2)));
        assertEquals(12345.6789f, message.getField(descriptor.findFieldByNumber(3)));
        assertEquals("C", message.getField(descriptor.findFieldByNumber(4)));
    }

    /**
     * Test to check <code>getDynamicMessageFromGenericRecord()</code> for Primitive types supported
     * by Avro but not offered by BigQuery. However <code>null</code> value is passed to the record
     * field to test for error.
     */
    @Test
    public void testAllRemainingAvroSupportedPrimitiveTypesConversionToDynamicMessageIncorrectly() {
        // Obtaining the Schema Provider and the Avro-Record.
        // -- Non-nullable Schema for descriptor
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRemainingPrimitiveTypes();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        // -- Nullable Schema for descriptor
        Schema avroSchema =
                TestBigQuerySchemas.getSchemaWithUnionOfRemainingPrimitiveTypes().getAvroSchema();
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("quantity", null)
                        .set("fixed_field", null)
                        .set("float_field", null)
                        .set("enum_field", null)
                        .build();

        // Check for the desired error.
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> getDynamicMessageFromGenericRecord(record, descriptor));
        Assertions.assertThat(exception)
                .hasMessageContaining("Received null value for non-nullable field");
    }

    /**
     * Test to check <code>serialize()</code> for Primitive types supported by BigQuery, but the
     * fields are <b>NULLABLE</b>, so conversion of <code>null</code> is tested - serialized byte
     * string should be empty.
     */
    @Test
    public void testAllBigQuerySupportedNullablePrimitiveTypesConversionToEmptyByteStringCorrectly()
            throws BigQuerySerializationException {
        // Obtaining the Schema Provider and the Avro-Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithNullablePrimitiveTypes();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("number", null)
                        .set("price", null)
                        .set("species", null)
                        .set("flighted", null)
                        .set("sound", null)
                        .set("required_record_field", null)
                        .build();

        // Form the Dynamic Message via the serializer.
        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        ByteString byteString = serializer.serialize(record);

        // Check for the desired results.
        assertEquals("", byteString.toStringUtf8());
    }

    /**
     * Test to check <code>serialize()</code> for Primitive types supported Avro but not by
     * BigQuery, but the fields are <b>NULLABLE</b>, so conversion of <code>null</code> is tested -
     * serialized byte string should be empty.
     */
    @Test
    public void
            testUnionOfAllRemainingAvroSupportedPrimitiveTypesConversionToEmptyByteStringCorrectly()
                    throws BigQuerySerializationException {
        // Obtaining the Schema Provider and the Avro-Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithUnionOfRemainingPrimitiveTypes();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("quantity", null)
                        .set("fixed_field", null)
                        .set("float_field", null)
                        .set("enum_field", null)
                        .build();

        // Form the Dynamic Message via the serializer.
        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        ByteString byteString = serializer.serialize(record);

        // Check for the desired results.
        assertEquals("", byteString.toStringUtf8());
    }

    // --------------- Test Logical Data Types (Nullable and Required) ---------------
    @Test
    public void testLogicalTypesConversion() {
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRequiredLogicalTypes();
        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();

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
        assertEquals(1710919250269000L, message.getField(descriptor.findFieldByNumber(1)));
        assertEquals("14:02:26.554456", message.getField(descriptor.findFieldByNumber(2)));
        assertEquals(
                "2024-03-20T13:59:04.787424", message.getField(descriptor.findFieldByNumber(3)));
        assertEquals(19802, message.getField(descriptor.findFieldByNumber(4)));
        // TODO: Check the ByteString.
        assertEquals(
                "GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (3 4, 5 6))",
                message.getField(descriptor.findFieldByNumber(7)));
        assertEquals(
                "{\"FirstName\": \"John\", \"LastName\": \"Doe\"}",
                message.getField(descriptor.findFieldByNumber(8)));
    }

    @Test
    public void testRemainingLogicalConversion() {
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRemainingLogicalTypes();
        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();

        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("ts_millis", DateTime.parse("2024-03-20T12:50:50.269+05:30"))
                        .set("time_millis", 45745727)
                        .set("lts_millis", 1710938587462L)
                        .set("uuid", UUID.fromString("8e25e7e5-0dc5-4292-b59b-3665b0ab8280"))
                        .build();

        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);
        assertEquals(1710919250269000L, message.getField(descriptor.findFieldByNumber(1)));
        assertEquals("12:42:25.727", message.getField(descriptor.findFieldByNumber(2)));
        assertEquals("2024-03-20T12:43:07.462", message.getField(descriptor.findFieldByNumber(3)));
        assertEquals(
                "8e25e7e5-0dc5-4292-b59b-3665b0ab8280",
                message.getField(descriptor.findFieldByNumber(4)));
    }

    @Test
    public void testNullableLogicalTypesConversion() throws BigQuerySerializationException {
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithNullableLogicalTypes();
        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();

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
        assertEquals("", byteString.toStringUtf8());
    }

    @Test
    public void testUnionOfRemainingLogicalConversion() throws BigQuerySerializationException {
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithUnionOfLogicalTypes();
        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();

        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("ts_millis", null)
                        .set("time_millis", null)
                        .set("lts_millis", null)
                        .set("uuid", null)
                        .build();

        ByteString byteString = serializer.serialize(record);
        assertEquals("", byteString.toStringUtf8());
    }

    // ------------ Test Schemas with Record of Different Types -----------
    /**
     * Test to check <code>getDynamicMessageFromGenericRecord()</code> for Record type schema having
     * an ARRAY field.
     */
    @Test
    public void testRecordOfArrayConversionToDynamicMessageCorrectly() {
        // Obtaining the Schema Provider and the Avro-Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRecordOfArray();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
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

        // Form the Dynamic Message.
        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);

        // Check for the desired results.
        message = (DynamicMessage) message.getField(fieldDescriptor);
        assertEquals(
                arrayList,
                message.getField(
                        descriptor
                                .findNestedTypeByName(fieldDescriptor.toProto().getTypeName())
                                .findFieldByNumber(1)));
    }

    /**
     * Test to check <code>getDynamicMessageFromGenericRecord()</code> for Record type schema having
     * an ARRAY field. However, an invalid value (integer value instead of Array) is passed to test
     * for error.
     */
    @Test
    public void testRecordOfArrayConversionToDynamicMessageIncorrectly() {
        // Obtaining the Schema Provider and the Avro-Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRecordOfArray();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set(
                                "record_with_array",
                                new GenericRecordBuilder(
                                                avroSchema.getField("record_with_array").schema())
                                        .set("array_in_record", 12345)
                                        .build())
                        .build();

        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> getDynamicMessageFromGenericRecord(record, descriptor));
        Assertions.assertThat(exception)
                .hasMessageContaining(
                        "Expected an Iterable,\n" + " Found class java.lang.Integer instead");
    }

    /**
     * Test to check <code>serialize()</code> for Record type schema having a UNION field (with
     * null). Since the record has a union of NULL field, <code>null</code> value is serialized. The
     * serialized byte string is checked to be empty.
     */
    @Test
    public void testRecordOfUnionSchemaConversionToEmptyByteStringCorrectly() {
        // Obtaining the Schema Provider and the Avro-Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRecordOfUnionType();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set(
                                "record_with_union",
                                new GenericRecordBuilder(
                                                avroSchema.getField("record_with_union").schema())
                                        .set("union_in_record", null)
                                        .build())
                        .build();
        FieldDescriptor fieldDescriptor = descriptor.findFieldByNumber(1);

        // Form the Dynamic Message.
        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);

        // Check for the desired results.
        ByteString byteString = ((DynamicMessage) message.getField(fieldDescriptor)).toByteString();
        assertEquals("", byteString.toStringUtf8());
    }

    /**
     * Test to check <code>serialize()</code> for Record type schema having a UNION field (of null
     * and boolean). To check an invalid value, an Integer is passed.
     */
    @Test
    public void testRecordOfUnionSchemaConversionToEmptyByteStringIncorrectly() {
        // Obtaining the Schema Provider and the Avro-Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRecordOfUnionType();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set(
                                "record_with_union",
                                new GenericRecordBuilder(
                                                avroSchema.getField("record_with_union").schema())
                                        .set("union_in_record", 12345)
                                        .build())
                        .build();
        ClassCastException exception =
                assertThrows(
                        ClassCastException.class,
                        () -> getDynamicMessageFromGenericRecord(record, descriptor));
        Assertions.assertThat(exception)
                .hasMessageFindingMatch(
                        "(class)? ?java.lang.Integer cannot be cast to (class)? ?java.lang.Boolean");
    }

    /**
     * Test to check <code>getDynamicMessageFromGenericRecord()</code> for Record type schema having
     * a RECORD type field.
     */
    @Test
    public void testRecordOfRecordConversionToDynamicMessageCorrectly() {
        // Obtaining the Schema Provider and the Avro-Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRecordOfRecord();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
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

        // Form the Dynamic Message.
        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);

        // Check for the desired results.
        message = (DynamicMessage) message.getField(descriptor.findFieldByNumber(1));
        descriptor =
                descriptor.findNestedTypeByName(
                        descriptor.findFieldByNumber(1).toProto().getTypeName());
        message = (DynamicMessage) message.getField(descriptor.findFieldByNumber(1));
        descriptor =
                descriptor.findNestedTypeByName(
                        descriptor.findFieldByNumber(1).toProto().getTypeName());
        assertEquals(7267611125055979836L, message.getField(descriptor.findFieldByNumber(1)));
        assertEquals(
                "yllgqpemxjnpsoaqlwlgbqjkywxnavntf",
                message.getField(descriptor.findFieldByNumber(2)));
    }

    /**
     * Test to check <code>getDynamicMessageFromGenericRecord()</code> for Record type schema having
     * all Primitive type fields (supported by BigQuery).
     */
    @Test
    public void testRecordOfAllBigQuerySupportedPrimitiveTypeConversionToDynamicMessageCorrectly() {
        // Obtaining the Schema Provider and the Avro-Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRecordOfPrimitiveTypes();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        Schema innerRecordSchema = avroSchema.getField("record_of_primitive_types").schema();
        byte[] byteArray = "Any String you want".getBytes();
        Schema recordSchema =
                avroSchema
                        .getField("record_of_primitive_types")
                        .schema()
                        .getField("required_record_field")
                        .schema();
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

        // Form the Dynamic Message.
        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);

        // Check for the desired results.
        message = (DynamicMessage) message.getField(descriptor.findFieldByNumber(1));
        descriptor =
                descriptor.findNestedTypeByName(
                        descriptor.findFieldByNumber(1).toProto().getTypeName());
        assertEquals(-7099548873856657385L, message.getField(descriptor.findFieldByNumber(1)));
        assertEquals(0.5616495161359795, message.getField(descriptor.findFieldByNumber(2)));
        assertEquals("String", message.getField(descriptor.findFieldByNumber(3)));
        assertEquals(true, message.getField(descriptor.findFieldByNumber(4)));
        assertEquals(
                ByteString.copyFrom(byteArray), message.getField(descriptor.findFieldByNumber(5)));
        FieldDescriptor fieldDescriptor = descriptor.findFieldByNumber(6);
        message = (DynamicMessage) message.getField(fieldDescriptor);
        assertEquals(
                "hello",
                message.getField(
                        descriptor
                                .findNestedTypeByName(fieldDescriptor.toProto().getTypeName())
                                .findFieldByNumber(1)));
    }

    /**
     * Test to check <code>getDynamicMessageFromGenericRecord()</code> for Record type schema having
     * all Primitive type fields (supported by Avro, not by BigQuery).
     */
    @Test
    public void
            testRecordOfAllRemainingAvroSupportedPrimitiveTypeConversionToDynamicMessageCorrectly() {
        // Obtaining the Schema Provider and the Avro-Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRecordOfRemainingPrimitiveTypes();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
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

        // Form the Dynamic Message via the serializer.
        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);

        // Check for the desired results.
        message = (DynamicMessage) message.getField(descriptor.findFieldByNumber(1));
        descriptor =
                descriptor.findNestedTypeByName(
                        descriptor.findFieldByNumber(1).toProto().getTypeName());
        assertEquals(1234, message.getField(descriptor.findFieldByNumber(1)));
        assertEquals(
                ByteString.copyFrom(byteArray), message.getField(descriptor.findFieldByNumber(2)));
        assertEquals(12345.6789f, message.getField(descriptor.findFieldByNumber(3)));
        assertEquals("C", message.getField(descriptor.findFieldByNumber(4)));
    }

    @Test
    public void testRecordOfLogicalTypeConversion() {
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRecordOfLogicalTypes();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
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
        assertEquals(1710919250269000L, message.getField(descriptor.findFieldByNumber(1)));
        assertEquals("14:02:26.554456", message.getField(descriptor.findFieldByNumber(2)));
        assertEquals(
                "2024-03-20T13:59:04.787424", message.getField(descriptor.findFieldByNumber(3)));
        assertEquals(19802, message.getField(descriptor.findFieldByNumber(4)));
        // TODO: Check the ByteString.
        assertEquals(
                "GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (3 4, 5 6))",
                message.getField(descriptor.findFieldByNumber(7)));
        assertEquals(
                "{\"FirstName\": \"John\", \"LastName\": \"Doe\"}",
                message.getField(descriptor.findFieldByNumber(8)));
    }

    @Test
    public void testRecordOfRemainingLogicalTypeConversion() {
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRecordOfRemainingLogicalTypes();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
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
        assertEquals("12:42:25.727", message.getField(descriptor.findFieldByNumber(2)));
        assertEquals("2024-03-20T12:43:07.462", message.getField(descriptor.findFieldByNumber(3)));
        assertEquals(
                "8e25e7e5-0dc5-4292-b59b-3665b0ab8280",
                message.getField(descriptor.findFieldByNumber(4)));
    }

    // ------------Test Schemas with ARRAY of Different Types -------------
    /**
     * Test to check <code>serialize()</code> for ARRAY type schema having a UNION type. Since
     * BigQuery does not allow <code>null</code> values in REPEATED type field, a descriptor is
     * created with long type ARRAY.
     *
     * <ol>
     *   <li>UNION of NULL, LONG:<br>
     *       An array is created with Long and null values. Since BigQuery cannot have null values
     *       in a REPEATED field, error is expected
     *   <li>UNION of LONG, INT:<br>
     *       An array is created with Long and Integer values. Since BigQuery cannot have multiple
     *       datatype values in a REPEATED field, error is expected
     * </ol>
     */
    @Test
    public void testArrayOfUnionConversionToByteStringIncorrectly() {
        // Obtaining the Schema Provider and the Avro-Record.
        String notNullString =
                " \"fields\": [\n"
                        + "{\"name\": \"array_with_union\", \"type\": {\"type\": \"array\", \"items\":  \"long\"}} ]";
        Schema notNullSchema = getAvroSchemaFromFieldString(notNullString);
        BigQuerySchemaProvider bigQuerySchemaProvider =
                new BigQuerySchemaProviderImpl(notNullSchema);
        // -- 1. check UNION with NULL
        String fieldString = TestBigQuerySchemas.getSchemaWithArrayOfUnionValue();
        Schema nullSchema = getAvroSchemaFromFieldString(fieldString);
        GenericRecord recordWithNullInUnion =
                new GenericRecordBuilder(nullSchema)
                        .set("array_with_union", Arrays.asList(1234567L, null))
                        .build();
        // -- 2. Check union of NOT NULL-multiple types.
        fieldString = TestBigQuerySchemas.getSchemaWithArrayOfMultipleValues();
        nullSchema = getAvroSchemaFromFieldString(fieldString);
        GenericRecord recordWithMultipleDatatypesInUnion =
                new GenericRecordBuilder(nullSchema)
                        .set("array_with_union", Arrays.asList(1234567L, 12345))
                        .build();

        // Form the Dynamic Message via the serializer.
        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        BigQuerySerializationException exceptionForNullInUnion =
                assertThrows(
                        BigQuerySerializationException.class,
                        () -> serializer.serialize(recordWithNullInUnion));
        BigQuerySerializationException exceptionForMultipleDatatypesInUnion =
                assertThrows(
                        BigQuerySerializationException.class,
                        () -> serializer.serialize(recordWithMultipleDatatypesInUnion));

        // Check for the desired results.
        Assertions.assertThat(exceptionForNullInUnion)
                .hasMessageContaining("Array cannot have NULLABLE datatype");
        Assertions.assertThat(exceptionForMultipleDatatypesInUnion)
                .hasMessageContaining("ARRAY cannot have multiple datatypes in BigQuery.");
    }

    /**
     * Test to check <code>serialize()</code> for ARRAY type schema having a NULL type. Since
     * BigQuery does not allow <code>null</code> values in REPEATED type field, a descriptor is
     * created with long type ARRAY. <br>
     * An array is created with null values. Since BigQuery cannot have null values in a REPEATED
     * field, error is expected.
     */
    @Test
    public void testArrayOfNullConversionToByteStringIncorrectly() {
        // Obtaining the Schema Provider and the Avro-Record.
        // -- Obtaining notNull schema for descriptor.
        String notNullString =
                " \"fields\": [\n"
                        + "{\"name\": \"array_with_null\", \"type\": {\"type\": \"array\", \"items\":  \"long\"}} ]";
        Schema notNullSchema = getAvroSchemaFromFieldString(notNullString);
        BigQuerySchemaProvider bigQuerySchemaProvider =
                new BigQuerySchemaProviderImpl(notNullSchema);
        // -- Obtaining null schema for descriptor.
        String fieldString = TestBigQuerySchemas.getSchemaWithArrayOfNullValue();
        Schema nullSchema = getAvroSchemaFromFieldString(fieldString);
        GenericRecord record =
                new GenericRecordBuilder(nullSchema)
                        .set("array_with_null", Arrays.asList(1234567L, null))
                        .build();

        // Form the Dynamic Message via the serializer.
        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        BigQuerySerializationException exception =
                assertThrows(
                        BigQuerySerializationException.class, () -> serializer.serialize(record));

        // Check for the desired results.
        Assertions.assertThat(exception)
                .hasMessageContaining("Array cannot have NULLABLE datatype");
    }

    /**
     * Test to check <code>getDynamicMessageFromGenericRecord()</code> for ARRAY type schema having
     * a RECORD type. <br>
     * An array is created with RECORD type values.
     */
    @Test
    public void testArrayOfRecordConversionToDynamicMessageCorrectly() {
        // Obtaining the Schema Provider and the Avro-Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithArrayOfRecord();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
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

        // Form the Dynamic Message.
        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);
        assertThat(message.getField(descriptor.findFieldByNumber(1))).isInstanceOf(List.class);
        List<DynamicMessage> arrayResult =
                (List<DynamicMessage>) message.getField(descriptor.findFieldByNumber(1));

        // Check for the desired results.
        assertThat(arrayResult).hasSize(2);
        // -- the descriptor for elements inside the array.
        descriptor =
                descriptor.findNestedTypeByName(
                        descriptor.findFieldByNumber(1).toProto().getTypeName());
        message = arrayResult.get(0);
        assertEquals(8034881802526489441L, message.getField(descriptor.findFieldByNumber(1)));
        assertEquals(
                "fefmmuyoosmglqtnwfxahgoxqpyhc", message.getField(descriptor.findFieldByNumber(2)));
        message = arrayResult.get(1);
        assertEquals(8034881802526489441L, message.getField(descriptor.findFieldByNumber(1)));
        assertEquals(
                "fefmmuyoosmglqtnwfxahgoxqpyhc", message.getField(descriptor.findFieldByNumber(2)));
    }

    /**
     * Test to check <code>getDynamicMessageFromGenericRecord()</code> for different ARRAYS having
     * all Primitive types. <br>
     * A record is created having six fields, each of ARRAY (different item type) types.
     *
     * <ul>
     *   <li>number - ARRAY of type LONG
     *   <li>price - ARRAY of type DOUBLE
     *   <li>species - ARRAY of type STRING
     *   <li>flighted - ARRAY of type BOOLEAN
     *   <li>sound - ARRAY of type BYTES
     *   <li>required_record_field - ARRAY of type RECORD
     * </ul>
     */
    @Test
    public void testArraysOfPrimitiveTypesConversionToDynamicMessageCorrectly() {
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithArraysOfPrimitiveTypes();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
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
        assertEquals(-250555967807021764L, arrayResult.get(0));

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(2));
        assertThat(arrayResult).hasSize(2);
        assertEquals(0.34593866360929726, arrayResult.get(0));
        assertEquals(0.35197578762609993, arrayResult.get(1));

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(3));
        assertThat(arrayResult).hasSize(3);
        assertEquals("nsguocxfjqaufhsunahvxmcpivutfqv", arrayResult.get(0));
        assertEquals("q", arrayResult.get(1));
        assertEquals("pldvejbqmfyosgxmbmqjsafjbcfqwhiagbckmti", arrayResult.get(2));

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(4));
        assertThat(arrayResult).hasSize(4);
        assertEquals(false, arrayResult.get(0));
        assertEquals(false, arrayResult.get(1));
        assertEquals(false, arrayResult.get(2));
        assertEquals(true, arrayResult.get(3));

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(5));
        assertThat(arrayResult).hasSize(5);
        // Not checking the rest since they are the same.
        assertEquals(ByteString.copyFrom("Hello".getBytes()), arrayResult.get(0));
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

        assertEquals("a", arrayResult.get(0));
        assertEquals("b", arrayResult.get(1));
        assertEquals("c", arrayResult.get(2));
        assertEquals("d", arrayResult.get(3));
        assertEquals("e", arrayResult.get(4));
        assertEquals("f", arrayResult.get(5));
    }

    /**
     * Test to check <code>getDynamicMessageFromGenericRecord()</code> for different ARRAYS having
     * all Primitive types (supported by Avro, not BQ). <br>
     * A record is created having four fields, each of ARRAY (different item type) types.
     *
     * <ul>
     *   <li>quantity - ARRAY of type INT
     *   <li>fixed_field - ARRAY of type FIXED
     *   <li>float_field - ARRAY of type FLOAT
     *   <li>enum_field - ARRAY of type ENUM
     * </ul>
     */
    @Test
    public void testArraysOfRemainingPrimitiveTypesConversionToDynamicMessageCorrectly() {
        // Obtaining the Schema Provider and the Avro-Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithArraysOfRemainingPrimitiveTypes();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        // -- Initialising the RECORD.
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

        // Form the Dynamic Message.
        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);

        // Check for the desired results.
        // -- 1. check field [1] - quantity
        List<Object> arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(1));
        assertThat(arrayResult).hasSize(1);
        assertEquals(89767285, arrayResult.get(0));
        // -- 2. check field [21] - fixed_field
        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(2));
        assertThat(arrayResult).hasSize(1);
        assertEquals(ByteString.copyFrom(byteArray), arrayResult.get(0));
        // -- 3. check field [3] - float_field
        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(3));
        assertThat(arrayResult).hasSize(4);
        assertEquals(0.26904225f, arrayResult.get(0));
        assertEquals(0.558431f, arrayResult.get(1));
        assertEquals(0.2269839f, arrayResult.get(2));
        assertEquals(0.70421267f, arrayResult.get(3));
        // -- 4. check field [4] - enum_field
        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(4));
        assertThat(arrayResult).hasSize(3);
        assertEquals("A", arrayResult.get(0));
        assertEquals("C", arrayResult.get(1));
        assertEquals("A", arrayResult.get(2));
    }

    @Test
    public void testArraysOfLogicalTypesConversion() {
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithArraysOfLogicalTypes();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
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
        assertEquals(1710919250269000L, arrayResult.get(0));

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(2));
        assertThat(arrayResult).hasSize(2);
        assertEquals("14:02:26.554456", arrayResult.get(0));

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(3));
        assertThat(arrayResult).hasSize(3);
        assertEquals("2024-03-20T13:59:04.787424", arrayResult.get(0));

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(4));
        assertThat(arrayResult).hasSize(4);
        assertEquals(19802, arrayResult.get(0));

        ArrayUtils.reverse(bytes);
        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(5));
        assertThat(arrayResult).hasSize(1);
        assertEquals(ByteString.copyFrom(bytes), arrayResult.get(0));

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(6));
        assertThat(arrayResult).hasSize(1);
        assertEquals(ByteString.copyFrom(bytes), arrayResult.get(0));

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(7));
        assertThat(arrayResult).hasSize(1);
        assertEquals("GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (3 4, 5 6))", arrayResult.get(0));

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(8));
        assertThat(arrayResult).hasSize(2);
        assertEquals("{\"FirstName\" : \"John\", \"LastName\": \"Doe\"}", arrayResult.get(0));
        assertEquals("{\"FirstName\" : \"Jane\", \"LastName\": \"Doe\"}", arrayResult.get(1));
    }

    @Test
    public void testArraysOfRemainingLogicalTypesConversion() {
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithArraysOfRemainingLogicalTypes();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
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
        assertEquals("12:42:25.727", arrayResult.get(0));

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(1));
        assertThat(arrayResult).hasSize(1);
        assertEquals(1710919250269000L, arrayResult.get(0));

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(3));
        assertThat(arrayResult).hasSize(2);
        assertEquals("2024-03-20T12:43:07.462", arrayResult.get(0));

        arrayResult = (List<Object>) message.getField(descriptor.findFieldByNumber(4));
        assertThat(arrayResult).hasSize(1);
        assertEquals("8e25e7e5-0dc5-4292-b59b-3665b0ab8280", arrayResult.get(0));
    }

    // ------------Test Schemas with UNION of Different Types (Excluding Primitive and Logical)
    /**
     * Test to check <code>serialize()</code> for UNION having ARRAY type. <br>
     * Since BigQuery does not support OPTIONAL/NULLABLE arrays, descriptor is created with ARRAY of
     * type float. <br>
     * A record is created with <code>null</code> value for this field. <br>
     * Byte String is expected to be empty (as Storage API will automatically cast it as <code>[]
     * </code>)
     */
    @Test
    public void testUnionOfArrayConversionToDynamicMessageCorrectly()
            throws BigQuerySerializationException {
        // Obtaining the Schema Provider and the Avro-Record.
        // -- Obtaining the nullable type for record formation
        String fieldString = TestBigQuerySchemas.getSchemaWithUnionOfArray();
        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        GenericRecord record =
                new GenericRecordBuilder(avroSchema).set("array_field_union", null).build();
        // -- Obtaining the non-nullable type for descriptor
        String nonNullFieldString =
                "\"fields\": [\n"
                        + "   {\"name\": \"array_field_union\", \"type\": {\"type\": \"array\","
                        + " \"items\": \"float\"}}\n"
                        + " ]";
        Schema nonNullSchema = getAvroSchemaFromFieldString(nonNullFieldString);
        BigQuerySchemaProvider bigQuerySchemaProvider =
                new BigQuerySchemaProviderImpl(nonNullSchema);

        // Form the Dynamic Message via the serializer.
        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        ByteString byteString = serializer.serialize(record);

        // Check for the desired results.
        assertEquals("", byteString.toStringUtf8());
    }

    /**
     * Test to check <code>serialize()</code> for UNION having ARRAY of Type RECORD. <br>
     * Since BigQuery does not support OPTIONAL/NULLABLE arrays, descriptor is created with ARRAY of
     * type RECORD. <br>
     * A record is created with <code>null</code> value for this field. <br>
     * Byte String is expected to be empty (as Storage API will automatically cast it as <code>[]
     * </code>)
     */
    @Test
    public void testUnionOfArrayOfRecordConversionToDynamicMessageCorrectly()
            throws BigQuerySerializationException {
        // Obtaining the Schema Provider and the Avro-Record.
        // -- Obtaining the nullable type for record formation
        String fieldString = TestBigQuerySchemas.getSchemaWithUnionOfArrayOfRecord();
        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        GenericRecord record =
                new GenericRecordBuilder(avroSchema).set("array_of_records_union", null).build();
        // -- Obtaining the non-nullable type for descriptor
        String nonNullFieldString =
                "\"fields\": [\n"
                        + "   {\"name\": \"array_of_records_union\", \"type\": "
                        + "{\"type\": \"array\", \"items\": {\"name\": \"inside_record_union\", "
                        + "\"type\": \"record\", \"fields\": "
                        + "[{\"name\": \"value\", \"type\": \"long\"},"
                        + "{\"name\": \"another_value\",\"type\": \"string\"}]}}}\n"
                        + " ]";
        Schema nonNullSchema = getAvroSchemaFromFieldString(nonNullFieldString);
        BigQuerySchemaProvider bigQuerySchemaProvider =
                new BigQuerySchemaProviderImpl(nonNullSchema);

        // Form the Dynamic Message via the serializer.
        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        ByteString byteString = serializer.serialize(record);

        // Check for the desired results.
        assertEquals("", byteString.toStringUtf8());
    }

    /**
     * Test to check <code>serialize()</code> for a REQUIRED RECORD. <br>
     * A record is created with <code>null</code> value for this field. <br>
     * This record is attempted to be serialized for a REQUIRED field, and is expected to throw an
     * error.
     */
    @Test
    public void testNullableRecordToByteStringIncorrectly() {
        // Obtaining the Schema Provider and the Avro-Record.
        String recordSchemaString =
                "\"fields\":[{\"name\": \"record_field_union\", \"type\":"
                        + getRecordSchema("inner_record")
                        + " }]";
        Schema recordSchema = getAvroSchemaFromFieldString(recordSchemaString);
        // -- Obtain the schema provider for descriptor with RECORD of MODE Required.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                new BigQuerySchemaProviderImpl(recordSchema);
        // -- Obtain the nullable type for descriptor
        Schema nullableRecordSchema =
                TestBigQuerySchemas.getSchemaWithUnionOfRecord().getAvroSchema();
        // -- Form a Null record.
        GenericRecord record =
                new GenericRecordBuilder(nullableRecordSchema)
                        .set("record_field_union", null)
                        .build();

        // Try to serialize, Form the Dynamic Message via the serializer.
        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        BigQuerySerializationException exception =
                assertThrows(
                        BigQuerySerializationException.class, () -> serializer.serialize(record));

        // Check for the desired results.
        Assertions.assertThat(exception)
                .hasMessageContaining(
                        "Received null value for non-nullable field record_field_union");
    }

    /**
     * Test to check <code>serialize()</code> for a NULLABLE RECORD. <br>
     * A record is created with <code>null</code> value for this field. <br>
     * This record is attempted to be serialized for a NULLABLE field, and an empty byte string is
     * expected.
     */
    @Test
    public void testUnionOfNullRecordConversionToByteStringCorrectly()
            throws BigQuerySerializationException {
        // Obtaining the Schema Provider and the Avro-Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithUnionOfRecord();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        // -- Record Is Null
        GenericRecord nullRecord =
                new GenericRecordBuilder(avroSchema).set("record_field_union", null).build();

        // Try to serialize, Form the Dynamic Message via the serializer.
        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        ByteString byteString = serializer.serialize(nullRecord);

        // Check for the desired results.
        assertEquals("", byteString.toStringUtf8());
    }

    /**
     * Test to check <code>getDynamicMessageFromGenericRecord()</code> for a UNION of a RECORD. <br>
     * This record is serialized for a NULLABLE field.
     */
    @Test
    public void testUnionOfRecordConversion() {
        // Obtaining the Schema Provider and the Avro-Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithUnionOfRecord();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        // -- Record is not null
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set(
                                "record_field_union",
                                new GenericRecordBuilder(
                                                getAvroSchemaFromFieldString(
                                                        "\"fields\":[{\"name\":\"value\","
                                                                + "\"type\":\"long\"},"
                                                                + "{\"name\":\"another_value\","
                                                                + "\"type\":\"string\"}]"))
                                        .set("value", 12345678910L)
                                        .set("another_value", "hello")
                                        .build())
                        .build();

        // Try to serialize, Form the Dynamic Message.
        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);

        // Check for the desired results.
        // -- Obtain "inner_record"
        message = (DynamicMessage) message.getField(descriptor.findFieldByNumber(1));
        // -- Update the descriptor to point to the "inner_record" now.
        descriptor =
                descriptor.findNestedTypeByName(
                        descriptor.findFieldByNumber(1).toProto().getTypeName());
        assertEquals(12345678910L, message.getField(descriptor.findFieldByNumber(1)));
        assertEquals("hello", message.getField(descriptor.findFieldByNumber(2)));
    }

    /**
     * Test to check <code>getDynamicMessageFromGenericRecord()</code> for different fields having
     * UNION of a single primitive type. The record is created with the following datatypes.
     *
     * <ol>
     *   <li>number - UNION of type LONG (["long"])
     *   <li>price - UNION of type DOUBLE (["double"])
     *   <li>species - UNION of type STRING (["string"])
     *   <li>flighted - UNION of type BOOLEAN (["boolean"])
     *   <li>sound - UNION of type BYTES (["bytes"])
     *   <li>required_record_field - UNION of type RECORD (["record"])
     * </ol>
     */
    @Test
    public void testUnionOfSinglePrimitiveTypeConversion() {
        // Obtaining the Schema Provider and the Avro-Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithAllPrimitiveSingleUnion();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        byte[] byteArray = "Any String you want".getBytes();
        String recordSchemaString =
                "{\"type\":\"record\",\"name\":\"required_record_field\","
                        + "\"doc\":\"Translated Avro Schema for required_record_field\","
                        + "\"fields\":[{\"name\":\"species\",\"type\":\"string\"}]}}";
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

        // Try to serialize, Form the Dynamic Message.
        DynamicMessage message = getDynamicMessageFromGenericRecord(record, descriptor);

        // Check for the desired results.
        assertEquals(-7099548873856657385L, message.getField(descriptor.findFieldByNumber(1)));
        assertEquals(0.5616495161359795, message.getField(descriptor.findFieldByNumber(2)));
        assertEquals("String", message.getField(descriptor.findFieldByNumber(3)));
        assertEquals(true, message.getField(descriptor.findFieldByNumber(4)));
        assertEquals(
                ByteString.copyFrom(byteArray), message.getField(descriptor.findFieldByNumber(5)));
        FieldDescriptor fieldDescriptor = descriptor.findFieldByNumber(6);
        message = (DynamicMessage) message.getField(fieldDescriptor);
        assertEquals(
                "hello",
                message.getField(
                        descriptor
                                .findNestedTypeByName(fieldDescriptor.toProto().getTypeName())
                                .findFieldByNumber(1)));
    }

    /**
     * Test to check <code>serialize()</code> for NULL type field. Since NULL type is not supported
     * in BigQuery, when a record having type NULL is serialized, error is expected.
     */
    @Test
    public void testNullTypeConversion() {
        // Obtaining the Schema Provider and the Avro-Record.
        // -- Obtain the not null schema for descriptor
        String notNullSchemaFieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"null_type_field\", \"type\": \"int\"}\n"
                        + " ]\n";
        Schema notNullSchema = getAvroSchemaFromFieldString(notNullSchemaFieldString);
        BigQuerySchemaProvider bigQuerySchemaProvider =
                new BigQuerySchemaProviderImpl(notNullSchema);
        // -- Obtain nullable schema for record formation.
        String fieldString = TestBigQuerySchemas.getSchemaWithNullType();
        Schema nullSchema = getAvroSchemaFromFieldString(fieldString);
        GenericRecord record =
                new GenericRecordBuilder(nullSchema).set("null_type_field", 1234).build();

        // Try to serialize, Form the Dynamic Message via the Serializer.
        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer();
        serializer.init(bigQuerySchemaProvider);
        BigQuerySerializationException exception =
                assertThrows(
                        BigQuerySerializationException.class, () -> serializer.serialize(record));

        // Check for the desired results.
        Assertions.assertThat(exception)
                .hasMessageContaining("Null Type Field not supported in BigQuery!");
    }
}
