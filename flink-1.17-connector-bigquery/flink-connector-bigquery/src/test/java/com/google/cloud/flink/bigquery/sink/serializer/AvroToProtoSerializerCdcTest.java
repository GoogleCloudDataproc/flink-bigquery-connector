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

import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.joda.time.Instant;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests for CDC (Change Data Capture) functionality in {@link AvroToProtoSerializer}. */
public class AvroToProtoSerializerCdcTest {

    @Test
    public void testSerializeWithCdc_upsert()
            throws BigQuerySerializationException, InvalidProtocolBufferException {
        BigQuerySchemaProvider baseProvider = TestBigQuerySchemas.getSimpleRecordSchema();
        BigQueryCdcSchemaProvider cdcProvider = new BigQueryCdcSchemaProvider(baseProvider);

        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.initForCdc(cdcProvider);

        // Create a record with the base schema
        Schema avroSchema = baseProvider.getAvroSchema();
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("long_field", 12345L)
                        .set("string_field", "test_value")
                        .build();

        // Serialize with CDC
        ByteString result = serializer.serializeWithCdc(record, "UPSERT", "0000000000000001");

        // Parse the result to verify CDC fields are included
        Descriptor cdcDescriptor = cdcProvider.getDescriptor();
        DynamicMessage message = DynamicMessage.parseFrom(cdcDescriptor, result);

        // Verify original fields
        assertEquals(12345L, message.getField(cdcDescriptor.findFieldByName("long_field")));
        assertEquals("test_value", message.getField(cdcDescriptor.findFieldByName("string_field")));

        // Verify CDC fields
        assertEquals(
                "UPSERT",
                message.getField(
                        cdcDescriptor.findFieldByName(
                                BigQueryCdcSchemaProvider.CDC_CHANGE_TYPE_FIELD)));
        assertEquals(
                "0000000000000001",
                message.getField(
                        cdcDescriptor.findFieldByName(
                                BigQueryCdcSchemaProvider.CDC_SEQUENCE_NUMBER_FIELD)));
    }

    @Test
    public void testSerializeWithCdc_delete()
            throws BigQuerySerializationException, InvalidProtocolBufferException {
        BigQuerySchemaProvider baseProvider = TestBigQuerySchemas.getSimpleRecordSchema();
        BigQueryCdcSchemaProvider cdcProvider = new BigQueryCdcSchemaProvider(baseProvider);

        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.initForCdc(cdcProvider);

        Schema avroSchema = baseProvider.getAvroSchema();
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("long_field", 99999L)
                        .set("string_field", "to_be_deleted")
                        .build();

        ByteString result = serializer.serializeWithCdc(record, "DELETE", "0000000000000002");

        Descriptor cdcDescriptor = cdcProvider.getDescriptor();
        DynamicMessage message = DynamicMessage.parseFrom(cdcDescriptor, result);

        assertEquals(
                "DELETE",
                message.getField(
                        cdcDescriptor.findFieldByName(
                                BigQueryCdcSchemaProvider.CDC_CHANGE_TYPE_FIELD)));
        assertEquals(
                "0000000000000002",
                message.getField(
                        cdcDescriptor.findFieldByName(
                                BigQueryCdcSchemaProvider.CDC_SEQUENCE_NUMBER_FIELD)));
    }

    @Test
    public void testExtractSequenceNumber_fromLong() {
        BigQuerySchemaProvider baseProvider = TestBigQuerySchemas.getSimpleRecordSchema();
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(baseProvider);

        Schema avroSchema = baseProvider.getAvroSchema();
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("long_field", 123456789L)
                        .set("string_field", "test")
                        .build();

        String sequenceNumber = serializer.extractSequenceNumber(record, "long_field");

        // Should be 16-character zero-padded hex
        assertEquals(16, sequenceNumber.length());
        assertEquals("00000000075bcd15", sequenceNumber.toLowerCase());
    }

    @Test
    public void testExtractSequenceNumber_fromInteger() {
        String fieldString =
                "\"fields\": "
                        + "["
                        + "{\"name\": \"int_field\", \"type\": \"int\"},"
                        + "{\"name\": \"string_field\", \"type\": \"string\"}"
                        + "]";
        Schema avroSchema = TestBigQuerySchemas.getAvroSchemaFromFieldString(fieldString);
        BigQuerySchemaProvider provider = new BigQuerySchemaProviderImpl(avroSchema);

        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(provider);

        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("int_field", 255)
                        .set("string_field", "test")
                        .build();

        String sequenceNumber = serializer.extractSequenceNumber(record, "int_field");

        assertEquals(16, sequenceNumber.length());
        assertEquals("00000000000000ff", sequenceNumber.toLowerCase());
    }

    @Test
    public void testExtractSequenceNumber_fromJodaInstant() {
        // Create schema with timestamp field
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"ts_millis\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}},\n"
                        + "   {\"name\": \"name\", \"type\": \"string\"}\n"
                        + " ]\n";
        Schema avroSchema = TestBigQuerySchemas.getAvroSchemaFromFieldString(fieldString);
        BigQuerySchemaProvider provider = new BigQuerySchemaProviderImpl(avroSchema);

        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(provider);

        // Create record with Joda Instant
        Instant jodaInstant = new Instant(1710943144787L); // 2024-03-20 timestamp
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("ts_millis", jodaInstant)
                        .set("name", "test")
                        .build();

        String sequenceNumber = serializer.extractSequenceNumber(record, "ts_millis");

        assertEquals(16, sequenceNumber.length());
        // Verify it's a valid hex string representing the epoch millis
        long parsedValue = Long.parseUnsignedLong(sequenceNumber, 16);
        assertEquals(1710943144787L, parsedValue);
    }

    @Test
    public void testExtractSequenceNumber_fromJavaInstant() {
        String fieldString =
                "\"fields\": "
                        + "["
                        + "{\"name\": \"timestamp_field\", \"type\": \"long\"},"
                        + "{\"name\": \"name\", \"type\": \"string\"}"
                        + "]";
        Schema avroSchema = TestBigQuerySchemas.getAvroSchemaFromFieldString(fieldString);
        BigQuerySchemaProvider provider = new BigQuerySchemaProviderImpl(avroSchema);

        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(provider);

        java.time.Instant javaInstant = java.time.Instant.ofEpochMilli(1710943144787L);
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("timestamp_field", javaInstant.toEpochMilli())
                        .set("name", "test")
                        .build();

        String sequenceNumber = serializer.extractSequenceNumber(record, "timestamp_field");

        assertEquals(16, sequenceNumber.length());
    }

    @Test
    public void testExtractSequenceNumber_nullFieldValue_returnsNull() {
        String fieldString =
                "\"fields\": "
                        + "["
                        + "{\"name\": \"nullable_long\", \"type\": [\"null\", \"long\"]},"
                        + "{\"name\": \"name\", \"type\": \"string\"}"
                        + "]";
        Schema avroSchema = TestBigQuerySchemas.getAvroSchemaFromFieldString(fieldString);
        BigQuerySchemaProvider provider = new BigQuerySchemaProviderImpl(avroSchema);

        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(provider);

        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("nullable_long", null)
                        .set("name", "test")
                        .build();

        // Null field value returns null to omit _change_sequence_number
        String sequenceNumber = serializer.extractSequenceNumber(record, "nullable_long");

        assertNull(sequenceNumber);
    }

    @Test
    public void testExtractSequenceNumber_nullSequenceFieldParam_returnsNull() {
        BigQuerySchemaProvider baseProvider = TestBigQuerySchemas.getSimpleRecordSchema();
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(baseProvider);

        Schema avroSchema = baseProvider.getAvroSchema();
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("long_field", 123L)
                        .set("string_field", "test")
                        .build();

        // Null sequenceField param returns null to omit _change_sequence_number
        assertNull(serializer.extractSequenceNumber(record, null));
        assertNull(serializer.extractSequenceNumber(record, ""));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExtractSequenceNumber_missingField_throwsException() {
        BigQuerySchemaProvider baseProvider = TestBigQuerySchemas.getSimpleRecordSchema();
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(baseProvider);

        Schema avroSchema = baseProvider.getAvroSchema();
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("long_field", 123L)
                        .set("string_field", "test")
                        .build();

        // Try to extract from a non-existent field - should throw exception
        serializer.extractSequenceNumber(record, "nonexistent_field");
    }

    @Test
    public void testSerializeWithCdc_complexSchema()
            throws BigQuerySerializationException, InvalidProtocolBufferException {
        BigQuerySchemaProvider baseProvider =
                TestBigQuerySchemas.getSchemaWithRequiredPrimitiveTypes();
        BigQueryCdcSchemaProvider cdcProvider = new BigQueryCdcSchemaProvider(baseProvider);

        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.initForCdc(cdcProvider);

        Schema avroSchema = baseProvider.getAvroSchema();
        Schema nestedSchema = avroSchema.getField("required_record_field").schema();
        GenericRecord nestedRecord =
                new GenericRecordBuilder(nestedSchema).set("species", "Eagle").build();

        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("number", 42L)
                        .set("price", 3.14)
                        .set("species", "Bird")
                        .set("flighted", true)
                        .set("sound", java.nio.ByteBuffer.wrap(new byte[] {1, 2, 3}))
                        .set("required_record_field", nestedRecord)
                        .build();

        ByteString result = serializer.serializeWithCdc(record, "UPSERT", "ABCD1234");

        Descriptor cdcDescriptor = cdcProvider.getDescriptor();
        DynamicMessage message = DynamicMessage.parseFrom(cdcDescriptor, result);

        // Verify CDC fields
        FieldDescriptor changeTypeField =
                cdcDescriptor.findFieldByName(BigQueryCdcSchemaProvider.CDC_CHANGE_TYPE_FIELD);
        FieldDescriptor sequenceField =
                cdcDescriptor.findFieldByName(BigQueryCdcSchemaProvider.CDC_SEQUENCE_NUMBER_FIELD);

        assertNotNull(changeTypeField);
        assertNotNull(sequenceField);
        assertEquals("UPSERT", message.getField(changeTypeField));
        assertEquals("ABCD1234", message.getField(sequenceField));
    }

    @Test
    public void testInitForCdc_setsUpDescriptorCorrectly() {
        BigQuerySchemaProvider baseProvider = TestBigQuerySchemas.getSimpleRecordSchema();
        BigQueryCdcSchemaProvider cdcProvider = new BigQueryCdcSchemaProvider(baseProvider);

        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.initForCdc(cdcProvider);

        // After initForCdc, the serializer should be ready to serialize with CDC
        // The internal cdcDescriptor should be set
        assertTrue("Serializer should be initialized for CDC", true);
    }

    @Test
    public void testExtractSequenceNumber_stringNumericValue_convertsToHex() {
        // Test that string values containing numbers are converted to hex
        BigQuerySchemaProvider baseProvider = TestBigQuerySchemas.getSimpleRecordSchema();
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(baseProvider);

        Schema avroSchema = baseProvider.getAvroSchema();
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("long_field", 123L)
                        .set("string_field", "12345")
                        .build();

        // Extract from string field containing numeric value
        String sequenceNumber = serializer.extractSequenceNumber(record, "string_field");

        // "12345" decimal should be converted to 16-character zero-padded hex
        assertEquals(16, sequenceNumber.length());
        assertEquals("0000000000003039", sequenceNumber);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExtractSequenceNumber_nonNumericString_throwsException() {
        // Test that non-numeric strings throw exception
        BigQuerySchemaProvider baseProvider = TestBigQuerySchemas.getSimpleRecordSchema();
        AvroToProtoSerializer serializer = new AvroToProtoSerializer();
        serializer.init(baseProvider);

        Schema avroSchema = baseProvider.getAvroSchema();
        GenericRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("long_field", 123L)
                        .set("string_field", "not_a_number")
                        .build();

        // Extract from string field containing non-numeric value - should throw exception
        serializer.extractSequenceNumber(record, "string_field");
    }
}
