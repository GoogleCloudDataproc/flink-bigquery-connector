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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.avro.Schema;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.cloud.flink.bigquery.sink.serializer.TestBigQuerySchemas.getAvroSchemaFromFieldString;
import static com.google.cloud.flink.bigquery.sink.serializer.TestBigQuerySchemas.getRecordSchema;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/** Test to check the RowData to proto Serializer. */
public class RowDataToProtoSerializerTest {

    /**
     * Test to check <code>getDynamicMessageFromRowData()</code> for Primitive types supported by
     * BigQuery.
     *
     * <ul>
     *   <li>BQ Type - Converted Table API Schema Type
     *   <li>"INTEGER" - LONG
     *   <li>"FLOAT" - FLOAT
     *   <li>"STRING" - STRING
     *   <li>"BOOLEAN" - BOOLEAN
     *   <li>"BYTES" - BYTES
     * </ul>
     */
    @Test
    public void testAllBigQuerySupportedPrimitiveTypesConversionToDynamicMessageCorrectly()
            throws BigQuerySerializationException {
        // Obtaining the Schema Provider and the Row Data Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRequiredPrimitiveTypes();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        LogicalType logicalType =
                BigQueryTableSchemaProvider.getDataTypeSchemaFromAvroSchema(avroSchema)
                        .getLogicalType();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);

        byte[] byteArray = "Any String you want".getBytes();
        GenericRowData row = new GenericRowData(6);
        row.setField(0, -7099548873856657385L);
        row.setField(1, 0.5616495161359795);
        row.setField(2, StringData.fromString("String"));
        row.setField(3, true);
        row.setField(4, byteArray);
        GenericRowData innerRow = new GenericRowData(1);
        innerRow.setField(0, StringData.fromString("hello"));
        row.setField(5, innerRow);

        // Form the Dynamic Message.
        DynamicMessage message =
                rowDataSerializer.getDynamicMessageFromRowData(row, descriptor, logicalType);

        // Check for the desired results.
        assertEquals(-7099548873856657385L, message.getField(descriptor.findFieldByNumber(1)));
        assertEquals(0.5616495161359795, message.getField(descriptor.findFieldByNumber(2)));
        assertEquals("String", message.getField(descriptor.findFieldByNumber(3)));
        assertEquals(true, message.getField(descriptor.findFieldByNumber(4)));
        assertEquals(
                ByteString.copyFrom(byteArray), message.getField(descriptor.findFieldByNumber(5)));
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByNumber(6);
        message = (DynamicMessage) message.getField(fieldDescriptor);
        assertEquals(
                "hello",
                message.getField(
                        descriptor
                                .findNestedTypeByName(fieldDescriptor.toProto().getTypeName())
                                .findFieldByNumber(1)));
    }

    /**
     * Test to check <code>getDynamicMessageFromRowData()</code> for Primitive types supported by
     * BigQuery. However <code>null</code> value is passed to the record field to test for error.
     */
    @Test
    public void testAllBigQuerySupportedPrimitiveTypesConversionToDynamicMessageIncorrectly() {
        // Obtaining the Schema Provider and the Row Data Record.
        // -- Non-nullable Schema for descriptor
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRequiredPrimitiveTypes();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        // -- Nullable Schema for descriptor
        Schema avroSchema =
                TestBigQuerySchemas.getSchemaWithNullablePrimitiveTypes().getAvroSchema();

        LogicalType logicalType =
                BigQueryTableSchemaProvider.getDataTypeSchemaFromAvroSchema(avroSchema)
                        .getLogicalType();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);
        GenericRowData row = new GenericRowData(6);
        row.setField(0, null);
        row.setField(1, null);
        row.setField(2, null);
        row.setField(3, null);
        row.setField(4, null);
        row.setField(5, null);

        // Check for the desired results.
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                rowDataSerializer.getDynamicMessageFromRowData(
                                        row, descriptor, logicalType));
        Assertions.assertThat(exception)
                .hasMessageContaining("Received null value for non-nullable field");
    }

    /**
     * Test to check <code>getDynamicMessageFromRowData()</code> for Primitive types supported by
     * Avro but not offered by BigQuery.
     *
     * <ul>
     *   <li>DOUBLE
     *   <li>ENUM - STRING
     *   <li>FIXED - VARBINARY(size)
     *   <li>INT
     * </ul>
     */
    @Test
    public void testAllRemainingAvroSupportedPrimitiveTypesConversionToDynamicMessageCorrectly() {
        // Obtaining the Schema Provider and the Row Data Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRemainingPrimitiveTypes();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        byte[] byteArray = "Any String you want".getBytes();

        LogicalType logicalType =
                BigQueryTableSchemaProvider.getDataTypeSchemaFromAvroSchema(avroSchema)
                        .getLogicalType();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);
        GenericRowData row = new GenericRowData(4);
        row.setField(0, 1234);
        row.setField(1, byteArray);
        row.setField(2, Float.parseFloat("12345.6789"));
        // Enum remains the same as "String"
        row.setField(3, StringData.fromString("C"));

        // Form the Dynamic Message.
        DynamicMessage message =
                rowDataSerializer.getDynamicMessageFromRowData(row, descriptor, logicalType);

        // Check for the desired results.
        assertEquals(1234, message.getField(descriptor.findFieldByNumber(1)));
        assertEquals(
                ByteString.copyFrom(byteArray), message.getField(descriptor.findFieldByNumber(2)));
        assertEquals(12345.6789f, message.getField(descriptor.findFieldByNumber(3)));
        assertEquals("C", message.getField(descriptor.findFieldByNumber(4)));
    }

    /**
     * Test to check <code>getDynamicMessageFromRowData()</code> for Primitive types supported by
     * Table API Schema but not offered by BigQuery. However <code>null</code> value is passed to
     * the record field to test for error.
     */
    @Test
    public void testAllRemainingAvroSupportedPrimitiveTypesConversionToDynamicMessageIncorrectly() {
        // Obtaining the Schema Provider and the Row Data Record.
        // -- Non-nullable Schema for descriptor
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRemainingPrimitiveTypes();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        // -- Nullable Schema for descriptor
        Schema avroSchema =
                TestBigQuerySchemas.getSchemaWithUnionOfRemainingPrimitiveTypes().getAvroSchema();
        LogicalType logicalType =
                BigQueryTableSchemaProvider.getDataTypeSchemaFromAvroSchema(avroSchema)
                        .getLogicalType();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);
        GenericRowData row = new GenericRowData(4);
        row.setField(0, null);
        row.setField(1, null);
        row.setField(2, null);
        row.setField(3, null);

        // Check for the desired error.
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                rowDataSerializer.getDynamicMessageFromRowData(
                                        row, descriptor, logicalType));
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
        // Obtaining the Schema Provider and the Row Data Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithNullablePrimitiveTypes();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        LogicalType logicalType =
                BigQueryTableSchemaProvider.getDataTypeSchemaFromAvroSchema(avroSchema)
                        .getLogicalType();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);
        GenericRowData row = new GenericRowData(6);
        row.setField(0, null);
        row.setField(1, null);
        row.setField(2, null);
        row.setField(3, null);
        row.setField(4, null);
        row.setField(5, null);

        // Form the Byte String.
        ByteString byteString = rowDataSerializer.serialize(row);

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
        LogicalType logicalType =
                BigQueryTableSchemaProvider.getDataTypeSchemaFromAvroSchema(avroSchema)
                        .getLogicalType();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);
        GenericRowData row = new GenericRowData(6);
        row.setField(0, null);
        row.setField(1, null);
        row.setField(2, null);
        row.setField(3, null);
        row.setField(4, null);
        row.setField(5, null);

        // Form the Byte String.
        ByteString byteString = rowDataSerializer.serialize(row);

        // Check for the desired results.
        assertEquals("", byteString.toStringUtf8());
    }

    // ------------Test Schemas with UNION of Different Types (Excluding Primitive and Logical)
    /**
     * Test to check <code>serialize()</code> for NULLABLE ARRAY type. <br>
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
        GenericRowData row = new GenericRowData(1);
        row.setField(0, null);

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
        LogicalType logicalType =
                BigQueryTableSchemaProvider.getDataTypeSchemaFromAvroSchema(nonNullSchema)
                        .getLogicalType();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);
        ByteString byteString = rowDataSerializer.serialize(row);

        // Check for the desired results.
        assertEquals("", byteString.toStringUtf8());
    }

    /**
     * Test to check <code>getDynamicMessageFromRowData()</code> for Logical types supported
     * BigQuery.
     *
     * <ul>
     *   <li>BQ Type - Converted Avro Type {Logical Type}
     *   <li>"TIMESTAMP" - LONG (microseconds since EPOCH) {timestamp-micros}
     *   <li>"TIME" - LONG (microseconds since MIDNIGHT) {time-micros}
     *   <li>"DATETIME" - INTEGER/LONG (microseconds since MIDNIGHT) {local-timestamp-micros}
     *   <li>"DATE" - INTEGER (number of days since EPOCH) {date}
     *   <li>"NUMERIC" - BYTES {decimal, isNumeric}
     *   <li>"BIGNUMERIC" - BYTES {decimal}
     *   <li>"GEOGRAPHY" - STRING {geography_wkt}
     *   <li>"JSON" - STRING {Json}
     * </ul>
     */
    @Test
    public void testAllBigQueryAvroSupportedLogicalTypesConversionToDynamicMessageCorrectly() {
        // Delete BIGNUMERIC AS IT IS NOT SUPPORTED YET.
        String mode = "REQUIRED";
        List<TableFieldSchema> fields =
                Arrays.asList(
                        new TableFieldSchema()
                                .setName("timestamp")
                                .setType("TIMESTAMP")
                                .setMode(mode),
                        new TableFieldSchema().setName("time").setType("TIME").setMode(mode),
                        new TableFieldSchema()
                                .setName("datetime")
                                .setType("DATETIME")
                                .setMode(mode),
                        new TableFieldSchema().setName("date").setType("DATE").setMode(mode),
                        new TableFieldSchema()
                                .setName("numeric_field")
                                .setType("NUMERIC")
                                .setMode(mode),
                        new TableFieldSchema()
                                .setName("geography")
                                .setType("GEOGRAPHY")
                                .setMode(mode),
                        new TableFieldSchema().setName("Json").setType("JSON").setMode(mode));

        TableSchema tableSchema = new TableSchema().setFields(fields);
        BigQuerySchemaProvider bigQuerySchemaProvider = new BigQuerySchemaProviderImpl(tableSchema);
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();

        // CONTINUE WITH THE TEST.
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        LogicalType logicalType =
                BigQueryTableSchemaProvider.getDataTypeSchemaFromAvroSchema(avroSchema)
                        .getLogicalType();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);

        BigDecimal bigDecimal = new BigDecimal("123456.7891011");
        GenericRowData row = new GenericRowData(7);
        byte[] bytes = "hello".getBytes();
        TimestampData myData = TimestampData.fromInstant(Instant.parse("2024-03-20T07:20:50.269Z"));
        row.setField(0, myData);
        row.setField(1, 50546554456L);
        row.setField(2, TimestampData.fromInstant(Instant.parse("2024-03-20T13:59:04.787424Z")));
        row.setField(3, 19802);
        row.setField(4, DecimalData.fromBigDecimal(new BigDecimal("12345.678910"), 11, 6));
        row.setField(
                5,
                StringData.fromString("GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (3 4, 5 6))"));
        row.setField(6, StringData.fromString("{\"FirstName\": \"John\", \"LastName\": \"Doe\"}"));

        // Check the expected value.
        DynamicMessage message =
                rowDataSerializer.getDynamicMessageFromRowData(row, descriptor, logicalType);
        assertEquals(1710919250269000L, message.getField(descriptor.findFieldByNumber(1)));
        assertEquals("14:02:26.554456", message.getField(descriptor.findFieldByNumber(2)));
        assertEquals(
                "2024-03-20T13:59:04.787424", message.getField(descriptor.findFieldByNumber(3)));
        assertEquals(19802, message.getField(descriptor.findFieldByNumber(4)));
        assertEquals(
                "GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (3 4, 5 6))",
                message.getField(descriptor.findFieldByNumber(6)));
        assertEquals(
                "{\"FirstName\": \"John\", \"LastName\": \"Doe\"}",
                message.getField(descriptor.findFieldByNumber(7)));
    }

    /**
     * Test to check <code>serialize()</code> for NULLABLE ARRAY of Type RECORD. <br>
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
        GenericRowData row = new GenericRowData(1);
        row.setField(0, null);

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
        LogicalType logicalType =
                BigQueryTableSchemaProvider.getDataTypeSchemaFromAvroSchema(nonNullSchema)
                        .getLogicalType();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);
        ByteString byteString = rowDataSerializer.serialize(row);

        // Check for the desired results.
        assertEquals("", byteString.toStringUtf8());
    }

    /**
     * Test to check <code>getDynamicMessageFromRowData()</code> for Logical types supported by avro
     * but not by BigQuery.
     *
     * <ul>
     *   <li>BQ Type - Converted Row Data Type {Logical Type}
     *   <li>"ts_millis" - LONG (milliseconds since EPOCH) {timestamp-millis}
     *   <li>"time_millis" - INTEGER (milliseconds since MIDNIGHT) {time-millis}
     *   <li>"lts_millis" - INTEGER (milliseconds since EPOCH) {local-timestamp-millis}
     *   <li>"uuid" - STRING (uuid string) {uuid}
     * </ul>
     */
    @Test
    public void testAllRemainingAvroSupportedLogicalTypesConversionToDynamicMessageCorrectly()
            throws BigQuerySerializationException {
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRemainingLogicalTypes();

        LogicalType logicalType =
                BigQueryTableSchemaProvider.getDataTypeSchemaFromAvroSchema(
                                bigQuerySchemaProvider.getAvroSchema())
                        .getLogicalType();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);

        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();

        GenericRowData row = new GenericRowData(4);
        row.setField(0, TimestampData.fromEpochMillis(1710919250269L));
        row.setField(1, 45745727);
        row.setField(2, TimestampData.fromEpochMillis(1710938587462L));
        row.setField(3, StringData.fromString("8e25e7e5-0dc5-4292-b59b-3665b0ab8280"));

        DynamicMessage message =
                rowDataSerializer.getDynamicMessageFromRowData(row, descriptor, logicalType);
        assertEquals(1710919250269000L, message.getField(descriptor.findFieldByNumber(1)));
        assertEquals("12:42:25.727", message.getField(descriptor.findFieldByNumber(2)));
        assertEquals("2024-03-20T12:43:07.462", message.getField(descriptor.findFieldByNumber(3)));
        assertEquals(
                "8e25e7e5-0dc5-4292-b59b-3665b0ab8280",
                message.getField(descriptor.findFieldByNumber(4)));
    }

    /**
     * Test to check <code>serialize()</code> for Logical types supported by avro but not by
     * BigQuery. However, the bigquery fields are <code>NULLABLE</code> so expecting an empty byte
     * string.
     */
    @Test
    public void
            testUnionOfAllRemainingAvroSupportedLogicalTypesConversionToEmptyByteStringCorrectly()
                    throws BigQuerySerializationException {
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithUnionOfLogicalTypes();
        LogicalType logicalType =
                BigQueryTableSchemaProvider.getDataTypeSchemaFromAvroSchema(
                                bigQuerySchemaProvider.getAvroSchema())
                        .getLogicalType();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);

        GenericRowData row = new GenericRowData(4);
        row.setField(0, null);
        row.setField(1, null);
        row.setField(2, null);
        row.setField(3, null);

        ByteString byteString = rowDataSerializer.serialize(row);
        assertEquals("", byteString.toStringUtf8());
    }

    /**
     * Test to check <code>getDynamicMessageFromRowData()</code> for Record type schema having an
     * ARRAY field.
     */
    @Test
    public void testRecordOfArrayConversionToDynamicMessageCorrectly() {
        // Obtaining the Schema Provider and the Avro-Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRecordOfArray();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        LogicalType logicalType =
                BigQueryTableSchemaProvider.getDataTypeSchemaFromAvroSchema(
                                bigQuerySchemaProvider.getAvroSchema())
                        .getLogicalType();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);
        List<Boolean> arrayList = Arrays.asList(false, true, false);

        GenericRowData row = new GenericRowData(1);
        GenericRowData innerRow = new GenericRowData(1);
        innerRow.setField(0, new GenericArrayData(arrayList.toArray()));
        row.setField(0, innerRow);

        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByNumber(1);

        // Form the Dynamic Message.
        DynamicMessage message =
                rowDataSerializer.getDynamicMessageFromRowData(row, descriptor, logicalType);

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
        GenericRowData row = new GenericRowData(1);
        row.setField(0, null);

        // Try to serialize, Form the Dynamic Message via the serializer.
        LogicalType logicalType =
                BigQueryTableSchemaProvider.getDataTypeSchemaFromAvroSchema(nullableRecordSchema)
                        .getLogicalType();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);

        BigQuerySerializationException exception =
                assertThrows(
                        BigQuerySerializationException.class,
                        () -> rowDataSerializer.serialize(row));

        // Check for the desired results.
        Assertions.assertThat(exception)
                .hasMessageContaining("Error while serialising Row Data record: +I(null)");
    }

    /**
     * Test to check <code>getDynamicMessageFromRowData()</code> for Record type schema having an
     * ARRAY field. However, an invalid value (integer value instead of Array) is passed to test for
     * error.
     */
    @Test
    public void testRecordOfArrayConversionToDynamicMessageIncorrectly() {
        // Obtaining the Schema Provider and the Row Data Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRecordOfArray();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();

        LogicalType logicalType =
                BigQueryTableSchemaProvider.getDataTypeSchemaFromAvroSchema(
                                bigQuerySchemaProvider.getAvroSchema())
                        .getLogicalType();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);

        GenericRowData row = new GenericRowData(1);
        GenericRowData innerRow = new GenericRowData(1);
        innerRow.setField(0, 12345);
        row.setField(0, innerRow);

        RuntimeException exception =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                rowDataSerializer.getDynamicMessageFromRowData(
                                        row, descriptor, logicalType));
        Assertions.assertThat(exception)
                .hasMessageFindingMatch(
                        "(class)? ?java.lang.Integer cannot be cast to (class)? ?org.apache.flink.table.data.ArrayData");
    }

    /**
     * Test to check <code>serialize()</code> for Record type schema having a UNION field (with
     * null). Since the record has a union of NULL field, <code>null</code> value is serialized. The
     * serialized byte string is checked to be empty.
     */
    @Test
    public void testRecordOfUnionSchemaConversionToEmptyByteStringCorrectly() {
        // Obtaining the Schema Provider and the Row Data Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRecordOfUnionType();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();

        LogicalType logicalType =
                BigQueryTableSchemaProvider.getDataTypeSchemaFromAvroSchema(
                                bigQuerySchemaProvider.getAvroSchema())
                        .getLogicalType();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);

        GenericRowData row = new GenericRowData(1);
        GenericRowData innerRow = new GenericRowData(1);
        innerRow.setField(0, null);
        row.setField(0, innerRow);

        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByNumber(1);

        // Form the Dynamic Message.
        DynamicMessage message =
                rowDataSerializer.getDynamicMessageFromRowData(row, descriptor, logicalType);

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
        LogicalType logicalType =
                BigQueryTableSchemaProvider.getDataTypeSchemaFromAvroSchema(
                                bigQuerySchemaProvider.getAvroSchema())
                        .getLogicalType();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);

        GenericRowData row = new GenericRowData(1);
        GenericRowData innerRow = new GenericRowData(1);
        innerRow.setField(0, 12345);
        row.setField(0, innerRow);

        RuntimeException exception =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                rowDataSerializer.getDynamicMessageFromRowData(
                                        row, descriptor, logicalType));
        Assertions.assertThat(exception)
                .hasMessageFindingMatch(
                        "(class)? ?java.lang.Integer cannot be cast to (class)? ?java.lang.Boolean");
    }

    /**
     * Test to check <code>getDynamicMessageFromRowData()</code> for Record type schema having a
     * RECORD type field.
     */
    @Test
    public void testRecordOfRecordConversionToDynamicMessageCorrectly() {
        // Obtaining the Schema Provider and the Row Data Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRecordOfRecord();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        LogicalType logicalType =
                BigQueryTableSchemaProvider.getDataTypeSchemaFromAvroSchema(
                                bigQuerySchemaProvider.getAvroSchema())
                        .getLogicalType();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);

        GenericRowData row = new GenericRowData(1);
        GenericRowData innerRow = new GenericRowData(1);
        GenericRowData innerInnerRow = new GenericRowData(2);
        innerInnerRow.setField(0, 7267611125055979836L);
        innerInnerRow.setField(1, StringData.fromString("yllgqpemxjnpsoaqlwlgbqjkywxnavntf"));
        innerRow.setField(0, innerInnerRow);
        row.setField(0, innerRow);

        // Form the Dynamic Message.
        DynamicMessage message =
                rowDataSerializer.getDynamicMessageFromRowData(row, descriptor, logicalType);

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
     * Test to check <code>getDynamicMessageFromRowData()</code> for Record type schema having all
     * Primitive type fields (supported by BigQuery).
     */
    @Test
    public void testRecordOfAllBigQuerySupportedPrimitiveTypeConversionToDynamicMessageCorrectly() {
        // Obtaining the Schema Provider and the Row Data Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRecordOfPrimitiveTypes();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        LogicalType logicalType =
                BigQueryTableSchemaProvider.getDataTypeSchemaFromAvroSchema(
                                bigQuerySchemaProvider.getAvroSchema())
                        .getLogicalType();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);

        GenericRowData row = new GenericRowData(1);
        GenericRowData innerRow = new GenericRowData(6);
        byte[] byteArray = "Any String you want".getBytes();
        innerRow.setField(0, -7099548873856657385L);
        innerRow.setField(1, 0.5616495161359795);
        innerRow.setField(2, StringData.fromString("String"));
        innerRow.setField(3, true);
        innerRow.setField(4, byteArray);
        GenericRowData innerInnerRow = new GenericRowData(1);
        innerInnerRow.setField(0, StringData.fromString("hello"));
        innerRow.setField(5, innerInnerRow);
        row.setField(0, innerRow);

        // Form the Dynamic Message.
        DynamicMessage message =
                rowDataSerializer.getDynamicMessageFromRowData(row, descriptor, logicalType);

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
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByNumber(6);
        message = (DynamicMessage) message.getField(fieldDescriptor);
        assertEquals(
                "hello",
                message.getField(
                        descriptor
                                .findNestedTypeByName(fieldDescriptor.toProto().getTypeName())
                                .findFieldByNumber(1)));
    }

    /**
     * Test to check <code>getDynamicMessageFromRowData()</code> for Record type schema having all
     * Primitive type fields (supported by Avro, not by BigQuery).
     */
    @Test
    public void
            testRecordOfAllRemainingAvroSupportedPrimitiveTypeConversionToDynamicMessageCorrectly() {
        // Obtaining the Schema Provider and the Row Data Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRecordOfRemainingPrimitiveTypes();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        LogicalType logicalType =
                BigQueryTableSchemaProvider.getDataTypeSchemaFromAvroSchema(
                                bigQuerySchemaProvider.getAvroSchema())
                        .getLogicalType();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);

        GenericRowData row = new GenericRowData(1);
        GenericRowData innerRow = new GenericRowData(4);
        byte[] byteArray = "Any String you want".getBytes();
        innerRow.setField(0, 1234);
        innerRow.setField(1, byteArray);
        innerRow.setField(2, Float.parseFloat("12345.6789"));
        innerRow.setField(3, StringData.fromString("C"));
        row.setField(0, innerRow);

        // Form the Dynamic Message via the serializer.
        DynamicMessage message =
                rowDataSerializer.getDynamicMessageFromRowData(row, descriptor, logicalType);

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

    /**
     * Test to check <code>getDynamicMessageFromRowData()</code> for Record type schema having all
     * Logical type fields (supported by Avro, not by BigQuery).
     */
    @Test
    public void
            testRecordOfAllRemainingAvroSupportedLogicalTypeConversionToDynamicMessageCorrectly() {
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithRecordOfRemainingLogicalTypes();
        LogicalType logicalType =
                BigQueryTableSchemaProvider.getDataTypeSchemaFromAvroSchema(
                                bigQuerySchemaProvider.getAvroSchema())
                        .getLogicalType();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);

        GenericRowData row = new GenericRowData(1);
        GenericRowData innerRow = new GenericRowData(4);
        innerRow.setField(0, TimestampData.fromInstant(Instant.parse("2024-03-20T12:50:50.269Z")));
        innerRow.setField(1, 45745727);
        innerRow.setField(2, TimestampData.fromEpochMillis(1710938587462L));
        innerRow.setField(3, StringData.fromString("8e25e7e5-0dc5-4292-b59b-3665b0ab8280"));
        row.setField(0, innerRow);

        DynamicMessage message =
                rowDataSerializer.getDynamicMessageFromRowData(row, descriptor, logicalType);
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
    public void testArrayOfUnionConversionToByteStringIncorrectly()
            throws BigQuerySerializationException {
        // Obtaining the Schema Provider and the Row Data Record.
        String notNullString =
                " \"fields\": [\n"
                        + "{\"name\": \"array_with_union\", \"type\": {\"type\": \"array\", \"items\":  \"long\"}} ]";
        Schema notNullSchema = getAvroSchemaFromFieldString(notNullString);
        BigQuerySchemaProvider bigQuerySchemaProvider =
                new BigQuerySchemaProviderImpl(notNullSchema);
        // -- 1. check UNION with NULL
        GenericRowData recordWithNullInUnion = new GenericRowData(1);
        recordWithNullInUnion.setField(
                0, new GenericArrayData(Arrays.asList(1234567L, null).toArray()));

        // -- 2. Check union of NOT NULL-multiple types.
        GenericRowData recordWithMultipleDatatypesInUnion = new GenericRowData(1);
        recordWithMultipleDatatypesInUnion.setField(
                0, new GenericArrayData(Arrays.asList(1234567L, 12345).toArray()));

        // Form the Dynamic Message via the serializer.
        LogicalType logicalType =
                BigQueryTableSchemaProvider.getDataTypeSchemaFromAvroSchema(
                                bigQuerySchemaProvider.getAvroSchema())
                        .getLogicalType();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);

        BigQuerySerializationException exceptionForNullInUnion =
                assertThrows(
                        BigQuerySerializationException.class,
                        () -> rowDataSerializer.serialize(recordWithNullInUnion));
        BigQuerySerializationException exceptionForMultipleDatatypesInUnion =
                assertThrows(
                        BigQuerySerializationException.class,
                        () -> rowDataSerializer.serialize(recordWithMultipleDatatypesInUnion));

        // Check for the desired results.
        Assertions.assertThat(exceptionForNullInUnion)
                .hasMessageContaining("Error while serialising Row Data record");
        Assertions.assertThat(exceptionForMultipleDatatypesInUnion)
                .hasMessageFindingMatch(
                        "(class)? ?java.lang.Integer cannot be cast to (class)? ?java.lang.Long");
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
        GenericRowData record = new GenericRowData(1);
        record.setField(0, new GenericArrayData(Arrays.asList(1234567L, null).toArray()));

        // Form the Dynamic Message via the serializer.
        LogicalType logicalType =
                BigQueryTableSchemaProvider.getDataTypeSchemaFromAvroSchema(
                                bigQuerySchemaProvider.getAvroSchema())
                        .getLogicalType();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);
        BigQuerySerializationException exception =
                assertThrows(
                        BigQuerySerializationException.class,
                        () -> rowDataSerializer.serialize(record));

        // Check for the desired results.
        Assertions.assertThat(exception)
                .hasMessageContaining("Error while serialising Row Data record");
    }

    /**
     * Test to check <code>getDynamicMessageFromRowData()</code> for ARRAY type schema having a
     * RECORD type. <br>
     * An array is created with RECORD type values.
     */
    @Test
    public void testArrayOfRecordConversionToDynamicMessageCorrectly() {
        // Obtaining the Schema Provider and the Avro-Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithArrayOfRecord();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();

        LogicalType logicalType =
                BigQueryTableSchemaProvider.getDataTypeSchemaFromAvroSchema(
                                bigQuerySchemaProvider.getAvroSchema())
                        .getLogicalType();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);

        GenericRowData row = new GenericRowData(1);
        GenericRowData innerRow = new GenericRowData(2);
        innerRow.setField(0, 8034881802526489441L);
        innerRow.setField(1, StringData.fromString("fefmmuyoosmglqtnwfxahgoxqpyhc"));
        row.setField(0, new GenericArrayData(Arrays.asList(innerRow, innerRow).toArray()));

        // Form the Dynamic Message.
        DynamicMessage message =
                rowDataSerializer.getDynamicMessageFromRowData(row, descriptor, logicalType);
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
     * Test to check <code>getDynamicMessageFromRowData()</code> for different ARRAYS having all
     * Primitive types. <br>
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
        LogicalType logicalType =
                BigQueryTableSchemaProvider.getDataTypeSchemaFromAvroSchema(
                                bigQuerySchemaProvider.getAvroSchema())
                        .getLogicalType();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();

        byte[] byteArray = "Hello".getBytes();

        GenericRowData row = new GenericRowData(6);
        row.setField(
                0, new GenericArrayData(Collections.singletonList(-250555967807021764L).toArray()));
        row.setField(
                1,
                new GenericArrayData(
                        Arrays.asList(0.34593866360929726, 0.35197578762609993).toArray()));
        row.setField(
                2,
                new GenericArrayData(
                        Arrays.asList(
                                        StringData.fromString("nsguocxfjqaufhsunahvxmcpivutfqv"),
                                        StringData.fromString("q"),
                                        StringData.fromString(
                                                "pldvejbqmfyosgxmbmqjsafjbcfqwhiagbckmti"))
                                .toArray()));
        row.setField(3, new GenericArrayData(Arrays.asList(false, false, false, true).toArray()));
        row.setField(
                4,
                new GenericArrayData(
                        Arrays.asList(byteArray, byteArray, byteArray, byteArray, byteArray)
                                .toArray()));
        GenericRowData innerRow = new GenericRowData(1);
        innerRow.setField(
                0,
                new GenericArrayData(
                        Arrays.asList(
                                        StringData.fromString("a"),
                                        StringData.fromString("b"),
                                        StringData.fromString("c"),
                                        StringData.fromString("d"),
                                        StringData.fromString("e"),
                                        StringData.fromString("f"))
                                .toArray()));
        row.setField(5, new GenericArrayData(Collections.singletonList(innerRow).toArray()));
        DynamicMessage message =
                rowDataSerializer.getDynamicMessageFromRowData(row, descriptor, logicalType);
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
     * Test to check <code>getDynamicMessageFromRowData()</code> for different ARRAYS having all
     * Primitive types (supported by Avro, not BQ). <br>
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
        // Obtaining the Schema Provider and the Row Data Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithArraysOfRemainingPrimitiveTypes();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        LogicalType logicalType =
                BigQueryTableSchemaProvider.getDataTypeSchemaFromAvroSchema(
                                bigQuerySchemaProvider.getAvroSchema())
                        .getLogicalType();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);
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

        GenericRowData row = new GenericRowData(4);
        row.setField(0, new GenericArrayData(Collections.singletonList(89767285).toArray()));
        row.setField(1, new GenericArrayData(Collections.singletonList(byteArray).toArray()));
        row.setField(
                2,
                new GenericArrayData(
                        Arrays.asList(0.26904225f, 0.558431f, 0.2269839f, 0.70421267f).toArray()));
        row.setField(
                3,
                new GenericArrayData(
                        Arrays.asList(
                                        StringData.fromString("A"),
                                        StringData.fromString("C"),
                                        StringData.fromString("A"))
                                .toArray()));

        // Form the Dynamic Message.
        DynamicMessage message =
                rowDataSerializer.getDynamicMessageFromRowData(row, descriptor, logicalType);

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

    /**
     * Test to check <code>getDynamicMessageFromRowData()</code> for different ARRAYS having all
     * Primitive types (supported by Table API, not BQ). <br>
     * A record is created having four fields, each of ARRAY (different item type) types.
     *
     * <ul>
     *   <li>time_millis - ARRAY of type TIMES (millisecond precision)
     *   <li>lts_millis - ARRAY of type DATETIME (millisecond precision)
     *   <li>ts_millis - ARRAY of type TIMESTAMP (millisecond precision)
     *   <li>uuid - ARRAY of type UUID
     * </ul>
     */
    @Test
    public void testArraysOfRemainingLogicalTypesConversionToDynamicMessageCorrectly() {
        // Obtaining the Schema Provider and the Row Data Record.
        BigQuerySchemaProvider bigQuerySchemaProvider =
                TestBigQuerySchemas.getSchemaWithArraysOfRemainingLogicalTypes();

        // -- Initialising the RECORD.
        LogicalType logicalType =
                BigQueryTableSchemaProvider.getDataTypeSchemaFromAvroSchema(
                                bigQuerySchemaProvider.getAvroSchema())
                        .getLogicalType();
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);
        GenericRowData row = new GenericRowData(4);
        row.setField(
                1, new GenericArrayData(Arrays.asList(45745727, 45745727, 45745727).toArray()));
        row.setField(
                2,
                new GenericArrayData(
                        Arrays.asList(
                                        TimestampData.fromEpochMillis(1710938587462L),
                                        TimestampData.fromEpochMillis(1710938587462L))
                                .toArray()));
        row.setField(
                0,
                new GenericArrayData(
                        Collections.singletonList(
                                        TimestampData.fromInstant(
                                                Instant.parse("2024-03-20T07:20:50.269Z")))
                                .toArray()));
        row.setField(
                3,
                new GenericArrayData(
                        Collections.singletonList(
                                        StringData.fromString(
                                                "8e25e7e5-0dc5-4292-b59b-3665b0ab8280"))
                                .toArray()));
        // Form the Dynamic Message.
        DynamicMessage message =
                rowDataSerializer.getDynamicMessageFromRowData(row, descriptor, logicalType);

        // Check for the desired results.
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

    @Test
    public void testSmallIntConversionToByteStringCorrectly() {

        // Form the Schema.
        DataType dataType =
                DataTypes.ROW(
                                DataTypes.FIELD("tinyint_type", DataTypes.TINYINT().notNull()),
                                DataTypes.FIELD("int_type", DataTypes.INT().notNull()))
                        .notNull();
        LogicalType logicalType = dataType.getLogicalType();
        Schema avroSchema = BigQueryTableSchemaProvider.getAvroSchemaFromLogicalSchema(logicalType);
        BigQuerySchemaProvider bigQuerySchemaProvider = new BigQuerySchemaProviderImpl(avroSchema);
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();

        // Initialize the record.
        GenericRowData row = new GenericRowData(2);
        row.setField(0, (short) 123);
        row.setField(1, 123);

        RowDataToProtoSerializer rowDataToProtoSerializer = new RowDataToProtoSerializer();
        rowDataToProtoSerializer.init(bigQuerySchemaProvider);
        rowDataToProtoSerializer.setLogicalType(logicalType);

        // Check for the desired results.
        DynamicMessage message =
                rowDataToProtoSerializer.getDynamicMessageFromRowData(row, descriptor, logicalType);
        assertEquals(123, message.getField(descriptor.findFieldByNumber(1)));
        assertEquals(123, message.getField(descriptor.findFieldByNumber(2)));
    }
}
