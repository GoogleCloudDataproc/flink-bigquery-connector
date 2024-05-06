package com.google.cloud.flink.bigquery.sink.serializer;

import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;

import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.avro.Schema;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.google.cloud.flink.bigquery.sink.serializer.TestBigQuerySchemas.getAvroSchemaFromFieldString;
import static com.google.cloud.flink.bigquery.sink.serializer.TestBigQuerySchemas.getRecordSchema;
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
        Descriptors.Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        LogicalType logicalType =
                BigQuerySchemaProviderTableImpl.getDataTypeSchemaFromAvroSchema(avroSchema)
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
        Descriptors.Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        // -- Nullable Schema for descriptor
        Schema avroSchema =
                TestBigQuerySchemas.getSchemaWithNullablePrimitiveTypes().getAvroSchema();

        LogicalType logicalType =
                BigQuerySchemaProviderTableImpl.getDataTypeSchemaFromAvroSchema(avroSchema)
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
        Descriptors.Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        Schema avroSchema = bigQuerySchemaProvider.getAvroSchema();
        byte[] byteArray = "Any String you want".getBytes();

        LogicalType logicalType =
                BigQuerySchemaProviderTableImpl.getDataTypeSchemaFromAvroSchema(avroSchema)
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
        Descriptors.Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        // -- Nullable Schema for descriptor
        Schema avroSchema =
                TestBigQuerySchemas.getSchemaWithUnionOfRemainingPrimitiveTypes().getAvroSchema();
        LogicalType logicalType =
                BigQuerySchemaProviderTableImpl.getDataTypeSchemaFromAvroSchema(avroSchema)
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
                BigQuerySchemaProviderTableImpl.getDataTypeSchemaFromAvroSchema(avroSchema)
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
                BigQuerySchemaProviderTableImpl.getDataTypeSchemaFromAvroSchema(avroSchema)
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
                BigQuerySchemaProviderTableImpl.getDataTypeSchemaFromAvroSchema(nonNullSchema)
                        .getLogicalType();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);
        ByteString byteString = rowDataSerializer.serialize(row);

        // Check for the desired results.
        assertEquals("", byteString.toStringUtf8());
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
                BigQuerySchemaProviderTableImpl.getDataTypeSchemaFromAvroSchema(nonNullSchema)
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
                BigQuerySchemaProviderTableImpl.getDataTypeSchemaFromAvroSchema(
                                bigQuerySchemaProvider.getAvroSchema())
                        .getLogicalType();
        RowDataToProtoSerializer rowDataSerializer = new RowDataToProtoSerializer();
        rowDataSerializer.init(bigQuerySchemaProvider);
        rowDataSerializer.setLogicalType(logicalType);

        Descriptors.Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();

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
                BigQuerySchemaProviderTableImpl.getDataTypeSchemaFromAvroSchema(
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
        Descriptors.Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        LogicalType logicalType =
                BigQuerySchemaProviderTableImpl.getDataTypeSchemaFromAvroSchema(
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
                BigQuerySchemaProviderTableImpl.getDataTypeSchemaFromAvroSchema(
                                nullableRecordSchema)
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
                .hasMessageContaining(
                        "Received null value for non-nullable field record_field_union");
    }
}
