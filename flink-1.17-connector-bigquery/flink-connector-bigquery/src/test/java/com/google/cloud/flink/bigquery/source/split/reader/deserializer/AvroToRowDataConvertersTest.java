package com.google.cloud.flink.bigquery.source.split.reader.deserializer;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import com.google.cloud.flink.bigquery.source.reader.deserializer.AvroToRowDataConverters;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.assertj.core.api.Assertions;
import org.joda.time.LocalDate;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.google.cloud.flink.bigquery.sink.serializer.TestBigQuerySchemas.getAvroSchemaFromFieldString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/** Tests for AvroToRowDataConvertors. */
public class AvroToRowDataConvertersTest {
    @Test
    public void testNullTypeConvertor() {
        // Create the logical type schema
        RowType rowType =
                new RowType(Collections.singletonList(new RowField("null_field", new NullType())));
        AvroToRowDataConverters.AvroToRowDataConverter converter =
                AvroToRowDataConverters.createRowConverter(rowType);
        // Create the AvroRecord
        String fieldString =
                " \"fields\": [\n" + "   {\"name\": \"null_field\", \"type\": \"null\"}]";
        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        // Null Type
        IndexedRecord record = new GenericRecordBuilder(avroSchema).set("null_field", null).build();
        // Convert and Assert.
        Object convertedObject = converter.convert(record);
        assertEquals(GenericRowData.ofKind(RowKind.INSERT, (Object) null), convertedObject);
    }

    @Test
    public void testTinyIntTypeConvertor() {
        // Create the logical type schema
        RowType rowType =
                new RowType(
                        Collections.singletonList(
                                new RowField("tinyint_field", new TinyIntType())));
        AvroToRowDataConverters.AvroToRowDataConverter converter =
                AvroToRowDataConverters.createRowConverter(rowType);

        // Create the AvroRecord
        String fieldString =
                " \"fields\": [\n" + "   {\"name\": \"tinyint_field\", \"type\": \"int\"}]";
        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        IndexedRecord record =
                new GenericRecordBuilder(avroSchema).set("tinyint_field", 123).build();
        // Convert and Assert.
        Object convertedObject = converter.convert(record);
        assertEquals(GenericRowData.of((byte) 123), convertedObject);
    }

    @Test
    public void testInvalidTimeTypeConvertor() {
        // Create the logical type schema
        RowType rowType =
                new RowType(Collections.singletonList(new RowField("time_field", new TimeType(9))));

        // Invalid TimeType
        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> AvroToRowDataConverters.createRowConverter(rowType));
        Assertions.assertThat(exception)
                .hasMessageContaining(
                        "The TIME type within the Avro schema uses a precision of '9', which is higher than the maximum supported TIME precision 6.");
    }

    @Test
    public void testInvalidDateTimeTypeConvertor() {
        // Create the logical type schema
        RowType rowType =
                new RowType(
                        Collections.singletonList(
                                new RowField("datetime_field", new TimestampType(9))));

        // Invalid TimeType
        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> AvroToRowDataConverters.createRowConverter(rowType));
        Assertions.assertThat(exception)
                .hasMessageContaining(
                        "The TIMESTAMP/DATETIME type within the Avro schema uses a precision of '9', which is higher than the maximum supported TIMESTAMP/DATETIME precision 6.");
    }

    @Test
    public void testDecimalTypeConvertor() {
        // Create the logical type schema
        RowType rowType =
                new RowType(
                        Collections.singletonList(
                                new RowField("decimal_field", new DecimalType(34))));
        AvroToRowDataConverters.AvroToRowDataConverter converter =
                AvroToRowDataConverters.createRowConverter(rowType);
        // Create the AvroRecord
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"decimal_field\", \"type\": {\"type\": \"map\", \"values\": \"string\"}}]";
        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        // Decimal Type
        IndexedRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("decimal_field", "abcd".getBytes())
                        .build();
        // Convert and Assert.
        Object convertedObject = converter.convert(record);
        assertEquals(
                GenericRowData.of(DecimalData.fromUnscaledBytes("abcd".getBytes(), 34, 0)),
                convertedObject);

        // Invalid Type
        IndexedRecord invalidRecord =
                new GenericRecordBuilder(avroSchema).set("decimal_field", "abcd").build();
        // Convert and Assert.
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class, () -> converter.convert(invalidRecord));
        Assertions.assertThat(exception).hasMessageContaining("Avro to RowData Conversion Error");
    }

    @Test
    public void testMapTypeConvertor() {
        // Create the logical type schema
        RowType rowType =
                new RowType(
                        Collections.singletonList(
                                new RowField(
                                        "map_field",
                                        new MapType(new VarCharType(), new VarCharType()))));
        AvroToRowDataConverters.AvroToRowDataConverter converter =
                AvroToRowDataConverters.createRowConverter(rowType);
        // Create the AvroRecord
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"map_field\", \"type\": {\"type\": \"map\", \"values\": \"string\"}}]";
        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        // Map Type
        Map<String, String> map = new HashMap<>();
        map.put("first", "test_value");
        IndexedRecord record = new GenericRecordBuilder(avroSchema).set("map_field", map).build();
        // Convert and Assert.
        Object convertedObject = converter.convert(record);
        MapData mapData = ((GenericRowData) convertedObject).getMap(0);
        ArrayData keyArray = mapData.keyArray();
        ArrayData valArray = mapData.valueArray();
        int pos = 0;
        for (String key : map.keySet()) {
            assertEquals(keyArray.getString(pos).toString(), key);
            assertEquals(valArray.getString(pos).toString(), map.get(key));
        }
    }

    @Test
    public void testDatetimeTypeConvertor() {
        // Create the logical type schema
        RowType rowType =
                new RowType(
                        Collections.singletonList(
                                new RowField("datetime_field", new TimestampType(3))));
        AvroToRowDataConverters.AvroToRowDataConverter converter =
                AvroToRowDataConverters.createRowConverter(rowType);
        // Create the AvroRecord
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"datetime_field\", \"type\": {\"type\": \"long\", \"logical_type\": \"local-timestamp-millis\"}}]";
        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        // Invalid Type
        IndexedRecord invalidRecord =
                new GenericRecordBuilder(avroSchema)
                        .set("datetime_field", "2024-01-01T00.00.00")
                        .build();
        // Convert and Assert.
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class, () -> converter.convert(invalidRecord));
        Assertions.assertThat(exception)
                .hasMessageContaining(
                        "The Avro datetime string input for conversion to Flink's 'TIMESTAMP_LTZ' Logical Type");
    }

    @Test
    public void testTimestampTypeConvertor() {
        // Create the logical type schema
        RowType rowType =
                new RowType(
                        Collections.singletonList(
                                new RowField("timestamp_field", new TimestampType(3))));
        AvroToRowDataConverters.AvroToRowDataConverter converter =
                AvroToRowDataConverters.createRowConverter(rowType);
        // Create the AvroRecord
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"timestamp_field\", \"type\": {\"type\": \"long\", \"logical_type\": \"timestamp-millis\"}}]";
        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        // Long Type.
        IndexedRecord record =
                new GenericRecordBuilder(avroSchema).set("timestamp_field", 1731888000L).build();
        // Convert and Assert.
        Object convertedObject = converter.convert(record);
        assertEquals(
                GenericRowData.of(
                        TimestampData.fromTimestamp(Timestamp.valueOf("1970-01-21 01:04:48"))),
                convertedObject);

        // Instant type.
        record =
                new GenericRecordBuilder(avroSchema)
                        .set("timestamp_field", Instant.ofEpochMilli(1731888000))
                        .build();
        // Convert and Assert.
        convertedObject = converter.convert(record);
        assertEquals(
                GenericRowData.of(
                        TimestampData.fromTimestamp(Timestamp.valueOf("1970-01-21 01:04:48"))),
                convertedObject);

        // Utf-8 type.
        record =
                new GenericRecordBuilder(avroSchema)
                        .set("timestamp_field", new Utf8("1970-01-21 01:04:48"))
                        .build();
        // Convert and Assert.
        convertedObject = converter.convert(record);
        assertEquals(
                GenericRowData.of(
                        TimestampData.fromTimestamp(Timestamp.valueOf("1970-01-21 01:04:48"))),
                convertedObject);

        // Invalid Type
        IndexedRecord invalidRecord =
                new GenericRecordBuilder(avroSchema)
                        .set("timestamp_field", "abcd".getBytes())
                        .build();
        // Convert and Assert.
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class, () -> converter.convert(invalidRecord));
        Assertions.assertThat(exception).hasMessageContaining("Avro to RowData Conversion Error");
    }

    @Test
    public void testDateTypeConvertor() {
        // Create the logical type schema
        RowType rowType =
                new RowType(Collections.singletonList(new RowField("date_field", new DateType())));
        AvroToRowDataConverters.AvroToRowDataConverter converter =
                AvroToRowDataConverters.createRowConverter(rowType);
        // Create the AvroRecord
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"date_field\", \"type\": {\"type\": \"long\", \"logical_type\": \"time-millis\"}}]";
        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        // Joda Type.
        IndexedRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("date_field", java.time.LocalDate.of(2024, 1, 1))
                        .build();
        // Convert and Assert.
        Object convertedObject = converter.convert(record);
        assertEquals(GenericRowData.of(19723), convertedObject);

        // Local Date type.
        record =
                new GenericRecordBuilder(avroSchema)
                        .set("date_field", LocalDate.parse("2024-01-01"))
                        .build();
        // Convert and Assert.
        convertedObject = converter.convert(record);
        assertEquals(GenericRowData.of(19723), convertedObject);

        // Invalid Type
        IndexedRecord invalidRecord =
                new GenericRecordBuilder(avroSchema).set("date_field", "invalid_type").build();
        // Convert and Assert.
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class, () -> converter.convert(invalidRecord));
        Assertions.assertThat(exception)
                .hasMessageContaining(
                        "Unexpected Avro object invalid_type of type 'class java.lang.String' input for conversion to Flink's 'DATE' logical type. Supported type(s): 'INT, org.joda.time.LocalDate and java.time.LocalDate");
    }

    @Test
    public void testMillisTimeTypeConvertor() {
        // Create the logical type schema
        RowType rowType =
                new RowType(Collections.singletonList(new RowField("time_field", new TimeType(3))));
        AvroToRowDataConverters.AvroToRowDataConverter converter =
                AvroToRowDataConverters.createRowConverter(rowType);
        // Create the AvroRecord
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"time_field\", \"type\": {\"type\": \"long\", \"logical_type\": \"time-millis\"}}]";
        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        // Local Time Type.
        IndexedRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("time_field", LocalTime.of(3, 45, 0, 123456000))
                        .build();
        // Convert and Assert.
        Object convertedObject = converter.convert(record);
        assertEquals(GenericRowData.of(13500123), convertedObject);

        // Integer Type.
        record = new GenericRecordBuilder(avroSchema).set("time_field", 13500123).build();
        // Convert and Assert.
        convertedObject = converter.convert(record);
        assertEquals(GenericRowData.of(13500123), convertedObject);

        // Invalid Type.
        IndexedRecord invalidRecord =
                new GenericRecordBuilder(avroSchema).set("time_field", "invalid_type").build();
        // Convert and Assert.
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class, () -> converter.convert(invalidRecord));
        Assertions.assertThat(exception)
                .hasMessageContaining(
                        "Unexpected Avro object invalid_type of type 'class java.lang.String' input for conversion to Flink's 'MILLIS-TIME' logical type. Supported type(s): 'INT and java.time.LocalTime'");
    }

    @Test
    public void testMicrosTimeTypeConvertor() {
        // Create the logical type schema
        RowType rowType =
                new RowType(Collections.singletonList(new RowField("time_field", new TimeType(6))));
        AvroToRowDataConverters.AvroToRowDataConverter converter =
                AvroToRowDataConverters.createRowConverter(rowType);
        // Create the AvroRecord
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"time_field\", \"type\": {\"type\": \"long\", \"logical_type\": \"time-micros\"}}]";
        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        // Local Time Type.
        IndexedRecord record =
                new GenericRecordBuilder(avroSchema)
                        .set("time_field", LocalTime.of(3, 45, 0, 123456000))
                        .build();
        // Convert and Assert.
        Object convertedObject = converter.convert(record);
        assertEquals(GenericRowData.of(13500123456L), convertedObject);

        // Invalid Type.
        IndexedRecord invalidRecord =
                new GenericRecordBuilder(avroSchema).set("time_field", "invalid_type").build();
        // Convert and Assert.
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class, () -> converter.convert(invalidRecord));
        Assertions.assertThat(exception)
                .hasMessageContaining(
                        "Unexpected Avro object invalid_type of type 'class java.lang.String' input for conversion to Flink's 'MICROS-TIME' logical type. Supported type(s): 'LONG and java.time.LocalTime'");
    }

    @Test
    public void testByteTypeConvertor() {
        // Create the logical type schema
        RowType rowType =
                new RowType(
                        Collections.singletonList(new RowField("byte_field", new VarBinaryType())));
        AvroToRowDataConverters.AvroToRowDataConverter converter =
                AvroToRowDataConverters.createRowConverter(rowType);
        // Create the AvroRecord
        String fieldString =
                " \"fields\": [\n" + "   {\"name\": \"byte_field\", \"type\": \"bytes\"}]";
        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        IndexedRecord record =
                new GenericRecordBuilder(avroSchema).set("byte_field", "abcd".getBytes()).build();
        // Convert and Assert.
        Object convertedObject = converter.convert(record);
        assertEquals(GenericRowData.of("abcd".getBytes()), convertedObject);
    }

    @Test
    public void testErrorByteTypeConvertor() {
        // Create the logical type schema
        RowType rowType =
                new RowType(
                        Collections.singletonList(new RowField("byte_field", new VarBinaryType())));
        AvroToRowDataConverters.AvroToRowDataConverter converter =
                AvroToRowDataConverters.createRowConverter(rowType);
        // Create the AvroRecord
        String fieldString =
                " \"fields\": [\n" + "   {\"name\": \"byte_field\", \"type\": \"bytes\"}]";
        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        IndexedRecord record =
                new GenericRecordBuilder(avroSchema).set("byte_field", "abcd").build();
        // Convert and Assert.
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> converter.convert(record));
        Assertions.assertThat(exception)
                .hasMessageContaining(
                        "Unexpected Avro object abcd of type 'class java.lang.String' input for conversion to Flink's 'BYTE' logical type. Supported type(s): 'GenericFixed, byte[] and java.nio.ByteBuffer'");
    }
}
