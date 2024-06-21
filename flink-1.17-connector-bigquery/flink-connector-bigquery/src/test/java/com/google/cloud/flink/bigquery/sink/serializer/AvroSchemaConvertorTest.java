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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.avro.Schema;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/** Test for {@link AvroSchemaConvertor}. */
public class AvroSchemaConvertorTest {

    // ------ Test conversion from Avro Schema to Data Type --------------------------
    @Test
    public void testInvalidAvroSchemaStringConversion() {
        // Get the avro schema
        String avroSchemaString = "this is not a valid avro schema string";

        // Check for the desired error.
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> AvroSchemaConvertor.convertToDataType(avroSchemaString));
        Assertions.assertThat(exception).hasMessageContaining("Could not parse Avro schema string");
    }

    @Test
    public void testNullAvroSchemaConversion() {
        // Check for the desired error.
        NullPointerException exception =
                assertThrows(
                        NullPointerException.class,
                        () -> AvroSchemaConvertor.convertToDataType(null));
        Assertions.assertThat(exception).hasMessageContaining("Avro schema must not be null.");
    }

    @Test
    public void testSingleDatatypeInUnionConversion() {
        // Get the avro schema
        String avroSchemaString =
                TestBigQuerySchemas.getAvroSchemaFromFieldString(
                                "\"fields\": [\n"
                                        + "   {\"name\": \"union_with_one_datatype\", \"type\": [\"string\"]}\n"
                                        + " ]\n")
                        .toString();

        // Form the Data Type
        DataType dataType = AvroSchemaConvertor.convertToDataType(avroSchemaString);

        // Check the expected type
        DataType dataTypeExpected =
                DataTypes.ROW(
                                DataTypes.FIELD(
                                        "union_with_one_datatype", DataTypes.STRING().notNull()))
                        .notNull();
        assertEquals(dataTypeExpected, dataType);
    }

    @Test
    public void testMapConversionToDatatypeConversion() {
        // Get the avro schema
        Schema avroSchema =
                TestBigQuerySchemas.getAvroSchemaFromFieldString(
                        TestBigQuerySchemas.getSchemaWithArrayOfMap());

        // Form the Data Type
        DataType dataType = AvroSchemaConvertor.convertToDataType(avroSchema.toString());

        // Check the expected type
        DataType dataTypeExpected =
                DataTypes.ROW(
                                DataTypes.FIELD(
                                        "array_of_map",
                                        DataTypes.ARRAY(
                                                        DataTypes.MAP(
                                                                        DataTypes.STRING()
                                                                                .notNull(),
                                                                        DataTypes.BYTES().notNull())
                                                                .notNull())
                                                .notNull()
                                                .notNull()))
                        .notNull();
        assertEquals(dataTypeExpected, dataType);
    }

    // ------ Test conversion from Logical Type Schema to Avro Schema --------------------------
    @Test
    public void testStringDataTypeConversionToAvroType() {
        // Form the Data Type
        DataType dataType =
                DataTypes.ROW(DataTypes.FIELD("string_type", DataTypes.STRING())).notNull();
        LogicalType logicalType = dataType.getLogicalType();

        Schema convertedAvroSchema = AvroSchemaConvertor.convertToSchema(logicalType);

        // Check the expected type
        String exectedFieldString =
                "\"fields\":[{\"name\":\"string_type\",\"type\":[\"null\",\"string\"]}]";
        Schema expectedAvroSchema = getAvroSchemaFromFieldString(exectedFieldString);

        // Check expected type.
        assertEquals(convertedAvroSchema, expectedAvroSchema);
    }

    @Test
    public void testNullDataTypeConversionToAvroType() {
        // Form the Data Type
        DataType dataType = DataTypes.ROW(DataTypes.FIELD("null_type", DataTypes.NULL())).notNull();
        LogicalType logicalType = dataType.getLogicalType();

        Schema convertedAvroSchema = AvroSchemaConvertor.convertToSchema(logicalType);

        // Check the expected type
        String exectedFieldString = "\"fields\":[{\"name\":\"null_type\",\"type\":\"null\"}]";
        Schema expectedAvroSchema = getAvroSchemaFromFieldString(exectedFieldString);

        // Check expected type.
        assertEquals(expectedAvroSchema, convertedAvroSchema);
    }

    @Test
    public void testBigintDataTypeConversionToAvroType() {
        // Form the Data Type
        DataType dataType =
                DataTypes.ROW(DataTypes.FIELD("bigint_type", DataTypes.BIGINT())).notNull();
        LogicalType logicalType = dataType.getLogicalType();

        Schema convertedAvroSchema = AvroSchemaConvertor.convertToSchema(logicalType);

        // Check the expected type
        String exectedFieldString =
                "\"fields\":[{\"name\":\"bigint_type\",\"type\":[\"null\",\"long\"]}]";
        Schema expectedAvroSchema = getAvroSchemaFromFieldString(exectedFieldString);

        // Check expected type.
        assertEquals(convertedAvroSchema, expectedAvroSchema);
    }

    @Test
    public void testBooleanDataTypeConversionToAvroType() {
        // Form the Data Type
        DataType dataType =
                DataTypes.ROW(DataTypes.FIELD("boolean_type", DataTypes.BOOLEAN())).notNull();
        LogicalType logicalType = dataType.getLogicalType();

        Schema convertedAvroSchema = AvroSchemaConvertor.convertToSchema(logicalType);

        // Check the expected type
        String exectedFieldString =
                "\"fields\":[{\"name\":\"boolean_type\",\"type\":[\"null\",\"boolean\"]}]";
        Schema expectedAvroSchema = getAvroSchemaFromFieldString(exectedFieldString);

        // Check expected type.
        assertEquals(convertedAvroSchema, expectedAvroSchema);
    }

    @Test
    public void testBytesDataTypeConversionToAvroType() {
        // Form the Data Type
        DataType dataType =
                DataTypes.ROW(DataTypes.FIELD("bytes_type", DataTypes.BYTES())).notNull();
        LogicalType logicalType = dataType.getLogicalType();

        Schema convertedAvroSchema = AvroSchemaConvertor.convertToSchema(logicalType);

        // Check the expected type
        String exectedFieldString =
                "\"fields\":[{\"name\":\"bytes_type\",\"type\":[\"null\",\"bytes\"]}]";
        Schema expectedAvroSchema = getAvroSchemaFromFieldString(exectedFieldString);

        // Check the expected type.
        assertEquals(convertedAvroSchema, expectedAvroSchema);
    }

    @Test
    public void testIntegerDataTypeConversionToAvroType() {
        // Form the Data Type
        DataType dataType =
                DataTypes.ROW(
                                DataTypes.FIELD("integer_type", DataTypes.INT()),
                                DataTypes.FIELD("tinyint_type", DataTypes.TINYINT()),
                                DataTypes.FIELD("smallint_type", DataTypes.SMALLINT()))
                        .notNull();
        LogicalType logicalType = dataType.getLogicalType();

        Schema convertedAvroSchema = AvroSchemaConvertor.convertToSchema(logicalType);

        // Check the expected type
        String exectedFieldString =
                "\"fields\":["
                        + "{\"name\":\"integer_type\",\"type\":[\"null\",\"int\"]},"
                        + "{\"name\":\"tinyint_type\",\"type\":[\"null\",\"int\"]},"
                        + "{\"name\":\"smallint_type\",\"type\":[\"null\",\"int\"]}"
                        + "]";
        Schema expectedAvroSchema = getAvroSchemaFromFieldString(exectedFieldString);

        // Check the expected type.
        assertEquals(convertedAvroSchema, expectedAvroSchema);
    }

    @Test
    public void testDoubleAndFloatDataTypeConversionToAvroType() {
        // Form the Data Type
        DataType dataType =
                DataTypes.ROW(
                                DataTypes.FIELD("double_type", DataTypes.DOUBLE()),
                                DataTypes.FIELD("float_type", DataTypes.FLOAT()))
                        .notNull();
        LogicalType logicalType = dataType.getLogicalType();

        Schema convertedAvroSchema = AvroSchemaConvertor.convertToSchema(logicalType);

        // Check the expected type
        String exectedFieldString =
                "\"fields\":["
                        + "{\"name\":\"double_type\",\"type\":[\"null\",\"double\"]},"
                        + "{\"name\":\"float_type\",\"type\":[\"null\",\"float\"]}"
                        + "]";
        Schema expectedAvroSchema = getAvroSchemaFromFieldString(exectedFieldString);

        // Check the expected type.
        assertEquals(convertedAvroSchema, expectedAvroSchema);
    }

    @Test
    public void testTimestampTypeConversionToAvroType() {
        // Form the Data Type
        DataType dataType =
                DataTypes.ROW(
                                DataTypes.FIELD(
                                        "timestamp_micros_type", DataTypes.TIMESTAMP().notNull()),
                                DataTypes.FIELD(
                                        "timestamp_millis_type", DataTypes.TIMESTAMP(3).notNull()))
                        .notNull();
        LogicalType logicalType = dataType.getLogicalType();

        Schema convertedAvroSchema = AvroSchemaConvertor.convertToSchema(logicalType);

        // Check the expected type
        String exectedFieldString =
                "\"fields\":["
                        + "{\"name\":\"timestamp_micros_type\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}},"
                        + "{\"name\":\"timestamp_millis_type\",\"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}}"
                        + "]";
        Schema expectedAvroSchema = getAvroSchemaFromFieldString(exectedFieldString);

        // Check the expected type.
        assertEquals(convertedAvroSchema, expectedAvroSchema);
    }

    @Test
    public void testDateTypeConversionToAvroType() {
        // Form the Data Type
        DataType dataType =
                DataTypes.ROW(DataTypes.FIELD("date_type", DataTypes.DATE().notNull())).notNull();
        LogicalType logicalType = dataType.getLogicalType();

        Schema convertedAvroSchema = AvroSchemaConvertor.convertToSchema(logicalType);

        // Check the expected type
        String exectedFieldString =
                "\"fields\":["
                        + "{\"name\":\"date_type\", \"type\": {\"type\": \"int\", \"logicalType\": \"date\"}}"
                        + "]";
        Schema expectedAvroSchema = getAvroSchemaFromFieldString(exectedFieldString);

        // Check the expected type.
        assertEquals(convertedAvroSchema, expectedAvroSchema);
    }

    @Test
    public void testTimeTypeConversionToAvroType() {
        // Form the Data Type
        DataType dataType =
                DataTypes.ROW(
                                DataTypes.FIELD("time_millis_type", DataTypes.TIME(3).notNull()),
                                DataTypes.FIELD("time_micros_type", DataTypes.TIME(6).notNull()))
                        .notNull();
        LogicalType logicalType = dataType.getLogicalType();

        Schema convertedAvroSchema = AvroSchemaConvertor.convertToSchema(logicalType);

        // Check the expected type
        String exectedFieldString =
                "\"fields\":["
                        + "{\"name\":\"time_millis_type\", \"type\": {\"type\": \"int\", \"logicalType\": \"time-millis\"}}, "
                        + "{\"name\":\"time_micros_type\", \"type\": {\"type\": \"long\", \"logicalType\": \"time-micros\"}}"
                        + "]";
        Schema expectedAvroSchema = getAvroSchemaFromFieldString(exectedFieldString);

        // Check the expected type.
        assertEquals(convertedAvroSchema, expectedAvroSchema);
    }

    @Test
    public void testDatetimeTypeConversionToAvroType() {
        // Form the Data Type
        DataType dataType =
                DataTypes.ROW(
                                DataTypes.FIELD(
                                        "datetime_millis_type",
                                        DataTypes.TIMESTAMP_LTZ(3).notNull()),
                                DataTypes.FIELD(
                                        "datetime_micros_type",
                                        DataTypes.TIMESTAMP_LTZ(6).notNull()))
                        .notNull();
        LogicalType logicalType = dataType.getLogicalType();

        Schema convertedAvroSchema = AvroSchemaConvertor.convertToSchema(logicalType);

        // Check the expected type
        String exectedFieldString =
                "\"fields\":["
                        + "{\"name\":\"datetime_millis_type\", \"type\": {\"type\": \"long\", \"logicalType\": \"local-timestamp-millis\"}}, "
                        + "{\"name\":\"datetime_micros_type\", \"type\": {\"type\": \"long\", \"logicalType\": \"local-timestamp-micros\"}}"
                        + "]";
        Schema expectedAvroSchema = getAvroSchemaFromFieldString(exectedFieldString);

        // Check the expected type.
        assertEquals(convertedAvroSchema, expectedAvroSchema);
    }

    @Test
    public void testNumericTypeConversionToAvroType() {
        // Form the Data Type
        DataType dataType =
                DataTypes.ROW(DataTypes.FIELD("numeric_type", DataTypes.DECIMAL(38, 9).notNull()))
                        .notNull();
        LogicalType logicalType = dataType.getLogicalType();

        Schema convertedAvroSchema = AvroSchemaConvertor.convertToSchema(logicalType);

        // Check the expected type
        String exectedFieldString =
                "\"fields\":["
                        + "{\"name\":\"numeric_type\", \"type\": {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"scale\" : 9, \"precision\": 38 , \"isNumeric\" : true}} "
                        + "]";
        Schema expectedAvroSchema = getAvroSchemaFromFieldString(exectedFieldString);

        // Check the expected type.
        assertEquals(expectedAvroSchema, convertedAvroSchema);
    }

    @Test
    public void testArrayTypeConversionToAvroType() {
        // Form the Data Type
        DataType dataType =
                DataTypes.ROW(
                                DataTypes.FIELD(
                                        "array_type", DataTypes.ARRAY(DataTypes.CHAR(2)).notNull()))
                        .notNull();
        LogicalType logicalType = dataType.getLogicalType();

        Schema convertedAvroSchema = AvroSchemaConvertor.convertToSchema(logicalType);

        // Check the expected type
        String exectedFieldString =
                "\"fields\":["
                        + "{\"name\":\"array_type\", \"type\": {\"type\": \"array\", \"items\": [\"null\", \"string\"]}} "
                        + "]";

        Schema expectedAvroSchema = getAvroSchemaFromFieldString(exectedFieldString);

        // Check the expected type.
        assertEquals(expectedAvroSchema, convertedAvroSchema);
    }

    @Test
    public void testMapTypeConversionToAvroType() {
        // Form the Data Type
        DataType dataType =
                DataTypes.ROW(
                                DataTypes.FIELD(
                                        "map_type",
                                        DataTypes.MAP(
                                                        DataTypes.STRING().notNull(),
                                                        DataTypes.DOUBLE().notNull())
                                                .notNull()))
                        .notNull();
        LogicalType logicalType = dataType.getLogicalType();

        Schema convertedAvroSchema = AvroSchemaConvertor.convertToSchema(logicalType);

        // Check the expected type
        String exectedFieldString =
                "\"fields\":["
                        + "{\"name\":\"map_type\", \"type\": {\"type\": \"map\", \"values\": \"double\"}} "
                        + "]";

        Schema expectedAvroSchema = getAvroSchemaFromFieldString(exectedFieldString);

        // Check the expected type.
        assertEquals(expectedAvroSchema, convertedAvroSchema);
    }

    @Test
    public void testMultisetTypeConversionToAvroType() {
        // Form the Data Type
        DataType dataType =
                DataTypes.ROW(
                                DataTypes.FIELD(
                                        "multiset_type",
                                        DataTypes.MULTISET(DataTypes.STRING().notNull()).notNull()))
                        .notNull();
        LogicalType logicalType = dataType.getLogicalType();

        Schema convertedAvroSchema = AvroSchemaConvertor.convertToSchema(logicalType);

        // Check the expected type
        String exectedFieldString =
                "\"fields\":["
                        + "{\"name\":\"multiset_type\", \"type\": {\"type\": \"map\", \"values\": [\"null\",\"int\"]}} "
                        + "]";

        Schema expectedAvroSchema = getAvroSchemaFromFieldString(exectedFieldString);

        // Check the expected type.
        assertEquals(expectedAvroSchema, convertedAvroSchema);
    }

    @Test
    public void testInvalidMapTypeConversionToAvroType() {
        // Form the Data Type
        DataType dataType =
                DataTypes.ROW(
                                DataTypes.FIELD(
                                        "invalid_map_type",
                                        DataTypes.MAP(
                                                        DataTypes.BIGINT().notNull(),
                                                        DataTypes.STRING().notNull())
                                                .notNull()))
                        .notNull();
        LogicalType logicalType = dataType.getLogicalType();

        // Check for the desired error.
        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> AvroSchemaConvertor.convertToSchema(logicalType));
        Assertions.assertThat(exception)
                .hasMessageContaining("Avro format doesn't support non-string as key type of map.");
    }

    @Test
    public void testUnsupportedTypeConversionToAvroType() {
        // Form the Data Type
        DataType dataType =
                DataTypes.ROW(
                                DataTypes.FIELD(
                                        "unsupported_type", DataTypes.TIMESTAMP_WITH_TIME_ZONE(6)))
                        .notNull();
        LogicalType logicalType = dataType.getLogicalType();

        // Check for the desired error.
        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> AvroSchemaConvertor.convertToSchema(logicalType));
        Assertions.assertThat(exception)
                .hasMessageContaining("Unsupported to derive Schema for type");
    }

    @Test
    public void testUnsupportedDatetimeTypeConversionToAvroType() {
        // Form the Data Type
        DataType dataType =
                DataTypes.ROW(
                                DataTypes.FIELD(
                                        "unsupported_datetime_type", DataTypes.TIMESTAMP_LTZ(9)))
                        .notNull();
        LogicalType logicalType = dataType.getLogicalType();

        // Check for the desired error.
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> AvroSchemaConvertor.convertToSchema(logicalType));
        Assertions.assertThat(exception)
                .hasMessageContaining("it only supports precision less than equal to 6.");
    }

    @Test
    public void testUnsupportedTimestampTypeConversionToAvroType() {
        // Form the Data Type
        DataType dataType =
                DataTypes.ROW(DataTypes.FIELD("unsupported_timestamp_type", DataTypes.TIMESTAMP(9)))
                        .notNull();
        LogicalType logicalType = dataType.getLogicalType();

        // Check for the desired error.
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> AvroSchemaConvertor.convertToSchema(logicalType));
        Assertions.assertThat(exception)
                .hasMessageContaining("it only supports precision less than equal to 6.");
    }

    @Test
    public void testUnsupportedTimeTypeConversionToAvroType() {
        // Form the Data Type
        DataType dataType =
                DataTypes.ROW(DataTypes.FIELD("unsupported_time_type", DataTypes.TIME(9)))
                        .notNull();
        LogicalType logicalType = dataType.getLogicalType();

        // Check for the desired error.
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> AvroSchemaConvertor.convertToSchema(logicalType));
        Assertions.assertThat(exception)
                .hasMessageContaining("it only supports precision less than equal to 6.");
    }

    public static Schema getAvroSchemaFromFieldString(String fieldString) {
        String avroSchemaString =
                "{\"type\": \"record\",\n"
                        + " \"name\": \"record\",\n"
                        + " \"namespace\": \"org.apache.flink.avro.generated\",\n"
                        + fieldString
                        + "}";
        return new Schema.Parser().parse(avroSchemaString);
    }
}
