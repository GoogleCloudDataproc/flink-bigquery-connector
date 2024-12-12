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

package com.google.cloud.flink.bigquery.common.utils;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.StandardSQLTypeName;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.Schema.Type;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.google.cloud.flink.bigquery.common.utils.AvroToBigQuerySchemaTransform.getUnsupportedTypeErrorMessage;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

/** Unit tests for {@link AvroToBigQuerySchemaTransform}. */
public class AvroToBigQuerySchemaTransformTest {

    private static final String GEOGRAPHY_LOGICAL_TYPE_NAME = "geography_wkt";
    private static final String JSON_LOGICAL_TYPE_NAME = "Json";

    private static Field createRequiredBigqueryField(String name, StandardSQLTypeName type) {
        return Field.newBuilder(name, type).setMode(Field.Mode.REQUIRED).build();
    }

    private static Field createNullableBigqueryField(String name, StandardSQLTypeName type) {
        return Field.newBuilder(name, type).setMode(Field.Mode.NULLABLE).build();
    }

    private static Field createRecordField(String name, Field... subFields) {
        return Field.newBuilder(name, LegacySQLTypeName.RECORD, FieldList.of(subFields))
                .setMode(Field.Mode.REQUIRED)
                .build();
    }

    private static Field createRepeatedBigqueryField(String name, StandardSQLTypeName type) {
        return Field.newBuilder(name, type).setMode(Field.Mode.REPEATED).build();
    }

    /** Tests Avro Schema with all Primitive Data Types. */
    @Test
    public void testAllTypesSchemaSuccessful() {
        String avroSchemaString =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"allTypes\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"string_field\", \"type\": \"string\"},\n"
                        + "    {\"name\": \"bytes_field\", \"type\": \"bytes\"},\n"
                        + "    {\"name\": \"integer_field\", \"type\": \"long\"},\n"
                        + "    {\"name\": \"array_field\", \"type\": {\"type\": \"array\", "
                        + "\"items\": \"double\"}},\n"
                        + "    {\"name\": \"numeric_field\", \"type\": [\"null\", {\"type\": "
                        + "\"bytes\", \"logicalType\": \"decimal\", \"precision\": 38, \"scale\":"
                        + " 9}]},\n"
                        + "    {\"name\": \"bignumeric_field\", \"type\": [\"null\", {\"type\": "
                        + "\"bytes\", \"logicalType\": \"decimal\", \"precision\": 76, \"scale\":"
                        + " 38}]},\n"
                        + "    {\"name\": \"boolean_field\", \"type\": [\"null\", \"boolean\"]},\n"
                        + "    {\"name\": \"ts_field\", \"type\": [\"null\", {\"type\": \"long\","
                        + " \"logicalType\": \"timestamp-micros\"}]},\n"
                        + "    {\"name\": \"date_field\", \"type\": [\"null\", {\"type\": "
                        + "\"int\", \"logicalType\": \"date\"}]},\n"
                        + "    {\"name\": \"time_field\", \"type\": [\"null\", {\"type\": "
                        + "\"long\", \"logicalType\": \"time-micros\"}]},\n"
                        + "    {\"name\": \"datetime_field\", \"type\": [\"null\", {\"type\": "
                        + "\"long\", \"logicalType\": \"local-timestamp-micros\"}]},\n"
                        + "    {\"name\": \"geography_field\", \"type\": [\"null\", {\"type\": "
                        + "\"string\", \"logicalType\": \"geography_wkt\"}]},\n"
                        + "    {\"name\": \"record_field\", \"type\": {\n"
                        + "      \"type\": \"record\",\n"
                        + "      \"name\": \"record_field\",\n"
                        + "      \"fields\": [\n"
                        + "        {\"name\": \"json_field\", \"type\": [\"null\", {\"type\": "
                        + "\"string\", \"logicalType\": \"Json\"}]},\n"
                        + "        {\"name\": \"geography_field\", \"type\": [\"null\", "
                        + "{\"type\": \"string\", \"logicalType\": \"geography_wkt\"}]}\n"
                        + "      ]\n"
                        + "    }}\n"
                        + "  ]\n"
                        + "}";
        Schema allTypesSchema = new Parser().parse(avroSchemaString);

        com.google.cloud.bigquery.Schema expectedBqSchema =
                com.google.cloud.bigquery.Schema.of(
                        createRequiredBigqueryField("string_field", StandardSQLTypeName.STRING),
                        createRequiredBigqueryField("bytes_field", StandardSQLTypeName.BYTES),
                        createRequiredBigqueryField("integer_field", StandardSQLTypeName.INT64),
                        createRepeatedBigqueryField("array_field", StandardSQLTypeName.FLOAT64),
                        createNullableBigqueryField("numeric_field", StandardSQLTypeName.NUMERIC)
                                .toBuilder()
                                .setPrecision(38L)
                                .setScale(9L)
                                .build(),
                        createNullableBigqueryField(
                                        "bignumeric_field", StandardSQLTypeName.BIGNUMERIC)
                                .toBuilder()
                                .setPrecision(76L)
                                .setScale(38L)
                                .build(),
                        createNullableBigqueryField("boolean_field", StandardSQLTypeName.BOOL),
                        createNullableBigqueryField("ts_field", StandardSQLTypeName.TIMESTAMP),
                        createNullableBigqueryField("date_field", StandardSQLTypeName.DATE),
                        createNullableBigqueryField("time_field", StandardSQLTypeName.TIME),
                        createNullableBigqueryField("datetime_field", StandardSQLTypeName.DATETIME),
                        createNullableBigqueryField(
                                "geography_field", StandardSQLTypeName.GEOGRAPHY),
                        Field.newBuilder(
                                        "record_field",
                                        LegacySQLTypeName.RECORD,
                                        FieldList.of(
                                                createNullableBigqueryField(
                                                        "json_field", StandardSQLTypeName.JSON),
                                                createNullableBigqueryField(
                                                        "geography_field",
                                                        StandardSQLTypeName.GEOGRAPHY)))
                                .setMode(Field.Mode.REQUIRED)
                                .build());

        com.google.cloud.bigquery.Schema bqSchema =
                AvroToBigQuerySchemaTransform.getBigQuerySchema(allTypesSchema);
        assertExactSchema(bqSchema, expectedBqSchema);
    }

    /** Tests Avro Schema with all Logical Data Types. */
    @Test
    public void testLogicalTypesSuccessful() {
        // Create an Avro schema with logical types
        Schema avroSchema = Schema.createRecord("RecordWithLogicalTypes", "", "", false);
        ArrayList<Schema.Field> logicalFields = new ArrayList<>();
        logicalFields.add(
                new Schema.Field(
                        "dateField",
                        LogicalTypes.date().addToSchema(Schema.create(Type.INT)),
                        null,
                        null));
        logicalFields.add(
                new Schema.Field(
                        "timeMillisField",
                        LogicalTypes.timeMillis().addToSchema(Schema.create(Type.INT)),
                        null,
                        null));
        logicalFields.add(
                new Schema.Field(
                        "timestampMillisField",
                        LogicalTypes.timestampMillis().addToSchema(Schema.create(Type.LONG)),
                        null,
                        null));
        logicalFields.add(
                new Schema.Field(
                        "localTimestampMillisField",
                        LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Type.LONG)),
                        null,
                        null));
        logicalFields.add(
                new Schema.Field(
                        "decimalField",
                        LogicalTypes.decimal(10, 2).addToSchema(Schema.create(Type.BYTES)),
                        "Decimal Field Description",
                        null));
        logicalFields.add(
                new Schema.Field(
                        "uuidField",
                        LogicalTypes.uuid().addToSchema(Schema.create(Type.STRING)),
                        null,
                        null));
        Schema durationSchema = Schema.create(Type.BYTES);
        durationSchema.addProp(LogicalType.LOGICAL_TYPE_PROP, "duration");
        logicalFields.add(new Schema.Field("durationField", durationSchema, null, null));
        Schema geoSchema = Schema.create(Schema.Type.STRING);
        geoSchema.addProp(LogicalType.LOGICAL_TYPE_PROP, GEOGRAPHY_LOGICAL_TYPE_NAME);
        logicalFields.add(new Schema.Field("geographyWKTField", geoSchema, null, null));
        Schema jsonSchema = Schema.create(Schema.Type.STRING);
        jsonSchema.addProp(LogicalType.LOGICAL_TYPE_PROP, JSON_LOGICAL_TYPE_NAME);
        logicalFields.add(new Schema.Field("jsonField", jsonSchema, null, null));
        avroSchema.setFields(logicalFields);

        com.google.cloud.bigquery.Schema expectedBqSchema =
                com.google.cloud.bigquery.Schema.of(
                        createRequiredBigqueryField("dateField", StandardSQLTypeName.DATE),
                        createRequiredBigqueryField("timeMillisField", StandardSQLTypeName.TIME),
                        createRequiredBigqueryField(
                                "timestampMillisField", StandardSQLTypeName.TIMESTAMP),
                        createRequiredBigqueryField(
                                "localTimestampMillisField", StandardSQLTypeName.DATETIME),
                        createRequiredBigqueryField("decimalField", StandardSQLTypeName.NUMERIC)
                                .toBuilder()
                                .setPrecision(10L)
                                .setDescription("Decimal Field Description")
                                .build(),
                        createRequiredBigqueryField("uuidField", StandardSQLTypeName.STRING),
                        createRequiredBigqueryField("durationField", StandardSQLTypeName.BYTES),
                        createRequiredBigqueryField(
                                "geographyWKTField", StandardSQLTypeName.GEOGRAPHY),
                        createRequiredBigqueryField("jsonField", StandardSQLTypeName.JSON));

        com.google.cloud.bigquery.Schema bqSchema =
                AvroToBigQuerySchemaTransform.getBigQuerySchema(avroSchema);
        assertExactSchema(bqSchema, expectedBqSchema);
    }

    /** Tests that an exception is thrown for invalid decimal precision and scale. */
    @Test
    public void testHandleDecimalLogicalTypeThrowsException() {
        Schema avroSchema = Schema.createRecord("RecordWithDecimalType", "", "", false);
        ArrayList<Schema.Field> decimalFields = new ArrayList<>();
        decimalFields.add(
                new Schema.Field(
                        "decimalField",
                        LogicalTypes.decimal(78, 10).addToSchema(Schema.create(Type.BYTES)),
                        "Decimal Field Description",
                        null));
        avroSchema.setFields(decimalFields);
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> AvroToBigQuerySchemaTransform.getBigQuerySchema(avroSchema));

        assertThat(exception.getMessage())
                .isEqualTo(
                        "Could not convert schema of avro field: decimalField to BigQuery table "
                                + "schema. BigQuery does not support Decimal types with precision"
                                + " 78 and scale 10.");
    }

    /** Tests Avro record schema with nesting upto 15 levels. */
    @Test
    public void testDeeplyNestedSchemaSuccessful() {
        Schema currentSchema = null;
        for (int i = 13; i >= 0; i--) {
            Schema nextSchema = Schema.createRecord("level_" + i, null, null, false);
            if (currentSchema != null) {
                ArrayList<Schema.Field> nestedFields = new ArrayList<>();
                nestedFields.add((new Schema.Field("level_" + (i + 1), currentSchema, null, null)));
                nextSchema.setFields(nestedFields);
            } else {
                ArrayList<Schema.Field> nestedFields = new ArrayList<>();
                nestedFields.add(new Schema.Field("value", Schema.create(Type.LONG), null, null));
                nextSchema.setFields(nestedFields);
            }
            currentSchema = nextSchema;
        }
        Schema level0Schema = currentSchema;
        Schema nestedSchema = Schema.createRecord("nestedTypeIT", null, null, false);
        ArrayList<Schema.Field> recordFields = new ArrayList<>();
        recordFields.add(new Schema.Field("level_0", level0Schema, null, null));
        nestedSchema.setFields(recordFields);

        Field currentField = createRequiredBigqueryField("value", StandardSQLTypeName.INT64);
        for (int i = 13; i >= 0; i--) {
            currentField = createRecordField("level_" + i, currentField);
        }
        Field level0Field = currentField;

        com.google.cloud.bigquery.Schema expectedBqSchema =
                com.google.cloud.bigquery.Schema.of(level0Field);

        com.google.cloud.bigquery.Schema bqSchema =
                AvroToBigQuerySchemaTransform.getBigQuerySchema(nestedSchema);
        assertExactSchema(bqSchema, expectedBqSchema);
    }

    /**
     * Tests Avro Schema: {"type": "record", "name": "LongList", "fields" : [{"name": "value",
     * "type": "long"}, {"name": "next", "type": ["null", "LongList"]}]}.
     *
     * <p>This should throw Exception as this is an infinite recursion and is not supported by
     * BigQuery
     */
    @Test
    public void testInfiniteRecursiveSchemaThrowsException() {
        // Build the Avro schema programmatically
        Schema longListSchema = Schema.createRecord("LongList", "", "", false);
        longListSchema.addAlias("LinkedLongs");

        ArrayList<Schema.Field> fields = new ArrayList<>();
        fields.add(new Schema.Field("value", Schema.create(Type.LONG), "", null));

        Schema nullableLongListSchema =
                Schema.createUnion(Schema.create(Type.NULL), longListSchema);
        fields.add(new Schema.Field("next", nullableLongListSchema, "", null));
        longListSchema.setFields(fields);

        assertThrows(
                IllegalArgumentException.class,
                () -> AvroToBigQuerySchemaTransform.getBigQuerySchema(longListSchema));
    }

    /**
     * Tests Avro record schema with more than 15 levels of nesting. This should throw an Exception.
     */
    @Test
    public void testNestedRecordExceedsLimitThrowsException() {
        Schema nestedSchema = Schema.createRecord("NestedRecord", "", "", false);
        List<Schema.Field> fields = new ArrayList<>();
        Schema currentSchema = nestedSchema;
        for (int i = 0; i < 16; i++) {
            Schema nextSchema = Schema.createRecord("NestedRecord" + i, "", "", false);
            fields.add(new Schema.Field("nestedField", nextSchema, "", null));
            currentSchema.setFields(fields);
            currentSchema = nextSchema;
            fields = new ArrayList<>();
        }

        Schema nestedRecord = Schema.createRecord("NestedRecord", "", "", false);
        ArrayList<Schema.Field> nestedFields = new ArrayList<>();
        nestedFields.add(new Schema.Field("nestedField", nestedRecord, "", null));
        nestedRecord.setFields(nestedFields);

        assertThrows(
                IllegalArgumentException.class,
                () -> AvroToBigQuerySchemaTransform.getBigQuerySchema(nestedRecord));
    }

    /**
     * Tests Avro schema: { "type": "record", "name": "ArrayRecord", "fields": [ { "name":
     * "arrayField", "type": { "type": "array", "items": { "type": "array", "items": "int" } } } ]
     * }. It should throw an exception since recursive Arrays are not supported by BigQuery
     */
    @Test
    public void testArrayOfArraysThrowsException() {
        String avroSchemaString =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"ArrayRecord\",\n"
                        + "  \"fields\": [\n"
                        + "    {\n"
                        + "      \"name\": \"arrayField\",\n"
                        + "      \"type\": {\n"
                        + "        \"type\": \"array\",\n"
                        + "        \"items\": {\n"
                        + "          \"type\": \"array\",\n"
                        + "          \"items\": \"int\"\n"
                        + "        }\n"
                        + "      }\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        Schema arrayRecord = new Parser().parse(avroSchemaString);

        assertThrows(
                IllegalArgumentException.class,
                () -> AvroToBigQuerySchemaTransform.getBigQuerySchema(arrayRecord));
    }

    /** Tests that an exception is thrown because BigQuery doesn't support nullable array types. */
    @Test
    public void testNullableArrayThrowsException() {
        String avroSchemaString =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"RecordWithNullableArray\",\n"
                        + "  \"fields\": [\n"
                        + "    {\n"
                        + "      \"name\": \"nullableArray\",\n"
                        + "      \"type\": [\"null\", {\"type\": \"array\", \"items\": \"int\"}]\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        Schema recordSchema = new Parser().parse(avroSchemaString);

        assertThrows(
                IllegalArgumentException.class,
                () -> AvroToBigQuerySchemaTransform.getBigQuerySchema(recordSchema));
    }

    /** Tests that an Avro array with a nullable inner type throws an exception. */
    @Test
    public void testArrayWithNullableInnerTypeThrowsException() {
        String avroSchemaString =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"ArrayRecord\",\n"
                        + "  \"fields\": [\n"
                        + "    {\n"
                        + "      \"name\": \"arrayField\",\n"
                        + "      \"type\": {\n"
                        + "        \"type\": \"array\",\n"
                        + "        \"items\": [\"null\", \"int\"]\n"
                        + "      }\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        Schema arrayRecord = new Parser().parse(avroSchemaString);

        assertThrows(
                IllegalArgumentException.class,
                () -> AvroToBigQuerySchemaTransform.getBigQuerySchema(arrayRecord));
    }

    /** Tests that an Avro array with multiple datatypes throws an exception. */
    @Test
    public void testArrayWithMultipleDatatypesThrowsException() {
        String avroSchemaString =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"ArrayRecord\",\n"
                        + "  \"fields\": [\n"
                        + "    {\n"
                        + "      \"name\": \"arrayMultipleDataTypesField\",\n"
                        + "      \"type\": {\n"
                        + "        \"type\": \"array\",\n"
                        + "        \"items\": [\"int\", \"string\", \"float\"]\n"
                        + "      }\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        Schema arrayRecord = new Parser().parse(avroSchemaString);

        assertThrows(
                IllegalArgumentException.class,
                () -> AvroToBigQuerySchemaTransform.getBigQuerySchema(arrayRecord));
    }

    /** Tests that an Avro array of records is correctly converted to a BigQuery schema. */
    @Test
    public void testArrayOfRecordsDatatypeSuccessful() {
        String avroSchemaString =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"OuterRecord\",\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"stringField\", \"type\": \"string\"},\n"
                        + "    {\n"
                        + "      \"name\": \"arrayField\",\n"
                        + "      \"type\": {\n"
                        + "        \"type\": \"array\",\n"
                        + "        \"items\": {\n"
                        + "          \"type\": \"record\",\n"
                        + "          \"name\": \"InnerRecord\",\n"
                        + "          \"fields\": [\n"
                        + "            {\"name\": \"stringField\", \"type\": \"string\"},\n"
                        + "            {\"name\": \"intField\", \"type\": \"int\"}\n"
                        + "          ]\n"
                        + "        }\n"
                        + "      }\n"
                        + "    },\n"
                        + "    {\"name\": \"tsField\", \"type\": {\"type\": \"long\", "
                        + "\"logicalType\": \"timestamp-micros\"}}\n"
                        + "  ]\n"
                        + "}";
        Schema arrayRecordSchema = new Parser().parse(avroSchemaString);

        ArrayList<Field> bqRecordFields = new ArrayList<>();
        bqRecordFields.add(createRequiredBigqueryField("stringField", StandardSQLTypeName.STRING));
        bqRecordFields.add(createRequiredBigqueryField("intField", StandardSQLTypeName.INT64));
        com.google.cloud.bigquery.Schema expectedBqSchema =
                com.google.cloud.bigquery.Schema.of(
                        createRequiredBigqueryField("stringField", StandardSQLTypeName.STRING),
                        Field.newBuilder(
                                        "arrayField",
                                        LegacySQLTypeName.RECORD,
                                        FieldList.of(bqRecordFields))
                                .setMode(Field.Mode.REPEATED)
                                .build(),
                        createRequiredBigqueryField("tsField", StandardSQLTypeName.TIMESTAMP));

        com.google.cloud.bigquery.Schema bqSchema =
                AvroToBigQuerySchemaTransform.getBigQuerySchema(arrayRecordSchema);
        assertExactSchema(bqSchema, expectedBqSchema);
    }

    /** Tests that an Avro union field with only a null type throws an exception. */
    @Test
    public void testUnionFieldWithOnlyNullTypeThrowsException() {
        String avroSchemaString =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"Record\",\n"
                        + "  \"fields\": [\n"
                        + "    {\n"
                        + "      \"name\": \"nullableField\",\n"
                        + "      \"type\": [\"null\"]\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        Schema recordSchema = new Parser().parse(avroSchemaString);

        assertThrows(
                IllegalArgumentException.class,
                () -> AvroToBigQuerySchemaTransform.getBigQuerySchema(recordSchema));
    }

    /** Tests that an Avro union field with multiple non-null types throws an exception. */
    @Test
    public void testUnionFieldWithMultipleNonNullTypesThrowsException() {
        String avroSchemaString =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"Record\",\n"
                        + "  \"fields\": [\n"
                        + "    {\n"
                        + "      \"name\": \"unionField\",\n"
                        + "      \"type\": [\"int\", \"string\"]\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        Schema recordSchema = new Parser().parse(avroSchemaString);

        assertThrows(
                IllegalArgumentException.class,
                () -> AvroToBigQuerySchemaTransform.getBigQuerySchema(recordSchema));
    }

    /** Tests that an Avro nullable field with a valid union is correctly converted. */
    @Test
    public void testNullableFieldWithValidUnion() {
        String avroSchemaString =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"Record\",\n"
                        + "  \"fields\": [\n"
                        + "    {\n"
                        + "      \"name\": \"nullableStringField\",\n"
                        + "      \"type\": [\"null\", \"string\"]\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        Schema recordSchema = new Parser().parse(avroSchemaString);

        com.google.cloud.bigquery.Schema expectedBqSchema =
                com.google.cloud.bigquery.Schema.of(
                        createNullableBigqueryField(
                                "nullableStringField", StandardSQLTypeName.STRING));

        com.google.cloud.bigquery.Schema bqSchema =
                AvroToBigQuerySchemaTransform.getBigQuerySchema(recordSchema);
        assertExactSchema(bqSchema, expectedBqSchema);
    }

    /**
     * Tested Avro schema tested: "string" It should throw an Exception since this schema has no
     * property as "name".
     */
    @Test
    public void testSchemaWithoutNamedFieldsThrowsException() {
        Schema avroUnNamedSchema = Schema.create(Type.STRING);

        assertThrows(
                IllegalArgumentException.class,
                () -> AvroToBigQuerySchemaTransform.getBigQuerySchema(avroUnNamedSchema));
    }

    /**
     * Tests that an Avro schema with a "map" type throws an exception with the correct error
     * message.
     */
    @Test
    public void testUnsupportedMapTypeThrowsExceptionWithCorrectMessage() {
        // Use an unsupported Avro type "map"
        String avroSchemaString =
                "{\n"
                        + "  \"type\": \"record\",\n"
                        + "  \"name\": \"RecordWithMapType\",\n"
                        + "  \"fields\": [\n"
                        + "    {\n"
                        + "      \"name\": \"mapField\",\n"
                        + "      \"type\": {\n"
                        + "        \"type\": \"map\",\n"
                        + "        \"values\": \"string\"\n"
                        + "      }\n"
                        + "    }\n"
                        + "  ]\n"
                        + "}";
        Schema recordSchema = new Parser().parse(avroSchemaString);

        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> AvroToBigQuerySchemaTransform.getBigQuerySchema(recordSchema));

        String expectedMessage = getUnsupportedTypeErrorMessage("MAP", "mapField");
        assertThat(exception.getMessage()).isEqualTo(expectedMessage);
    }

    // Helper function to assert equality of two BigQuery schemas
    private void assertExactSchema(
            com.google.cloud.bigquery.Schema actual, com.google.cloud.bigquery.Schema expected) {
        assertThat(actual.getFields().size()).isEqualTo(expected.getFields().size());
        for (int i = 0; i < actual.getFields().size(); i++) {
            Field actualField = actual.getFields().get(i);
            Field expectedField = expected.getFields().get(i);
            assertThat(actualField.getName()).isEqualTo(expectedField.getName());
            assertThat(actualField.getType()).isEqualTo(expectedField.getType());
            assertThat(actualField.getMode()).isEqualTo(expectedField.getMode());
            if (expectedField.getPrecision() != null) {
                assertThat(actualField.getPrecision()).isEqualTo(expectedField.getPrecision());
            }
            if (expectedField.getScale() != null) {
                assertThat(actualField.getScale()).isEqualTo(expectedField.getScale());
            }
            if (actualField.getType() == LegacySQLTypeName.RECORD) {
                assertExactSchema(
                        com.google.cloud.bigquery.Schema.of(actualField.getSubFields()),
                        com.google.cloud.bigquery.Schema.of(expectedField.getSubFields()));
            }
        }
    }
}
