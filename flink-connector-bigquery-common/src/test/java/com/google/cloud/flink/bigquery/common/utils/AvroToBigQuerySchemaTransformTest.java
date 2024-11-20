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

package com.google.cloud.flink.bigquery.common.utils;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

/** Unit tests for {@link AvroToBigQuerySchemaTransform}. */
public class AvroToBigQuerySchemaTransformTest {

    /** Tests Avro Schema with all Primitive Data Types. */
    @Test
    public void testAllTypesSchemaSuccessful() {
        org.apache.avro.Schema allTypesSchema =
                org.apache.avro.Schema.createRecord("allTypes", "", "", false);

        ArrayList<org.apache.avro.Schema.Field> fields = new ArrayList<>();
        fields.add(
                new org.apache.avro.Schema.Field(
                        "string_field", org.apache.avro.Schema.create(Type.STRING), "", null));
        fields.add(
                new org.apache.avro.Schema.Field(
                        "bytes_field", org.apache.avro.Schema.create(Type.BYTES), "", null));
        fields.add(
                new org.apache.avro.Schema.Field(
                        "integer_field", org.apache.avro.Schema.create(Type.LONG), "", null));

        org.apache.avro.Schema arrayFieldSchema =
                org.apache.avro.Schema.createArray(org.apache.avro.Schema.create(Type.DOUBLE));
        fields.add(new org.apache.avro.Schema.Field("array_field", arrayFieldSchema, "", null));

        org.apache.avro.Schema numericFieldSchema =
                org.apache.avro.Schema.createUnion(
                        org.apache.avro.Schema.create(Type.NULL),
                        LogicalTypes.decimal(38, 9)
                                .addToSchema(org.apache.avro.Schema.create(Type.BYTES)));
        fields.add(new org.apache.avro.Schema.Field("numeric_field", numericFieldSchema, "", null));

        org.apache.avro.Schema bignumericFieldSchema =
                org.apache.avro.Schema.createUnion(
                        org.apache.avro.Schema.create(Type.NULL),
                        LogicalTypes.decimal(77, 38)
                                .addToSchema(org.apache.avro.Schema.create(Type.BYTES)));
        fields.add(
                new org.apache.avro.Schema.Field(
                        "bignumeric_field", bignumericFieldSchema, "", null));

        fields.add(
                new org.apache.avro.Schema.Field(
                        "boolean_field",
                        org.apache.avro.Schema.createUnion(
                                org.apache.avro.Schema.create(Type.NULL),
                                org.apache.avro.Schema.create(Type.BOOLEAN)),
                        "",
                        null));

        fields.add(
                new org.apache.avro.Schema.Field(
                        "ts_field",
                        org.apache.avro.Schema.createUnion(
                                org.apache.avro.Schema.create(Type.NULL),
                                LogicalTypes.timestampMicros()
                                        .addToSchema(org.apache.avro.Schema.create(Type.LONG))),
                        "",
                        null));

        fields.add(
                new org.apache.avro.Schema.Field(
                        "date_field",
                        org.apache.avro.Schema.createUnion(
                                org.apache.avro.Schema.create(Type.NULL),
                                LogicalTypes.date()
                                        .addToSchema(org.apache.avro.Schema.create(Type.INT))),
                        "",
                        null));

        fields.add(
                new org.apache.avro.Schema.Field(
                        "time_field",
                        org.apache.avro.Schema.createUnion(
                                org.apache.avro.Schema.create(Type.NULL),
                                LogicalTypes.timeMicros()
                                        .addToSchema(org.apache.avro.Schema.create(Type.LONG))),
                        "",
                        null));

        fields.add(
                new org.apache.avro.Schema.Field(
                        "datetime_field",
                        org.apache.avro.Schema.createUnion(
                                org.apache.avro.Schema.create(Type.NULL),
                                LogicalTypes.localTimestampMicros()
                                        .addToSchema(org.apache.avro.Schema.create(Type.LONG))),
                        "",
                        null));

        org.apache.avro.Schema geoSchema =
                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING);
        geoSchema.addProp(LogicalType.LOGICAL_TYPE_PROP, "geography_wkt");
        org.apache.avro.Schema geographyFieldSchema =
                org.apache.avro.Schema.createUnion(
                        org.apache.avro.Schema.create(Type.NULL), geoSchema);
        fields.add(
                new org.apache.avro.Schema.Field(
                        "geography_field", geographyFieldSchema, "", null));

        org.apache.avro.Schema jsonSchema =
                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING);
        jsonSchema.addProp(LogicalType.LOGICAL_TYPE_PROP, "Json");
        org.apache.avro.Schema recordFieldSchema =
                org.apache.avro.Schema.createRecord(
                        "record_field",
                        "Translated Avro Schema for record_field",
                        "com.google.cloud.flink.bigquery",
                        false);
        ArrayList<org.apache.avro.Schema.Field> nestedFields = new ArrayList<>();
        nestedFields.add(
                new org.apache.avro.Schema.Field(
                        "json_field",
                        org.apache.avro.Schema.createUnion(
                                org.apache.avro.Schema.create(Type.NULL), jsonSchema)));
        nestedFields.add(
                new org.apache.avro.Schema.Field(
                        "geography_field", geographyFieldSchema, "", null));
        recordFieldSchema.setFields(nestedFields);
        fields.add(new org.apache.avro.Schema.Field("record_field", recordFieldSchema, "", null));
        allTypesSchema.setFields(fields);

        Schema expectedBqSchema =
                Schema.of(
                        createRequiredBigqueryField("string_field", StandardSQLTypeName.STRING),
                        createRequiredBigqueryField("bytes_field", StandardSQLTypeName.BYTES),
                        createRequiredBigqueryField("integer_field", StandardSQLTypeName.INT64),
                        createRepeatedBigqueryField("array_field", StandardSQLTypeName.FLOAT64),
                        createNullableBigqueryField("numeric_field", StandardSQLTypeName.NUMERIC),
                        createNullableBigqueryField(
                                "bignumeric_field", StandardSQLTypeName.BIGNUMERIC),
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

        Schema bqSchema = AvroToBigQuerySchemaTransform.getBigQuerySchema(allTypesSchema);
        assertExactSchema(bqSchema, expectedBqSchema);
    }

    /** Tests Avro Schema with all Logical Data Types. */
    @Test
    public void testLogicalTypesSuccessful() {
        // Create an Avro schema with logical types
        org.apache.avro.Schema avroSchema =
                org.apache.avro.Schema.createRecord("RecordWithLogicalTypes", "", "", false);
        ArrayList<org.apache.avro.Schema.Field> logicalFields = new ArrayList<>();
        logicalFields.add(
                new org.apache.avro.Schema.Field(
                        "dateField",
                        LogicalTypes.date().addToSchema(org.apache.avro.Schema.create(Type.INT)),
                        null,
                        null));
        logicalFields.add(
                new org.apache.avro.Schema.Field(
                        "timeMillisField",
                        LogicalTypes.timeMillis()
                                .addToSchema(org.apache.avro.Schema.create(Type.INT)),
                        null,
                        null));
        logicalFields.add(
                new org.apache.avro.Schema.Field(
                        "timestampMillisField",
                        LogicalTypes.timestampMillis()
                                .addToSchema(org.apache.avro.Schema.create(Type.LONG)),
                        null,
                        null));
        logicalFields.add(
                new org.apache.avro.Schema.Field(
                        "localTimestampMillisField",
                        LogicalTypes.localTimestampMillis()
                                .addToSchema(org.apache.avro.Schema.create(Type.LONG)),
                        null,
                        null));
        logicalFields.add(
                new org.apache.avro.Schema.Field(
                        "decimalField",
                        LogicalTypes.decimal(10, 2)
                                .addToSchema(org.apache.avro.Schema.create(Type.BYTES)),
                        "Decimal Field Description",
                        null));
        logicalFields.add(
                new org.apache.avro.Schema.Field(
                        "uuidField",
                        LogicalTypes.uuid().addToSchema(org.apache.avro.Schema.create(Type.STRING)),
                        null,
                        null));
        org.apache.avro.Schema durationSchema = org.apache.avro.Schema.create(Type.BYTES);
        durationSchema.addProp(LogicalType.LOGICAL_TYPE_PROP, "duration");
        logicalFields.add(
                new org.apache.avro.Schema.Field("durationField", durationSchema, null, null));
        org.apache.avro.Schema geoSchema =
                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING);
        geoSchema.addProp(LogicalType.LOGICAL_TYPE_PROP, "geography_wkt");
        logicalFields.add(
                new org.apache.avro.Schema.Field("geographyWKTField", geoSchema, null, null));
        org.apache.avro.Schema jsonSchema =
                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING);
        jsonSchema.addProp(LogicalType.LOGICAL_TYPE_PROP, "Json");
        logicalFields.add(new org.apache.avro.Schema.Field("jsonField", jsonSchema, null, null));
        avroSchema.setFields(logicalFields);

        Schema expectedBqSchema =
                Schema.of(
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

        Schema bqSchema = AvroToBigQuerySchemaTransform.getBigQuerySchema(avroSchema);
        assertExactSchema(bqSchema, expectedBqSchema);
    }

    /** Tests Avro record schema with nesting upto 15 levels. */
    @Test
    public void testDeeplyNestedSchemaSuccessful() {
        org.apache.avro.Schema currentSchema = null;
        for (int i = 13; i >= 0; i--) {
            org.apache.avro.Schema nextSchema =
                    org.apache.avro.Schema.createRecord("level_" + i, null, null, false);
            if (currentSchema != null) {
                ArrayList<org.apache.avro.Schema.Field> nestedFields = new ArrayList<>();
                nestedFields.add(
                        (new org.apache.avro.Schema.Field(
                                "level_" + (i + 1), currentSchema, null, null)));
                nextSchema.setFields(nestedFields);
            } else {
                ArrayList<org.apache.avro.Schema.Field> nestedFields = new ArrayList<>();
                nestedFields.add(
                        new org.apache.avro.Schema.Field(
                                "value", org.apache.avro.Schema.create(Type.LONG), null, null));
                nextSchema.setFields(nestedFields);
            }
            currentSchema = nextSchema;
        }
        org.apache.avro.Schema level0Schema = currentSchema;
        org.apache.avro.Schema nestedSchema =
                org.apache.avro.Schema.createRecord("nestedTypeIT", null, null, false);
        ArrayList<org.apache.avro.Schema.Field> recordFields = new ArrayList<>();
        recordFields.add(
                new org.apache.avro.Schema.Field(
                        "unique_key", org.apache.avro.Schema.create(Type.STRING), null, null));
        recordFields.add(
                new org.apache.avro.Schema.Field(
                        "name", org.apache.avro.Schema.create(Type.STRING), null, null));
        recordFields.add(
                new org.apache.avro.Schema.Field(
                        "number", org.apache.avro.Schema.create(Type.LONG), null, null));
        recordFields.add(
                new org.apache.avro.Schema.Field(
                        "ts",
                        LogicalTypes.timestampMicros()
                                .addToSchema(org.apache.avro.Schema.create(Type.LONG)),
                        null,
                        null));
        recordFields.add(new org.apache.avro.Schema.Field("level_0", level0Schema, null, null));
        nestedSchema.setFields(recordFields);

        Field currentField = createRequiredBigqueryField("value", StandardSQLTypeName.INT64);
        for (int i = 13; i >= 0; i--) {
            currentField = createRecordField("level_" + i, currentField);
        }
        Field level0Field = currentField;

        Schema expectedBqSchema =
                Schema.of(
                        createRequiredBigqueryField("unique_key", StandardSQLTypeName.STRING),
                        createRequiredBigqueryField("name", StandardSQLTypeName.STRING),
                        createRequiredBigqueryField("number", StandardSQLTypeName.INT64),
                        createRequiredBigqueryField("ts", StandardSQLTypeName.TIMESTAMP),
                        level0Field);

        Schema bqSchema = AvroToBigQuerySchemaTransform.getBigQuerySchema(nestedSchema);
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
        org.apache.avro.Schema longListSchema =
                org.apache.avro.Schema.createRecord("LongList", "", "", false);
        longListSchema.addAlias("LinkedLongs");

        ArrayList<org.apache.avro.Schema.Field> fields = new ArrayList<>();
        fields.add(
                new org.apache.avro.Schema.Field(
                        "value", org.apache.avro.Schema.create(Type.LONG), "", null));

        org.apache.avro.Schema nullableLongListSchema =
                org.apache.avro.Schema.createUnion(
                        org.apache.avro.Schema.create(Type.NULL), longListSchema);
        fields.add(new org.apache.avro.Schema.Field("next", nullableLongListSchema, "", null));
        longListSchema.setFields(fields);

        assertThrows(
                UnsupportedOperationException.class,
                () -> AvroToBigQuerySchemaTransform.getBigQuerySchema(longListSchema));
    }

    /**
     * Tests Avro record schema with more than 15 levels of nesting. This should throw an Exception.
     */
    @Test
    public void testNestedRecordExceedsLimitThrowsException() {
        org.apache.avro.Schema nestedSchema =
                org.apache.avro.Schema.createRecord("NestedRecord", "", "", false);
        List<org.apache.avro.Schema.Field> fields = new ArrayList<>();
        org.apache.avro.Schema currentSchema = nestedSchema;
        for (int i = 0; i < 16; i++) {
            org.apache.avro.Schema nextSchema =
                    org.apache.avro.Schema.createRecord("NestedRecord" + i, "", "", false);
            fields.add(new org.apache.avro.Schema.Field("nestedField", nextSchema, "", null));
            currentSchema.setFields(fields);
            currentSchema = nextSchema;
            fields = new ArrayList<>();
        }

        org.apache.avro.Schema nestedRecord =
                org.apache.avro.Schema.createRecord("NestedRecord", "", "", false);
        ArrayList<org.apache.avro.Schema.Field> nestedFields = new ArrayList<>();
        nestedFields.add(new org.apache.avro.Schema.Field("nestedField", nestedRecord, "", null));
        nestedRecord.setFields(nestedFields);

        assertThrows(
                UnsupportedOperationException.class,
                () -> AvroToBigQuerySchemaTransform.getBigQuerySchema(nestedRecord));
    }

    /**
     * Tests Avro schema: { "type": "record", "name": "ArrayRecord", "fields": [ { "name":
     * "arrayField", "type": { "type": "array", "items": { "type": "array", "items": "int" } } } ]
     * }. It should throw an exception since recursive Arrays are not supported by BigQuery
     */
    @Test
    public void testArrayOfArraysThrowsException() {
        org.apache.avro.Schema arrayOfArraysSchema =
                org.apache.avro.Schema.createArray(
                        org.apache.avro.Schema.createArray(
                                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT)));

        org.apache.avro.Schema arrayRecord =
                org.apache.avro.Schema.createRecord("ArrayRecord", "", "", false);
        ArrayList<org.apache.avro.Schema.Field> arrayFields = new ArrayList<>();
        arrayFields.add(
                new org.apache.avro.Schema.Field("arrayField", arrayOfArraysSchema, "", null));
        arrayRecord.setFields(arrayFields);

        assertThrows(
                UnsupportedOperationException.class,
                () -> AvroToBigQuerySchemaTransform.getBigQuerySchema(arrayRecord));
    }

    /**
     * Avro Schema: {"type": "record", "name": "RecordWithNullableArray", "fields": [{"name":
     * "nullableArray", "type": ["null", {"type": "array", "items": "int"}]}]} Tests that an
     * exception is thrown because BigQuery doesn't support nullable array types.
     */
    @Test
    public void testNullableArrayThrowsException() {
        org.apache.avro.Schema intArraySchema =
                org.apache.avro.Schema.createArray(org.apache.avro.Schema.create(Type.INT));
        org.apache.avro.Schema nullableArraySchema =
                org.apache.avro.Schema.createUnion(
                        org.apache.avro.Schema.create(Type.NULL), intArraySchema);

        org.apache.avro.Schema recordSchema =
                org.apache.avro.Schema.createRecord("RecordWithNullableArray", "", "", false);
        ArrayList<org.apache.avro.Schema.Field> fields = new ArrayList<>();
        fields.add(
                new org.apache.avro.Schema.Field("nullableArray", nullableArraySchema, "", null));
        recordSchema.setFields(fields);

        assertThrows(
                UnsupportedOperationException.class,
                () -> AvroToBigQuerySchemaTransform.getBigQuerySchema(recordSchema));
    }

    /**
     * Tests Avro schema: { "type": "record", "name": "ArrayRecord", "fields": [ { "name":
     * "arrayMultipleDataTypesField", "type": { "type": "array", "items": [ "int", "string", "float"
     * ] } } ] }.
     */
    @Test
    public void testArrayWithMultipleDatatypesThrowsException() {
        org.apache.avro.Schema unionArraySchema =
                org.apache.avro.Schema.createArray(
                        org.apache.avro.Schema.createUnion(
                                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),
                                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
                                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.FLOAT)));

        org.apache.avro.Schema arrayRecord =
                org.apache.avro.Schema.createRecord("ArrayRecord", "", "", false);
        ArrayList<org.apache.avro.Schema.Field> arrayFields = new ArrayList<>();
        arrayFields.add(
                new org.apache.avro.Schema.Field(
                        "arrayMultipleDataTypesField", unionArraySchema, "", null));
        arrayRecord.setFields(arrayFields);

        assertThrows(
                UnsupportedOperationException.class,
                () -> AvroToBigQuerySchemaTransform.getBigQuerySchema(arrayRecord));
    }

    /**
     * Tests Avro Schema: { "type": "record", "name": "OuterRecord", "fields": [ { "name":
     * "stringField", "type": "string" }, { "name": "arrayField", "type": { "type": "array",
     * "items": { "type": "record", "name": "InnerRecord", "fields": [ { "name": "stringField",
     * "type": "string" }, { "name": "intField", "type": "int" } ] } } }, { "name": "tsField",
     * "type": { "type": "long", "logicalType": "timestamp-micros" } } ] }.
     */
    @Test
    public void testArrayOfRecordsDatatypeSuccessful() {
        org.apache.avro.Schema innerRecordSchema =
                org.apache.avro.Schema.createRecord("InnerRecord", "", "", false);
        ArrayList<org.apache.avro.Schema.Field> innerFields = new ArrayList<>();
        innerFields.add(
                new org.apache.avro.Schema.Field(
                        "stringField", org.apache.avro.Schema.create(Type.STRING), "", null));
        innerFields.add(
                new org.apache.avro.Schema.Field(
                        "intField", org.apache.avro.Schema.create(Type.INT), "", null));
        innerRecordSchema.setFields(innerFields);

        org.apache.avro.Schema arrayOfRecordsSchema =
                org.apache.avro.Schema.createArray(innerRecordSchema);

        org.apache.avro.Schema arrayRecordSchema =
                org.apache.avro.Schema.createRecord("OuterRecord", "", "", false);
        ArrayList<org.apache.avro.Schema.Field> arrayFields = new ArrayList<>();
        arrayFields.add(
                new org.apache.avro.Schema.Field(
                        "stringField", org.apache.avro.Schema.create(Type.STRING), "", null));
        arrayFields.add(
                new org.apache.avro.Schema.Field("arrayField", arrayOfRecordsSchema, "", null));
        arrayFields.add(
                new org.apache.avro.Schema.Field(
                        "tsField",
                        LogicalTypes.timestampMicros()
                                .addToSchema(org.apache.avro.Schema.create(Type.LONG)),
                        "",
                        null));
        arrayRecordSchema.setFields(arrayFields);

        ArrayList<Field> bqRecordFields = new ArrayList<>();
        bqRecordFields.add(createRequiredBigqueryField("stringField", StandardSQLTypeName.STRING));
        bqRecordFields.add(createRequiredBigqueryField("intField", StandardSQLTypeName.INT64));
        Schema expectedBqSchema =
                Schema.of(
                        createRequiredBigqueryField("stringField", StandardSQLTypeName.STRING),
                        Field.newBuilder(
                                        "arrayField",
                                        LegacySQLTypeName.RECORD,
                                        FieldList.of(bqRecordFields))
                                .setMode(Field.Mode.REPEATED)
                                .build(),
                        createRequiredBigqueryField("tsField", StandardSQLTypeName.TIMESTAMP));

        Schema bqSchema = AvroToBigQuerySchemaTransform.getBigQuerySchema(arrayRecordSchema);
        assertExactSchema(bqSchema, expectedBqSchema);
    }

    /**
     * Tests Avro Schema: {"type": "record", "name": "Record", "fields": [{"name": "nullableField",
     * "type": ["null"]}]}.
     */
    @Test
    public void testNullableFieldWithOnlyNullTypeThrowsException() {
        org.apache.avro.Schema nullableSchema =
                org.apache.avro.Schema.createUnion(
                        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL));

        org.apache.avro.Schema recordSchema =
                org.apache.avro.Schema.createRecord("Record", "", "", false);
        ArrayList<org.apache.avro.Schema.Field> fields = new ArrayList<>();
        fields.add(new org.apache.avro.Schema.Field("nullableField", nullableSchema, "", null));
        recordSchema.setFields(fields);

        assertThrows(
                UnsupportedOperationException.class,
                () -> AvroToBigQuerySchemaTransform.getBigQuerySchema(recordSchema));
    }

    /**
     * Tests Avro Schema: {"type": "record", "name": "Record", "fields": [{"name": "unionField",
     * "type": ["int", "string"]}]}.
     */
    @Test
    public void testUnionFieldWithMultipleNonNullTypesThrowsException() {
        org.apache.avro.Schema unionSchema =
                org.apache.avro.Schema.createUnion(
                        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),
                        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));

        org.apache.avro.Schema recordSchema =
                org.apache.avro.Schema.createRecord("Record", "", "", false);
        ArrayList<org.apache.avro.Schema.Field> fields = new ArrayList<>();
        fields.add(new org.apache.avro.Schema.Field("unionField", unionSchema, "", null));
        recordSchema.setFields(fields);

        assertThrows(
                UnsupportedOperationException.class,
                () -> AvroToBigQuerySchemaTransform.getBigQuerySchema(recordSchema));
    }

    /**
     * Tests Avro Schema: {"type": "record", "name": "Record", "fields": [{"name":
     * "nullableStringField", "type": ["null", "string"]}]}.
     */
    @Test
    public void testNullableFieldWithValidUnion() {
        org.apache.avro.Schema nullableStringSchema =
                org.apache.avro.Schema.createUnion(
                        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL),
                        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));

        org.apache.avro.Schema recordSchema =
                org.apache.avro.Schema.createRecord("Record", "", "", false);
        ArrayList<org.apache.avro.Schema.Field> fields = new ArrayList<>();
        fields.add(
                new org.apache.avro.Schema.Field(
                        "nullableStringField", nullableStringSchema, "", null));
        recordSchema.setFields(fields);
        Schema expectedBqSchema =
                Schema.of(
                        createNullableBigqueryField(
                                "nullableStringField", StandardSQLTypeName.STRING));

        Schema bqSchema = AvroToBigQuerySchemaTransform.getBigQuerySchema(recordSchema);
        assertExactSchema(bqSchema, expectedBqSchema);
    }

    /**
     * Tested Avro schema tested: "string" It should throw an Exception since this schema has no
     * property as "name".
     */
    @Test
    public void testSchemaWithoutNamedFieldsThrowsException() {
        org.apache.avro.Schema avroUnNamedSchema = org.apache.avro.Schema.create(Type.STRING);

        assertThrows(
                IllegalArgumentException.class,
                () -> AvroToBigQuerySchemaTransform.getBigQuerySchema(avroUnNamedSchema));
    }

    /**
     * Tested Avro schema: { "type": "enum", "name": "EnumSchema", "symbols": [ "enumValue" ] } This
     * should be successful as even without "fields", this schema has "name" and a "type" property.
     */
    @Test
    public void testSchemaWithoutFieldsSuccessful() {
        // Create a simple enum schema without fields
        org.apache.avro.Schema avroSchema =
                SchemaBuilder.enumeration("EnumSchema").symbols("enumValue");

        // Convert to BigQuery schema
        Schema bqSchema = AvroToBigQuerySchemaTransform.getBigQuerySchema(avroSchema);

        Schema expectedBqSchema =
                Schema.of(createRequiredBigqueryField("EnumSchema", StandardSQLTypeName.STRING));

        assertExactSchema(bqSchema, expectedBqSchema);
    }

    // Helper function to assert equality of two BigQuery schemas
    private void assertExactSchema(Schema actual, Schema expected) {
        assertThat(actual.getFields().size()).isEqualTo(expected.getFields().size());
        for (int i = 0; i < actual.getFields().size(); i++) {
            Field actualField = actual.getFields().get(i);
            Field expectedField = expected.getFields().get(i);
            assertThat(actualField.getName()).isEqualTo(expectedField.getName());
            assertThat(actualField.getType()).isEqualTo(expectedField.getType());
            assertThat(actualField.getMode()).isEqualTo(expectedField.getMode());
            if (actualField.getType() == LegacySQLTypeName.RECORD) {
                assertExactSchema(
                        Schema.of(actualField.getSubFields()),
                        Schema.of(expectedField.getSubFields()));
            }
        }
    }

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
}
