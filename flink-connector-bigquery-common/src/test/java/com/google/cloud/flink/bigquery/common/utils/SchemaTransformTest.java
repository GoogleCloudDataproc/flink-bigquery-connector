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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;

/** */
public class SchemaTransformTest {
    private final List<TableFieldSchema> subFields =
            Arrays.asList(
                    new TableFieldSchema()
                            .setName("species")
                            .setType("STRING")
                            .setMode("NULLABLE"));
    /*
     * Note that the quality and quantity fields do not have their mode set, so they should default
     * to NULLABLE. This is an important test of BigQuery semantics.
     *
     * All the other fields we set in this function are required on the Schema response.
     *
     * See https://cloud.google.com/bigquery/docs/reference/v2/tables#schema
     */
    private final List<TableFieldSchema> fields =
            Arrays.asList(
                    new TableFieldSchema().setName("number").setType("INTEGER").setMode("REQUIRED"),
                    new TableFieldSchema().setName("species").setType("STRING").setMode("NULLABLE"),
                    new TableFieldSchema()
                            .setName("quality")
                            .setType("FLOAT") /* default to NULLABLE */,
                    new TableFieldSchema()
                            .setName("quantity")
                            .setType("INTEGER") /* default to NULLABLE */,
                    new TableFieldSchema()
                            .setName("birthday")
                            .setType("TIMESTAMP")
                            .setMode("NULLABLE"),
                    new TableFieldSchema()
                            .setName("birthdayMoney")
                            .setType("NUMERIC")
                            .setMode("NULLABLE"),
                    new TableFieldSchema()
                            .setName("lotteryWinnings")
                            .setType("BIGNUMERIC")
                            .setMode("NULLABLE"),
                    new TableFieldSchema()
                            .setName("flighted")
                            .setType("BOOLEAN")
                            .setMode("NULLABLE"),
                    new TableFieldSchema().setName("sound").setType("BYTES").setMode("NULLABLE"),
                    new TableFieldSchema()
                            .setName("anniversaryDate")
                            .setType("DATE")
                            .setMode("NULLABLE"),
                    new TableFieldSchema()
                            .setName("anniversaryDatetime")
                            .setType("DATETIME")
                            .setMode("NULLABLE"),
                    new TableFieldSchema()
                            .setName("anniversaryTime")
                            .setType("TIME")
                            .setMode("NULLABLE"),
                    new TableFieldSchema()
                            .setName("scion")
                            .setType("RECORD")
                            .setMode("NULLABLE")
                            .setFields(subFields),
                    new TableFieldSchema()
                            .setName("associates")
                            .setType("RECORD")
                            .setMode("REPEATED")
                            .setFields(subFields),
                    new TableFieldSchema()
                            .setName("geoPositions")
                            .setType("GEOGRAPHY")
                            .setMode("NULLABLE"));

    @Test
    public void testConvertBigQuerySchemaToAvroSchema() {
        TableSchema tableSchema = new TableSchema();
        tableSchema.setFields(fields);
        Schema avroSchema =
                SchemaTransform.toGenericAvroSchema("testSchema", tableSchema.getFields());

        assertThat(avroSchema.getField("number").schema())
                .isEqualTo(Schema.create(Schema.Type.LONG));

        assertThat(avroSchema.getField("species").schema())
                .isEqualTo(
                        Schema.createUnion(
                                Schema.create(Schema.Type.NULL),
                                Schema.create(Schema.Type.STRING)));
        assertThat(avroSchema.getField("quality").schema())
                .isEqualTo(
                        Schema.createUnion(
                                Schema.create(Schema.Type.NULL),
                                Schema.create(Schema.Type.DOUBLE)));
        assertThat(avroSchema.getField("quantity").schema())
                .isEqualTo(
                        Schema.createUnion(
                                Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG)));
        assertThat(avroSchema.getField("birthday").schema())
                .isEqualTo(
                        Schema.createUnion(
                                Schema.create(Schema.Type.NULL),
                                LogicalTypes.timestampMicros()
                                        .addToSchema(Schema.create(Schema.Type.LONG))));
        assertThat(avroSchema.getField("birthdayMoney").schema())
                .isEqualTo(
                        Schema.createUnion(
                                Schema.create(Schema.Type.NULL),
                                LogicalTypes.decimal(38, 9)
                                        .addToSchema(Schema.create(Schema.Type.BYTES))));
        assertThat(avroSchema.getField("lotteryWinnings").schema())
                .isEqualTo(
                        Schema.createUnion(
                                Schema.create(Schema.Type.NULL),
                                LogicalTypes.decimal(77, 38)
                                        .addToSchema(Schema.create(Schema.Type.BYTES))));
        assertThat(avroSchema.getField("flighted").schema())
                .isEqualTo(
                        Schema.createUnion(
                                Schema.create(Schema.Type.NULL),
                                Schema.create(Schema.Type.BOOLEAN)));
        assertThat(avroSchema.getField("sound").schema())
                .isEqualTo(
                        Schema.createUnion(
                                Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BYTES)));
        assertThat(avroSchema.getField("anniversaryDate").schema())
                .isEqualTo(
                        Schema.createUnion(
                                Schema.create(Schema.Type.NULL),
                                Schema.create(Schema.Type.STRING)));
        assertThat(avroSchema.getField("anniversaryDatetime").schema())
                .isEqualTo(
                        Schema.createUnion(
                                Schema.create(Schema.Type.NULL),
                                Schema.create(Schema.Type.STRING)));
        assertThat(avroSchema.getField("anniversaryTime").schema())
                .isEqualTo(
                        Schema.createUnion(
                                Schema.create(Schema.Type.NULL),
                                Schema.create(Schema.Type.STRING)));
        Schema geoSchema = Schema.create(Schema.Type.STRING);
        geoSchema.addProp(LogicalType.LOGICAL_TYPE_PROP, "geography_wkt");
        assertThat(avroSchema.getField("geoPositions").schema())
                .isEqualTo(Schema.createUnion(Schema.create(Schema.Type.NULL), geoSchema));
        assertThat(avroSchema.getField("scion").schema())
                .isEqualTo(
                        Schema.createUnion(
                                Schema.create(Schema.Type.NULL),
                                Schema.createRecord(
                                        "scion",
                                        "Translated Avro Schema for scion",
                                        SchemaTransform.DEFAULT_NAMESPACE,
                                        false,
                                        Arrays.asList(
                                                new Schema.Field(
                                                        "species",
                                                        Schema.createUnion(
                                                                Schema.create(Schema.Type.NULL),
                                                                Schema.create(Schema.Type.STRING)),
                                                        null,
                                                        (Object) null)))));
        assertThat(avroSchema.getField("associates").schema())
                .isEqualTo(
                        Schema.createArray(
                                Schema.createRecord(
                                        "associates",
                                        "Translated Avro Schema for associates",
                                        SchemaTransform.DEFAULT_NAMESPACE,
                                        false,
                                        Arrays.asList(
                                                new Schema.Field(
                                                        "species",
                                                        Schema.createUnion(
                                                                Schema.create(Schema.Type.NULL),
                                                                Schema.create(Schema.Type.STRING)),
                                                        null,
                                                        (Object) null)))));
    }

    @Test
    public void testBQSchemaToTableSchema() {

        String fieldName = "field1";

        TableSchema expected =
                new TableSchema()
                        .setFields(
                                Arrays.asList(
                                        new TableFieldSchema()
                                                .setName(fieldName)
                                                .setType("STRING")
                                                .setFields(Arrays.asList())));

        com.google.cloud.bigquery.Schema schema =
                com.google.cloud.bigquery.Schema.of(
                        Arrays.asList(Field.of(fieldName, StandardSQLTypeName.STRING)));

        TableSchema transformed = SchemaTransform.bigQuerySchemaToTableSchema(schema);

        assertThat(transformed).isEqualTo(expected);
    }

    @Test
    public void testBQTableFieldSchemaTypeToSQLType() {
        Map<String, StandardSQLTypeName> expectedMapping =
                new LinkedHashMap() {
                    {
                        put("INTEGER", StandardSQLTypeName.INT64);
                        put("INT64", StandardSQLTypeName.INT64);
                        put("STRING", StandardSQLTypeName.STRING);
                        put("FLOAT", StandardSQLTypeName.FLOAT64);
                        put("BIGNUMERIC", StandardSQLTypeName.BIGNUMERIC);
                        put("BOOLEAN", StandardSQLTypeName.BOOL);
                        put("BYTES", StandardSQLTypeName.BYTES);
                        put("DATE", StandardSQLTypeName.DATE);
                        put("RECORD", StandardSQLTypeName.STRUCT);
                    }
                };

        for (Map.Entry<String, StandardSQLTypeName> entry : expectedMapping.entrySet()) {
            assertThat(SchemaTransform.bigQueryTableFieldSchemaTypeToSQLType(entry.getKey()))
                    .isEqualTo(entry.getValue());
        }
    }
}
