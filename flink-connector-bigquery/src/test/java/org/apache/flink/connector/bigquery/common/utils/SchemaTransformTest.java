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

package org.apache.flink.connector.bigquery.common.utils;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.List;

/** */
public class SchemaTransformTest {
    private List<TableFieldSchema> subFields =
            Lists.newArrayList(
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
    private List<TableFieldSchema> fields =
            Lists.newArrayList(
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

        Assertions.assertThat(avroSchema.getField("number").schema())
                .isEqualTo(Schema.create(Schema.Type.LONG));

        Assertions.assertThat(avroSchema.getField("species").schema())
                .isEqualTo(
                        Schema.createUnion(
                                Schema.create(Schema.Type.NULL),
                                Schema.create(Schema.Type.STRING)));
        Assertions.assertThat(avroSchema.getField("quality").schema())
                .isEqualTo(
                        Schema.createUnion(
                                Schema.create(Schema.Type.NULL),
                                Schema.create(Schema.Type.DOUBLE)));
        Assertions.assertThat(avroSchema.getField("quantity").schema())
                .isEqualTo(
                        Schema.createUnion(
                                Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG)));
        Assertions.assertThat(avroSchema.getField("birthday").schema())
                .isEqualTo(
                        Schema.createUnion(
                                Schema.create(Schema.Type.NULL),
                                LogicalTypes.timestampMicros()
                                        .addToSchema(Schema.create(Schema.Type.LONG))));
        Assertions.assertThat(avroSchema.getField("birthdayMoney").schema())
                .isEqualTo(
                        Schema.createUnion(
                                Schema.create(Schema.Type.NULL),
                                LogicalTypes.decimal(38, 9)
                                        .addToSchema(Schema.create(Schema.Type.BYTES))));
        Assertions.assertThat(avroSchema.getField("lotteryWinnings").schema())
                .isEqualTo(
                        Schema.createUnion(
                                Schema.create(Schema.Type.NULL),
                                LogicalTypes.decimal(77, 38)
                                        .addToSchema(Schema.create(Schema.Type.BYTES))));
        Assertions.assertThat(avroSchema.getField("flighted").schema())
                .isEqualTo(
                        Schema.createUnion(
                                Schema.create(Schema.Type.NULL),
                                Schema.create(Schema.Type.BOOLEAN)));
        Assertions.assertThat(avroSchema.getField("sound").schema())
                .isEqualTo(
                        Schema.createUnion(
                                Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BYTES)));
        Assertions.assertThat(avroSchema.getField("anniversaryDate").schema())
                .isEqualTo(
                        Schema.createUnion(
                                Schema.create(Schema.Type.NULL),
                                Schema.create(Schema.Type.STRING)));
        Assertions.assertThat(avroSchema.getField("anniversaryDatetime").schema())
                .isEqualTo(
                        Schema.createUnion(
                                Schema.create(Schema.Type.NULL),
                                Schema.create(Schema.Type.STRING)));
        Assertions.assertThat(avroSchema.getField("anniversaryTime").schema())
                .isEqualTo(
                        Schema.createUnion(
                                Schema.create(Schema.Type.NULL),
                                Schema.create(Schema.Type.STRING)));
        Schema geoSchema = Schema.create(Schema.Type.STRING);
        geoSchema.addProp(LogicalType.LOGICAL_TYPE_PROP, "geography_wkt");
        Assertions.assertThat(avroSchema.getField("geoPositions").schema())
                .isEqualTo(Schema.createUnion(Schema.create(Schema.Type.NULL), geoSchema));
        Assertions.assertThat(avroSchema.getField("scion").schema())
                .isEqualTo(
                        Schema.createUnion(
                                Schema.create(Schema.Type.NULL),
                                Schema.createRecord(
                                        "scion",
                                        "Translated Avro Schema for scion",
                                        SchemaTransform.NAMESPACE,
                                        false,
                                        ImmutableList.of(
                                                new Schema.Field(
                                                        "species",
                                                        Schema.createUnion(
                                                                Schema.create(Schema.Type.NULL),
                                                                Schema.create(Schema.Type.STRING)),
                                                        null,
                                                        (Object) null)))));
        Assertions.assertThat(avroSchema.getField("associates").schema())
                .isEqualTo(
                        Schema.createArray(
                                Schema.createRecord(
                                        "associates",
                                        "Translated Avro Schema for associates",
                                        SchemaTransform.NAMESPACE,
                                        false,
                                        ImmutableList.of(
                                                new Schema.Field(
                                                        "species",
                                                        Schema.createUnion(
                                                                Schema.create(Schema.Type.NULL),
                                                                Schema.create(Schema.Type.STRING)),
                                                        null,
                                                        (Object) null)))));
    }
}
