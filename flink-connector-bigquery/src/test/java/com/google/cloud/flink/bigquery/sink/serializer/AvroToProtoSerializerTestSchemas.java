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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.protobuf.Descriptors.Descriptor;
import org.apache.avro.Schema;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * {@link BigQuerySchemaProvider}s for {@link AvroToProtoSerializerTest} and {@link
 * BigQuerySchemaProviderTest}.
 */
public class AvroToProtoSerializerTestSchemas {

    // Private Constructor to ensure no instantiation.
    private AvroToProtoSerializerTestSchemas() {}

    public static Schema getAvroSchemaFromFieldString(String fieldString) {
        String avroSchemaString =
                "{\"namespace\": \"project.dataset\",\n"
                        + " \"type\": \"record\",\n"
                        + " \"name\": \"table\",\n"
                        + " \"doc\": \"Translated Avro Schema for project.dataset.table\",\n"
                        + fieldString
                        + "}";

        return new Schema.Parser().parse(avroSchemaString);
    }

    public static String getRecordSchema(String name) {
        return "{\"name\": "
                + "\""
                + name
                + "\", "
                + "\"type\": \"record\", "
                + "\"fields\": "
                + "["
                + "{\"name\": \"value\", \"type\": \"long\"},"
                + "{\"name\": \"another_value\",\"type\": \"string\"}"
                + "]"
                + "}";
    }

    public static BigQuerySchemaProvider getSchemaWithPrimitiveTypes(Boolean isNullable) {
        String mode = isNullable ? "NULLABLE" : "REQUIRED";
        List<TableFieldSchema> subFieldsNullable =
                Collections.singletonList(
                        new TableFieldSchema().setName("species").setType("STRING").setMode(mode));
        List<TableFieldSchema> fields =
                Arrays.asList(
                        new TableFieldSchema().setName("number").setType("INTEGER").setMode(mode),
                        new TableFieldSchema().setName("price").setType("FLOAT").setMode(mode),
                        new TableFieldSchema().setName("species").setType("STRING").setMode(mode),
                        new TableFieldSchema().setName("flighted").setType("BOOLEAN").setMode(mode),
                        new TableFieldSchema().setName("sound").setType("BYTES").setMode(mode),
                        new TableFieldSchema()
                                .setName("required_record_field")
                                .setType("RECORD")
                                .setMode(mode)
                                .setFields(subFieldsNullable));
        TableSchema tableSchema = new TableSchema().setFields(fields);
        BigQuerySchemaProvider bigQuerySchemaProvider = new BigQuerySchemaProviderImpl(tableSchema);
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        return new TestSchemaProvider(bigQuerySchemaProvider.getAvroSchema(), descriptor);
    }

    public static BigQuerySchemaProvider getSchemaWithRequiredPrimitiveTypes() {
        return getSchemaWithPrimitiveTypes(false);
    }

    public static BigQuerySchemaProvider getSchemaWithNullablePrimitiveTypes() {
        return getSchemaWithPrimitiveTypes(true);
    }

    public static BigQuerySchemaProvider getSchemaWithRemainingPrimitiveTypes() {
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"quantity\", \"type\": \"int\"},\n"
                        + "   {\"name\": \"fixed_field\", \"type\": {\"type\": "
                        + "\"fixed\", \"size\": 10,\"name\": \"hash\" }},\n"
                        + "   {\"name\": \"float_field\", \"type\": \"float\"},\n"
                        + "   {\"name\": \"enum_field\", \"type\": {\"type\":\"enum\","
                        + " \"symbols\": [\"A\", \"B\", \"C\", \"D\"], \"name\": \"ALPHABET\"}}\n"
                        + " ]\n";
        return getSchemaAndDescriptor(fieldString);
    }

    public static BigQuerySchemaProvider getSchemaWithUnionOfRemainingPrimitiveTypes() {
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"quantity\", \"type\": [\"null\", \"int\"]},\n"
                        + "   {\"name\": \"fixed_field\", \"type\": [\"null\", {\"type\": "
                        + "\"fixed\", \"size\": 10,\"name\": \"hash\"}]},\n"
                        + "   {\"name\": \"float_field\", \"type\": [\"null\", \"float\"]},\n"
                        + "   {\"name\": \"enum_field\", \"type\": [\"null\", {\"type\":\"enum\","
                        + " \"symbols\": [\"A\", \"B\", \"C\", \"D\"], \"name\": \"ALPHABET\"}]}\n"
                        + " ]\n";
        return getSchemaAndDescriptor(fieldString);
    }

    public static BigQuerySchemaProvider getSchemaWithLogicalTypes(Boolean isNullable) {
        String mode = isNullable ? "NULLABLE" : "REQUIRED";
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
                                .setName("bignumeric_field")
                                .setType("BIGNUMERIC")
                                .setMode(mode),
                        new TableFieldSchema()
                                .setName("geography")
                                .setType("GEOGRAPHY")
                                .setMode(mode),
                        new TableFieldSchema().setName("Json").setType("JSON").setMode(mode));

        TableSchema tableSchema = new TableSchema().setFields(fields);
        BigQuerySchemaProvider bigQuerySchemaProvider = new BigQuerySchemaProviderImpl(tableSchema);
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        return new TestSchemaProvider(bigQuerySchemaProvider.getAvroSchema(), descriptor);
    }

    public static BigQuerySchemaProvider getSchemaWithRequiredLogicalTypes() {
        return getSchemaWithLogicalTypes(false);
    }

    public static BigQuerySchemaProvider getSchemaWithNullableLogicalTypes() {
        return getSchemaWithLogicalTypes(true);
    }

    public static BigQuerySchemaProvider getSchemaWithRemainingLogicalTypes() {

        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"ts_millis\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}},\n"
                        + "   {\"name\": \"time_millis\", \"type\": {\"type\": \"int\", \"logicalType\": \"time-millis\"}},\n"
                        + "   {\"name\": \"lts_millis\", \"type\": {\"type\": \"long\", \"logicalType\": \"local-timestamp-millis\"}},\n"
                        + "   {\"name\": \"uuid\", \"type\": {\"type\": \"string\", \"logicalType\": \"uuid\"}}\n"
                        + " ]\n";
        return getSchemaAndDescriptor(fieldString);
    }

    public static BigQuerySchemaProvider getSchemaWithUnionOfLogicalTypes() {
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"ts_millis\", \"type\": [\"null\",{\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}]},\n"
                        + "   {\"name\": \"time_millis\", \"type\": [\"null\",{\"type\": \"int\", \"logicalType\": \"time-millis\"}]},\n"
                        + "   {\"name\": \"lts_millis\", \"type\": [\"null\",{\"type\": \"long\", \"logicalType\": \"local-timestamp-millis\"}]},\n"
                        + "   {\"name\": \"uuid\", \"type\": [\"null\",{\"type\": \"string\", \"logicalType\": \"uuid\"}]}\n"
                        + " ]\n";
        return getSchemaAndDescriptor(fieldString);
    }

    public static BigQuerySchemaProvider getSchemaWithRecordOfUnionTypes() {
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"record_with_union\", \"type\": "
                        + "{\"name\": \"record_with_union_field\","
                        + " \"type\": \"record\", \"fields\":"
                        + " [{\"name\": \"union_in_record\", \"type\": "
                        + "[\"boolean\", \"null\"], \"default\": true}]}}\n"
                        + " ]\n";
        return getSchemaAndDescriptor(fieldString);
    }

    public static BigQuerySchemaProvider getSchemaWithRecordOfLogicalTypes() {
        String fieldString =
                " \"fields\": [\n"
                        + "{\"name\": \"record_of_logical_type\","
                        + " \"type\": "
                        + "{"
                        + "\"name\": \"record_name\", "
                        + "\"type\": \"record\","
                        + " \"fields\": "
                        + "["
                        + "   {\"name\": \"ts_micros\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}},\n"
                        + "   {\"name\": \"ts_millis\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}},\n"
                        + "   {\"name\": \"time_micros\", \"type\": {\"type\": \"long\", \"logicalType\": \"time-micros\"}},\n"
                        + "   {\"name\": \"time_millis\", \"type\": {\"type\": \"int\", \"logicalType\": \"time-millis\"}},\n"
                        + "   {\"name\": \"lts_micros\", \"type\": {\"type\": \"long\", \"logicalType\": \"local-timestamp-micros\"}},\n"
                        + "   {\"name\": \"lts_millis\", \"type\": {\"type\": \"long\", \"logicalType\": \"local-timestamp-millis\"}},\n"
                        + "   {\"name\": \"date\", \"type\": {\"type\": \"int\", \"logicalType\": \"date\"}},\n"
                        + "   {\"name\": \"decimal\", \"type\": {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 4, \"scale\": 2}},\n"
                        + "   {\"name\": \"uuid\", \"type\": {\"type\": \"string\", \"logicalType\": \"uuid\"}},\n"
                        + "   {\"name\": \"geography\", \"type\": {\"type\": \"string\", \"logicalType\": \"geography_wkt\"}}\n"
                        + "]"
                        + "}"
                        + "}\n"
                        + " ]\n";
        return getSchemaAndDescriptor(fieldString);
    }

    public static BigQuerySchemaProvider getSchemaWithDefaultValue() {
        String fieldString =
                " \"fields\": [\n"
                        + "{\"name\": \"long_with_default\", \"type\": [\"long\", \"null\"],"
                        + " \"default\": 100}"
                        + " ]\n";
        return getSchemaAndDescriptor(fieldString);
    }

    private static BigQuerySchemaProvider getSchemaAndDescriptor(String fieldString) {
        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        BigQuerySchemaProvider bigQuerySchemaProvider = new BigQuerySchemaProviderImpl(avroSchema);
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        return new TestSchemaProvider(avroSchema, descriptor);
    }
}
