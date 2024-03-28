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
public class TestBigQuerySchemas {

    // Private Constructor to ensure no instantiation.
    private TestBigQuerySchemas() {}

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

    public static Schema getAvroSchemaFromFieldString(String fieldString, String namespace) {
        String avroSchemaString =
                "{\"namespace\": \""
                        + namespace
                        + "\", \n"
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

    public static BigQuerySchemaProvider getSchemaWithPrimitiveTypes(String mode) {
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
        return getSchemaWithPrimitiveTypes("REQUIRED");
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

    public static BigQuerySchemaProvider getSchemaWithNullablePrimitiveTypes() {
        return getSchemaWithPrimitiveTypes("NULLABLE");
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

    public static BigQuerySchemaProvider getSchemaWithLogicalTypes(String mode) {
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
        return getSchemaWithLogicalTypes("REQUIRED");
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

    public static BigQuerySchemaProvider getSchemaWithNullableLogicalTypes() {
        return getSchemaWithLogicalTypes("NULLABLE");
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

    // ------------Test Schemas with Record of Different Types -----------
    public static BigQuerySchemaProvider getSchemaWithRecordOfArray() {
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"record_with_array\", \"type\": {\"name\": \"record_with_array_field\", \"type\": \"record\", \"fields\": [{\"name\": \"array_in_record\", \"type\": {\"type\": \"array\", \"items\": \"boolean\"}}]}}\n"
                        + " ]\n";
        return getSchemaAndDescriptor(fieldString);
    }

    public static BigQuerySchemaProvider getSchemaWithRecordOfUnionType() {
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"record_with_union\", \"type\": {\"name\": \"record_with_union_field\", \"type\": \"record\", \"fields\": [{\"name\": \"union_in_record\", \"type\": [\"boolean\", \"null\"], \"default\": true}]}}\n"
                        + " ]\n";
        return getSchemaAndDescriptor(fieldString);
    }

    public static String getSchemaWithRecordOfMap() {
        return " \"fields\": [\n"
                + "   {\"name\": \"record_with_map\", "
                + "\"type\": {\"name\": \"actual_record\", \"type\": \"record\","
                + " \"fields\": [{\"name\": \"map_in_record\", \"type\": "
                + "{ \"type\": \"map\", \"values\": \"long\"}}]}}\n"
                + " ]\n";
    }

    public static BigQuerySchemaProvider getSchemaWithRecordOfRecord() {
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"record_in_record\", \"type\": {\"name\": \"record_name\","
                        + " \"type\": \"record\", \"fields\": "
                        + "[{ \"name\":\"record_field\", \"type\": "
                        + getRecordSchema("record_inside_record")
                        + "}]"
                        + "}}\n"
                        + " ]\n";
        return getSchemaAndDescriptor(fieldString);
    }

    public static BigQuerySchemaProvider getSchemaWithRecordOfPrimitiveTypes() {
        String fieldString =
                " \"fields\": [\n" + "{\"name\": \"record_of_primitive_types\"," + " \"type\": ";
        fieldString += getSchemaWithRequiredPrimitiveTypes().getAvroSchema().toString();
        fieldString += "}]\n";
        return getSchemaAndDescriptor(fieldString);
    }

    public static BigQuerySchemaProvider getSchemaWithRecordOfRemainingPrimitiveTypes() {
        String fieldString =
                " \"fields\": [\n"
                        + "{\"name\": \"record_of_remaining_primitive_types\","
                        + " \"type\": ";
        fieldString += getSchemaWithRemainingPrimitiveTypes().getAvroSchema().toString();
        fieldString += "}]\n";
        return getSchemaAndDescriptor(fieldString, "inner");
    }

    public static BigQuerySchemaProvider getSchemaWithRecordOfLogicalTypes() {
        String fieldString =
                " \"fields\": [\n" + "{\"name\": \"record_of_logical_types\"," + " \"type\": ";
        fieldString += getSchemaWithRequiredLogicalTypes().getAvroSchema().toString();
        fieldString += "}]\n";
        return getSchemaAndDescriptor(fieldString);
    }

    public static BigQuerySchemaProvider getSchemaWithRecordOfRemainingLogicalTypes() {
        String fieldString =
                " \"fields\": [\n"
                        + "{\"name\": \"record_of_remaining_logical_types\","
                        + " \"type\": ";
        fieldString += getSchemaWithRemainingLogicalTypes().getAvroSchema().toString();
        fieldString += "}]\n";
        return getSchemaAndDescriptor(fieldString, "inner");
    }

    // ------------Test Schemas with MAP of Different Types --------------
    public static String getSchemaWithMapOfArray() {
        return " \"fields\": [\n"
                + "   {\"name\": \"map_of_array\", \"type\": {\"type\": \"map\","
                + " \"values\": {\"type\": \"array\", \"items\": \"long\","
                + " \"name\": \"array_in_map\"}}}\n"
                + " ]\n";
    }

    public static String getSchemaWithMapOfUnionType() {
        return " \"fields\": [\n"
                + "   {\"name\": \"map_of_union\", \"type\": {\"type\": \"map\","
                + " \"values\": [\"float\", \"null\"]}}\n"
                + " ]\n";
    }

    public static String getSchemaWithMapOfMap() {
        return " \"fields\": [\n"
                + "   {\"name\": \"map_of_map\", \"type\": {\"type\": \"map\", "
                + "\"values\": {\"type\": \"map\", \"values\": \"bytes\"}}}\n"
                + " ]\n";
    }

    public static String getSchemaWithMapOfRecord() {
        return " \"fields\": [\n"
                + "   {\"name\": \"map_of_records\", \"type\": "
                + "{\"type\": \"map\", \"values\": "
                + getRecordSchema("record_inside_map")
                + "}}\n"
                + " ]\n";
    }

    public static String getSchemaWithMapType() {
        return " \"fields\": [\n"
                + "   {\"name\": \"map_field\", \"type\": "
                + "{\"type\": \"map\", \"values\": \"long\"}}\n"
                + " ]\n";
    }

    // ------------Test Schemas with ARRAY of Different Types -------------

    public static String getSchemaWithArrayOfArray() {
        return " \"fields\": [\n"
                + "{\"name\": \"nested_arrays\", \"type\":{\"type\": \"array\", \"items\": "
                + "{\"name\": \"array_inside\", \"type\": \"array\", \"items\": \"long\"}"
                + "}}"
                + " ]\n";
    }

    public static String getSchemaWithArrayOfUnionValue() {
        return " \"fields\": [\n"
                + "{\"name\": \"array_with_union\", \"type\": "
                + "{\"type\": \"array\", \"items\":  [\"long\", \"null\"]}}"
                + " ]\n";
    }

    public static String getSchemaWithArrayOfMap() {
        return " \"fields\": [\n"
                + "   {\"name\": \"array_of_map\", \"type\": {\"type\": \"array\", \"items\": {\"type\": \"map\", \"values\": \"bytes\"}}}\n"
                + " ]\n";
    }

    public static String getSchemaWithArrayOfUnionOfMap() {
        return " \"fields\": [\n"
                + "   {\"name\": \"array_of_map_union\", \"type\": [\"null\", {\"type\": \"array\", \"items\": {\"type\": \"map\", \"values\": \"bytes\"}}]}\n"
                + " ]\n";
    }

    public static BigQuerySchemaProvider getSchemaWithArrayOfRecord() {
        String fieldString =
                " \"fields\": [\n"
                        + "{\"name\": \"array_of_records\", \"type\":{\"type\": \"array\", \"items\": "
                        + getRecordSchema("inside_record")
                        + "}}"
                        + " ]\n";

        return getSchemaAndDescriptor(fieldString);
    }

    public static BigQuerySchemaProvider getSchemaWithArraysOfPrimitiveTypes() {
        return getSchemaWithPrimitiveTypes("REPEATED");
    }

    public static BigQuerySchemaProvider getSchemaWithArraysOfLogicalTypes() {
        return getSchemaWithLogicalTypes("REPEATED");
    }

    public static BigQuerySchemaProvider getSchemaWithArraysOfRemainingPrimitiveTypes() {
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"quantity\", \"type\": {\"type\": \"array\", \"items\": \"int\"}},\n"
                        + "   {\"name\": \"fixed_field\", \"type\": {\"type\": \"array\", \"items\": {\"type\": "
                        + "\"fixed\", \"size\": 10,\"name\": \"hash\"}}},\n"
                        + "   {\"name\": \"float_field\", \"type\": {\"type\": \"array\", \"items\": \"float\"}},\n"
                        + "   {\"name\": \"enum_field\", \"type\": {\"type\": \"array\", \"items\": {\"type\":\"enum\","
                        + " \"symbols\": [\"A\", \"B\", \"C\", \"D\"], \"name\": \"ALPHABET\"}}}\n"
                        + " ]\n";
        return getSchemaAndDescriptor(fieldString);
    }

    public static BigQuerySchemaProvider getSchemaWithArraysOfRemainingLogicalTypes() {
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"ts_millis\",\"type\": {\"type\": \"array\", \"items\": {\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}}},\n"
                        + "   {\"name\": \"time_millis\", \"type\": {\"type\": \"array\", \"items\": {\"type\": \"int\", \"logicalType\": \"time-millis\"}}},\n"
                        + "   {\"name\": \"lts_millis\", \"type\": {\"type\": \"array\", \"items\": {\"type\": \"long\", \"logicalType\": \"local-timestamp-millis\"}}},\n"
                        + "   {\"name\": \"uuid\", \"type\": {\"type\": \"array\", \"items\": {\"type\": \"string\", \"logicalType\": \"uuid\"}}}\n"
                        + " ]\n";
        return getSchemaAndDescriptor(fieldString);
    }

    // ------------Test Schemas with UNION of Different Types (Excluding Primitive and Logical)
    public static String getSchemaWithUnionOfArray() {
        return " \"fields\": [\n"
                + "   {\"name\": \"array_field_union\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"float\"}]}\n"
                + " ]\n";
    }

    public static String getSchemaWithUnionOfArrayOfRecord() {
        return " \"fields\": [\n"
                + "   {\"name\": \"array_of_records_union\", \"type\": [\"null\", {\"type\": \"array\", \"items\": "
                + getRecordSchema("inside_record_union")
                + "}]}\n"
                + " ]\n";
    }

    public static String getSchemaWithUnionOfMap() {
        return " \"fields\": [\n"
                + "   {\"name\": \"map_field_union\", \"type\": [\"null\", {\"type\": \"map\", \"values\": \"long\"}]}\n"
                + " ]\n";
    }

    public static BigQuerySchemaProvider getSchemaWithUnionOfRecord() {
        String fieldString =
                " \"fields\": [\n"
                        + "{\"name\": \"record_field_union\","
                        + " \"type\": [\"null\", "
                        + getRecordSchema("inside_record")
                        + "]}\n"
                        + " ]\n";
        return getSchemaAndDescriptor(fieldString);
    }

    public static BigQuerySchemaProvider getSchemaWithAllPrimitiveSingleUnion() {
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"number\", \"type\": [\"long\"]},\n"
                        + "   {\"name\": \"price\", \"type\": [\"double\"]},\n"
                        + "   {\"name\": \"species\", \"type\": [\"string\"]},\n"
                        + "   {\"name\": \"flighted\", \"type\": [\"boolean\"]},\n"
                        + "   {\"name\": \"sound\", \"type\": [\"bytes\"]},\n"
                        + "   {\"name\": \"required_record_field\", \"type\": [{\"name\": \"record_with_string\", \"type\":\"record\","
                        + " \"fields\": [{\"name\": \"species\", \"type\": \"string\"}]}]}\n"
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

    private static BigQuerySchemaProvider getSchemaAndDescriptorHelper(Schema avroSchema) {
        BigQuerySchemaProvider bigQuerySchemaProvider = new BigQuerySchemaProviderImpl(avroSchema);
        Descriptor descriptor = bigQuerySchemaProvider.getDescriptor();
        return new TestSchemaProvider(avroSchema, descriptor);
    }

    private static BigQuerySchemaProvider getSchemaAndDescriptor(String fieldString) {
        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        return getSchemaAndDescriptorHelper(avroSchema);
    }

    private static BigQuerySchemaProvider getSchemaAndDescriptor(
            String fieldString, String namespace) {
        // When we need to reuse the schema but cannot let them have the same namespace.
        Schema avroSchema = getAvroSchemaFromFieldString(fieldString, namespace);
        return getSchemaAndDescriptorHelper(avroSchema);
    }
}
