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

package com.google.cloud.flink.bigquery.sink.serializer;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.Descriptors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

/** Tests for {@link AvroToProtoSerializer}. */
public class AvroToProtoSerializerTest {

    private final List<TableFieldSchema> subFieldsNullable =
            Arrays.asList(
                    new TableFieldSchema()
                            .setName("species")
                            .setType("STRING")
                            .setMode("REQUIRED"));

    private final List<TableFieldSchema> fields =
            Arrays.asList(
                    new TableFieldSchema().setName("number").setType("INTEGER").setMode("REQUIRED"),
                    new TableFieldSchema().setName("price").setType("FLOAT").setMode("REQUIRED"),
                    new TableFieldSchema().setName("species").setType("STRING").setMode("REQUIRED"),
                    new TableFieldSchema()
                            .setName("flighted")
                            .setType("BOOLEAN")
                            .setMode("REQUIRED"),
                    new TableFieldSchema().setName("sound").setType("BYTES").setMode("REQUIRED"),
                    new TableFieldSchema()
                            .setName("required_record_field")
                            .setType("RECORD")
                            .setMode("REQUIRED")
                            .setFields(subFieldsNullable));
    private final TableSchema tableSchema = new TableSchema().setFields(fields);

    @Test
    public void testSerializeIsUnsupported() {
        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> new AvroToProtoSerializer(tableSchema).serialize(null));
        assertThat(exception).hasMessageThat().contains("serialize method is not supported");
    }

    @Test
    public void testPrimitiveTypesConversion() throws Descriptors.DescriptorValidationException {

        BigQueryProtoSerializer<GenericRecord> avroToProtoSerializer =
                new AvroToProtoSerializer(tableSchema);
        DescriptorProto descriptorProto = avroToProtoSerializer.getDescriptorProto();
        Descriptors.Descriptor descriptor =
                BigQueryProtoSerializer.getDescriptorFromDescriptorProto(descriptorProto);

        assertThat(descriptor.findFieldByNumber(1).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("number")
                                .setNumber(1)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(2).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_DOUBLE)
                                .setName("price")
                                .setNumber(2)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(3).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("species")
                                .setNumber(3)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(4).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BOOL)
                                .setName("flighted")
                                .setNumber(4)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(5).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BYTES)
                                .setName("sound")
                                .setNumber(5)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(6).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setName("required_record_field")
                                .setNumber(6)
                                .setTypeName(
                                        descriptor.findFieldByNumber(6).toProto().getTypeName())
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.getNestedTypes()).hasSize(1);

        assertThat(
                        descriptor
                                .findNestedTypeByName(
                                        descriptor.findFieldByNumber(6).toProto().getTypeName())
                                .findFieldByNumber(1)
                                .toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("species")
                                .setNumber(1)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());
    }

    private Schema getAvroSchemaFromFieldString(String fieldString) {
        String avroSchemaString =
                "{\"namespace\": \"project.dataset\",\n"
                        + " \"type\": \"record\",\n"
                        + " \"name\": \"table\",\n"
                        + " \"doc\": \"Translated Avro Schema for project.dataset.table\",\n"
                        + fieldString
                        + "}";

        return new Schema.Parser().parse(avroSchemaString);
    }

    @Test
    public void testAllPrimitiveSchemaConversion()
            throws Descriptors.DescriptorValidationException {

        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"name\", \"type\": \"string\"},\n"
                        + "   {\"name\": \"number\", \"type\": \"long\"},\n"
                        + "   {\"name\": \"quantity\", \"type\": \"int\"},\n"
                        + "   {\"name\": \"fixed_field\", \"type\": {\"type\": \"fixed\", \"size\": 10,\"name\": \"hash\" }},\n"
                        + "   {\"name\": \"price\", \"type\": \"float\"},\n"
                        + "   {\"name\": \"double_field\", \"type\": \"double\"},\n"
                        + "   {\"name\": \"boolean_field\", \"type\": \"boolean\"},\n"
                        + "   {\"name\": \"enum_field\", \"type\": {\"type\":\"enum\", \"symbols\": [\"A\", \"B\", \"C\", \"D\"], \"name\": \"ALPHABET\"}},\n"
                        + "   {\"name\": \"byte_field\", \"type\": \"bytes\"}\n"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);

        DescriptorProto descriptorProto =
                AvroToProtoSerializer.getDescriptorSchemaFromAvroSchema(avroSchema);

        Descriptors.Descriptor descriptor =
                BigQueryProtoSerializer.getDescriptorFromDescriptorProto(descriptorProto);

        assertThat(descriptor.findFieldByNumber(1).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("name")
                                .setNumber(1)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(2).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("number")
                                .setNumber(2)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(3).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("quantity")
                                .setNumber(3)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(4).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BYTES)
                                .setName("fixed_field")
                                .setNumber(4)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        // TODO: This is different than beam
        assertThat(descriptor.findFieldByNumber(5).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_FLOAT)
                                .setName("price")
                                .setNumber(5)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(6).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_DOUBLE)
                                .setName("double_field")
                                .setNumber(6)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(7).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BOOL)
                                .setName("boolean_field")
                                .setNumber(7)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(8).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("enum_field")
                                .setNumber(8)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(9).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BYTES)
                                .setName("byte_field")
                                .setNumber(9)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());
    }

    @Test
    public void testAllLogicalSchemaConversion() throws Descriptors.DescriptorValidationException {

        String fieldString =
                " \"fields\": [\n"
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
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);

        DescriptorProto descriptorProto =
                AvroToProtoSerializer.getDescriptorSchemaFromAvroSchema(avroSchema);

        Descriptors.Descriptor descriptor =
                BigQueryProtoSerializer.getDescriptorFromDescriptorProto(descriptorProto);

        assertThat(descriptor.findFieldByNumber(1).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("ts_micros")
                                .setNumber(1)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(2).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("ts_millis")
                                .setNumber(2)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(3).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("time_micros")
                                .setNumber(3)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(4).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("time_millis")
                                .setNumber(4)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(5).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("lts_micros")
                                .setNumber(5)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(6).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("lts_millis")
                                .setNumber(6)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(7).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT32)
                                .setName("date")
                                .setNumber(7)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(8).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BYTES)
                                .setName("decimal")
                                .setNumber(8)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(9).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("uuid")
                                .setNumber(9)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());

        assertThat(descriptor.findFieldByNumber(10).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("geography")
                                .setNumber(10)
                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                .build());
    }

    @Test
    public void testAllUnionLogicalSchemaConversion()
            throws Descriptors.DescriptorValidationException {

        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"ts_micros\", \"type\": [\"null\", {\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}]},\n"
                        + "   {\"name\": \"ts_millis\", \"type\": [\"null\",{\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}]},\n"
                        + "   {\"name\": \"time_micros\", \"type\": [\"null\",{\"type\": \"long\", \"logicalType\": \"time-micros\"}]},\n"
                        + "   {\"name\": \"time_millis\", \"type\": [\"null\",{\"type\": \"int\", \"logicalType\": \"time-millis\"}]},\n"
                        + "   {\"name\": \"lts_micros\", \"type\": [\"null\",{\"type\": \"long\", \"logicalType\": \"local-timestamp-micros\"}]},\n"
                        + "   {\"name\": \"lts_millis\", \"type\": [\"null\",{\"type\": \"long\", \"logicalType\": \"local-timestamp-millis\"}]},\n"
                        + "   {\"name\": \"date\", \"type\": [\"null\",{\"type\": \"int\", \"logicalType\": \"date\"}]},\n"
                        + "   {\"name\": \"decimal\", \"type\": [\"null\",{\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 4, \"scale\": 2}]},\n"
                        + "   {\"name\": \"uuid\", \"type\": [\"null\",{\"type\": \"string\", \"logicalType\": \"uuid\"}]},\n"
                        + "   {\"name\": \"geography\", \"type\": [\"null\",{\"type\": \"string\", \"logicalType\": \"geography_wkt\"}]}\n"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);

        DescriptorProto descriptorProto =
                AvroToProtoSerializer.getDescriptorSchemaFromAvroSchema(avroSchema);

        Descriptors.Descriptor descriptor =
                BigQueryProtoSerializer.getDescriptorFromDescriptorProto(descriptorProto);

        assertThat(descriptor.findFieldByNumber(1).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("ts_micros")
                                .setNumber(1)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(2).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("ts_millis")
                                .setNumber(2)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(3).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("time_micros")
                                .setNumber(3)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(4).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("time_millis")
                                .setNumber(4)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(5).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("lts_micros")
                                .setNumber(5)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(6).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("lts_millis")
                                .setNumber(6)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(7).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT32)
                                .setName("date")
                                .setNumber(7)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(8).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BYTES)
                                .setName("decimal")
                                .setNumber(8)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(9).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("uuid")
                                .setNumber(9)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(10).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("geography")
                                .setNumber(10)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());
    }

    @Test
    public void testAllUnionPrimitiveSchemaConversion()
            throws Descriptors.DescriptorValidationException {

        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"name\", \"type\": [\"null\", \"string\"]},\n"
                        + "   {\"name\": \"number\", \"type\": [\"null\",\"long\"]},\n"
                        + "   {\"name\": \"quantity\", \"type\": [\"null\",\"int\"]},\n"
                        + "   {\"name\": \"fixed_field\", \"type\": [\"null\",{\"type\": \"fixed\", \"size\": 10,\"name\": \"hash\"}]},\n"
                        + "   {\"name\": \"price\", \"type\": [\"null\",\"float\"]},\n"
                        + "   {\"name\": \"double_field\", \"type\": [\"null\",\"double\"]},\n"
                        + "   {\"name\": \"boolean_field\", \"type\": [\"null\",\"boolean\"]},\n"
                        + "   {\"name\": \"enum_field\", \"type\": [\"null\",{\"type\":\"enum\", \"symbols\": [\"A\", \"B\", \"C\", \"D\"], \"name\": \"ALPHABET\"}]},\n"
                        + "   {\"name\": \"byte_field\", \"type\": [\"null\",\"bytes\"]}\n"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);

        DescriptorProto descriptorProto =
                AvroToProtoSerializer.getDescriptorSchemaFromAvroSchema(avroSchema);

        Descriptors.Descriptor descriptor =
                BigQueryProtoSerializer.getDescriptorFromDescriptorProto(descriptorProto);

        assertThat(descriptor.findFieldByNumber(1).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("name")
                                .setNumber(1)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(2).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("number")
                                .setNumber(2)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(3).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setName("quantity")
                                .setNumber(3)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(4).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BYTES)
                                .setName("fixed_field")
                                .setNumber(4)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        // TODO: This is different than beam
        assertThat(descriptor.findFieldByNumber(5).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_FLOAT)
                                .setName("price")
                                .setNumber(5)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(6).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_DOUBLE)
                                .setName("double_field")
                                .setNumber(6)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(7).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BOOL)
                                .setName("boolean_field")
                                .setNumber(7)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(8).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setName("enum_field")
                                .setNumber(8)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());

        assertThat(descriptor.findFieldByNumber(9).toProto())
                .isEqualTo(
                        FieldDescriptorProto.newBuilder()
                                .setType(FieldDescriptorProto.Type.TYPE_BYTES)
                                .setName("byte_field")
                                .setNumber(9)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .build());
    }
}
