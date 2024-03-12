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
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

/** Tests for {@link AvroToProtoSerializer}. */
public class AvroToProtoSerializerTest {

    private final List<TableFieldSchema> subFieldsNullable =
            Collections.singletonList(
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
    public void testPrimitiveTypesConversion() throws DescriptorValidationException {

        BigQueryProtoSerializer<GenericRecord> avroToProtoSerializer =
                new AvroToProtoSerializer(tableSchema);
        Descriptor descriptor = avroToProtoSerializer.getDescriptor();

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
    public void testAllPrimitiveSchemaConversion() throws DescriptorValidationException {

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
        BigQueryProtoSerializer<GenericRecord> avroToProtoSerializer =
                new AvroToProtoSerializer(avroSchema);
        Descriptor descriptor = avroToProtoSerializer.getDescriptor();

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
    public void testAllLogicalSchemaConversion() throws DescriptorValidationException {

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
        BigQueryProtoSerializer<GenericRecord> avroToProtoSerializer =
                new AvroToProtoSerializer(avroSchema);
        Descriptor descriptor = avroToProtoSerializer.getDescriptor();

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
    public void testAllUnionLogicalSchemaConversion() throws DescriptorValidationException {

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
        BigQueryProtoSerializer<GenericRecord> avroToProtoSerializer =
                new AvroToProtoSerializer(avroSchema);
        Descriptor descriptor = avroToProtoSerializer.getDescriptor();

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
    public void testAllUnionPrimitiveSchemaConversion() throws DescriptorValidationException {

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
        BigQueryProtoSerializer<GenericRecord> avroToProtoSerializer =
                new AvroToProtoSerializer(avroSchema);
        Descriptor descriptor = avroToProtoSerializer.getDescriptor();

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

    @Test
    public void testUnionInRecordSchemaConversation() throws DescriptorValidationException {

        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"record_with_union\", \"type\": {\"name\": \"record_with_union_field\", \"type\": \"record\", \"fields\": [{\"name\": \"union_in_record\", \"type\": [\"boolean\", \"null\"], \"default\": true}]}}\n"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        BigQueryProtoSerializer<GenericRecord> avroToProtoSerializer =
                new AvroToProtoSerializer(avroSchema);
        Descriptor descriptor = avroToProtoSerializer.getDescriptor();

        FieldDescriptorProto fieldDescriptorProto = descriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptorProto.getName()).isEqualTo("record_with_union");
        assertThat(fieldDescriptorProto.getNumber()).isEqualTo(1);
        assertThat(fieldDescriptorProto.getLabel())
                .isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        assertThat(fieldDescriptorProto.getType())
                .isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(fieldDescriptorProto.hasTypeName()).isTrue();

        Descriptor nestedDescriptor =
                descriptor.findNestedTypeByName(fieldDescriptorProto.getTypeName());
        FieldDescriptor fieldDescriptor = nestedDescriptor.findFieldByNumber(1);
        assertThat(fieldDescriptor.isOptional()).isTrue();
        assertThat(fieldDescriptor.getType()).isEqualTo(FieldDescriptor.Type.BOOL);
        assertThat(fieldDescriptor.getName()).isEqualTo("union_in_record");
        assertThat(fieldDescriptor.hasDefaultValue()).isTrue();
        assertThat(fieldDescriptor.getDefaultValue()).isEqualTo(true);
    }

    @Test
    public void testMapOfUnionSpecialSchemaConversion() {
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"map_field_union\", \"type\": [\"null\", {\"type\": \"map\", \"values\": \"long\"}]}\n"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);

        assertThrows(
                UnsupportedOperationException.class,
                () -> AvroToProtoSerializer.getDescriptorSchemaFromAvroSchema(avroSchema));
    }

    @Test
    public void testMapOfArraySpecialSchemaConversion() {
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"array_field_union\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"float\"}]}\n"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);

        assertThrows(
                UnsupportedOperationException.class,
                () -> AvroToProtoSerializer.getDescriptorSchemaFromAvroSchema(avroSchema));
    }

    @Test
    public void testAllUnionSpecialSchemaConversion() throws DescriptorValidationException {

        String fieldString =
                " \"fields\": [\n"
                        + "{\"name\": \"record_field_union\","
                        + " \"type\": [\"null\", "
                        + "{\"name\": \"inside_record\", "
                        + "\"type\": \"record\", "
                        + "\"fields\": "
                        + "["
                        + "{\"name\": \"value\", \"type\": \"long\"},"
                        + "{\"name\": \"another_value\",\"type\": \"string\"}"
                        + "]"
                        + "}]},\n"
                        + "   {\"name\": \"record_field\", \"type\": {\"name\": \"inside_record_2\", \"type\": \"record\", \"fields\": [{\"name\": \"value\", \"type\": \"long\"},{\"name\": \"another_value\", \"type\": \"string\"}]}},\n"
                        + "   {\"name\": \"map_field\", \"type\": {\"type\": \"map\", \"values\": \"long\"}},\n"
                        + "   {\"name\": \"array_field\", \"type\": {\"type\": \"array\", \"items\": \"float\"}}\n"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        BigQueryProtoSerializer<GenericRecord> avroToProtoSerializer =
                new AvroToProtoSerializer(avroSchema);
        Descriptor descriptor = avroToProtoSerializer.getDescriptor();

        FieldDescriptorProto field = descriptor.findFieldByNumber(1).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("record_field_union");
        assertThat(field.getNumber()).isEqualTo(1);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_OPTIONAL);
        assertThat(field.hasTypeName()).isTrue();
        assertThat(descriptor.findNestedTypeByName(field.getTypeName()).toProto())
                .isEqualTo(
                        DescriptorProto.newBuilder()
                                .setName(field.getTypeName())
                                .addField(
                                        FieldDescriptorProto.newBuilder()
                                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                                .setName("value")
                                                .setNumber(1)
                                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                                .build())
                                .addField(
                                        FieldDescriptorProto.newBuilder()
                                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                                .setName("another_value")
                                                .setNumber(2)
                                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                                .build())
                                .build());

        field = descriptor.findFieldByNumber(2).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("record_field");
        assertThat(field.getNumber()).isEqualTo(2);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        assertThat(field.hasTypeName()).isTrue();
        assertThat(descriptor.findNestedTypeByName(field.getTypeName()).toProto())
                .isEqualTo(
                        DescriptorProto.newBuilder()
                                .setName(field.getTypeName())
                                .addField(
                                        FieldDescriptorProto.newBuilder()
                                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                                .setName("value")
                                                .setNumber(1)
                                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                                .build())
                                .addField(
                                        FieldDescriptorProto.newBuilder()
                                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                                .setName("another_value")
                                                .setNumber(2)
                                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                                .build())
                                .build());

        field = descriptor.findFieldByNumber(3).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("map_field");
        assertThat(field.getNumber()).isEqualTo(3);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REPEATED);
        assertThat(field.hasTypeName()).isTrue();
        assertThat(descriptor.findNestedTypeByName(field.getTypeName()).toProto())
                .isEqualTo(
                        DescriptorProto.newBuilder()
                                .setName(field.getTypeName())
                                .addField(
                                        FieldDescriptorProto.newBuilder()
                                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                                .setName("key")
                                                .setNumber(1)
                                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                                .build())
                                .addField(
                                        FieldDescriptorProto.newBuilder()
                                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                                .setName("value")
                                                .setNumber(2)
                                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                                .build())
                                .build());

        field = descriptor.findFieldByNumber(4).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_FLOAT);
        assertThat(field.getName()).isEqualTo("array_field");
        assertThat(field.getNumber()).isEqualTo(4);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REPEATED);
    }

    private String getRecord(String name) {
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

    @Test
    public void testUnionArrayOfRecordSchemaConversion() {

        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"array_of_records_union\", \"type\": [\"null\", {\"type\": \"array\", \"items\": "
                        + getRecord("inside_record_union")
                        + "}]}\n"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);

        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> AvroToProtoSerializer.getDescriptorSchemaFromAvroSchema(avroSchema));
        assertThat(exception)
                .hasMessageThat()
                .contains("MAP/ARRAYS in UNION types are not supported");
    }

    @Test
    public void testArrayOfRecordSchemaConversion() throws DescriptorValidationException {

        String fieldString =
                " \"fields\": [\n"
                        + "{\"name\": \"array_of_records\", \"type\":{\"type\": \"array\", \"items\": "
                        + getRecord("inside_record")
                        + "}}"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        BigQueryProtoSerializer<GenericRecord> avroToProtoSerializer =
                new AvroToProtoSerializer(avroSchema);
        Descriptor descriptor = avroToProtoSerializer.getDescriptor();

        FieldDescriptorProto field = descriptor.findFieldByNumber(1).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("array_of_records");
        assertThat(field.getNumber()).isEqualTo(1);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REPEATED);
        assertThat(field.hasTypeName()).isTrue();
        assertThat(descriptor.findNestedTypeByName(field.getTypeName()).toProto())
                .isEqualTo(
                        DescriptorProto.newBuilder()
                                .setName(field.getTypeName())
                                .addField(
                                        FieldDescriptorProto.newBuilder()
                                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                                .setName("value")
                                                .setNumber(1)
                                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                                .build())
                                .addField(
                                        FieldDescriptorProto.newBuilder()
                                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                                .setName("another_value")
                                                .setNumber(2)
                                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                                .build())
                                .build());
    }

    @Test
    public void testArrayOfMapSchemaConversion() {
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"array_of_map\", \"type\": {\"type\": \"array\", \"items\": {\"type\": \"map\", \"values\": \"bytes\"}}}\n"
                        + " ]\n";
        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> AvroToProtoSerializer.getDescriptorSchemaFromAvroSchema(avroSchema));
        assertThat(exception).hasMessageThat().contains("Array of Type MAP not supported yet.");
    }

    @Test
    public void testArrayOfUnionMapSchemaConversion() {

        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"array_of_map_union\", \"type\": [\"null\", {\"type\": \"array\", \"items\": {\"type\": \"map\", \"values\": \"bytes\"}}]}\n"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> AvroToProtoSerializer.getDescriptorSchemaFromAvroSchema(avroSchema));
        assertThat(exception)
                .hasMessageThat()
                .contains("MAP/ARRAYS in UNION types are not supported");
    }

    @Test
    public void testArrayInRecordSchemaConversion() throws DescriptorValidationException {

        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"record_with_array\", \"type\": {\"name\": \"record_with_array_field\", \"type\": \"record\", \"fields\": [{\"name\": \"array_in_record\", \"type\": {\"type\": \"array\", \"items\": \"boolean\"}}]}}\n"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        BigQueryProtoSerializer<GenericRecord> avroToProtoSerializer =
                new AvroToProtoSerializer(avroSchema);
        Descriptor descriptor = avroToProtoSerializer.getDescriptor();

        FieldDescriptorProto field = descriptor.findFieldByNumber(1).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("record_with_array");
        assertThat(field.getNumber()).isEqualTo(1);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        assertThat(field.hasTypeName()).isTrue();
        assertThat(descriptor.findNestedTypeByName(field.getTypeName()).toProto())
                .isEqualTo(
                        DescriptorProto.newBuilder()
                                .setName(field.getTypeName())
                                .addField(
                                        FieldDescriptorProto.newBuilder()
                                                .setType(FieldDescriptorProto.Type.TYPE_BOOL)
                                                .setName("array_in_record")
                                                .setNumber(1)
                                                .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
                                                .build())
                                .build());
    }

    @Test
    public void testMapOfRecordSchemaConversion() throws DescriptorValidationException {

        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"map_of_records\", \"type\": {\"type\": \"map\", \"values\": "
                        + getRecord("record_inside_map")
                        + "}}\n"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        BigQueryProtoSerializer<GenericRecord> avroToProtoSerializer =
                new AvroToProtoSerializer(avroSchema);
        Descriptor descriptor = avroToProtoSerializer.getDescriptor();

        FieldDescriptorProto field = descriptor.findFieldByNumber(1).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("map_of_records");
        assertThat(field.getNumber()).isEqualTo(1);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REPEATED);
        assertThat(field.hasTypeName()).isTrue();
        Descriptors.Descriptor nestedDescriptor =
                descriptor.findNestedTypeByName(field.getTypeName());
        FieldDescriptorProto fieldDescriptor = nestedDescriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptor.getName()).isEqualTo("key");
        assertThat(fieldDescriptor.getNumber()).isEqualTo(1);
        assertThat(fieldDescriptor.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_STRING);
        assertThat(fieldDescriptor.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        fieldDescriptor = nestedDescriptor.findFieldByNumber(2).toProto();
        assertThat(fieldDescriptor.getName()).isEqualTo("value");
        assertThat(fieldDescriptor.getNumber()).isEqualTo(2);
        assertThat(fieldDescriptor.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(fieldDescriptor.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        assertThat(fieldDescriptor.hasTypeName()).isTrue();
        assertThat(nestedDescriptor.findNestedTypeByName(fieldDescriptor.getTypeName()).toProto())
                .isEqualTo(
                        DescriptorProto.newBuilder()
                                .setName(fieldDescriptor.getTypeName())
                                .addField(
                                        FieldDescriptorProto.newBuilder()
                                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                                .setName("value")
                                                .setNumber(1)
                                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                                .build())
                                .addField(
                                        FieldDescriptorProto.newBuilder()
                                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                                .setName("another_value")
                                                .setNumber(2)
                                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                                .build())
                                .build());
    }

    @Test
    public void testMapOfMapSchemaConversion() throws DescriptorValidationException {

        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"map_of_map\", \"type\": {\"type\": \"map\", \"values\": {\"type\": \"map\", \"values\": \"bytes\"}}}\n"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        BigQueryProtoSerializer<GenericRecord> avroToProtoSerializer =
                new AvroToProtoSerializer(avroSchema);
        Descriptor descriptor = avroToProtoSerializer.getDescriptor();

        FieldDescriptorProto field = descriptor.findFieldByNumber(1).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("map_of_map");
        assertThat(field.getNumber()).isEqualTo(1);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REPEATED);
        assertThat(field.hasTypeName()).isTrue();
        Descriptors.Descriptor nestedDescriptor =
                descriptor.findNestedTypeByName(field.getTypeName());
        FieldDescriptorProto fieldDescriptor = nestedDescriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptor.getName()).isEqualTo("key");
        assertThat(fieldDescriptor.getNumber()).isEqualTo(1);
        assertThat(fieldDescriptor.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_STRING);
        assertThat(fieldDescriptor.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        fieldDescriptor = nestedDescriptor.findFieldByNumber(2).toProto();
        assertThat(fieldDescriptor.getName()).isEqualTo("value");
        assertThat(fieldDescriptor.getNumber()).isEqualTo(2);
        assertThat(fieldDescriptor.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(fieldDescriptor.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REPEATED);
        assertThat(fieldDescriptor.hasTypeName()).isTrue();
        assertThat(nestedDescriptor.findNestedTypeByName(fieldDescriptor.getTypeName()).toProto())
                .isEqualTo(
                        DescriptorProto.newBuilder()
                                .setName(fieldDescriptor.getTypeName())
                                .addField(
                                        FieldDescriptorProto.newBuilder()
                                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                                .setName("key")
                                                .setNumber(1)
                                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                                .build())
                                .addField(
                                        FieldDescriptorProto.newBuilder()
                                                .setType(FieldDescriptorProto.Type.TYPE_BYTES)
                                                .setName("value")
                                                .setNumber(2)
                                                .setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
                                                .build())
                                .build());
    }

    @Test
    public void testRecordInRecordSchemaConversion() throws DescriptorValidationException {

        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"record_in_record\", \"type\": {\"name\": \"record_name\", \"type\": \"record\", \"fields\": "
                        + "[{ \"name\":\"record_field\", \"type\": "
                        + getRecord("record_inside_record")
                        + "}]"
                        + "}}\n"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        BigQueryProtoSerializer<GenericRecord> avroToProtoSerializer =
                new AvroToProtoSerializer(avroSchema);
        Descriptor descriptor = avroToProtoSerializer.getDescriptor();

        FieldDescriptorProto field = descriptor.findFieldByNumber(1).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("record_in_record");
        assertThat(field.getNumber()).isEqualTo(1);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        assertThat(field.hasTypeName()).isTrue();
        Descriptors.Descriptor nestedDescriptor =
                descriptor.findNestedTypeByName(field.getTypeName());

        field = nestedDescriptor.findFieldByNumber(1).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("record_field");
        assertThat(field.getNumber()).isEqualTo(1);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        assertThat(field.hasTypeName()).isTrue();

        nestedDescriptor = nestedDescriptor.findNestedTypeByName(field.getTypeName());
        field = nestedDescriptor.findFieldByNumber(1).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_INT64);
        assertThat(field.getName()).isEqualTo("value");
        assertThat(field.getNumber()).isEqualTo(1);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);

        field = nestedDescriptor.findFieldByNumber(2).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_STRING);
        assertThat(field.getName()).isEqualTo("another_value");
        assertThat(field.getNumber()).isEqualTo(2);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
    }

    @Test
    public void testRecordOfLogicalTypeSchemaConversion() throws DescriptorValidationException {

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

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        BigQueryProtoSerializer<GenericRecord> avroToProtoSerializer =
                new AvroToProtoSerializer(avroSchema);
        Descriptor descriptor = avroToProtoSerializer.getDescriptor();

        FieldDescriptorProto fieldDescriptorProto = descriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptorProto.getName()).isEqualTo("record_of_logical_type");
        assertThat(fieldDescriptorProto.getNumber()).isEqualTo(1);
        assertThat(fieldDescriptorProto.getLabel())
                .isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        assertThat(fieldDescriptorProto.getType())
                .isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(fieldDescriptorProto.hasTypeName()).isTrue();
        descriptor = descriptor.findNestedTypeByName(fieldDescriptorProto.getTypeName());

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
    public void testMultipleDatatypeUnionSchemaConversion() {
        String fieldString =
                " \"fields\": [\n"
                        + "{\"name\": \"multiple_type_union\","
                        + " \"type\":[\"null\", \"string\", \"int\"]"
                        + "}"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> AvroToProtoSerializer.getDescriptorSchemaFromAvroSchema(avroSchema));
        assertThat(exception)
                .hasMessageThat()
                .contains("Multiple non-null union types are not supported.");
    }

    @Test
    public void testNestedArraysSchemaConversion() {
        String fieldString =
                " \"fields\": [\n"
                        + "{\"name\": \"nested_arrays\", \"type\":{\"type\": \"array\", \"items\": "
                        + "{\"name\": \"array_inside\", \"type\": \"array\", \"items\": \"long\"}"
                        + "}}"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);

        IllegalStateException exception =
                assertThrows(
                        IllegalStateException.class,
                        () -> AvroToProtoSerializer.getDescriptorSchemaFromAvroSchema(avroSchema));
        assertThat(exception).hasMessageThat().contains("Nested arrays not supported by BigQuery.");
    }

    @Test
    public void testDefaultValueSchemaConversion() throws DescriptorValidationException {
        String fieldString =
                " \"fields\": [\n"
                        + "{\"name\": \"long_with_default\", \"type\": [\"long\", \"null\"], \"default\": 100}"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        BigQueryProtoSerializer<GenericRecord> avroToProtoSerializer =
                new AvroToProtoSerializer(avroSchema);
        Descriptor descriptor = avroToProtoSerializer.getDescriptor();

        FieldDescriptorProto fieldDescriptorProto = descriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptorProto.getName()).isEqualTo("long_with_default");
        assertThat(fieldDescriptorProto.getNumber()).isEqualTo(1);
        assertThat(fieldDescriptorProto.getLabel())
                .isEqualTo(FieldDescriptorProto.Label.LABEL_OPTIONAL);
        assertThat(fieldDescriptorProto.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_INT64);
        assertThat(fieldDescriptorProto.hasDefaultValue()).isTrue();
        assertThat(fieldDescriptorProto.getDefaultValue()).isEqualTo("100");
    }

    @Test
    public void testArrayWithUnionValueSchemaConversion() throws DescriptorValidationException {
        String fieldString =
                " \"fields\": [\n"
                        + "{\"name\": \"array_with_union\", \"type\": "
                        + "{\"type\": \"array\", \"items\":  [\"long\", \"null\"]}}"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> AvroToProtoSerializer.getDescriptorSchemaFromAvroSchema(avroSchema));
        assertThat(exception).hasMessageThat().contains("Array cannot have a NULLABLE element");
    }

    @Test
    public void testMapOfUnionSchemaConversion() throws DescriptorValidationException {

        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"map_of_union\", \"type\": {\"type\": \"map\", \"values\": [\"float\", \"null\"]}}\n"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        BigQueryProtoSerializer<GenericRecord> avroToProtoSerializer =
                new AvroToProtoSerializer(avroSchema);
        Descriptor descriptor = avroToProtoSerializer.getDescriptor();

        FieldDescriptorProto field = descriptor.findFieldByNumber(1).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("map_of_union");
        assertThat(field.getNumber()).isEqualTo(1);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REPEATED);
        assertThat(field.hasTypeName()).isTrue();
        Descriptors.Descriptor nestedDescriptor =
                descriptor.findNestedTypeByName(field.getTypeName());
        FieldDescriptorProto fieldDescriptor = nestedDescriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptor.getName()).isEqualTo("key");
        assertThat(fieldDescriptor.getNumber()).isEqualTo(1);
        assertThat(fieldDescriptor.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_STRING);
        assertThat(fieldDescriptor.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        fieldDescriptor = nestedDescriptor.findFieldByNumber(2).toProto();
        assertThat(fieldDescriptor.getName()).isEqualTo("value");
        assertThat(fieldDescriptor.getNumber()).isEqualTo(2);
        assertThat(fieldDescriptor.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_FLOAT);
        assertThat(fieldDescriptor.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }

    @Test
    public void testMapOfArraySchemaConversion() throws DescriptorValidationException {

        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"map_of_array\", \"type\": {\"type\": \"map\", \"values\": {\"type\": \"array\", \"items\": \"long\", \"name\": \"array_in_map\"}}}\n"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        BigQueryProtoSerializer<GenericRecord> avroToProtoSerializer =
                new AvroToProtoSerializer(avroSchema);
        Descriptor descriptor = avroToProtoSerializer.getDescriptor();

        FieldDescriptorProto field = descriptor.findFieldByNumber(1).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("map_of_array");
        assertThat(field.getNumber()).isEqualTo(1);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REPEATED);
        assertThat(field.hasTypeName()).isTrue();
        Descriptors.Descriptor nestedDescriptor =
                descriptor.findNestedTypeByName(field.getTypeName());
        FieldDescriptorProto fieldDescriptor = nestedDescriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptor.getName()).isEqualTo("key");
        assertThat(fieldDescriptor.getNumber()).isEqualTo(1);
        assertThat(fieldDescriptor.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_STRING);
        assertThat(fieldDescriptor.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        fieldDescriptor = nestedDescriptor.findFieldByNumber(2).toProto();
        assertThat(fieldDescriptor.getName()).isEqualTo("value");
        assertThat(fieldDescriptor.getNumber()).isEqualTo(2);
        assertThat(fieldDescriptor.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_INT64);
        assertThat(fieldDescriptor.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REPEATED);
    }

    @Test
    public void testMapInRecordSchemaConversion() throws DescriptorValidationException {

        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"record_with_map\", "
                        + "\"type\": {\"name\": \"actual_record\", \"type\": \"record\", \"fields\": [{\"name\": \"map_in_record\", \"type\": { \"type\": \"map\", \"values\": \"long\"}}]}}\n"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        BigQueryProtoSerializer<GenericRecord> avroToProtoSerializer =
                new AvroToProtoSerializer(avroSchema);
        Descriptor descriptor = avroToProtoSerializer.getDescriptor();

        FieldDescriptorProto field = descriptor.findFieldByNumber(1).toProto();
        assertThat(field.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(field.getName()).isEqualTo("record_with_map");
        assertThat(field.getNumber()).isEqualTo(1);
        assertThat(field.getLabel()).isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        assertThat(field.hasTypeName()).isTrue();

        Descriptors.Descriptor nestedDescriptor =
                descriptor.findNestedTypeByName(field.getTypeName());
        FieldDescriptorProto fieldDescriptorProto = nestedDescriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptorProto.getName()).isEqualTo("map_in_record");
        assertThat(fieldDescriptorProto.getNumber()).isEqualTo(1);
        assertThat(fieldDescriptorProto.getType())
                .isEqualTo(FieldDescriptorProto.Type.TYPE_MESSAGE);
        assertThat(fieldDescriptorProto.getLabel())
                .isEqualTo(FieldDescriptorProto.Label.LABEL_REPEATED);
        assertThat(fieldDescriptorProto.hasTypeName()).isTrue();

        nestedDescriptor =
                nestedDescriptor.findNestedTypeByName(fieldDescriptorProto.getTypeName());
        fieldDescriptorProto = nestedDescriptor.findFieldByNumber(1).toProto();
        assertThat(fieldDescriptorProto.getName()).isEqualTo("key");
        assertThat(fieldDescriptorProto.getNumber()).isEqualTo(1);
        assertThat(fieldDescriptorProto.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_STRING);
        assertThat(fieldDescriptorProto.getLabel())
                .isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
        fieldDescriptorProto = nestedDescriptor.findFieldByNumber(2).toProto();
        assertThat(fieldDescriptorProto.getName()).isEqualTo("value");
        assertThat(fieldDescriptorProto.getNumber()).isEqualTo(2);
        assertThat(fieldDescriptorProto.getType()).isEqualTo(FieldDescriptorProto.Type.TYPE_INT64);
        assertThat(fieldDescriptorProto.getLabel())
                .isEqualTo(FieldDescriptorProto.Label.LABEL_REQUIRED);
    }
}
