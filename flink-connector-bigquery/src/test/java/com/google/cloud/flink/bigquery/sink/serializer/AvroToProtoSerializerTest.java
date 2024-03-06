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
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.google.cloud.flink.bigquery.sink.serializer.AvroToProtoSerializer.getBeamResult;
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
        String avroSchemaString =
                "{\"namespace\": \"project.dataset\",\n"
                        + " \"type\": \"record\",\n"
                        + " \"name\": \"table\",\n"
                        + " \"doc\": \"Translated Avro Schema for project.dataset.table\",\n"
                        + fieldString
                        + "}";

        org.apache.avro.Schema avroSchema =
                new org.apache.avro.Schema.Parser().parse(avroSchemaString);

        DescriptorProto descriptorProto =
                AvroToProtoSerializer.getDescriptorSchemaFromAvroSchema(avroSchema);
        System.out.println("descriptorProto Obtained:\n" + descriptorProto);
        DescriptorProto supposedDescriptorProto = getBeamResult(avroSchema);
        System.out.println("descriptorProto Supposed:\n" + supposedDescriptorProto);
    }
}
