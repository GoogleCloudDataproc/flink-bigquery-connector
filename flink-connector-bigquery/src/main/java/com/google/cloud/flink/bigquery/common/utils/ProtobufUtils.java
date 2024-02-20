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

import com.google.cloud.flink.bigquery.sink.writer.BigQueryWriter;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class ProtobufUtils {

    public static final DescriptorProtos.DescriptorProto DESCRIPTOR_PROTO;
    public static final Descriptors.Descriptor DESCRIPTOR;
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryWriter.class);

    static {
        DescriptorProtos.DescriptorProto.Builder descriptorProtoBuilder =
                DescriptorProtos.DescriptorProto.newBuilder();
        descriptorProtoBuilder.setName("Schema");
        descriptorProtoBuilder.addField(
                DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("name")
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED)
                        .setNumber(1)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING));
        descriptorProtoBuilder.addField(
                DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("number")
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED)
                        .setNumber(2)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING));
        descriptorProtoBuilder.addField(
                DescriptorProtos.FieldDescriptorProto.newBuilder()
                        .setName("ts")
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED)
                        .setNumber(3)
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.T));
        DESCRIPTOR_PROTO = descriptorProtoBuilder.build();

        DescriptorProtos.FileDescriptorProto fileDescriptorProto =
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .addMessageType(DESCRIPTOR_PROTO)
                        .build();

        try {
            DESCRIPTOR =
                    Descriptors.FileDescriptor.buildFrom(
                                    fileDescriptorProto, new Descriptors.FileDescriptor[] {})
                            .getMessageTypes()
                            .get(0);
        } catch (Exception e) {
            throw new RuntimeException("Could not create proto descriptor", e);
        }
    }

    public static DynamicMessage createMessage(String value) {
        return DynamicMessage.newBuilder(DESCRIPTOR)
                .setField(DESCRIPTOR.findFieldByNumber(1), value)
                .build();
    }

    public static DynamicMessage createMessage(GenericRecord value) {
        DynamicMessage.Builder message = DynamicMessage.newBuilder(DESCRIPTOR);
            message.setField(field, value.get(field.getName()).toString());

        return message.build();
    }
}
