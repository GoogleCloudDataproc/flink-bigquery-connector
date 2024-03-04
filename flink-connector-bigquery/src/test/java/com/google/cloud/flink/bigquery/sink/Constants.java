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

package com.google.cloud.flink.bigquery.sink;

import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.protobuf.DescriptorProtos;

/** */
public class Constants {

    public static final ProtoSchema SINGLE_STRING_FIELD_PROTO_SCHEMA =
            ProtoSchema.newBuilder()
                    .setProtoDescriptor(
                            DescriptorProtos.DescriptorProto.newBuilder()
                                    .setName("SingleStringFieldSchema")
                                    .addField(
                                            DescriptorProtos.FieldDescriptorProto.newBuilder()
                                                    .setName("name")
                                                    .setLabel(
                                                            DescriptorProtos.FieldDescriptorProto
                                                                    .Label.LABEL_REQUIRED)
                                                    .setNumber(1)
                                                    .setType(
                                                            DescriptorProtos.FieldDescriptorProto
                                                                    .Type.TYPE_STRING))
                                    .build())
                    .build();
}
