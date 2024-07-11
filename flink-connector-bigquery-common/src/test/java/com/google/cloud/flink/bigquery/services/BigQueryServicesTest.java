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

package com.google.cloud.flink.bigquery.services;

import org.apache.flink.util.function.SerializableSupplier;

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.junit.Test;

import java.io.IOException;

import static com.google.common.truth.Truth.assertThat;

/** */
public class BigQueryServicesTest {
    @Test
    public void testFactoryWithTestServices() throws IOException {
        SerializableSupplier<BigQueryServices> dummyServices =
                () ->
                        new BigQueryServices() {
                            @Override
                            public BigQueryServices.QueryDataClient createQueryDataClient(
                                    CredentialsOptions credentialsOptions) {
                                return null;
                            }

                            @Override
                            public BigQueryServices.StorageReadClient createStorageReadClient(
                                    CredentialsOptions credentialsOptions) throws IOException {
                                return null;
                            }

                            @Override
                            public BigQueryServices.StorageWriteClient createStorageWriteClient(
                                    CredentialsOptions credentialsOptions) throws IOException {
                                return null;
                            }
                        };
        BigQueryServicesFactory original =
                BigQueryServicesFactory.instance(
                        BigQueryConnectOptions.builderForQuerySource()
                                .setTestingBigQueryServices(dummyServices)
                                .build());

        assertThat(original.getIsTestingEnabled()).isTrue();
        assertThat(original.getTestingServices()).isNotNull();
        assertThat(original.queryClient()).isNull();
        assertThat(original.storageRead()).isNull();
        assertThat(original.storageWrite()).isNull();

        original.defaultImplementation();

        assertThat(original.getIsTestingEnabled()).isFalse();
        assertThat(original.getTestingServices()).isNull();
    }

    @Test
    public void testWriteApi() throws IOException {
        BigQueryConnectOptions connectOptions =
                BigQueryConnectOptions.builder()
                        .setProjectId("testproject-398714")
                        .setDataset("sink_test_data")
                        .setTable("string_table_1")
                        .build();
        BigQueryServices.StorageWriteClient writeClient =
                BigQueryServicesFactory.instance(connectOptions).storageWrite();
        String tablePath =
                String.format(
                        "projects/%s/datasets/%s/tables/%s",
                        connectOptions.getProjectId(),
                        connectOptions.getDataset(),
                        connectOptions.getTable());
        String streamName =
                writeClient.createWriteStream(tablePath, WriteStream.Type.BUFFERED).getName();
        ProtoSchema protoSchema =
                ProtoSchema.newBuilder().setProtoDescriptor(ProtobufUtils.DESCRIPTOR_PROTO).build();
        StreamWriter streamWriter = writeClient.createStreamWriter(streamName, protoSchema, false);
        try {
            AppendRowsResponse res1 =
                    streamWriter
                            .append(
                                    ProtoRows.newBuilder()
                                            .addSerializedRows(
                                                    ProtobufUtils.createMessage("LukaModric")
                                                            .toByteString())
                                            .build(),
                                    0)
                            .get();
            System.out.println(res1);
            AppendRowsResponse res2 =
                    streamWriter
                            .append(
                                    ProtoRows.newBuilder()
                                            .addSerializedRows(
                                                    ProtobufUtils.createMessage("ToniKross")
                                                            .toByteString())
                                            .build(),
                                    1)
                            .get();
            System.out.println(res2);
        } catch (Exception e) {
            System.out.println(e);
        }
        streamWriter.close();
        writeClient.finalizeWriteStream(streamName);
    }

    static class ProtobufUtils {
        public static final DescriptorProtos.DescriptorProto DESCRIPTOR_PROTO;
        public static final Descriptors.Descriptor DESCRIPTOR;

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
    }
}
