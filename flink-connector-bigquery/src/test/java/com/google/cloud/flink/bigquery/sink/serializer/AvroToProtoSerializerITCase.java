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

import org.apache.flink.FlinkVersion;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.protobuf.Descriptors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/** Tests for {@link AvroToProtoSerializer}. */
public class AvroToProtoSerializerITCase {

    private static final HeaderProvider USER_AGENT_HEADER_PROVIDER =
            FixedHeaderProvider.create(
                    "user-agent", "Apache_Flink_Java/" + FlinkVersion.current().toString());

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

    /** Class to obtain write stream. */
    public class WriteStreamClass {

        BigQueryWriteClient client;

        public WriteStreamClass(CredentialsOptions options) throws IOException {

            BigQueryWriteSettings.Builder settingsBuilder =
                    BigQueryWriteSettings.newBuilder()
                            .setCredentialsProvider(
                                    FixedCredentialsProvider.create(options.getCredentials()))
                            .setTransportChannelProvider(
                                    BigQueryWriteSettings.defaultGrpcTransportProviderBuilder()
                                            .setHeaderProvider(USER_AGENT_HEADER_PROVIDER)
                                            .build());

            client = BigQueryWriteClient.create(settingsBuilder.build());
        }

        public WriteStream createWriteStream(CreateWriteStreamRequest request) {
            return client.createWriteStream(request);
        }

        public StreamWriter createStreamWriter(
                ProtoSchema protoSchema, RetrySettings retrySettings, String writeStreamName) {
            try {
                StreamWriter.Builder streamWriter =
                        StreamWriter.newBuilder(writeStreamName, client)
                                .setWriterSchema(protoSchema)
                                .setRetrySettings(retrySettings);
                return streamWriter.build();
            } catch (IOException e) {
                throw new RuntimeException("Could not build stream-writer", e);
            }
        }
    }

    private StreamWriter getStreamWriter(String tableId, BigQueryProtoSerializer serializer)
            throws IOException {
        BigQueryReadOptions writeOptions =
                BigQueryReadOptions.builder()
                        .setBigQueryConnectOptions(
                                BigQueryConnectOptions.builder()
                                        .setProjectId("bqrampupprashasti")
                                        .setDataset("testing_dataset")
                                        .setTable(tableId)
                                        .build())
                        .build();
        BigQueryConnectOptions writeConnectOptions = writeOptions.getBigQueryConnectOptions();
        String tablePath =
                "projects/"
                        + writeConnectOptions.getProjectId()
                        + "/datasets/"
                        + writeConnectOptions.getDataset()
                        + "/tables/"
                        + writeConnectOptions.getTable();
        String writeStreamName = String.format("%s/streams/_default", tablePath);
        WriteStreamClass writeStreamClass =
                new WriteStreamClass(writeConnectOptions.getCredentialsOptions());
        ProtoSchema protoSchema =
                ProtoSchema.newBuilder()
                        .setProtoDescriptor(serializer.getDescriptorProto())
                        .build();
        StreamWriter streamWriter =
                writeStreamClass.createStreamWriter(
                        protoSchema, RetrySettings.newBuilder().build(), writeStreamName);
        return streamWriter;
    }

    @Test
    public void testAllPrimitiveSchemaConversion()
            throws Descriptors.DescriptorValidationException, IOException, ExecutionException,
                    InterruptedException, BigQuerySerializationException {

        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"name\", \"type\": \"string\"},\n"
                        + "   {\"name\": \"number\", \"type\": \"long\"},\n"
                        + "   {\"name\": \"quantity\", \"type\": \"int\"},\n"
                        + "   {\"name\": \"fixed_field\", \"type\": {\"type\": \"fixed\", \"size\": 10,\"name\": \"hash\" }},\n"
                        + "   {\"name\": \"price\", \"type\": \"string\"},\n"
                        + "   {\"name\": \"double_field\", \"type\": \"double\"},\n"
                        + "   {\"name\": \"boolean_field\", \"type\": \"boolean\"},\n"
                        + "   {\"name\": \"enum_field\", \"type\": {\"type\":\"enum\", \"symbols\": [\"A\", \"B\", \"C\", \"D\"], \"name\": \"ALPHABET\"}},\n"
                        + "   {\"name\": \"byte_field\", \"type\": \"bytes\"}\n"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        GenericRecord record = StorageClientFaker.createRecord(avroSchema);
        System.out.println("@prashastia record write [" + record + "]");

        fieldString =
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

        avroSchema = getAvroSchemaFromFieldString(fieldString);

        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer(avroSchema);


        StreamWriter streamWriter = getStreamWriter("primitive_types", serializer);
        System.out.println("@prashastia streamWriter formed " + streamWriter);
        System.out.println(
                "@prashastia streamWriter.getStreamName() formed " + streamWriter.getStreamName());
        ProtoRows.Builder protoRowsBuilder = ProtoRows.newBuilder();

        protoRowsBuilder.addSerializedRows(serializer.serialize(record));
        System.out.println("@prashastia addSerialisedRow completed.");

        ProtoRows rowsToAppend = protoRowsBuilder.build();
        System.out.println("@prashastia append()  Started...");
        AppendRowsResponse response = streamWriter.append(rowsToAppend).get();
        System.out.println("@prashastia: [response]" + response);
    }

    //    @Test
    //    public void testAllLogicalSchemaConversion() throws
    // Descriptors.DescriptorValidationException {
    //
    //        String fieldString =
    //                " \"fields\": [\n"
    //                        + "   {\"name\": \"tsMicros\", \"type\": {\"type\": \"long\",
    // \"logicalType\": \"timestamp-micros\"}},\n"
    //                        + "   {\"name\": \"tsMillis\", \"type\": {\"type\": \"long\",
    // \"logicalType\": \"timestamp-millis\"}},\n"
    //                        + "   {\"name\": \"timeMicros\", \"type\": {\"type\": \"long\",
    // \"logicalType\": \"time-micros\"}},\n"
    //                        + "   {\"name\": \"timeMillis\", \"type\": {\"type\": \"int\",
    // \"logicalType\": \"time-millis\"}},\n"
    //                        + "   {\"name\": \"ltsMicros\", \"type\": {\"type\": \"long\",
    // \"logicalType\": \"local-timestamp-micros\"}},\n"
    //                        + "   {\"name\": \"ltsMillis\", \"type\": {\"type\": \"long\",
    // \"logicalType\": \"local-timestamp-millis\"}},\n"
    //                        + "   {\"name\": \"date\", \"type\": {\"type\": \"int\",
    // \"logicalType\": \"date\"}},\n"
    //                        + "   {\"name\": \"decimal\", \"type\": {\"type\": \"bytes\",
    // \"logicalType\": \"decimal\", \"precision\": 4, \"scale\": 2}},\n"
    //                        + "   {\"name\": \"uuid\", \"type\": {\"type\": \"string\",
    // \"logicalType\": \"uuid\"}},\n"
    //                        + "   {\"name\": \"geography\", \"type\": {\"type\": \"string\",
    // \"logicalType\": \"geography_wkt\"}}\n"
    //                        + " ]\n";
    //
    //        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
    //
    //        DescriptorProto descriptorProto =
    //                AvroToProtoSerializer.getDescriptorSchemaFromAvroSchema(avroSchema);
    //
    //        Descriptors.Descriptor descriptor =
    //                BigQueryProtoSerializer.getDescriptorFromDescriptorProto(descriptorProto);
    //
    //        GenericRecord record = StorageClientFaker.createRecord(avroSchema);
    //    }

}
