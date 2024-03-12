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
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
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
import com.google.cloud.flink.bigquery.source.BigQuerySource;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

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

    private List<GenericRecord> readRows(String tableId) throws Exception {

        BigQueryReadOptions writeOptions =
                BigQueryReadOptions.builder()
                        .setBigQueryConnectOptions(
                                BigQueryConnectOptions.builder()
                                        .setProjectId("bqrampupprashasti")
                                        .setDataset("testing_dataset")
                                        .setTable(tableId)
                                        .build())
                        .build();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        BigQuerySource<GenericRecord> source = BigQuerySource.readAvros(writeOptions);
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "BigQuerySource")
                .executeAndCollect(10);
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
    public void testAllPrimitiveSchemaConversion() throws Exception {

        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"name\", \"type\": [\"null\", \"string\"]},\n"
                        + "   {\"name\": \"number\", \"type\": \"long\"},\n"
                        + "   {\"name\": \"quantity\", \"type\": \"int\"},\n"
                        + "   {\"name\": \"fixed_field\", \"type\": {\"type\": \"fixed\", \"size\": 10,\"name\": \"hash\" }},\n"
                        + "   {\"name\": \"price\", \"type\": \"float\"},\n"
                        + "   {\"name\": \"double_field\", \"type\": \"double\"},\n"
                        + "   {\"name\": \"boolean_field\", \"type\": \"boolean\"},\n"
                        + "   {\"name\": \"enum_field\", \"type\": {\"type\":\"enum\", \"symbols\": [\"A\", \"B\", \"C\", \"D\"], \"name\": \"ALPHABET\"}},\n"
                        + "   {\"name\": \"byte_field\", \"type\": \"bytes\"},\n"
                        + "   {\"name\": \"length_limited\", \"type\": [\"string\", \"null\"], \"default\": \"ho\"}\n"
                        + " ]\n";

        //        String fieldString =
        //                " \"fields\": [\n"
        //                        + "   {\"name\": \"name\", \"type\": {\"type\": \"array\",
        // \"items\": \"string\", \"default\": \"hello\"}}\n"
        //                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        System.out.println(avroSchema);
        GenericRecord record = StorageClientFaker.createRecord(avroSchema);

        record.put("length_limited", null);
        System.out.println("@prashastia record write [" + record + "]");

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
        try {
            AppendRowsResponse response = streamWriter.append(rowsToAppend).get();
            System.out.println("@prashastia response " + response);
            System.out.println("@prashastia errorList " + response.getRowErrorsList());
        } catch (Exception e) {
            System.out.println("!!! FAILED!!!");
            System.out.println();
            e.printStackTrace();
            streamWriter.close();
        }

        //        System.out.println(response.getRowErrors(0));
        System.out.println("@prashastia: [READ ROWS:]");
        for (GenericRecord element : readRows("primitive_types")) {
            System.out.println(element);
        }
    }

    @Test
    public void testAllLogicalSchemaConversion() throws Exception {

        String fieldString =
                " \"fields\": [\n"
                        //                        + "   {\"name\": \"ts\", \"type\": {\"type\":
                        // \"long\", \"logicalType\": \"timestamp-micros\"}}\n"
                        //                        + "   {\"name\": \"ts\", \"type\": {\"type\":
                        // \"long\", \"logicalType\": \"timestamp-millis\"}}\n"
                        //                        + "   {\"name\": \"timeMicros\", \"type\":
                        // {\"type\": \"long\", \"logicalType\": \"time-micros\"}},\n"
                        //                        + "   {\"name\": \"timeMillis\", \"type\":
                        // {\"type\": \"long\", \"logicalType\": \"time-millis\"}},\n"
                        //                        + "   {\"name\": \"ltsMicros\", \"type\":
                        // {\"type\": \"long\", \"logicalType\": \"local-timestamp-micros\"}},\n"
                        //                        + "   {\"name\": \"ltsMillis\", \"type\":
                        // {\"type\": \"long\", \"logicalType\": \"local-timestamp-millis\"}},\n"
                        //                        + "   {\"name\": \"date\", \"type\": {\"type\":
                        // \"int\", \"logicalType\": \"date\"}},\n"
                        //                        + "   {\"name\": \"decimal\", \"type\": {\"type\":
                        // \"bytes\", \"logicalType\": \"decimal\", \"precision\": 4, \"scale\":
                        // 2}},\n"
                        //                        + "   {\"name\": \"uuid\", \"type\": {\"type\":
                        // \"string\", \"logicalType\": \"uuid\"}}\n"
                        + "   {\"name\": \"geography\", \"type\": {\"type\": \"string\", \"logicalType\": \"geography_wkt\"}}\n"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        GenericRecord record = StorageClientFaker.createRecord(avroSchema);

        record.put("geography", "GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (3 4, 5 6))");
        System.out.println("@prashastia record write [" + record + "]");

        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer(avroSchema);
        System.out.println("@prashastia Descriptor proto Obtained: ");
        System.out.println(serializer.getDescriptorProto());
        ProtoRows.Builder protoRowsBuilder = ProtoRows.newBuilder();

        protoRowsBuilder.addSerializedRows(serializer.serialize(record));
        System.out.println("@prashastia addSerialisedRow completed.");

        StreamWriter streamWriter = getStreamWriter("geography", serializer);
        System.out.println(
                "@prashastia streamWriter.getStreamName() formed " + streamWriter.getStreamName());

        ProtoRows rowsToAppend = protoRowsBuilder.build();
        System.out.println("@prashastia append()  Started...");

        ApiFuture<AppendRowsResponse> messageIdFuture = streamWriter.append(rowsToAppend);
        ApiFutures.addCallback(
                messageIdFuture,
                new ApiFutureCallback<AppendRowsResponse>() {
                    public void onSuccess(AppendRowsResponse response) {
                        if (response.getAppendResult().hasOffset()) {
                            System.out.println(
                                    "Written with offset: "
                                            + response.getAppendResult().getOffset());
                        } else {
                            System.out.println(
                                    "received an in stream error: " + response.getRowErrorsList());
                        }
                    }

                    public void onFailure(Throwable t) {
                        System.out.println("failed to write: " + t);
                    }
                },
                MoreExecutors.directExecutor());

        System.out.println("@prashastia: [READ ROWS:]");
        for (GenericRecord element : readRows("geography")) {
            System.out.println(element);
        }
    }

    @Test
    public void testJsonSchemaInsertion() throws Exception {

        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"json\", \"type\": {\"type\": \"string\", \"logicalType\": \"Json\"}}\n"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        GenericRecord record = StorageClientFaker.createRecord(avroSchema);

        record.put("json", "{\"name\":\"John\", \"age\":30, \"car\":null}");
        System.out.println("Record write: [" + record + "]");

        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer(avroSchema);
        ProtoRows.Builder protoRowsBuilder = ProtoRows.newBuilder();
        protoRowsBuilder.addSerializedRows(serializer.serialize(record));
        StreamWriter streamWriter = getStreamWriter("json", serializer);
        ProtoRows rowsToAppend = protoRowsBuilder.build();
        AppendRowsResponse response = streamWriter.append(rowsToAppend).get();
        streamWriter.close();
        assertThat(response).isNotNull();
    }

    @Test
    public void testArraySchemaInsertion() throws Exception {

        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"name\", \"type\": {\"type\": \"array\", \"items\": \"string\", \"default\": \"hello\"}}\n"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        GenericRecord record = StorageClientFaker.createRecord(avroSchema);

        System.out.println("Record write: [" + record + "]");

        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer(avroSchema);
        ProtoRows.Builder protoRowsBuilder = ProtoRows.newBuilder();
        protoRowsBuilder.addSerializedRows(serializer.serialize(record));
        StreamWriter streamWriter = getStreamWriter("array_table", serializer);
        ProtoRows rowsToAppend = protoRowsBuilder.build();
        AppendRowsResponse response = streamWriter.append(rowsToAppend).get();
        streamWriter.close();
        assertThat(response).isNotNull();
    }

    @Test
    public void testRecordschemaInsertion() throws Exception {

        String fieldString =
                "\"fields\":[{\"name\":\"level_0\",\"type\":{\"type\":\"record\",\"namespace\":\"root\",\"name\":\"Level_0\",\"fields\":[{\"name\":\"level_1\",\"type\":{\"type\":\"record\",\"namespace\":\"root.level_0\",\"name\":\"Level_1\",\"fields\":[{\"name\":\"level_2\",\"type\":{\"type\":\"record\",\"namespace\":\"root.level_0.level_1\",\"name\":\"Level_2\",\"fields\":[{\"name\":\"level_3\",\"type\":{\"type\":\"record\",\"namespace\":\"root.level_0.level_1.level_2\",\"name\":\"Level_3\",\"fields\":[{\"name\":\"level_4\",\"type\":{\"type\":\"record\",\"namespace\":\"root.level_0.level_1.level_2.level_3\",\"name\":\"Level_4\",\"fields\":[{\"name\":\"level_5\",\"type\":{\"type\":\"record\",\"namespace\":\"root.level_0.level_1.level_2.level_3.level_4\",\"name\":\"Level_5\",\"fields\":[{\"name\":\"level_6\",\"type\":{\"type\":\"record\",\"namespace\":\"root.level_0.level_1.level_2.level_3.level_4.level_5\",\"name\":\"Level_6\",\"fields\":[{\"name\":\"level_7\",\"type\":{\"type\":\"record\",\"namespace\":\"root.level_0.level_1.level_2.level_3.level_4.level_5.level_6\",\"name\":\"Level_7\",\"fields\":[{\"name\":\"level_8\",\"type\":{\"type\":\"record\",\"namespace\":\"root.level_0.level_1.level_2.level_3.level_4.level_5.level_6.level_7\",\"name\":\"Level_8\",\"fields\":[{\"name\":\"level_9\",\"type\":{\"type\":\"record\",\"namespace\":\"root.level_0.level_1.level_2.level_3.level_4.level_5.level_6.level_7.level_8\",\"name\":\"Level_9\",\"fields\":[{\"name\":\"level_10\",\"type\":{\"type\":\"record\",\"namespace\":\"root.level_0.level_1.level_2.level_3.level_4.level_5.level_6.level_7.level_8.level_9\",\"name\":\"Level_10\",\"fields\":[{\"name\":\"level_11\",\"type\":{\"type\":\"record\",\"namespace\":\"root.level_0.level_1.level_2.level_3.level_4.level_5.level_6.level_7.level_8.level_9.level_10\",\"name\":\"Level_11\",\"fields\":[{\"name\":\"level_12\",\"type\":{\"type\":\"record\",\"namespace\":\"root.level_0.level_1.level_2.level_3.level_4.level_5.level_6.level_7.level_8.level_9.level_10.level_11\",\"name\":\"Level_12\",\"fields\":[{\"name\":\"level_13\",\"type\":{\"type\":\"record\",\"namespace\":\"root.level_0.level_1.level_2.level_3.level_4.level_5.level_6.level_7.level_8.level_9.level_10.level_11.level_12\",\"name\":\"Level_13\",\"fields\":[{\"name\":\"value\",\"type\":\"long\"}]}}]}}]}}]}}]}}]}}]}}]}}]}}]}}]}}]}}]}}]}}]";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        GenericRecord record = StorageClientFaker.createRecord(avroSchema);

        System.out.println("Record write: [" + record + "]");

        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer(avroSchema);
        ProtoRows.Builder protoRowsBuilder = ProtoRows.newBuilder();
        protoRowsBuilder.addSerializedRows(serializer.serialize(record));
        StreamWriter streamWriter = getStreamWriter("nested_record_table", serializer);
        ProtoRows rowsToAppend = protoRowsBuilder.build();
        AppendRowsResponse response = streamWriter.append(rowsToAppend).get();
        streamWriter.close();
        assertThat(response).isNotNull();
    }

    @Test
    public void testDateSchemaInsertion() throws Exception {

        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"date\", \"type\": {\"type\": \"int\", \"logicalType\": \"date\"}}\n"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        GenericRecord record = StorageClientFaker.createRecord(avroSchema);
        record.put("date", DateTime.now());

        System.out.println("Record write: [" + record + "]");

        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer(avroSchema);
        ProtoRows.Builder protoRowsBuilder = ProtoRows.newBuilder();
        protoRowsBuilder.addSerializedRows(serializer.serialize(record));
        StreamWriter streamWriter = getStreamWriter("date", serializer);
        ProtoRows rowsToAppend = protoRowsBuilder.build();
        AppendRowsResponse response = streamWriter.append(rowsToAppend).get();
        streamWriter.close();
        assertThat(response).isNotNull();
    }

    @Test
    public void testTimeSchemaMicroSecondsInsertion() throws Exception {

        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"time\", \"type\": {\"type\": \"long\", \"logicalType\": \"time-micros\"}}\n"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        GenericRecord record = StorageClientFaker.createRecord(avroSchema);
        // Get Time in microsecond precision.
        LocalDateTime time = LocalDateTime.now();
        // convert to timestamp and add the microseconds
        // NOTE: FLAKY TEST.
        long timestamp =
                TimeUnit.MILLISECONDS.toMicros(
                        time.toInstant(ZoneOffset.ofHoursMinutes(5, 30)).toEpochMilli());
        timestamp += (time.getNano() % 1000000) / 1000;
        record.put("time", timestamp);

        System.out.println("Record write: [" + record + "]");
        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer(avroSchema);
        ProtoRows.Builder protoRowsBuilder = ProtoRows.newBuilder();
        protoRowsBuilder.addSerializedRows(serializer.serialize(record));
        StreamWriter streamWriter = getStreamWriter("time", serializer);
        ProtoRows rowsToAppend = protoRowsBuilder.build();
        AppendRowsResponse response = streamWriter.append(rowsToAppend).get();
        streamWriter.close();
        assertThat(response).isNotNull();
    }

    @Test
    public void testTimeSchemaMilliSecondsInsertion() throws Exception {

        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"time\", \"type\": {\"type\": \"long\", \"logicalType\": \"time-millis\"}}\n"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        GenericRecord record = StorageClientFaker.createRecord(avroSchema);
        // convert to timestamp and add the microseconds
        long timestamp = Instant.now().getMillis();
        record.put("time", timestamp);

        System.out.println("Record write: [" + record + "]");
        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer(avroSchema);
        ProtoRows.Builder protoRowsBuilder = ProtoRows.newBuilder();
        protoRowsBuilder.addSerializedRows(serializer.serialize(record));
        StreamWriter streamWriter = getStreamWriter("time", serializer);
        ProtoRows rowsToAppend = protoRowsBuilder.build();
        AppendRowsResponse response = streamWriter.append(rowsToAppend).get();
        streamWriter.close();
        assertThat(response).isNotNull();
    }

    @Test
    public void testDateTimeSchemaMicroSecondsInsertion() throws Exception {

        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"datetime\", \"type\": {\"type\": \"long\", \"logicalType\": \"local-timestamp-micros\"}}\n"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        GenericRecord record = StorageClientFaker.createRecord(avroSchema);
        // Get Time in microsecond precision.
        LocalDateTime time = LocalDateTime.now();
        // convert to timestamp and add the microseconds
        // NOTE: FLAKY TEST.
        long timestamp =
                TimeUnit.MILLISECONDS.toMicros(
                        time.toInstant(ZoneOffset.ofHoursMinutes(5, 30)).toEpochMilli());
        timestamp += (time.getNano() % 1000000) / 1000;
        record.put("datetime", timestamp);

        System.out.println("Record write: [" + record + "]");
        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer(avroSchema);
        ProtoRows.Builder protoRowsBuilder = ProtoRows.newBuilder();
        protoRowsBuilder.addSerializedRows(serializer.serialize(record));
        StreamWriter streamWriter = getStreamWriter("datetime", serializer);
        ProtoRows rowsToAppend = protoRowsBuilder.build();
        AppendRowsResponse response = streamWriter.append(rowsToAppend).get();
        streamWriter.close();
        assertThat(response).isNotNull();
    }

    @Test
    public void testDateTimeSchemaMilliSecondsInsertion() throws Exception {

        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"datetime\", \"type\": {\"type\": \"long\", \"logicalType\": \"local-timestamp-millis\"}}\n"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        GenericRecord record = StorageClientFaker.createRecord(avroSchema);
        // convert to timestamp and add the microseconds
        long timestamp = Instant.now().getMillis();
        record.put("datetime", timestamp);

        System.out.println("Record write: [" + record + "]");
        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer(avroSchema);
        ProtoRows.Builder protoRowsBuilder = ProtoRows.newBuilder();
        protoRowsBuilder.addSerializedRows(serializer.serialize(record));
        StreamWriter streamWriter = getStreamWriter("datetime", serializer);
        ProtoRows rowsToAppend = protoRowsBuilder.build();
        AppendRowsResponse response = streamWriter.append(rowsToAppend).get();
        streamWriter.close();
        assertThat(response).isNotNull();
    }

    private String getRecord(String name) {
        String record =
                "{\"name\": "
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

        return record;
    }

    @Test
    public void testArrayOfRecordsSchemaInsertion() throws Exception {

        String fieldString =
                " \"fields\": [\n"
                        + "{\"name\": \"array_of_records\", \"type\":{\"type\": \"array\", \"items\": "
                        + getRecord("inside_record")
                        + "}}"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        GenericRecord record = StorageClientFaker.createRecord(avroSchema);

        System.out.println("Record write: [" + record + "]");
        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer(avroSchema);
        ProtoRows.Builder protoRowsBuilder = ProtoRows.newBuilder();
        protoRowsBuilder.addSerializedRows(serializer.serialize(record));
        StreamWriter streamWriter = getStreamWriter("array_of_records", serializer);
        ProtoRows rowsToAppend = protoRowsBuilder.build();
        AppendRowsResponse response = streamWriter.append(rowsToAppend).get();
        streamWriter.close();
        assertThat(response).isNotNull();
    }

    @Test
    public void testArrayInRecordSchemaInsertion() throws Exception {

        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"record_with_array\", \"type\": {\"name\": \"record_with_array_field\", \"type\": \"record\", \"fields\": [{\"name\": \"array_in_record\", \"type\": {\"type\": \"array\", \"items\": \"boolean\"}}]}}\n"
                        + " ]\n";

        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        GenericRecord record = StorageClientFaker.createRecord(avroSchema);

        System.out.println("Record write: [" + record + "]");
        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer(avroSchema);
        ProtoRows.Builder protoRowsBuilder = ProtoRows.newBuilder();
        protoRowsBuilder.addSerializedRows(serializer.serialize(record));
        StreamWriter streamWriter = getStreamWriter("record_with_array", serializer);
        ProtoRows rowsToAppend = protoRowsBuilder.build();
        AppendRowsResponse response = streamWriter.append(rowsToAppend).get();
        streamWriter.close();
        assertThat(response).isNotNull();
    }

    //    @Test
    //    public void readRows() throws Exception {
    //
    //        BigQueryReadOptions writeOptions =
    //                BigQueryReadOptions.builder()
    //                        .setBigQueryConnectOptions(
    //                                BigQueryConnectOptions.builder()
    //                                        .setProjectId("bqrampupprashasti")
    //                                        .setDataset("testing_dataset")
    //                                        .setTable("bignumeric")
    //                                        .build())
    //                        .build();
    //        final StreamExecutionEnvironment env =
    // StreamExecutionEnvironment.getExecutionEnvironment();
    //        System.out.println("env formed");
    //        BigQuerySource<GenericRecord> source = BigQuerySource.readAvros(writeOptions);
    //        System.out.println("source formed");
    //        List<GenericRecord> list =
    //                env.fromSource(source, WatermarkStrategy.noWatermarks(), "BigQuerySource")
    //                        .executeAndCollect(100);
    //        for (GenericRecord element : list) {
    //            System.out.println(element);
    //        }
    //    }

    private static List<GenericRecord> getRows(String tableId) throws Exception {

        BigQueryReadOptions writeOptions =
                BigQueryReadOptions.builder()
                        .setBigQueryConnectOptions(
                                BigQueryConnectOptions.builder()
                                        .setProjectId("bqrampupprashasti")
                                        .setDataset("testing_dataset")
                                        .setTable(tableId)
                                        .build())
                        .build();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        BigQuerySource<GenericRecord> source = BigQuerySource.readAvros(writeOptions);
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "BigQuerySource")
                .executeAndCollect(100);
    }

    @Test
    public void testDecimalSchemaConversion() throws Exception {
        String tableId = "numeric";
        String fieldString =
                " \"fields\": [\n"
                        + "   {\"name\": \"bignumeric\", \"type\": {\"type\":"
                        + "\"bytes\", \"logicalType\": \"decimal\", \"precision\": 4, \"scale\":"
                        + " 2}}\n"
                        + " ]\n";
        Schema avroSchema = getAvroSchemaFromFieldString(fieldString);
        BigQueryProtoSerializer<GenericRecord> serializer = new AvroToProtoSerializer(avroSchema);
        ProtoRows.Builder protoRowsBuilder = ProtoRows.newBuilder();

        List<GenericRecord> list = getRows(tableId);
        for (GenericRecord record : list) {
            System.out.println("Record read: [" + record + "]");
            GenericRecord ele = StorageClientFaker.createRecord(avroSchema);
            ele.put("bignumeric", record.get("numeric"));
            System.out.println("Record to write: [" + ele + "]");
            protoRowsBuilder.addSerializedRows(serializer.serialize(ele));
        }
        ProtoRows rowsToAppend = protoRowsBuilder.build();
        StreamWriter streamWriter = getStreamWriter("bignumeric", serializer);
        AppendRowsResponse response = streamWriter.append(rowsToAppend).get();
        streamWriter.close();
        assertThat(response).isNotNull();
    }
}
