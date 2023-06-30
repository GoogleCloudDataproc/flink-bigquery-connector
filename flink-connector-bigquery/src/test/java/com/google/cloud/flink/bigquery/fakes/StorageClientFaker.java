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

package com.google.cloud.flink.bigquery.fakes;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.function.SerializableFunction;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.JobStatistics2;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.AvroRows;
import com.google.cloud.bigquery.storage.v1.AvroSchema;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.cloud.bigquery.storage.v1.StreamStats;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.QueryResultInfo;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.protobuf.ByteString;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.RandomData;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Utility class to generate mocked objects for the BQ storage client. */
public class StorageClientFaker {

    /** Implementation for the BigQuery services for testing purposes. */
    public static class FakeBigQueryServices implements BigQueryServices {

        private final FakeBigQueryStorageReadClient storageReadClient;

        public FakeBigQueryServices(FakeBigQueryStorageReadClient storageReadClient) {
            this.storageReadClient = storageReadClient;
        }

        @Override
        public StorageReadClient getStorageClient(CredentialsOptions readOptions)
                throws IOException {
            return storageReadClient;
        }

        @Override
        public QueryDataClient getQueryDataClient(CredentialsOptions readOptions) {
            return new QueryDataClient() {

                @Override
                public List<String> retrieveTablePartitions(
                        String project, String dataset, String table) {
                    return Lists.newArrayList(
                            LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHH")));
                }

                @Override
                public Optional<Tuple2<String, StandardSQLTypeName>> retrievePartitionColumnName(
                        String project, String dataset, String table) {
                    return Optional.of(Tuple2.of("ts", StandardSQLTypeName.TIMESTAMP));
                }

                @Override
                public TableSchema getTableSchema(String project, String dataset, String table) {
                    return SIMPLE_BQ_TABLE_SCHEMA;
                }

                @Override
                public Optional<QueryResultInfo> runQuery(String projectId, String query) {
                    return Optional.of(QueryResultInfo.succeed("", "", ""));
                }

                @Override
                public Job dryRunQuery(String projectId, String query) {
                    return new Job()
                            .setStatistics(
                                    new JobStatistics()
                                            .setQuery(
                                                    new JobStatistics2()
                                                            .setSchema(SIMPLE_BQ_TABLE_SCHEMA)));
                }
            };
        }

        /** Implementation of the server stream for testing purposes. */
        public static class FakeBigQueryServerStream
                implements BigQueryServices.BigQueryServerStream<ReadRowsResponse> {

            private final List<ReadRowsResponse> toReturn;

            public FakeBigQueryServerStream(
                    SerializableFunction<RecordGenerationParams, List<GenericRecord>> dataGenerator,
                    String schema,
                    String dataPrefix,
                    Long size,
                    Long offset) {
                this.toReturn =
                        createResponse(
                                schema,
                                dataGenerator
                                        .apply(new RecordGenerationParams(schema, size.intValue()))
                                        .stream()
                                        .skip(offset)
                                        .collect(Collectors.toList()),
                                0,
                                size);
            }

            @Override
            public Iterator<ReadRowsResponse> iterator() {
                return toReturn.iterator();
            }

            @Override
            public void cancel() {}
        }

        /** Implementation for the storage read client for testing purposes. */
        public static class FakeBigQueryStorageReadClient implements StorageReadClient {

            private final ReadSession session;
            private final SerializableFunction<RecordGenerationParams, List<GenericRecord>>
                    dataGenerator;

            public FakeBigQueryStorageReadClient(
                    ReadSession session,
                    SerializableFunction<RecordGenerationParams, List<GenericRecord>>
                            dataGenerator) {
                this.session = session;
                this.dataGenerator = dataGenerator;
            }

            @Override
            public ReadSession createReadSession(CreateReadSessionRequest request) {
                return session;
            }

            @Override
            public BigQueryServerStream<ReadRowsResponse> readRows(ReadRowsRequest request) {
                try {
                    // introduce some random delay
                    Thread.sleep(new Random().nextInt(500));
                } catch (InterruptedException ex) {
                }
                return new FakeBigQueryServerStream(
                        dataGenerator,
                        session.getAvroSchema().getSchema(),
                        request.getReadStream(),
                        session.getEstimatedRowCount(),
                        request.getOffset());
            }

            @Override
            public void close() {}
        }
    }

    public static final String SIMPLE_AVRO_SCHEMA_FIELDS_STRING =
            " \"fields\": [\n"
                    + "   {\"name\": \"name\", \"type\": \"string\"},\n"
                    + "   {\"name\": \"number\", \"type\": \"long\"},\n"
                    + "   {\"name\" : \"ts\", \"type\" : {\"type\" : \"long\",\"logicalType\" : \"timestamp-micros\"}}\n"
                    + " ]\n";
    public static final String SIMPLE_AVRO_SCHEMA_STRING =
            "{\"namespace\": \"project.dataset\",\n"
                    + " \"type\": \"record\",\n"
                    + " \"name\": \"table\",\n"
                    + " \"doc\": \"Translated Avro Schema for project.dataset.table\",\n"
                    + SIMPLE_AVRO_SCHEMA_FIELDS_STRING
                    + "}";
    public static final String SIMPLE_AVRO_SCHEMA_FORQUERY_STRING =
            "{\"namespace\": \"project.dataset\",\n"
                    + " \"type\": \"record\",\n"
                    + " \"name\": \"queryresultschema\",\n"
                    + " \"namespace\": \"org.apache.flink.connector.bigquery\",\n"
                    + " \"doc\": \"Translated Avro Schema for queryresultschema\",\n"
                    + SIMPLE_AVRO_SCHEMA_FIELDS_STRING
                    + "}";
    public static final Schema SIMPLE_AVRO_SCHEMA =
            new Schema.Parser().parse(SIMPLE_AVRO_SCHEMA_STRING);

    public static final TableSchema SIMPLE_BQ_TABLE_SCHEMA =
            new TableSchema()
                    .setFields(
                            Lists.newArrayList(
                                    new TableFieldSchema()
                                            .setName("name")
                                            .setType("STRING")
                                            .setMode("REQUIRED"),
                                    new TableFieldSchema()
                                            .setName("number")
                                            .setType("INTEGER")
                                            .setMode("REQUIRED"),
                                    new TableFieldSchema()
                                            .setName("ts")
                                            .setType("TIMESTAMP")
                                            .setMode("REQUIRED")));

    /** Represents the parameters needed for the Avro data generation. */
    public static class RecordGenerationParams implements Serializable {
        private final String avroSchemaString;
        private final Integer recordCount;

        public RecordGenerationParams(String avroSchemaString, Integer recordCount) {
            this.avroSchemaString = avroSchemaString;
            this.recordCount = recordCount;
        }

        public String getAvroSchemaString() {
            return avroSchemaString;
        }

        public Integer getRecordCount() {
            return recordCount;
        }
    }

    public static ReadSession fakeReadSession(
            Integer expectedRowCount, Integer expectedReadStreamCount, String avroSchemaString) {
        // setup the response for read session request
        List<ReadStream> readStreams =
                IntStream.range(0, expectedReadStreamCount)
                        .mapToObj(i -> ReadStream.newBuilder().setName("stream" + i).build())
                        .collect(Collectors.toList());
        return ReadSession.newBuilder()
                .addAllStreams(readStreams)
                .setEstimatedRowCount(expectedRowCount)
                .setDataFormat(DataFormat.AVRO)
                .setAvroSchema(AvroSchema.newBuilder().setSchema(avroSchemaString))
                .build();
    }

    public static List<GenericRecord> createRecordList(RecordGenerationParams params) {
        Schema schema = new Schema.Parser().parse(params.getAvroSchemaString());
        return IntStream.range(0, params.getRecordCount())
                .mapToObj(i -> createRecord(schema))
                .collect(Collectors.toList());
    }

    public static GenericRecord createRecord(Schema schema) {
        return (GenericRecord) new RandomData(schema, 0).iterator().next();
    }

    private static final EncoderFactory ENCODER_FACTORY = EncoderFactory.get();

    @SuppressWarnings("deprecation")
    public static List<ReadRowsResponse> createResponse(
            String schemaString,
            List<GenericRecord> genericRecords,
            double progressAtResponseStart,
            double progressAtResponseEnd) {
        // BigQuery delivers the data in 1024 elements chunks, so we partition the generated list
        // into multiple ones with that size max.
        List<List<GenericRecord>> responsesData = Lists.partition(genericRecords, 1024);

        return responsesData.stream()
                // for each data response chunk we generate a read response object
                .map(
                        genRecords -> {
                            try {
                                Schema schema = new Schema.Parser().parse(schemaString);
                                GenericDatumWriter<GenericRecord> writer =
                                        new GenericDatumWriter<>(schema);
                                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                                Encoder binaryEncoder =
                                        ENCODER_FACTORY.binaryEncoder(outputStream, null);
                                for (GenericRecord genericRecord : genRecords) {
                                    writer.write(genericRecord, binaryEncoder);
                                }

                                binaryEncoder.flush();

                                return ReadRowsResponse.newBuilder()
                                        .setAvroRows(
                                                AvroRows.newBuilder()
                                                        .setSerializedBinaryRows(
                                                                ByteString.copyFrom(
                                                                        outputStream.toByteArray()))
                                                        .setRowCount(genRecords.size()))
                                        .setAvroSchema(
                                                AvroSchema.newBuilder()
                                                        .setSchema(schema.toString())
                                                        .build())
                                        .setRowCount(genRecords.size())
                                        .setStats(
                                                StreamStats.newBuilder()
                                                        .setProgress(
                                                                StreamStats.Progress.newBuilder()
                                                                        .setAtResponseStart(
                                                                                progressAtResponseStart)
                                                                        .setAtResponseEnd(
                                                                                progressAtResponseEnd)))
                                        .build();
                            } catch (Exception ex) {
                                throw new RuntimeException(
                                        "Problems generating faked response.", ex);
                            }
                        })
                .collect(Collectors.toList());
    }

    public static BigQueryReadOptions createReadOptions(
            Integer expectedRowCount, Integer expectedReadStreamCount, String avroSchemaString)
            throws IOException {
        return createReadOptions(
                expectedRowCount,
                expectedReadStreamCount,
                avroSchemaString,
                params -> StorageClientFaker.createRecordList(params));
    }

    public static BigQueryReadOptions createReadOptions(
            Integer expectedRowCount,
            Integer expectedReadStreamCount,
            String avroSchemaString,
            SerializableFunction<RecordGenerationParams, List<GenericRecord>> dataGenerator)
            throws IOException {
        return BigQueryReadOptions.builder()
                .setBigQueryConnectOptions(
                        BigQueryConnectOptions.builder()
                                .setDataset("dataset")
                                .setProjectId("project")
                                .setTable("table")
                                .setCredentialsOptions(null)
                                .setTestingBigQueryServices(
                                        () -> {
                                            return new StorageClientFaker.FakeBigQueryServices(
                                                    new StorageClientFaker.FakeBigQueryServices
                                                            .FakeBigQueryStorageReadClient(
                                                            StorageClientFaker.fakeReadSession(
                                                                    expectedRowCount,
                                                                    expectedReadStreamCount,
                                                                    avroSchemaString),
                                                            dataGenerator));
                                        })
                                .build())
                .build();
    }
}