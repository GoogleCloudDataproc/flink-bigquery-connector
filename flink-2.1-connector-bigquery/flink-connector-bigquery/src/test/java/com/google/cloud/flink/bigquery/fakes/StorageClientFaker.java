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

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SerializableFunction;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.AvroRows;
import com.google.cloud.bigquery.storage.v1.AvroSchema;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamResponse;
import com.google.cloud.bigquery.storage.v1.FlushRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.cloud.bigquery.storage.v1.StreamStats;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import com.google.cloud.flink.bigquery.common.utils.SchemaTransform;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.TablePartitionInfo;
import com.google.cloud.flink.bigquery.services.TablePartitionInfo.PartitionType;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.protobuf.ByteString;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.RandomData;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Utility class to generate mocked objects for the BQ storage client. */
public class StorageClientFaker {

    /** Implementation for the BigQuery services for testing purposes. */
    public static class FakeBigQueryServices implements BigQueryServices {

        private final FakeBigQueryStorageReadClient storageReadClient;
        private final FakeBigQueryStorageWriteClient storageWriteClient;
        private final FakeQueryDataClient queryDataClient;

        private FakeBigQueryServices(
                FakeBigQueryStorageReadClient storageReadClient,
                FakeBigQueryStorageWriteClient storageWriteClient,
                FakeQueryDataClient queryDataClient) {
            this.storageReadClient = storageReadClient;
            this.storageWriteClient = storageWriteClient;
            this.queryDataClient = queryDataClient;
        }

        static FakeBigQueryServices getInstance(
                FakeBigQueryStorageReadClient storageReadClient,
                FakeBigQueryStorageWriteClient storageWriteClient,
                FakeQueryDataClient queryDataClient) {
            FakeBigQueryServices instance =
                    new FakeBigQueryServices(
                            storageReadClient, storageWriteClient, queryDataClient);
            return instance;
        }

        static FakeBigQueryServices getInstance(
                FakeBigQueryStorageReadClient storageReadClient,
                FakeBigQueryStorageWriteClient storageWriteClient) {
            // Use default instance if not provided.
            return getInstance(
                    storageReadClient,
                    storageWriteClient,
                    (FakeQueryDataClient) FakeQueryDataClient.getInstance());
        }

        public static final java.util.concurrent.atomic.AtomicInteger
                STORAGE_READ_CLIENT_INVOCATIONS = new java.util.concurrent.atomic.AtomicInteger(0);

        @Override
        public StorageReadClient createStorageReadClient(CredentialsOptions options)
                throws IOException {
            STORAGE_READ_CLIENT_INVOCATIONS.incrementAndGet();
            return storageReadClient;
        }

        @Override
        public StorageWriteClient createStorageWriteClient(CredentialsOptions options)
                throws IOException {
            return storageWriteClient;
        }

        @Override
        public QueryDataClient createQueryDataClient(CredentialsOptions options) {
            return queryDataClient;
        }

        /** Implementation of the BQ query client for testing purposes. */
        public static class FakeQueryDataClient implements QueryDataClient {

            private final boolean tableExists;
            private final RuntimeException tableExistsError;
            private final RuntimeException createDatasetError;
            private final RuntimeException createTableError;

            private boolean datasetExists;
            private String datasetRegion;
            private int tableExistsInvocations;
            private int createDatasetInvocations;
            private int createTableInvocations;

            public FakeQueryDataClient(
                    boolean tableExists,
                    RuntimeException tableExistsError,
                    RuntimeException createDatasetError,
                    RuntimeException createTableError) {
                this.tableExists = tableExists;
                this.tableExistsError = tableExistsError;
                this.createDatasetError = createDatasetError;
                this.createTableError = createTableError;
                tableExistsInvocations = 0;
                createDatasetInvocations = 0;
                createTableInvocations = 0;
                datasetExists = true;
                datasetRegion = "us-central1";
            }

            public FakeQueryDataClient(boolean datasetExists, String datasetRegion) {
                this.datasetExists = datasetExists;
                this.datasetRegion = datasetRegion;
                tableExists = false;
                tableExistsError = null;
                createDatasetError = null;
                createTableError = null;
            }

            static FakeQueryDataClient defaultInstance =
                    new FakeQueryDataClient(true, null, null, null);

            static QueryDataClient getInstance() {
                return defaultInstance;
            }

            @Override
            public List<String> retrieveTablePartitions(
                    String project, String dataset, String table) {
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMddHH");

                return Arrays.asList(
                        Instant.now().atOffset(ZoneOffset.UTC).minusHours(5).format(dtf),
                        Instant.now().atOffset(ZoneOffset.UTC).minusHours(4).format(dtf),
                        Instant.now().atOffset(ZoneOffset.UTC).minusHours(3).format(dtf),
                        Instant.now().atOffset(ZoneOffset.UTC).format(dtf));
            }

            @Override
            public Optional<TablePartitionInfo> retrievePartitionColumnInfo(
                    String project, String dataset, String table) {
                return Optional.of(
                        new TablePartitionInfo(
                                "ts",
                                PartitionType.HOUR,
                                StandardSQLTypeName.TIMESTAMP,
                                Instant.now()));
            }

            @Override
            public TableSchema getTableSchema(String project, String dataset, String table) {
                return SIMPLE_BQ_TABLE_SCHEMA;
            }

            @Override
            public Dataset getDataset(String project, String dataset) {
                Dataset mockedDataset = Mockito.mock(Dataset.class);
                Mockito.when(mockedDataset.exists()).thenReturn(datasetExists);
                Mockito.when(mockedDataset.getLocation()).thenReturn(datasetRegion);
                return mockedDataset;
            }

            @Override
            public void createDataset(String project, String dataset, String region) {
                createDatasetInvocations++;
                if (createDatasetError != null) {
                    throw createDatasetError;
                }
            }

            @Override
            public Boolean tableExists(String project, String dataset, String table) {
                tableExistsInvocations++;
                if (tableExistsError != null) {
                    throw tableExistsError;
                }
                return tableExists;
            }

            @Override
            public void createTable(
                    String project, String dataset, String table, TableDefinition tableDefinition) {
                createTableInvocations++;
                if (createTableError != null) {
                    throw createTableError;
                }
            }

            public int getTableExistsInvocatioks() {
                return tableExistsInvocations;
            }

            public int getCreateDatasetInvocatioks() {
                return createDatasetInvocations;
            }

            public int getCreateTableInvocatioks() {
                return createTableInvocations;
            }
        }

        static class FaultyIterator<T> implements Iterator<T> {

            private final Iterator<T> realIterator;
            private final Double errorPercentage;
            private final Random random = new Random();

            public FaultyIterator(Iterator<T> realIterator, Double errorPercentage) {
                this.realIterator = realIterator;
                Preconditions.checkState(
                        0 <= errorPercentage && errorPercentage <= 100,
                        "The error percentage should be between 0 and 100");
                this.errorPercentage = errorPercentage;
            }

            @Override
            public boolean hasNext() {
                return realIterator.hasNext();
            }

            @Override
            public T next() {
                if (random.nextDouble() * 100 < errorPercentage) {
                    throw new RuntimeException(
                            "Faulty iterator has failed, it will happen with a chance of: "
                                    + errorPercentage);
                }
                return realIterator.next();
            }

            @Override
            public void remove() {
                realIterator.remove();
            }

            @Override
            public void forEachRemaining(Consumer<? super T> action) {
                realIterator.forEachRemaining(action);
            }
        }

        /** Implementation of the server stream for testing purposes. */
        public static class FakeBigQueryServerStream
                implements BigQueryServices.BigQueryServerStream<ReadRowsResponse> {

            private final List<ReadRowsResponse> toReturn;
            private final Double errorPercentage;

            public FakeBigQueryServerStream(
                    SerializableFunction<RecordGenerationParams, List<GenericRecord>> dataGenerator,
                    String schema,
                    String dataPrefix,
                    Long size,
                    Long offset,
                    Double errorPercentage) {
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
                this.errorPercentage = errorPercentage;
            }

            @Override
            public Iterator<ReadRowsResponse> iterator() {
                return new FaultyIterator<>(toReturn.iterator(), errorPercentage);
            }

            @Override
            public void cancel() {}
        }

        /** Implementation of the storage read client for testing purposes. */
        public static class FakeBigQueryStorageReadClient implements StorageReadClient {

            private final ReadSession session;
            private final SerializableFunction<RecordGenerationParams, List<GenericRecord>>
                    dataGenerator;
            private final Double errorPercentage;

            public FakeBigQueryStorageReadClient(
                    ReadSession session,
                    SerializableFunction<RecordGenerationParams, List<GenericRecord>>
                            dataGenerator) {
                this(session, dataGenerator, 0D);
            }

            public FakeBigQueryStorageReadClient(
                    ReadSession session,
                    SerializableFunction<RecordGenerationParams, List<GenericRecord>> dataGenerator,
                    Double errorPercentage) {
                this.session = session;
                this.dataGenerator = dataGenerator;
                this.errorPercentage = errorPercentage;
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
                        request.getOffset(),
                        errorPercentage);
            }

            @Override
            public void close() {}
        }

        /** Implementation of the storage write client for testing purposes. */
        public static class FakeBigQueryStorageWriteClient implements StorageWriteClient {

            private final StreamWriter mockedWriter;
            private final WriteStream writeStream;
            private final FlushRowsResponse flushResponse;
            private final FinalizeWriteStreamResponse finalizeResponse;

            private int createWriteStreamInvocations;
            private int finalizeWriteStreamInvocations;

            public FakeBigQueryStorageWriteClient(AppendRowsResponse appendResponse) {
                mockedWriter = Mockito.mock(StreamWriter.class);
                Mockito.when(mockedWriter.append((ProtoRows) Mockito.any(), Mockito.anyLong()))
                        .thenReturn(ApiFutures.immediateFuture(appendResponse));
                writeStream = null;
                flushResponse = null;
                finalizeResponse = null;
            }

            public FakeBigQueryStorageWriteClient(
                    ApiFuture[] appendResponseFutures,
                    WriteStream writeStream,
                    FlushRowsResponse flushResponse,
                    FinalizeWriteStreamResponse finalizeResponse) {
                mockedWriter = Mockito.mock(StreamWriter.class);
                // Mockito cannot unbox "any()" for primitive types, throwing the dreaded
                // NullPointerException. Use primitive variants for argument matching.
                OngoingStubbing stubbing =
                        Mockito.when(
                                mockedWriter.append((ProtoRows) Mockito.any(), Mockito.anyLong()));

                if (appendResponseFutures.length == 0) {
                    stubbing.thenThrow(
                            new IllegalStateException(
                                    "Test should provide append response future if append is invoked"));
                }
                for (ApiFuture future : appendResponseFutures) {
                    stubbing = stubbing.thenReturn(future);
                }
                this.writeStream = writeStream;
                this.flushResponse = flushResponse;
                this.finalizeResponse = finalizeResponse;
                createWriteStreamInvocations = 0;
                finalizeWriteStreamInvocations = 0;
            }

            @Override
            public StreamWriter createStreamWriter(
                    String streamName,
                    ProtoSchema protoSchema,
                    boolean enableConnectionPool,
                    String traceId) {
                return mockedWriter;
            }

            @Override
            public WriteStream createWriteStream(String tablePath, WriteStream.Type streamType) {
                createWriteStreamInvocations++;
                assert streamType == WriteStream.Type.BUFFERED;
                if (writeStream == null) {
                    throw new RuntimeException("testing error scenario");
                }
                return writeStream;
            }

            @Override
            public FlushRowsResponse flushRows(String streamName, long offset) {
                if (flushResponse == null) {
                    throw new ApiException(
                            new RuntimeException("testing error scenario"),
                            new TestStatusCode(),
                            false);
                }
                return flushResponse;
            }

            @Override
            public FinalizeWriteStreamResponse finalizeWriteStream(String streamName) {
                finalizeWriteStreamInvocations++;
                if (finalizeResponse == null) {
                    throw new RuntimeException("testing error scenario");
                }
                return finalizeResponse;
            }

            @Override
            public void close() {
                Mockito.when(mockedWriter.isUserClosed()).thenReturn(true);
            }

            public int getCreateWriteStreamInvocations() {
                return createWriteStreamInvocations;
            }

            public int getFinalizeWriteStreamInvocations() {
                return finalizeWriteStreamInvocations;
            }

            public void verifytAppendWithOffsetInvocations(int expectedInvocations) {
                Mockito.verify(mockedWriter, Mockito.times(expectedInvocations))
                        .append((ProtoRows) Mockito.any(), Mockito.anyLong());
            }
        }

        static class TestStatusCode implements StatusCode {

            @Override
            public Code getCode() {
                return null;
            }

            @Override
            public Object getTransportCode() {
                return null;
            }
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
                    + " \"namespace\": \""
                    + SchemaTransform.DEFAULT_NAMESPACE
                    + "\",\n"
                    + " \"doc\": \"Translated Avro Schema for queryresultschema\",\n"
                    + SIMPLE_AVRO_SCHEMA_FIELDS_STRING
                    + "}";
    public static final Schema SIMPLE_AVRO_SCHEMA =
            new Schema.Parser().parse(SIMPLE_AVRO_SCHEMA_STRING);

    public static final TableSchema SIMPLE_BQ_TABLE_SCHEMA =
            new TableSchema()
                    .setFields(
                            Arrays.asList(
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
        return IntStream.range(0, genericRecords.size())
                .collect(
                        () -> new HashMap<Integer, List<GenericRecord>>(),
                        (map, idx) ->
                                map.computeIfAbsent(idx / 1024, key -> new ArrayList<>())
                                        .add(genericRecords.get(idx)),
                        (map1, map2) ->
                                map2.entrySet()
                                        .forEach(
                                                entry ->
                                                        map1.merge(
                                                                entry.getKey(),
                                                                entry.getValue(),
                                                                (list1, list2) -> {
                                                                    list1.addAll(list2);
                                                                    return list1;
                                                                })))
                .values().stream()
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
                                                                        outputStream
                                                                                .toByteArray())))
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
            Integer expectedRowCount,
            Integer expectedReadStreamCount,
            String avroSchemaString,
            Integer readLimit) {
        return createReadOptions(
                expectedRowCount,
                expectedReadStreamCount,
                avroSchemaString,
                params -> StorageClientFaker.createRecordList(params),
                0D,
                readLimit);
    }

    public static BigQueryReadOptions createReadOptions(
            Integer expectedRowCount, Integer expectedReadStreamCount, String avroSchemaString) {
        return createReadOptions(
                expectedRowCount,
                expectedReadStreamCount,
                avroSchemaString,
                params -> StorageClientFaker.createRecordList(params),
                0D,
                -1);
    }

    public static BigQueryReadOptions createReadOptions(
            Integer expectedRowCount,
            Integer expectedReadStreamCount,
            String avroSchemaString,
            SerializableFunction<RecordGenerationParams, List<GenericRecord>> dataGenerator) {
        return createReadOptions(
                expectedRowCount, expectedReadStreamCount, avroSchemaString, dataGenerator, 0D, -1);
    }

    public static BigQueryReadOptions createReadOptions(
            Integer expectedRowCount,
            Integer expectedReadStreamCount,
            String avroSchemaString,
            SerializableFunction<RecordGenerationParams, List<GenericRecord>> dataGenerator,
            Double errorPercentage) {
        return createReadOptions(
                expectedRowCount,
                expectedReadStreamCount,
                avroSchemaString,
                dataGenerator,
                errorPercentage,
                -1);
    }

    public static BigQueryReadOptions createReadOptions(
            Integer expectedRowCount,
            Integer expectedReadStreamCount,
            String avroSchemaString,
            SerializableFunction<RecordGenerationParams, List<GenericRecord>> dataGenerator,
            Double errorPercentage,
            Integer readLimit) {
        return BigQueryReadOptions.builder()
                .setSnapshotTimestampInMillis(Instant.now().toEpochMilli())
                .setLimit(readLimit)
                .setBigQueryConnectOptions(
                        BigQueryConnectOptions.builder()
                                .setDataset("dataset")
                                .setProjectId("project")
                                .setTable("table")
                                .setCredentialsOptions(null)
                                .setTestingBigQueryServices(
                                        () -> {
                                            return FakeBigQueryServices.getInstance(
                                                    new StorageClientFaker.FakeBigQueryServices
                                                            .FakeBigQueryStorageReadClient(
                                                            StorageClientFaker.fakeReadSession(
                                                                    expectedRowCount,
                                                                    expectedReadStreamCount,
                                                                    avroSchemaString),
                                                            dataGenerator,
                                                            errorPercentage),
                                                    null);
                                        })
                                .build())
                .build();
    }

    public static BigQueryReadOptions createReadAndWriteOptions(
            Integer expectedRowCount,
            Integer expectedReadStreamCount,
            String avroSchemaString,
            SerializableFunction<RecordGenerationParams, List<GenericRecord>> dataGenerator,
            AppendRowsResponse appendRowsResponse) {
        return createReadAndWriteOptions(
                expectedRowCount,
                expectedReadStreamCount,
                avroSchemaString,
                dataGenerator,
                appendRowsResponse,
                0D,
                -1);
    }

    public static BigQueryReadOptions createReadAndWriteOptions(
            Integer expectedRowCount,
            Integer expectedReadStreamCount,
            String avroSchemaString,
            SerializableFunction<RecordGenerationParams, List<GenericRecord>> dataGenerator,
            AppendRowsResponse appendRowsResponse,
            Double errorPercentage,
            Integer readLimit) {
        return BigQueryReadOptions.builder()
                .setSnapshotTimestampInMillis(Instant.now().toEpochMilli())
                .setLimit(readLimit)
                .setBigQueryConnectOptions(
                        BigQueryConnectOptions.builder()
                                .setDataset("dataset")
                                .setProjectId("project")
                                .setTable("table")
                                .setCredentialsOptions(null)
                                .setTestingBigQueryServices(
                                        () -> {
                                            return FakeBigQueryServices.getInstance(
                                                    new StorageClientFaker.FakeBigQueryServices
                                                            .FakeBigQueryStorageReadClient(
                                                            StorageClientFaker.fakeReadSession(
                                                                    expectedRowCount,
                                                                    expectedReadStreamCount,
                                                                    avroSchemaString),
                                                            dataGenerator,
                                                            errorPercentage),
                                                    new FakeBigQueryServices
                                                            .FakeBigQueryStorageWriteClient(
                                                            appendRowsResponse));
                                        })
                                .build())
                .build();
    }

    public static BigQueryReadOptions createTableReadOptions(
            Integer expectedRowCount,
            Integer expectedReadStreamCount,
            String avroSchemaString,
            SerializableFunction<RecordGenerationParams, List<GenericRecord>> dataGenerator) {
        return createTableReadOptions(
                expectedRowCount, expectedReadStreamCount, avroSchemaString, dataGenerator, 0D, -1);
    }

    public static BigQueryReadOptions createTableReadOptions(
            Integer expectedRowCount,
            Integer expectedReadStreamCount,
            String avroSchemaString,
            SerializableFunction<RecordGenerationParams, List<GenericRecord>> dataGenerator,
            Double errorPercentage,
            Integer readLimit) {
        return BigQueryReadOptions.builder()
                .setSnapshotTimestampInMillis(Instant.now().toEpochMilli())
                .setLimit(readLimit)
                .setBigQueryConnectOptions(
                        BigQueryConnectOptions.builder()
                                .setDataset("dataset")
                                .setProjectId("project")
                                .setTable("table")
                                .setCredentialsOptions(null)
                                .setTestingBigQueryServices(
                                        () -> {
                                            return FakeBigQueryTableServices.getInstance(
                                                    new StorageClientFaker.FakeBigQueryServices
                                                            .FakeBigQueryStorageReadClient(
                                                            StorageClientFaker.fakeReadSession(
                                                                    expectedRowCount,
                                                                    expectedReadStreamCount,
                                                                    avroSchemaString),
                                                            dataGenerator,
                                                            errorPercentage),
                                                    null);
                                        })
                                .build())
                .build();
    }

    public static BigQueryConnectOptions createConnectOptionsForWrite(
            AppendRowsResponse appendResponse) {
        FakeBigQueryServices.FakeBigQueryStorageWriteClient writeClient =
                new FakeBigQueryServices.FakeBigQueryStorageWriteClient(appendResponse);
        return createConnectOptions(null, writeClient);
    }

    public static BigQueryConnectOptions createConnectOptionsForWrite(
            ApiFuture[] appendResponseFutures,
            WriteStream writeStream,
            FlushRowsResponse flushResponse,
            FinalizeWriteStreamResponse finalizeResponse) {
        FakeBigQueryServices.FakeBigQueryStorageWriteClient writeClient =
                new FakeBigQueryServices.FakeBigQueryStorageWriteClient(
                        appendResponseFutures, writeStream, flushResponse, finalizeResponse);
        return createConnectOptions(null, writeClient);
    }

    public static BigQueryConnectOptions createConnectOptionsForQuery(
            boolean tableExists,
            RuntimeException tableExistsError,
            RuntimeException createDatasetError,
            RuntimeException createTableError) {
        FakeBigQueryServices.FakeQueryDataClient queryClient =
                new FakeBigQueryServices.FakeQueryDataClient(
                        tableExists, tableExistsError, createDatasetError, createTableError);
        return createConnectOptions(null, null, queryClient);
    }

    public static BigQueryConnectOptions createConnectOptionsForQuery(
            boolean datasetExists, String datasetRegion) {
        FakeBigQueryServices.FakeQueryDataClient queryClient =
                new FakeBigQueryServices.FakeQueryDataClient(datasetExists, datasetRegion);
        return createConnectOptions(null, null, queryClient);
    }

    public static BigQueryConnectOptions createConnectOptions(
            FakeBigQueryServices.FakeBigQueryStorageReadClient readClient,
            FakeBigQueryServices.FakeBigQueryStorageWriteClient writeClient,
            FakeBigQueryServices.FakeQueryDataClient queryClient) {
        return BigQueryConnectOptions.builder()
                .setDataset("dataset")
                .setProjectId("project")
                .setTable("table")
                .setCredentialsOptions(null)
                .setTestingBigQueryServices(
                        () -> {
                            return FakeBigQueryServices.getInstance(
                                    readClient, writeClient, queryClient);
                        })
                .build();
    }

    public static BigQueryConnectOptions createConnectOptions(
            FakeBigQueryServices.FakeBigQueryStorageReadClient readClient,
            FakeBigQueryServices.FakeBigQueryStorageWriteClient writeClient) {
        return BigQueryConnectOptions.builder()
                .setDataset("dataset")
                .setProjectId("project")
                .setTable("table")
                .setCredentialsOptions(null)
                .setTestingBigQueryServices(
                        () -> {
                            return FakeBigQueryServices.getInstance(readClient, writeClient);
                        })
                .build();
    }

    public static final TableSchema SIMPLE_BQ_TABLE_SCHEMA_TABLE =
            new TableSchema()
                    .setFields(
                            Arrays.asList(
                                    new TableFieldSchema()
                                            .setName("id")
                                            .setType("INTEGER")
                                            .setMode("REQUIRED"),
                                    new TableFieldSchema()
                                            .setName("optDouble")
                                            .setType("FLOAT")
                                            .setMode("NULLABLE"),
                                    new TableFieldSchema()
                                            .setName("optString")
                                            .setType("STRING")
                                            .setMode("NULLABLE"),
                                    new TableFieldSchema()
                                            .setName("ts")
                                            .setType("TIMESTAMP")
                                            .setMode("REQUIRED"),
                                    new TableFieldSchema()
                                            .setName("reqSubRecord")
                                            .setType("RECORD")
                                            .setMode("REQUIRED")
                                            .setFields(
                                                    Arrays.asList(
                                                            new TableFieldSchema()
                                                                    .setName("reqBoolean")
                                                                    .setMode("REQUIRED")
                                                                    .setType("BOOLEAN"),
                                                            new TableFieldSchema()
                                                                    .setName("reqTs")
                                                                    .setType("TIMESTAMP")
                                                                    .setMode("REQUIRED"))),
                                    new TableFieldSchema()
                                            .setName("optArraySubRecords")
                                            .setType("RECORD")
                                            .setMode("REPEATED")
                                            .setFields(
                                                    Arrays.asList(
                                                            new TableFieldSchema()
                                                                    .setName("reqLong")
                                                                    .setMode("REQUIRED")
                                                                    .setType("INTEGER"),
                                                            new TableFieldSchema()
                                                                    .setName("optBytes")
                                                                    .setType("BYTES")
                                                                    .setMode("NULLABLE")))));

    /** Implementation of {@link BigQueryServices} for testing Table API. */
    public static class FakeBigQueryTableServices implements BigQueryServices {

        private final FakeBigQueryServices.FakeBigQueryStorageReadClient storageReadClient;
        private final FakeBigQueryServices.FakeBigQueryStorageWriteClient storageWriteClient;
        private final FakeBigQueryServices.FakeQueryDataClient queryDataClient;

        private FakeBigQueryTableServices(
                FakeBigQueryServices.FakeBigQueryStorageReadClient storageReadClient,
                FakeBigQueryServices.FakeBigQueryStorageWriteClient storageWriteClient,
                FakeBigQueryServices.FakeQueryDataClient queryDataClient) {
            this.storageReadClient = storageReadClient;
            this.storageWriteClient = storageWriteClient;
            this.queryDataClient = queryDataClient;
        }

        static FakeBigQueryTableServices getInstance(
                FakeBigQueryServices.FakeBigQueryStorageReadClient storageReadClient,
                FakeBigQueryServices.FakeBigQueryStorageWriteClient storageWriteClient,
                FakeBigQueryServices.FakeQueryDataClient queryDataClient) {
            FakeBigQueryTableServices instance =
                    Mockito.spy(
                            new FakeBigQueryTableServices(
                                    storageReadClient, storageWriteClient, queryDataClient));
            return instance;
        }

        static FakeBigQueryTableServices getInstance(
                FakeBigQueryServices.FakeBigQueryStorageReadClient storageReadClient,
                FakeBigQueryServices.FakeBigQueryStorageWriteClient storageWriteClient) {
            return getInstance(
                    storageReadClient,
                    storageWriteClient,
                    (FakeBigQueryServices.FakeQueryDataClient)
                            FakeQueryTableDataClient.getInstance());
        }

        @Override
        public StorageReadClient createStorageReadClient(CredentialsOptions readOptions)
                throws IOException {
            return storageReadClient;
        }

        @Override
        public StorageWriteClient createStorageWriteClient(CredentialsOptions readOptions)
                throws IOException {
            return storageWriteClient;
        }

        @Override
        public QueryDataClient createQueryDataClient(CredentialsOptions readOptions) {
            return queryDataClient;
        }

        static class FakeQueryTableDataClient extends FakeBigQueryServices.FakeQueryDataClient {

            public FakeQueryTableDataClient(
                    boolean tableExists,
                    RuntimeException tableExistsError,
                    RuntimeException createDatasetError,
                    RuntimeException createTableError) {
                super(tableExists, tableExistsError, createDatasetError, createTableError);
            }

            static FakeQueryTableDataClient defaultTableInstance =
                    Mockito.spy(new FakeQueryTableDataClient(true, null, null, null));

            static QueryDataClient getInstance() {
                return defaultTableInstance;
            }

            @Override
            public TableSchema getTableSchema(String project, String dataset, String table) {
                return SIMPLE_BQ_TABLE_SCHEMA_TABLE;
            }
        }
    }
}
