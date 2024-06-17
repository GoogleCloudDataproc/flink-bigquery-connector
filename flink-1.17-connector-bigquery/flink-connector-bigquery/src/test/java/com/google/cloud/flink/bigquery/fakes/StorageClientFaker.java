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

import com.google.api.core.ApiFutures;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.JobStatistics2;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.AvroRows;
import com.google.cloud.bigquery.storage.v1.AvroSchema;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.cloud.bigquery.storage.v1.StreamStats;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import com.google.cloud.flink.bigquery.common.utils.BigQueryPartitionUtils;
import com.google.cloud.flink.bigquery.common.utils.SchemaTransform;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.PartitionIdWithInfoAndStatus;
import com.google.cloud.flink.bigquery.services.QueryResultInfo;
import com.google.cloud.flink.bigquery.services.TablePartitionInfo;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.protobuf.ByteString;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.RandomData;
import org.mockito.Mockito;

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

        static volatile FakeBigQueryServices instance = null;
        static final Object LOCK = new Object();

        private final FakeBigQueryStorageReadClient storageReadClient;
        private final FakeBigQueryStorageWriteClient storageWriteClient;

        private FakeBigQueryServices(
                FakeBigQueryStorageReadClient storageReadClient,
                FakeBigQueryStorageWriteClient storageWriteClient) {
            this.storageReadClient = storageReadClient;
            this.storageWriteClient = storageWriteClient;
        }

        static FakeBigQueryServices getInstance(
                FakeBigQueryStorageReadClient storageReadClient,
                FakeBigQueryStorageWriteClient storageWriteClient) {
            if (instance == null) {
                synchronized (LOCK) {
                    if (instance == null) {
                        instance =
                                Mockito.spy(
                                        new FakeBigQueryServices(
                                                storageReadClient, storageWriteClient));
                    }
                }
            }
            return instance;
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
            return FakeQueryDataClient.getInstance();
        }

        static class FakeQueryDataClient implements QueryDataClient {

            static FakeQueryDataClient instance = Mockito.spy(new FakeQueryDataClient());

            static QueryDataClient getInstance() {
                return instance;
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
                                BigQueryPartitionUtils.PartitionType.HOUR,
                                StandardSQLTypeName.TIMESTAMP,
                                Instant.now()));
            }

            @Override
            public TableSchema getTableSchema(String project, String dataset, String table) {
                return SIMPLE_BQ_TABLE_SCHEMA;
            }

            @Override
            public Optional<QueryResultInfo> runQuery(String projectId, String query) {
                return Optional.of(
                        QueryResultInfo.succeed("some-project", "some-dataset", "some-table"));
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

            @Override
            public List<PartitionIdWithInfoAndStatus> retrievePartitionsStatus(
                    String project, String dataset, String table) {
                return retrieveTablePartitions(project, dataset, table).stream()
                        .map(
                                pId ->
                                        new PartitionIdWithInfoAndStatus(
                                                pId,
                                                retrievePartitionColumnInfo(project, dataset, table)
                                                        .get(),
                                                BigQueryPartitionUtils.PartitionStatus.COMPLETED))
                        .collect(Collectors.toList());
            }
        }

        static class FaultyIterator<T> implements Iterator<T> {

            private final Iterator<T> realIterator;
            private final Double errorPercentage;
            private final Random random = new Random(42);

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

        /** Implementation for the storage read client for testing purposes. */
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

        /** Implementation for the storage write client for testing purposes. */
        public static class FakeBigQueryStorageWriteClient implements StorageWriteClient {

            private final StreamWriter mockedWriter;

            public FakeBigQueryStorageWriteClient(AppendRowsResponse appendResponse) {
                mockedWriter = Mockito.mock(StreamWriter.class);
                Mockito.when(mockedWriter.append(Mockito.any()))
                        .thenReturn(ApiFutures.immediateFuture(appendResponse));
            }

            @Override
            public StreamWriter createStreamWriter(
                    String streamName, ProtoSchema protoSchema, boolean enableConnectionPool) {
                return mockedWriter;
            }

            @Override
            public void close() {
                Mockito.when(mockedWriter.isUserClosed()).thenReturn(true);
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
            Integer readLimit)
            throws IOException {
        return createReadOptions(
                expectedRowCount,
                expectedReadStreamCount,
                avroSchemaString,
                params -> StorageClientFaker.createRecordList(params),
                0D,
                readLimit);
    }

    public static BigQueryReadOptions createReadOptions(
            Integer expectedRowCount, Integer expectedReadStreamCount, String avroSchemaString)
            throws IOException {
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
            SerializableFunction<RecordGenerationParams, List<GenericRecord>> dataGenerator)
            throws IOException {
        return createReadOptions(
                expectedRowCount, expectedReadStreamCount, avroSchemaString, dataGenerator, 0D, -1);
    }

    public static BigQueryReadOptions createReadOptions(
            Integer expectedRowCount,
            Integer expectedReadStreamCount,
            String avroSchemaString,
            SerializableFunction<RecordGenerationParams, List<GenericRecord>> dataGenerator,
            Double errorPercentage)
            throws IOException {
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
            Integer readLimit)
            throws IOException {
        return BigQueryReadOptions.builder()
                .setSnapshotTimestampInMillis(Instant.now().toEpochMilli())
                .setLimit(readLimit)
                .setQuery("SELECT 1")
                .setQueryExecutionProject("some-gcp-project")
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

    public static BigQueryReadOptions createTableReadOptions(
            Integer expectedRowCount,
            Integer expectedReadStreamCount,
            String avroSchemaString,
            Integer readLimit)
            throws IOException {
        return createTableReadOptions(
                expectedRowCount,
                expectedReadStreamCount,
                avroSchemaString,
                params -> StorageClientFaker.createRecordList(params),
                0D,
                readLimit);
    }

    public static BigQueryReadOptions createTableReadOptions(
            Integer expectedRowCount, Integer expectedReadStreamCount, String avroSchemaString)
            throws IOException {
        return createTableReadOptions(
                expectedRowCount,
                expectedReadStreamCount,
                avroSchemaString,
                params -> StorageClientFaker.createRecordList(params),
                0D,
                -1);
    }

    public static BigQueryReadOptions createTableReadOptions(
            Integer expectedRowCount,
            Integer expectedReadStreamCount,
            String avroSchemaString,
            SerializableFunction<RecordGenerationParams, List<GenericRecord>> dataGenerator)
            throws IOException {
        return createTableReadOptions(
                expectedRowCount, expectedReadStreamCount, avroSchemaString, dataGenerator, 0D, -1);
    }

    public static BigQueryReadOptions createTableReadOptions(
            Integer expectedRowCount,
            Integer expectedReadStreamCount,
            String avroSchemaString,
            SerializableFunction<RecordGenerationParams, List<GenericRecord>> dataGenerator,
            Double errorPercentage)
            throws IOException {
        return createTableReadOptions(
                expectedRowCount,
                expectedReadStreamCount,
                avroSchemaString,
                dataGenerator,
                errorPercentage,
                -1);
    }

    public static BigQueryReadOptions createTableReadOptions(
            Integer expectedRowCount,
            Integer expectedReadStreamCount,
            String avroSchemaString,
            SerializableFunction<RecordGenerationParams, List<GenericRecord>> dataGenerator,
            Double errorPercentage,
            Integer readLimit)
            throws IOException {
        return BigQueryReadOptions.builder()
                .setSnapshotTimestampInMillis(Instant.now().toEpochMilli())
                .setLimit(readLimit)
                .setQuery("SELECT 1")
                .setQueryExecutionProject("some-gcp-project")
                .setBigQueryConnectOptions(
                        BigQueryConnectOptions.builder()
                                .setDataset("dataset")
                                .setProjectId("project")
                                .setTable("table")
                                .setCredentialsOptions(null)
                                .setTestingBigQueryServices(
                                        () -> {
                                            return FakeBigQueryTableServices.getInstance(
                                                    new StorageClientFaker.FakeBigQueryTableServices
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
            AppendRowsResponse appendResponse) throws IOException {
        return BigQueryConnectOptions.builder()
                .setDataset("dataset")
                .setProjectId("project")
                .setTable("table")
                .setCredentialsOptions(null)
                .setTestingBigQueryServices(
                        () -> {
                            return FakeBigQueryServices.getInstance(
                                    null,
                                    new StorageClientFaker.FakeBigQueryServices
                                            .FakeBigQueryStorageWriteClient(appendResponse));
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

        static volatile FakeBigQueryTableServices instance = null;
        static final Object LOCK = new Object();

        private final FakeBigQueryStorageReadClient storageReadClient;
        private final FakeBigQueryStorageWriteClient storageWriteClient;

        private FakeBigQueryTableServices(
                FakeBigQueryStorageReadClient storageReadClient,
                FakeBigQueryStorageWriteClient storageWriteClient) {
            this.storageReadClient = storageReadClient;
            this.storageWriteClient = storageWriteClient;
        }

        static FakeBigQueryTableServices getInstance(
                FakeBigQueryStorageReadClient storageReadClient,
                FakeBigQueryStorageWriteClient storageWriteClient) {
            if (instance == null) {
                synchronized (LOCK) {
                    if (instance == null) {
                        instance =
                                Mockito.spy(
                                        new FakeBigQueryTableServices(
                                                storageReadClient, storageWriteClient));
                    }
                }
            }
            return instance;
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
            return FakeQueryTableDataClient.getInstance();
        }

        static class FakeQueryTableDataClient extends FakeBigQueryServices.FakeQueryDataClient {

            static FakeQueryTableDataClient instance = Mockito.spy(new FakeQueryTableDataClient());

            static QueryDataClient getInstance() {
                return instance;
            }

            @Override
            public TableSchema getTableSchema(String project, String dataset, String table) {
                return SIMPLE_BQ_TABLE_SCHEMA_TABLE;
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
                return new FakeBigQueryServices.FaultyIterator<>(
                        toReturn.iterator(), errorPercentage);
            }

            @Override
            public void cancel() {}
        }

        /** Implementation for the storage read client for testing purposes. */
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

        /** Implementation for the storage write client for testing purposes. */
        public static class FakeBigQueryStorageWriteClient implements StorageWriteClient {

            private final StreamWriter mockedWriter;

            public FakeBigQueryStorageWriteClient(AppendRowsResponse appendResponse) {
                mockedWriter = Mockito.mock(StreamWriter.class);
                Mockito.when(mockedWriter.append(Mockito.any()))
                        .thenReturn(ApiFutures.immediateFuture(appendResponse));
            }

            @Override
            public StreamWriter createStreamWriter(
                    String streamName, ProtoSchema protoSchema, boolean enableConnectionPool) {
                return mockedWriter;
            }

            @Override
            public void close() {
                Mockito.when(mockedWriter.isUserClosed()).thenReturn(true);
            }
        }
    }
}
