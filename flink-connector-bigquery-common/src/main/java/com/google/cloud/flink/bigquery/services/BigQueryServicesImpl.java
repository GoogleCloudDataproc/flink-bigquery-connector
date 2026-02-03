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

import org.apache.flink.FlinkVersion;
import org.apache.flink.annotation.Internal;
import org.apache.flink.util.StringUtils;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.UnaryCallSettings;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamResponse;
import com.google.cloud.bigquery.storage.v1.FlushRowsRequest;
import com.google.cloud.bigquery.storage.v1.FlushRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.SplitReadStreamRequest;
import com.google.cloud.bigquery.storage.v1.SplitReadStreamResponse;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import com.google.cloud.flink.bigquery.common.utils.BigQueryPartitionUtils;
import com.google.cloud.flink.bigquery.common.utils.BigQueryTableInfo;
import com.google.cloud.flink.bigquery.services.TablePartitionInfo.PartitionType;
import com.google.protobuf.Int64Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Implementation of the {@link BigQueryServices} interface that wraps the actual clients. */
@Internal
public class BigQueryServicesImpl implements BigQueryServices {

    public static final int ALREADY_EXISTS_ERROR_CODE = 409;

    public static final String TRACE_ID_FORMAT = "Flink:%s_%s";

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryServicesImpl.class);

    private static final HeaderProvider USER_AGENT_HEADER_PROVIDER =
            FixedHeaderProvider.create(
                    "User-Agent", "flink-bigquery-connector/" + FlinkVersion.current().toString());

    private static final String FLINK_VERSION = FlinkVersion.current().toString();

    @Override
    public StorageReadClient createStorageReadClient(CredentialsOptions credentialsOptions)
            throws IOException {
        return new StorageReadClientImpl(credentialsOptions);
    }

    @Override
    public StorageWriteClient createStorageWriteClient(CredentialsOptions credentialsOptions)
            throws IOException {
        return new StorageWriteClientImpl(credentialsOptions);
    }

    @Override
    public QueryDataClient createQueryDataClient(CredentialsOptions creadentialsOptions) {
        return new QueryDataClientImpl(creadentialsOptions);
    }

    /**
     * A simple implementation that wraps a BigQuery ServerStream.
     *
     * @param <T> The type of the underlying streamed data.
     */
    public static class BigQueryServerStreamImpl<T> implements BigQueryServerStream<T> {

        private final ServerStream<T> serverStream;

        public BigQueryServerStreamImpl(ServerStream<T> serverStream) {
            this.serverStream = serverStream;
        }

        @Override
        public Iterator<T> iterator() {
            return serverStream.iterator();
        }

        @Override
        public void cancel() {
            serverStream.cancel();
        }
    }

    /** Implementation of a BigQuery read client wrapper. */
    public static class StorageReadClientImpl implements StorageReadClient {
        private final BigQueryReadClient client;
        private final String quotaProjectId;

        private StorageReadClientImpl(CredentialsOptions options) throws IOException {
            quotaProjectId = options.getQuotaProjectId();

            BigQueryReadSettings.Builder settingsBuilder =
                    BigQueryReadSettings.newBuilder()
                            .setCredentialsProvider(
                                    FixedCredentialsProvider.create(options.getCredentials()))
                            .setHeaderProvider(USER_AGENT_HEADER_PROVIDER)
                            .setTransportChannelProvider(
                                    BigQueryReadSettings.defaultGrpcTransportProviderBuilder()
                                            .build());

            if (quotaProjectId != null) {
                settingsBuilder = settingsBuilder.setQuotaProjectId(quotaProjectId);
            }

            UnaryCallSettings.Builder<CreateReadSessionRequest, ReadSession>
                    createReadSessionSettings =
                            settingsBuilder.getStubSettingsBuilder().createReadSessionSettings();

            createReadSessionSettings.setRetrySettings(
                    createReadSessionSettings
                            .getRetrySettings()
                            .toBuilder()
                            .setInitialRpcTimeout(Duration.ofHours(2))
                            .setMaxRpcTimeout(Duration.ofHours(2))
                            .setTotalTimeout(Duration.ofHours(2))
                            .build());

            UnaryCallSettings.Builder<SplitReadStreamRequest, SplitReadStreamResponse>
                    splitReadStreamSettings =
                            settingsBuilder.getStubSettingsBuilder().splitReadStreamSettings();

            splitReadStreamSettings.setRetrySettings(
                    splitReadStreamSettings
                            .getRetrySettings()
                            .toBuilder()
                            .setInitialRpcTimeout(Duration.ofSeconds(30))
                            .setMaxRpcTimeout(Duration.ofSeconds(30))
                            .setTotalTimeout(Duration.ofSeconds(30))
                            .build());

            this.client = BigQueryReadClient.create(settingsBuilder.build());
        }

        @Override
        public ReadSession createReadSession(CreateReadSessionRequest request) {
            CreateReadSessionRequest updatedRequest =
                    BigQueryUtils.updateWithQuotaProject(request, quotaProjectId);
            return client.createReadSession(updatedRequest);
        }

        @Override
        public BigQueryServerStream<ReadRowsResponse> readRows(ReadRowsRequest request) {
            return new BigQueryServerStreamImpl<>(client.readRowsCallable().call(request));
        }

        @Override
        public void close() {
            client.close();
        }
    }

    /** Implementation of a BigQuery write client wrapper. */
    public static class StorageWriteClientImpl implements StorageWriteClient {
        private final BigQueryWriteClient client;

        private StorageWriteClientImpl(CredentialsOptions options) throws IOException {
            BigQueryWriteSettings.Builder settingsBuilder =
                    BigQueryWriteSettings.newBuilder()
                            .setCredentialsProvider(
                                    FixedCredentialsProvider.create(options.getCredentials()))
                            .setHeaderProvider(USER_AGENT_HEADER_PROVIDER)
                            .setTransportChannelProvider(
                                    BigQueryReadSettings.defaultGrpcTransportProviderBuilder()
                                            .build());

            String quotaProjectId = options.getQuotaProjectId();
            if (quotaProjectId != null) {
                settingsBuilder = settingsBuilder.setQuotaProjectId(quotaProjectId);
            }

            UnaryCallSettings.Builder<CreateWriteStreamRequest, WriteStream>
                    createWriteStreamSettings =
                            settingsBuilder.getStubSettingsBuilder().createWriteStreamSettings();
            createWriteStreamSettings.setRetrySettings(
                    createWriteStreamSettings
                            .getRetrySettings()
                            .toBuilder()
                            .setMaxAttempts(5)
                            .setInitialRpcTimeout(Duration.ofSeconds(60))
                            .setRpcTimeoutMultiplier(1.2)
                            .setInitialRetryDelay(Duration.ofSeconds(2))
                            .setRetryDelayMultiplier(1.2)
                            .build());

            UnaryCallSettings.Builder<FlushRowsRequest, FlushRowsResponse> flushRowsSettings =
                    settingsBuilder.getStubSettingsBuilder().flushRowsSettings();
            flushRowsSettings.setRetrySettings(
                    flushRowsSettings
                            .getRetrySettings()
                            .toBuilder()
                            .setMaxAttempts(5)
                            .setInitialRpcTimeout(Duration.ofSeconds(30))
                            .setRpcTimeoutMultiplier(1)
                            .setInitialRetryDelay(Duration.ofSeconds(1))
                            .setRetryDelayMultiplier(1.2)
                            .build());

            UnaryCallSettings.Builder<FinalizeWriteStreamRequest, FinalizeWriteStreamResponse>
                    finalizeWriteStreamSettings =
                            settingsBuilder.getStubSettingsBuilder().finalizeWriteStreamSettings();
            finalizeWriteStreamSettings.setRetrySettings(
                    finalizeWriteStreamSettings
                            .getRetrySettings()
                            .toBuilder()
                            .setMaxAttempts(5)
                            .setInitialRpcTimeout(Duration.ofSeconds(30))
                            .setRpcTimeoutMultiplier(1)
                            .setInitialRetryDelay(Duration.ofSeconds(1))
                            .setRetryDelayMultiplier(1.2)
                            .build());

            this.client = BigQueryWriteClient.create(settingsBuilder.build());
        }

        @Override
        public StreamWriter createStreamWriter(
                String streamName,
                ProtoSchema protoSchema,
                boolean enableConnectionPool,
                String traceId)
                throws IOException {
            /**
             * Enable client lib automatic retries on request level errors.
             *
             * <p>Immediate Retry code: ABORTED, UNAVAILABLE, CANCELLED, INTERNAL,
             * DEADLINE_EXCEEDED.
             *
             * <p>Back-off Retry code: RESOURCE_EXHAUSTED.
             */
            RetrySettings retrySettings =
                    RetrySettings.newBuilder()
                            .setMaxAttempts(5) // maximum number of retries
                            .setTotalTimeout(
                                    Duration.ofMinutes(5)) // total duration of retry process
                            .setInitialRpcTimeout(Duration.ofSeconds(30)) // intial timeout
                            .setMaxRpcTimeout(Duration.ofMinutes(2)) // maximum RPC timeout
                            .setRpcTimeoutMultiplier(1.6) // change in RPC timeout
                            .setRetryDelayMultiplier(1.6) // change in delay before next retry
                            .setInitialRetryDelay(
                                    Duration.ofMillis(1250)) // delay before first retry
                            .setMaxRetryDelay(Duration.ofSeconds(5)) // maximum delay before retry
                            .build();
            return StreamWriter.newBuilder(streamName, client)
                    .setEnableConnectionPool(enableConnectionPool)
                    .setTraceId(traceId)
                    .setRetrySettings(retrySettings)
                    .setWriterSchema(protoSchema)
                    .build();
        }

        @Override
        public WriteStream createWriteStream(String tablePath, WriteStream.Type streamType) {
            return this.client.createWriteStream(
                    CreateWriteStreamRequest.newBuilder()
                            .setParent(tablePath)
                            .setWriteStream(WriteStream.newBuilder().setType(streamType).build())
                            .build());
        }

        @Override
        public FlushRowsResponse flushRows(String streamName, long offset) {
            return this.client.flushRows(
                    FlushRowsRequest.newBuilder()
                            .setWriteStream(streamName)
                            .setOffset(Int64Value.of(offset))
                            .build());
        }

        @Override
        public FinalizeWriteStreamResponse finalizeWriteStream(String streamName) {
            return client.finalizeWriteStream(streamName);
        }

        @Override
        public void close() {
            client.close();
        }
    }

    /** A wrapper implementation for the BigQuery service client library methods. */
    public static class QueryDataClientImpl implements QueryDataClient {
        private final BigQuery bigQuery;
        private final Bigquery bigquery;

        public QueryDataClientImpl(CredentialsOptions options) {
            BigQueryOptions.Builder bigQueryBuilder =
                    BigQueryOptions.newBuilder()
                            .setCredentials(options.getCredentials())
                            .setHeaderProvider(USER_AGENT_HEADER_PROVIDER);
            String quotaProjectId = options.getQuotaProjectId();
            if (quotaProjectId != null) {
                bigQueryBuilder = bigQueryBuilder.setQuotaProjectId(quotaProjectId);
            }

            bigQuery = bigQueryBuilder.build().getService();
            // It is not possible to set the quota project on the legacy
            // bigquery client without modifying the credentials, so the
            // configured quota project is ignored by this client.
            bigquery = BigQueryUtils.newBigqueryBuilder(options).build();
        }

        @Override
        public List<String> retrieveTablePartitions(String project, String dataset, String table) {
            try {
                String query =
                        Arrays.asList(
                                        "SELECT",
                                        "  partition_id",
                                        "FROM",
                                        String.format(
                                                "  `%s.%s.INFORMATION_SCHEMA.PARTITIONS`",
                                                project, dataset),
                                        "WHERE",
                                        " partition_id <> '__STREAMING_UNPARTITIONED__'",
                                        String.format(" AND table_catalog = '%s'", project),
                                        String.format(" AND table_schema = '%s'", dataset),
                                        String.format(" AND table_name = '%s'", table),
                                        "ORDER BY 1 ASC;")
                                .stream()
                                .collect(Collectors.joining("\n"));

                QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

                TableResult results = bigQuery.query(queryConfig);

                List<String> result =
                        StreamSupport.stream(results.iterateAll().spliterator(), false)
                                .flatMap(row -> row.stream())
                                .map(fValue -> fValue.getStringValue())
                                .collect(Collectors.toList());
                LOG.info("Table partitions: {}", result);
                return result;
            } catch (Exception ex) {
                throw new RuntimeException(
                        String.format(
                                "Problems while trying to retrieve table partitions"
                                        + " (table: %s.%s.%s).",
                                project, dataset, table),
                        ex);
            }
        }

        @Override
        public Optional<TablePartitionInfo> retrievePartitionColumnInfo(
                String project, String dataset, String table) {
            try {
                // TODO: tableInfo API may not convey if it's a BigQuery native table or a Biglake
                // external table. Presently, the connector is expected to be used with native BQ
                // tables only. However, in case external tables need to be supported in future,
                // then consider using the com.google.cloud.bigquery.BigQuery.getTable API.
                Table tableInfo = BigQueryUtils.tableInfo(bigquery, project, dataset, table);

                if (tableInfo.getRangePartitioning() == null
                        && tableInfo.getTimePartitioning() == null) {
                    return Optional.empty();
                }
                Instant bqStreamingBufferOldestEntryTime =
                        Optional.ofNullable(tableInfo.getStreamingBuffer())
                                .map(sbuffer -> sbuffer.getOldestEntryTime().longValue())
                                .map(millisFromEpoch -> Instant.ofEpochMilli(millisFromEpoch))
                                .orElse(Instant.MAX);
                return Optional.ofNullable(tableInfo.getTimePartitioning())
                        .map(
                                tp ->
                                        Optional.of(
                                                new TablePartitionInfo(
                                                        tp.getField(),
                                                        PartitionType.valueOf(tp.getType()),
                                                        BigQueryPartitionUtils
                                                                .retrievePartitionColumnType(
                                                                        tableInfo.getSchema(),
                                                                        tp.getField()),
                                                        bqStreamingBufferOldestEntryTime)))
                        .orElseGet(
                                () ->
                                        Optional.of(
                                                new TablePartitionInfo(
                                                        tableInfo.getRangePartitioning().getField(),
                                                        PartitionType.INT_RANGE,
                                                        StandardSQLTypeName.INT64,
                                                        bqStreamingBufferOldestEntryTime)));
            } catch (Exception ex) {
                throw new RuntimeException(
                        String.format(
                                "Problems while trying to retrieve table partition's"
                                        + " column name (table: %s.%s.%s).",
                                project, dataset, table),
                        ex);
            }
        }

        @Override
        public TableSchema getTableSchema(String project, String dataset, String table) {
            return BigQueryTableInfo.getSchema(bigQuery, project, dataset, table);
        }

        @Override
        public Dataset getDataset(String project, String dataset) {
            DatasetId datasetId = DatasetId.of(project, dataset);
            return bigQuery.getDataset(datasetId);
        }

        @Override
        public void createDataset(String project, String dataset, String region) {
            DatasetInfo.Builder datasetInfo = DatasetInfo.newBuilder(project, dataset);
            if (!StringUtils.isNullOrWhitespaceOnly(region)) {
                datasetInfo.setLocation(region);
            }
            bigQuery.create(datasetInfo.build());
        }

        @Override
        public Boolean tableExists(String projectName, String datasetName, String tableName) {
            com.google.cloud.bigquery.Table table =
                    bigQuery.getTable(TableId.of(projectName, datasetName, tableName));
            return (table != null && table.exists());
        }

        @Override
        public void createTable(
                String project, String dataset, String table, TableDefinition tableDefinition) {
            TableId tableId = TableId.of(project, dataset, table);
            TableInfo tableInfo = TableInfo.of(tableId, tableDefinition);
            bigQuery.create(tableInfo);
        }
    }

    public static final String generateTraceId(String suffix) {
        return String.format(TRACE_ID_FORMAT, FLINK_VERSION, suffix);
    }
}
