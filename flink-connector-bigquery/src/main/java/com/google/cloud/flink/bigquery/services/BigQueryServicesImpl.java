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

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.UnaryCallSettings;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.SplitReadStreamRequest;
import com.google.cloud.bigquery.storage.v1.SplitReadStreamResponse;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import com.google.cloud.flink.bigquery.common.utils.BigQueryPartition;
import com.google.cloud.flink.bigquery.common.utils.SchemaTransform;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Implementation of the {@link BigQueryServices} interface that wraps the actual clients. */
@Internal
public class BigQueryServicesImpl implements BigQueryServices {

    @Override
    public StorageReadClient getStorageClient(CredentialsOptions credentialsOptions)
            throws IOException {
        return new StorageReadClientImpl(credentialsOptions);
    }

    @Override
    public QueryDataClient getQueryDataClient(CredentialsOptions creadentialsOptions) {
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

    /** A simple implementation of a mocked BigQuery read client wrapper. */
    public static class StorageReadClientImpl implements StorageReadClient {
        private static final HeaderProvider USER_AGENT_HEADER_PROVIDER =
                FixedHeaderProvider.create(
                        "user-agent", "Apache_Flink_Java/" + FlinkVersion.current().toString());

        private final BigQueryReadClient client;

        private StorageReadClientImpl(CredentialsOptions options) throws IOException {
            BigQueryReadSettings.Builder settingsBuilder =
                    BigQueryReadSettings.newBuilder()
                            .setCredentialsProvider(
                                    FixedCredentialsProvider.create(options.getCredentials()))
                            .setTransportChannelProvider(
                                    BigQueryReadSettings.defaultGrpcTransportProviderBuilder()
                                            .setHeaderProvider(USER_AGENT_HEADER_PROVIDER)
                                            .build());

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
            return client.createReadSession(request);
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

    /** A wrapper implementation for the BigQuery service client library methods. */
    public static class QueryDataClientImpl implements QueryDataClient {
        private final BigQuery bigQuery;
        private final Bigquery bigquery;

        public QueryDataClientImpl(CredentialsOptions options) {
            bigQuery =
                    BigQueryOptions.newBuilder()
                            .setCredentials(options.getCredentials())
                            .build()
                            .getService();
            bigquery = BigQueryUtils.newBigqueryBuilder(options).build();
        }

        @Override
        public List<String> retrieveTablePartitions(String project, String dataset, String table) {
            try {
                String query =
                        Lists.newArrayList(
                                        "SELECT",
                                        "  partition_id",
                                        "FROM",
                                        String.format(
                                                "  `%s.%s.INFORMATION_SCHEMA.PARTITIONS`",
                                                project, dataset),
                                        "WHERE",
                                        " partition_id <> '__STREAMING_UNPARTITIONED__'",
                                        String.format(" table_catalog = '%s'", project),
                                        String.format(" AND table_schema = '%s'", dataset),
                                        String.format(" AND table_name = '%s'", table),
                                        "ORDER BY 1 DESC;")
                                .stream()
                                .collect(Collectors.joining("\n"));

                QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

                TableResult results = bigQuery.query(queryConfig);

                return StreamSupport.stream(results.iterateAll().spliterator(), false)
                        .flatMap(row -> row.stream())
                        .map(fValue -> fValue.getStringValue())
                        .collect(Collectors.toList());
            } catch (Exception ex) {
                throw new RuntimeException(
                        String.format(
                                "Problems while trying to retrieve table partitions"
                                        + " (table: %s.%s.%s).",
                                project, dataset, table),
                        ex);
            }
        }

        public List<PartitionIdWithInfoAndStatus> retrievePartitionsStatus(
                String project, String dataset, String table) {
            try {
                return retrievePartitionColumnInfo(project, dataset, table)
                        .map(
                                info ->
                                        info
                                                .toPartitionsWithInfo(
                                                        retrieveTablePartitions(
                                                                project, dataset, table))
                                                .stream()
                                                .map(
                                                        pInfo ->
                                                                BigQueryPartition
                                                                        .checkPartitionCompleted(
                                                                                pInfo))
                                                .collect(Collectors.toList()))
                        .orElse(Lists.newArrayList());
            } catch (Exception ex) {
                throw new RuntimeException(
                        String.format(
                                "Problems while trying to retrieve table partitions status"
                                        + " (table: %s.%s.%s).",
                                project, dataset, table),
                        ex);
            }
        }

        @Override
        public Optional<TablePartitionInfo> retrievePartitionColumnInfo(
                String project, String dataset, String table) {
            try {
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
                                                        BigQueryPartition.PartitionType.valueOf(
                                                                tp.getType()),
                                                        BigQueryPartition
                                                                .retrievePartitionColumnType(
                                                                        tableInfo.getSchema(),
                                                                        tp.getField()),
                                                        bqStreamingBufferOldestEntryTime)))
                        .orElseGet(
                                () ->
                                        Optional.of(
                                                new TablePartitionInfo(
                                                        tableInfo.getRangePartitioning().getField(),
                                                        BigQueryPartition.PartitionType.INT_RANGE,
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
            return Optional.ofNullable(bigQuery.getTable(TableId.of(project, dataset, table)))
                    .map(t -> t.getDefinition().getSchema())
                    .map(schema -> SchemaTransform.bigQuerySchemaToTableSchema(schema))
                    .orElseThrow(
                            () ->
                                    new IllegalArgumentException(
                                            String.format(
                                                    "The provided table %s.%s.%s does not exists.",
                                                    project, dataset, table)));
        }

        @Override
        public Job dryRunQuery(String projectId, String query) {
            try {
                JobConfigurationQuery queryConfiguration =
                        new JobConfigurationQuery()
                                .setQuery(query)
                                .setUseQueryCache(true)
                                .setUseLegacySql(false);
                /** first we need to execute a dry-run to understand the expected query location. */
                return BigQueryUtils.dryRunQuery(bigquery, projectId, queryConfiguration, null);
            } catch (Exception ex) {
                throw new RuntimeException(
                        "Problems occurred while trying to dry-run a BigQuery query job.", ex);
            }
        }

        @Override
        public Optional<QueryResultInfo> runQuery(String projectId, String query) {
            try {
                JobConfigurationQuery queryConfiguration =
                        new JobConfigurationQuery()
                                .setQuery(query)
                                .setUseQueryCache(true)
                                .setUseLegacySql(false);
                /** first we need to execute a dry-run to understand the expected query location. */
                Job dryRun =
                        BigQueryUtils.dryRunQuery(bigquery, projectId, queryConfiguration, null);

                if (dryRun.getStatus().getErrors() != null) {
                    return Optional.of(dryRun.getStatus().getErrors())
                            .map(errors -> processErrorMessages(errors))
                            .map(errors -> QueryResultInfo.failed(errors));
                }
                List<TableReference> referencedTables =
                        dryRun.getStatistics().getQuery().getReferencedTables();
                TableReference firstTable = referencedTables.get(0);
                Dataset dataset =
                        BigQueryUtils.datasetInfo(
                                bigquery, firstTable.getProjectId(), firstTable.getDatasetId());

                /**
                 * Then we run the query and check the results to provide errors or a set of
                 * project, dataset and table to be read.
                 */
                Job job =
                        BigQueryUtils.runQuery(
                                bigquery, projectId, queryConfiguration, dataset.getLocation());

                TableReference queryDestTable =
                        job.getConfiguration().getQuery().getDestinationTable();

                return Optional.of(
                        Optional.ofNullable(job.getStatus())
                                .flatMap(s -> Optional.ofNullable(s.getErrors()))
                                .map(errors -> processErrorMessages(errors))
                                .map(errors -> QueryResultInfo.failed(errors))
                                .orElse(
                                        QueryResultInfo.succeed(
                                                queryDestTable.getProjectId(),
                                                queryDestTable.getDatasetId(),
                                                queryDestTable.getTableId())));
            } catch (Exception ex) {
                throw new RuntimeException(
                        "Problems occurred while trying to run a BigQuery query job.", ex);
            }
        }

        static List<String> processErrorMessages(List<ErrorProto> errors) {
            return errors.stream()
                    .map(
                            error ->
                                    String.format(
                                            "Message: '%s'," + " reason: '%s'," + " location: '%s'",
                                            error.getMessage(),
                                            error.getReason(),
                                            error.getLocation()))
                    .collect(Collectors.toList());
        }
    }
}
