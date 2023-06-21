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

package org.apache.flink.connector.bigquery.services;

import org.apache.flink.FlinkVersion;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.bigquery.common.config.CredentialsOptions;
import org.apache.flink.connector.bigquery.common.utils.SchemaTransform;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.UnaryCallSettings;
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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Implementation of the {@link BigQueryServices} interface that wraps the actual clients. */
@Internal
public class BigQueryServiceImpl implements BigQueryServices {

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
                            .setInitialRpcTimeout(org.threeten.bp.Duration.ofHours(2))
                            .setMaxRpcTimeout(org.threeten.bp.Duration.ofHours(2))
                            .setTotalTimeout(org.threeten.bp.Duration.ofHours(2))
                            .build());

            UnaryCallSettings.Builder<SplitReadStreamRequest, SplitReadStreamResponse>
                    splitReadStreamSettings =
                            settingsBuilder.getStubSettingsBuilder().splitReadStreamSettings();

            splitReadStreamSettings.setRetrySettings(
                    splitReadStreamSettings
                            .getRetrySettings()
                            .toBuilder()
                            .setInitialRpcTimeout(org.threeten.bp.Duration.ofSeconds(30))
                            .setMaxRpcTimeout(org.threeten.bp.Duration.ofSeconds(30))
                            .setTotalTimeout(org.threeten.bp.Duration.ofSeconds(30))
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

        public QueryDataClientImpl(CredentialsOptions options) {
            bigQuery =
                    BigQueryOptions.newBuilder()
                            .setCredentials(options.getCredentials())
                            .build()
                            .getService();
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
                                "Problems while trying to retrieve table partitions (table: %s.%s.%s).",
                                project, dataset, table),
                        ex);
            }
        }

        @Override
        public Optional<Tuple2<String, StandardSQLTypeName>> retrievePartitionColumnName(
                String project, String dataset, String table) {
            try {
                String query =
                        Lists.newArrayList(
                                        "SELECT",
                                        "  column_name, data_type",
                                        "FROM",
                                        String.format(
                                                "  `%s.%s.INFORMATION_SCHEMA.COLUMNS`",
                                                project, dataset),
                                        "WHERE",
                                        String.format(" table_catalog = '%s'", project),
                                        String.format(" AND table_schema = '%s'", dataset),
                                        String.format(" AND table_name = '%s'", table),
                                        " AND is_partitioning_column = 'YES';")
                                .stream()
                                .collect(Collectors.joining("\n"));

                QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

                TableResult results = bigQuery.query(queryConfig);

                return StreamSupport.stream(results.iterateAll().spliterator(), false)
                        .map(
                                row ->
                                        row.stream()
                                                .map(fValue -> fValue.getStringValue())
                                                .collect(Collectors.toList()))
                        .map(list -> new Tuple2<String, String>(list.get(0), list.get(1)))
                        .map(
                                t ->
                                        new Tuple2<String, StandardSQLTypeName>(
                                                t.f0, StandardSQLTypeName.valueOf(t.f1)))
                        .findFirst();
            } catch (Exception ex) {
                throw new RuntimeException(
                        String.format(
                                "Problems while trying to retrieve table partition's column name (table: %s.%s.%s).",
                                project, dataset, table),
                        ex);
            }
        }

        @Override
        public TableSchema getTableSchema(String project, String dataset, String table) {
            return SchemaTransform.bigQuerySchemaToTableSchema(
                    bigQuery.getTable(TableId.of(project, dataset, table))
                            .getDefinition()
                            .getSchema());
        }
    }
}
