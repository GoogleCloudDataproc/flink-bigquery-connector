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
import org.apache.flink.connector.bigquery.common.config.CredentialsOptions;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.UnaryCallSettings;
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

/** Implementation of the {@link BigQueryServices} interface that wraps the actual clients. */
@Internal
public class BigQueryServiceImpl implements BigQueryServices {

    @Override
    public StorageReadClient getStorageClient(CredentialsOptions credentialsOptions)
            throws IOException {
        return new StorageReadClientImpl(credentialsOptions);
    }

    /**
     * A simple implementation of a mocked GRPC server stream.
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
}
