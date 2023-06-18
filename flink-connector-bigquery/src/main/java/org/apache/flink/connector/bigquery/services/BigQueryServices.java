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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.bigquery.common.config.CredentialsOptions;

import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;

import java.io.IOException;
import java.io.Serializable;

/**
 * Interface defining the behavior to access and operate the needed BigQuery services. This
 * definitions should simplify the faking or mocking of the actual implementations when testing.
 */
@Internal
public interface BigQueryServices extends Serializable {
    /**
     * Returns a real, mock, or fake {@link StorageClient}.
     *
     * @param readOptions The options for the read operation.
     * @return a storage read client object.
     * @throws java.io.IOException
     */
    StorageReadClient getStorageClient(CredentialsOptions readOptions) throws IOException;

    /**
     * Container for reading data from streaming endpoints.
     *
     * <p>An implementation does not need to be thread-safe.
     *
     * @param <T> The type of the response.
     */
    interface BigQueryServerStream<T> extends Iterable<T>, Serializable {
        /**
         * Cancels the stream, releasing any client- and server-side resources. This method may be
         * called multiple times and from any thread.
         */
        void cancel();
    }

    /** An interface representing a client object for making calls to the BigQuery Storage API. */
    interface StorageReadClient extends AutoCloseable {
        /**
         * Create a new read session against an existing table.This method variant collects request
         * count metric, table id in the request.
         *
         * @param request
         * @return
         */
        ReadSession createReadSession(CreateReadSessionRequest request);

        /**
         * Read rows in the context of a specific read stream.
         *
         * @param request
         * @return
         */
        BigQueryServerStream<ReadRowsResponse> readRows(ReadRowsRequest request);

        /**
         * Close the client object.
         *
         * <p>The override is required since {@link AutoCloseable} allows the close method to raise
         * an exception.
         */
        @Override
        void close();
    }
}
