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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.bigquery.common.config.CredentialsOptions;

import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/**
 * Interface defining the behavior to access and operate the needed BigQuery services. This
 * definitions should simplify the faking or mocking of the actual implementations when testing.
 */
@Internal
public interface BigQueryServices extends Serializable {

    /**
     * Retrieves a real, mock or fake {@link QueryDataClient}.
     *
     * @param credentialsOptions The options for the read operation.
     * @return a Query data client for BigQuery.
     */
    QueryDataClient getQueryDataClient(CredentialsOptions credentialsOptions);

    /**
     * Returns a real, mock, or fake {@link StorageReadClient}.
     *
     * @param credentialsOptions The options for the read operation.
     * @return a storage read client object.
     * @throws IOException
     */
    StorageReadClient getStorageClient(CredentialsOptions credentialsOptions) throws IOException;

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
         * @param request The request for the storage API
         * @return a server stream response with the read rows.
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

    /**
     * An interface representing the client interactions needed to retrieve data from BigQuery using
     * SQL queries.
     */
    interface QueryDataClient extends Serializable {
        /**
         * Returns a list with the table's existing partitions.
         *
         * @param project The GCP project.
         * @param dataset The BigQuery dataset.
         * @param table The BigQuery table.
         * @return A list of the partition identifiers.
         */
        List<String> retrieveTablePartitions(String project, String dataset, String table);

        /**
         * Returns, in case of having one, the partition column name of the table and its type.
         *
         * @param project The GCP project.
         * @param dataset The BigQuery dataset.
         * @param table The BigQuery table.
         * @return The partition column name and its type, if it has one.
         */
        Optional<Tuple2<String, StandardSQLTypeName>> retrievePartitionColumnName(
                String project, String dataset, String table);

        /**
         * Returns the {@link TableSchema} of the specified BigQuery table.
         *
         * @param project The GCP project.
         * @param dataset The BigQuery dataset.
         * @param table The BigQuery table.
         * @return The BigQuery table {@link TableSchema}.
         */
        TableSchema getTableSchema(String project, String dataset, String table);

        /**
         * Executes a BigQuery query and returns the information about the execution results
         * (including if succeeded of failed related information). No data is being returned by this
         * method, just a description of what happened with the execution.
         *
         * @param projectId The project where the query will be run.
         * @param query The query to run.
         * @return Possibly information of the query execution or empty when not run.
         */
        Optional<QueryResultInfo> runQuery(String projectId, String query);

        /**
         * Executes a BigQuery dry run for the provided query and return the job information.
         *
         * @param projectId The project where the query will be run.
         * @param query The query to run.
         * @return The dry run job's information.
         */
        Job dryRunQuery(String projectId, String query);
    }
}
