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

package com.google.cloud.flink.bigquery.source.split.assigner;

import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.utils.flink.annotations.Internal;
import com.google.cloud.flink.bigquery.common.utils.flink.core.Preconditions;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import com.google.cloud.flink.bigquery.services.QueryResultInfo;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.source.enumerator.BigQuerySourceEnumState;
import com.google.cloud.flink.bigquery.source.split.SplitDiscoverer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Optional;

/**
 * A bounded implementation for a split assigner based on the BigQuery {@link ReadSession} streams.
 */
@Internal
public class BoundedSplitAssigner extends BigQuerySourceSplitAssigner {
    private static final Logger LOG = LoggerFactory.getLogger(BoundedSplitAssigner.class);

    BoundedSplitAssigner(BigQueryReadOptions readOptions, BigQuerySourceEnumState sourceEnumState) {
        super(readOptions, sourceEnumState);
    }

    @Override
    public void discoverSplits() {
        BigQueryConnectOptions connectionOptions =
                fetchOptionsFromQueryRun().orElse(this.readOptions.getBigQueryConnectOptions());

        this.remainingTableStreams.addAll(
                SplitDiscoverer.discoverSplits(
                        connectionOptions,
                        DataFormat.AVRO,
                        this.readOptions.getColumnNames(),
                        this.readOptions.getRowRestriction(),
                        this.readOptions.getSnapshotTimestampInMillis(),
                        this.readOptions.getMaxStreamCount()));
    }

    /**
     * Reviews the read options argument and see if a query has been configured, in that case run
     * that query and then return a modified version of the connect options pointing to the
     * temporary location (project, dataset and table) of the query results.
     *
     * @return The BigQuery connect options with the right project, dataset and table given the
     *     specified configuration.
     */
    Optional<BigQueryConnectOptions> fetchOptionsFromQueryRun() {
        return this.readOptions
                .getQuery()
                // if query is available, execute it using the configured GCP project and gather the
                // results
                .flatMap(query -> runQuery(query))
                // with the query results return the new connection options, fail if the query
                // failed
                .map(
                        result -> {
                            if (result.getStatus().equals(QueryResultInfo.Status.FAILED)) {
                                throw new IllegalStateException(
                                        "The BigQuery query execution failed with errors: "
                                                + result.getErrorMessages()
                                                        .orElse(new ArrayList<>()));
                            }
                            String projectId = result.getDestinationProject().get();
                            String dataset = result.getDestinationDataset().get();
                            String table = result.getDestinationTable().get();
                            LOG.info(
                                    "After BigQuery query execution, switching connect options"
                                            + " to read from table {}.{}.{}",
                                    projectId,
                                    dataset,
                                    table);
                            return this.readOptions
                                    .getBigQueryConnectOptions()
                                    .toBuilder()
                                    .setProjectId(projectId)
                                    .setDataset(dataset)
                                    .setTable(table)
                                    .build();
                        });
    }

    private Optional<QueryResultInfo> runQuery(String query) {
        return this.readOptions
                .getQueryExecutionProject()
                .flatMap(
                        gcpProject ->
                                BigQueryServicesFactory.instance(
                                                this.readOptions.getBigQueryConnectOptions())
                                        .queryClient()
                                        .runQuery(gcpProject, query));
    }

    @Override
    public boolean noMoreSplits() {
        Preconditions.checkState(
                initialized, "The noMoreSplits method was called but not initialized.");
        return remainingTableStreams.isEmpty() && remainingSourceSplits.isEmpty();
    }
}
