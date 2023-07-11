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

package com.google.cloud.flink.bigquery.source.split;

import org.apache.flink.annotation.Internal;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableModifiers;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import com.google.cloud.flink.bigquery.services.QueryResultInfo;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.source.enumerator.BigQuerySourceEnumState;
import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

/** A simple split assigner based on the BigQuery {@link ReadSession} streams. */
@Internal
public class BigQuerySourceSplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySourceSplitAssigner.class);

    private final BigQueryReadOptions readOptions;

    private final ArrayDeque<String> remainingTableStreams;
    private final List<String> alreadyProcessedTableStreams;
    private final ArrayDeque<BigQuerySourceSplit> remainingSourceSplits;
    private final Map<String, BigQuerySourceSplit> assignedSourceSplits;
    private boolean initialized;

    public BigQuerySourceSplitAssigner(
            BigQueryReadOptions readOptions, BigQuerySourceEnumState sourceEnumState) {
        this.readOptions = readOptions;
        this.remainingTableStreams = new ArrayDeque<>(sourceEnumState.getRemaniningTableStreams());
        this.alreadyProcessedTableStreams = sourceEnumState.getCompletedTableStreams();
        this.remainingSourceSplits = new ArrayDeque<>(sourceEnumState.getRemainingSourceSplits());
        this.assignedSourceSplits = sourceEnumState.getAssignedSourceSplits();
        this.initialized = sourceEnumState.isInitialized();
    }

    /**
     * Reviews the read options argument and see if a query has been configured, in that case run
     * that query and then return a modified version of the connect options pointing to the
     * temporary location (project, dataset and table) of the query results.
     *
     * @return The BigQuery connect options with the right project, dataset and table given the
     *     specified configuration.
     */
    BigQueryConnectOptions checkOptionsAndRunQueryIfNeededReturningModifiedOptions() {
        return Optional.ofNullable(this.readOptions.getQuery())
                // if query is available, execute it using the configured GCP project and gather the
                // results
                .flatMap(
                        query ->
                                BigQueryServicesFactory.instance(
                                                this.readOptions.getBigQueryConnectOptions())
                                        .queryClient()
                                        .runQuery(
                                                this.readOptions.getQueryExecutionProject(), query))
                // with the query results return the new connection options, fail if the query
                // failed
                .map(
                        result -> {
                            if (result.getStatus().equals(QueryResultInfo.Status.FAILED)) {
                                throw new IllegalStateException(
                                        "The BigQuery query execution failed with errors: "
                                                + result.getErrorMessages()
                                                        .orElse(Lists.newArrayList()));
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
                        })
                // in case no query configured, just return the configured options.
                .orElse(this.readOptions.getBigQueryConnectOptions());
    }

    public void open() {
        LOG.info("BigQuery source split assigner is opening.");
        if (!initialized) {
            BigQueryConnectOptions connectionOptions =
                    checkOptionsAndRunQueryIfNeededReturningModifiedOptions();
            try (BigQueryServices.StorageReadClient client =
                    BigQueryServicesFactory.instance(connectionOptions).storageRead()) {
                String parent = String.format("projects/%s", connectionOptions.getProjectId());

                String srcTable =
                        String.format(
                                "projects/%s/datasets/%s/tables/%s",
                                connectionOptions.getProjectId(),
                                connectionOptions.getDataset(),
                                connectionOptions.getTable());

                // We specify the columns to be projected by adding them to the selected fields,
                // and set a simple filter to restrict which rows are transmitted.
                TableReadOptions.Builder optionsBuilder = TableReadOptions.newBuilder();

                readOptions
                        .getColumnNames()
                        .forEach(name -> optionsBuilder.addSelectedFields(name));
                optionsBuilder.setRowRestriction(readOptions.getRowRestriction());

                TableReadOptions options = optionsBuilder.build();

                // Start specifying the read session we want created.
                ReadSession.Builder sessionBuilder =
                        ReadSession.newBuilder()
                                .setTable(srcTable)
                                .setDataFormat(DataFormat.AVRO)
                                .setReadOptions(options);

                // Optionally specify the snapshot time.  When unspecified, snapshot time is "now".
                if (readOptions.getSnapshotTimestampInMillis() != null) {
                    Timestamp t =
                            Timestamp.newBuilder()
                                    .setSeconds(readOptions.getSnapshotTimestampInMillis() / 1000)
                                    .setNanos(
                                            (int)
                                                    ((readOptions.getSnapshotTimestampInMillis()
                                                                    % 1000)
                                                            * 1000000))
                                    .build();
                    TableModifiers modifiers =
                            TableModifiers.newBuilder().setSnapshotTime(t).build();
                    sessionBuilder.setTableModifiers(modifiers);
                }

                // Begin building the session creation request.
                CreateReadSessionRequest.Builder builder =
                        CreateReadSessionRequest.newBuilder()
                                .setParent(parent)
                                .setReadSession(sessionBuilder)
                                .setMaxStreamCount(readOptions.getMaxStreamCount());

                // request the session
                ReadSession session = client.createReadSession(builder.build());
                LOG.info(
                        "BigQuery Storage Read session, name: {},"
                                + " estimated row count {}, estimated scanned bytes {},"
                                + " streams count {}, expired time {} (seconds after epoch).",
                        session.getName(),
                        session.getEstimatedRowCount(),
                        session.getEstimatedTotalBytesScanned(),
                        session.getStreamsCount(),
                        session.getExpireTime().getSeconds());
                // get all the stream names added to the initialized state
                remainingTableStreams.addAll(
                        session.getStreamsList().stream()
                                .map(stream -> stream.getName())
                                .collect(Collectors.toList()));
                initialized = true;
            } catch (IOException ex) {
                throw new RuntimeException(
                        "Problems creating the BigQuery Storage Read session.", ex);
            }
        }
    }

    public void addSplitsBack(List<BigQuerySourceSplit> splits) {
        for (BigQuerySourceSplit split : splits) {
            remainingSourceSplits.add((BigQuerySourceSplit) split);
            // we should remove the add-backed splits from the assigned list,
            // because they are failed
            assignedSourceSplits.remove(split.splitId());
        }
    }

    public BigQuerySourceEnumState snapshotState(long checkpointId) {
        return new BigQuerySourceEnumState(
                Lists.newArrayList(remainingTableStreams),
                alreadyProcessedTableStreams,
                Lists.newArrayList(remainingSourceSplits),
                assignedSourceSplits,
                initialized);
    }

    public void close() {
        // so far not much to be done here
        LOG.info("BigQuery source split assigner is closed.");
    }

    public Optional<BigQuerySourceSplit> getNext() {
        if (!remainingSourceSplits.isEmpty()) {
            // return remaining splits firstly
            BigQuerySourceSplit split = remainingSourceSplits.poll();
            assignedSourceSplits.put(split.splitId(), split);
            return Optional.of(split);
        } else {
            // it's turn for next collection
            String nextStream = remainingTableStreams.poll();
            if (nextStream != null) {
                BigQuerySourceSplit split = new BigQuerySourceSplit(nextStream);
                remainingSourceSplits.add(split);
                alreadyProcessedTableStreams.add(nextStream);
                return getNext();
            } else {
                return Optional.empty();
            }
        }
    }

    public boolean noMoreSplits() {
        checkState(initialized, "The noMoreSplits method was called but not initialized.");
        return remainingTableStreams.isEmpty() && remainingSourceSplits.isEmpty();
    }
}
