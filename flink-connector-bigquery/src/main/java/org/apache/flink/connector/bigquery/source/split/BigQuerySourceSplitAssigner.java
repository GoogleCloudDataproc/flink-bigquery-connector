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

package org.apache.flink.connector.bigquery.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.bigquery.common.config.BigQueryConnectOptions;
import org.apache.flink.connector.bigquery.services.BigQueryServices.StorageReadClient;
import org.apache.flink.connector.bigquery.services.BigQueryServicesFactory;
import org.apache.flink.connector.bigquery.source.config.BigQueryReadOptions;
import org.apache.flink.connector.bigquery.source.enumerator.BigQuerySourceEnumState;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableModifiers;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions;
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

    public void open() {
        LOG.info("BigQuery source split assigner is opening.");
        if (!initialized) {
            BigQueryConnectOptions connectionOptions = this.readOptions.getBigQueryConnectOptions();
            try (StorageReadClient client =
                    BigQueryServicesFactory.instance(connectionOptions)
                            .storageRead(connectionOptions.getCredentialsOptions())) {
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
                // get all the stream names added to the initialized state
                remainingTableStreams.addAll(
                        session.getStreamsList().stream()
                                .map(stream -> stream.getName())
                                .collect(Collectors.toList()));
                initialized = true;
            } catch (IOException ex) {
                throw new RuntimeException("Problems creating the BigQuery read client", ex);
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