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

import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.exceptions.BigQueryConnectorException;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * In charge of discovering read splits (or read stream in BigQuery realm) to be processed. It can
 * be set to discover only once (Bounded use case) or to periodically check (Unbounded use case).
 */
@Internal
public class SplitDiscoverer {
    private static final Logger LOG = LoggerFactory.getLogger(SplitDiscoverer.class);

    /**
     * Given the provided connection options, the format, columns, restriction, snapshot time and
     * the max expected stream count, this method will create a BigQuery read session, extract the
     * read stream ids in a list and return it.
     *
     * @param connectionOptions The BigQuery connection options
     * @param format The format of the data returned by BigQuery
     * @param columnNames The names of the columns expected in the result set
     * @param rowRestriction The row restriction applied for the read session
     * @param snapshotTimeInMillis The storage snapshot time in millis since epoch, if null is
     *     considered as now.
     * @param maxStreamCount The max stream count required, -1 let BigQuery decide the stream count.
     * @return A list with the read stream identifiers.
     */
    public static List<String> discoverSplits(
            BigQueryConnectOptions connectionOptions,
            DataFormat format,
            List<String> columnNames,
            String rowRestriction,
            Optional<Long> snapshotTimeInMillis,
            Integer maxStreamCount) {
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
            ReadSession.TableReadOptions.Builder optionsBuilder =
                    ReadSession.TableReadOptions.newBuilder();

            columnNames.forEach(name -> optionsBuilder.addSelectedFields(name));
            optionsBuilder.setRowRestriction(rowRestriction);

            ReadSession.TableReadOptions options = optionsBuilder.build();

            // Start specifying the read session we want created.
            ReadSession.Builder sessionBuilder =
                    ReadSession.newBuilder()
                            .setTable(srcTable)
                            .setDataFormat(format)
                            //                            .setTraceId(TRACE_ID)  // Set Trace ID for
                            // Read API once support is added.
                            .setReadOptions(options);

            // Optionally specify the snapshot time.  When unspecified, snapshot time is "now".
            if (snapshotTimeInMillis.isPresent()) {
                long snapshotTimestampInMs = snapshotTimeInMillis.get();
                Timestamp t =
                        Timestamp.newBuilder()
                                .setSeconds(snapshotTimestampInMs / 1000)
                                .setNanos((int) ((snapshotTimestampInMs % 1000) * 1000000))
                                .build();
                ReadSession.TableModifiers modifiers =
                        ReadSession.TableModifiers.newBuilder().setSnapshotTime(t).build();
                sessionBuilder.setTableModifiers(modifiers);
            }

            // Begin building the session creation request.
            CreateReadSessionRequest.Builder builder =
                    CreateReadSessionRequest.newBuilder()
                            .setParent(parent)
                            .setReadSession(sessionBuilder)
                            .setMaxStreamCount(maxStreamCount);

            // request the session
            ReadSession session = client.createReadSession(builder.build());
            LOG.info(
                    "BigQuery Storage Read session, name: {},"
                            + " estimated row count {}, estimated scanned bytes {},"
                            + " streams count {}, expired time {} (seconds after epoch), "
                            + " format: {}, column names: {}, row restriction: \"{}\","
                            + " snapshot time: {}, max stream count: {}.",
                    session.getName(),
                    session.getEstimatedRowCount(),
                    session.getEstimatedTotalBytesScanned(),
                    session.getStreamsCount(),
                    session.getExpireTime().getSeconds(),
                    format,
                    columnNames,
                    rowRestriction,
                    snapshotTimeInMillis,
                    maxStreamCount);
            // get all the stream names added to the initialized state
            return session.getStreamsList().stream()
                    .map(stream -> stream.getName())
                    .collect(Collectors.toList());
        } catch (IOException ex) {
            throw new BigQueryConnectorException(
                    "Problems creating the BigQuery Storage Read session.", ex);
        }
    }
}
