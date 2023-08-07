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

import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.annotation.Internal;

/**
 * In charge of discover read splits (or read stream in BigQuery realm) to be processed. It can be
 * set to discover only once (Bounded use case) or to periodically check (Unbounded use case).
 */
@Internal
public class SplitDiscoverer {
    private static final Logger LOG = LoggerFactory.getLogger(SplitDiscoverer.class);

    public static List<String> discoverSplits(
            BigQueryConnectOptions connectionOptions,
            DataFormat format,
            List<String> columnNames,
            String rowRestriction,
            Long snapshotTimeInMillis,
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
                            .setReadOptions(options);

            // Optionally specify the snapshot time.  When unspecified, snapshot time is "now".
            if (snapshotTimeInMillis != null) {
                Timestamp t =
                        Timestamp.newBuilder()
                                .setSeconds(snapshotTimeInMillis / 1000)
                                .setNanos((int) ((snapshotTimeInMillis % 1000) * 1000000))
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
                            + " streams count {}, expired time {} (seconds after epoch).",
                    session.getName(),
                    session.getEstimatedRowCount(),
                    session.getEstimatedTotalBytesScanned(),
                    session.getStreamsCount(),
                    session.getExpireTime().getSeconds());
            // get all the stream names added to the initialized state
            return session.getStreamsList().stream()
                    .map(stream -> stream.getName())
                    .collect(Collectors.toList());
        } catch (IOException ex) {
            throw new RuntimeException("Problems creating the BigQuery Storage Read session.", ex);
        }
    }
}
