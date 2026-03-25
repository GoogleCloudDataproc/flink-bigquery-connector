/*
 * Copyright (C) 2024 Google Inc.
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

package com.google.cloud.flink.bigquery.sink;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.file.sink.writer.FileWriterBucketState;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.SupportsPostCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.sink.committer.BigQueryLoadJobOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

/**
 * BigQuery sink using indirect writes (GCS files + load jobs).
 *
 * <p>This sink writes data as Avro files to GCS using Flink's {@link FileSink} and then submits
 * BigQuery load jobs in the post-commit topology to load the data into the destination table.
 *
 * <p>The pipeline is:
 *
 * <pre>
 * RowData
 *   -&gt; FileSink's FileWriter (BulkWriter.Factory uses RowDataToAvroConverters)
 *   -&gt; [Checkpoint]
 *   -&gt; FileSink's FileCommitter (finalizes .inprogress -&gt; final GCS files)
 *   -&gt; PostCommitTopology: BigQueryLoadJobOperator
 *        -&gt; Aggregates committed files per (subtask, checkpoint)
 *        -&gt; Submits BigQuery load job(s) with deterministic job IDs
 *        -&gt; Cleans up GCS files
 * </pre>
 *
 * <p>The sink provides EXACTLY_ONCE delivery guarantee.
 *
 * <p>Requires the GCS Hadoop connector plugin ({@code flink-gs-fs-hadoop}) to be installed in the
 * Flink cluster's {@code plugins/} directory.
 */
public final class BigQueryIndirectSink<IN>
        implements Sink<IN>,
                SupportsWriterState<IN, FileWriterBucketState>,
                SupportsCommitter<FileSinkCommittable>,
                SupportsPostCommitTopology<FileSinkCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryIndirectSink.class);

    private final BigQueryConnectOptions connectOptions;
    private final FileSink<IN> fileSink;

    public BigQueryIndirectSink(final BigQueryIndirectSinkConfig<IN> config) {
        this.connectOptions = config.getConnectOptions();

        // Build GCS path for temporary files
        final String gcsBasePath =
                config.getGcsTempPath()
                        + "/"
                        + String.format(
                                "%s/%s/%s",
                                connectOptions.getProjectId(),
                                connectOptions.getDataset(),
                                connectOptions.getTable());

        LOG.info(
                "Creating BigQueryIndirectSink writing to GCS path: {} for table: {}.{}.{}",
                gcsBasePath,
                connectOptions.getProjectId(),
                connectOptions.getDataset(),
                connectOptions.getTable());

        this.fileSink =
                FileSink.forBulkFormat(new Path(gcsBasePath), config.getBulkWriterFactory())
                        .withRollingPolicy(SizeBasedCheckpointRollingPolicy.build())
                        .withBucketAssigner(new BasePathBucketAssigner<>())
                        .build();
    }

    @Override
    public SinkWriter<IN> createWriter(final WriterInitContext context) throws IOException {
        return fileSink.createWriter(context);
    }

    @Override
    public StatefulSinkWriter<IN, FileWriterBucketState> restoreWriter(
            final WriterInitContext context, final Collection<FileWriterBucketState> recoveredState)
            throws IOException {
        return fileSink.restoreWriter(context, recoveredState);
    }

    @Override
    public Committer<FileSinkCommittable> createCommitter(final CommitterInitContext context)
            throws IOException {
        return fileSink.createCommitter(context);
    }

    @Override
    public SimpleVersionedSerializer<FileSinkCommittable> getCommittableSerializer() {
        return fileSink.getCommittableSerializer();
    }

    @Override
    public SimpleVersionedSerializer<FileWriterBucketState> getWriterStateSerializer() {
        return fileSink.getWriterStateSerializer();
    }

    @Override
    public void addPostCommitTopology(
            final DataStream<CommittableMessage<FileSinkCommittable>> committables) {
        committables
                .flatMap(
                        new BigQueryLoadJobOperator(
                                connectOptions.getProjectId(),
                                connectOptions.getDataset(),
                                connectOptions.getTable(),
                                connectOptions))
                .setParallelism(1);
    }
}
