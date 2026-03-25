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

package com.google.cloud.flink.bigquery.sink.committer;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.util.Collector;

import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.exceptions.BigQueryConnectorException;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/** Collects committed GCS file paths per checkpoint and submits BigQuery load jobs. */
public class BigQueryLoadJobOperator
        extends RichFlatMapFunction<CommittableMessage<FileSinkCommittable>, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryLoadJobOperator.class);

    private final String projectId;
    private final String dataset;
    private final String table;
    @Nullable private final BigQueryConnectOptions connectOptions;

    /**
     * Generated during job graph construction, and thus it persists across Flink restarts on
     * failure.
     */
    private final String uuid;

    private transient BigQueryServices.QueryDataClient queryClient;
    private transient String sanitizedJobName;
    private transient int subtaskIndex;

    /** Checkpoint-grouped files. Maps checkpointId to its tracking state. */
    private transient Map<Long, CheckpointState> checkpoints;

    /** Tracks completion state for a single checkpoint's committable messages. */
    private static class CheckpointState {
        int numberOfSubtasks;
        int summariesReceived;
        int totalExpectedCommittables;
        int committablesReceived;
        final List<GcsFileInfo> files = new ArrayList<>();

        boolean isComplete() {
            return summariesReceived == numberOfSubtasks
                    && committablesReceived == totalExpectedCommittables;
        }
    }

    public BigQueryLoadJobOperator(
            final String projectId,
            final String dataset,
            final String table,
            @Nullable final BigQueryConnectOptions connectOptions) {
        this.projectId = projectId;
        this.dataset = dataset;
        this.table = table;
        this.connectOptions = connectOptions;
        this.uuid = UUID.randomUUID().toString().replace("-", "");
    }

    /** Package-private constructor for testing. */
    BigQueryLoadJobOperator(
            final String projectId,
            final String dataset,
            final String table,
            final BigQueryConnectOptions connectOptions,
            final String uuid) {
        this.projectId = projectId;
        this.dataset = dataset;
        this.table = table;
        this.connectOptions = connectOptions;
        this.uuid = uuid;
    }

    @Override
    public void open(final OpenContext openContext) throws Exception {
        this.checkpoints = new HashMap<>();
        this.queryClient = BigQueryServicesFactory.instance(connectOptions).queryClient();
        this.sanitizedJobName = sanitizeJobName(getRuntimeContext().getJobInfo().getJobName());
        this.subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        LOG.info(
                "BigQueryLoadJobOperator opened: uuid={}, sanitizedJobName={}, subtaskIndex={}, "
                        + "table={}.{}.{}",
                uuid,
                sanitizedJobName,
                subtaskIndex,
                projectId,
                dataset,
                table);
    }

    @Override
    public void flatMap(
            final CommittableMessage<FileSinkCommittable> value, final Collector<Void> out)
            throws Exception {
        long checkpointId = -1;

        if (value instanceof CommittableSummary) {
            final CommittableSummary<FileSinkCommittable> summary =
                    (CommittableSummary<FileSinkCommittable>) value;
            checkpointId = summary.getCheckpointId();
            final CheckpointState state =
                    checkpoints.computeIfAbsent(checkpointId, k -> new CheckpointState());
            state.numberOfSubtasks = summary.getNumberOfSubtasks();
            state.summariesReceived++;
            state.totalExpectedCommittables += summary.getNumberOfCommittables();
            LOG.debug(
                    "Received CommittableSummary for checkpoint {}: numberOfSubtasks={}, "
                            + "numberOfCommittables={}, summariesReceived={}, "
                            + "totalExpectedCommittables={}",
                    checkpointId,
                    summary.getNumberOfSubtasks(),
                    summary.getNumberOfCommittables(),
                    state.summariesReceived,
                    state.totalExpectedCommittables);
        } else if (value instanceof CommittableWithLineage) {
            final CommittableWithLineage<FileSinkCommittable> lineage =
                    (CommittableWithLineage<FileSinkCommittable>) value;
            checkpointId = lineage.getCheckpointId();
            final CheckpointState state =
                    checkpoints.computeIfAbsent(checkpointId, k -> new CheckpointState());
            state.committablesReceived++;

            final FileSinkCommittable committable = lineage.getCommittable();
            if (committable.hasPendingFile()) {
                final org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter
                                .PendingFileRecoverable
                        pendingFile = committable.getPendingFile();
                final Path filePath = pendingFile.getPath();
                if (filePath != null) {
                    final String uri = filePath.toUri().toString();
                    final long size = pendingFile.getSize();
                    state.files.add(new GcsFileInfo(uri, size));
                    LOG.debug(
                            "Collected file {} ({} bytes) for checkpoint {}",
                            uri,
                            size,
                            checkpointId);
                }
            }
        }

        // Submit load jobs as soon as the checkpoint is complete
        if (checkpointId >= 0) {
            final CheckpointState state = checkpoints.get(checkpointId);
            if (state != null && state.isComplete()) {
                submitCompleteCheckpoints();
            }
        }
    }

    /** Submits load jobs for all complete checkpoints, in checkpoint ID order. */
    private void submitCompleteCheckpoints() throws InterruptedException {
        final List<Long> sortedCheckpointIds = new ArrayList<>(checkpoints.keySet());
        Collections.sort(sortedCheckpointIds);

        for (final long checkpointId : sortedCheckpointIds) {
            final CheckpointState state = checkpoints.get(checkpointId);
            if (state.isComplete()) {
                if (!state.files.isEmpty()) {
                    submitLoadJobForCheckpoint(checkpointId, state.files);
                }
                checkpoints.remove(checkpointId);
            }
        }
    }

    /** Submits a load job for a single checkpoint's files. */
    private void submitLoadJobForCheckpoint(final long checkpointId, final List<GcsFileInfo> files)
            throws InterruptedException {

        final long totalBytes = files.stream().mapToLong(GcsFileInfo::getSizeInBytes).sum();
        LOG.info(
                "Processing {} GCS files ({} bytes) for checkpoint {} BigQuery load job",
                files.size(),
                totalBytes,
                checkpointId);

        final TableId finalTableId = TableId.of(projectId, dataset, table);
        final String jobId = generateJobId(checkpointId);
        final List<String> gcsUris =
                files.stream().map(GcsFileInfo::getUri).collect(Collectors.toList());

        LOG.info("Processing load job {} with {} files", jobId, files.size());

        final JobConfiguration config =
                LoadJobConfiguration.newBuilder(finalTableId, gcsUris)
                        .setFormatOptions(FormatOptions.avro())
                        .setUseAvroLogicalTypes(true)
                        .build();

        findOrSubmitJob(jobId, config);

        // Best-effort cleanup of GCS files
        cleanupGcsFiles(files);
    }

    /** Finds an existing successful job or submits a new one. Throws on failure — no retries. */
    private void findOrSubmitJob(final String jobId, final JobConfiguration config)
            throws InterruptedException {

        final Job job;
        final Job maybeExistingJob = queryClient.getJob(projectId, jobId);
        if (maybeExistingJob == null) {
            LOG.info("Submitting load job {}", jobId);
            job = queryClient.submitJob(projectId, jobId, config);
        } else {
            job = maybeExistingJob;
        }

        final Job completedJob = queryClient.waitForJob(job);
        if (completedJob.getStatus().getError() != null) {
            throw new BigQueryConnectorException(
                    String.format(
                            "Load job %s failed: %s", jobId, completedJob.getStatus().getError()));
        } else {
            LOG.info("Load job {} completed successfully", jobId);
        }
    }

    /**
     * Generate a job ID for load jobs.
     *
     * <p>BigQuery job IDs are globally unique and immutable: once a job ID completes, resubmitting
     * the same ID is a no-op (BigQuery returns the existing result rather than running a new job).
     * We use this to guarantee exactly-once writes in indirect mode; jobIds are generated
     * deterministically as a function of flink job name + UUID + checkpoint ID + subtask index.
     *
     * <p>Subtask index is needed in case the user has multiple BigQuery indirect sinks in their
     * Flink job.
     */
    String generateJobId(final long checkpointId) {
        return String.format(
                "flink_bq_load_%s_%s_c%d_s%d", sanitizedJobName, uuid, checkpointId, subtaskIndex);
    }

    /** Delete GCS files after successful load. Best-effort operation. */
    private void cleanupGcsFiles(final List<GcsFileInfo> files) {
        int cleaned = 0;
        int failures = 0;
        for (final GcsFileInfo file : files) {
            final String uri = file.getUri();
            try {
                final Path path = new Path(uri);
                final FileSystem fs = path.getFileSystem();
                if (fs.exists(path)) {
                    final boolean deleted = fs.delete(path, false);
                    if (deleted) {
                        LOG.debug("Deleted GCS file: {}", uri);
                        cleaned++;
                    } else {
                        LOG.warn("Failed to delete GCS file: {}", uri);
                        failures++;
                    }
                } else {
                    cleaned++;
                }
            } catch (IOException e) {
                LOG.warn("Error deleting GCS file {}: {}", uri, e.getMessage());
                failures++;
            }
        }
        LOG.info(
                "GCS cleanup: {}/{} files cleaned up, {} failures",
                cleaned,
                files.size(),
                failures);
    }

    /**
     * Sanitizes a Flink job name for use in BigQuery job IDs. Replaces characters outside {@code
     * [a-zA-Z0-9_-]} with underscores, collapses consecutive underscores, and truncates to 100
     * chars.
     */
    static String sanitizeJobName(final String jobName) {
        String sanitized = jobName.replaceAll("[^a-zA-Z0-9_-]", "_");
        sanitized = sanitized.replaceAll("_+", "_");
        if (sanitized.length() > 100) {
            sanitized = sanitized.substring(0, 100);
        }
        return sanitized;
    }
}
