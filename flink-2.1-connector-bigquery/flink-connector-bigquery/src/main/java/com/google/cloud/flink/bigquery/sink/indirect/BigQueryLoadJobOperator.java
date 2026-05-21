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

package com.google.cloud.flink.bigquery.sink.indirect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.util.Collector;
import org.apache.flink.util.function.SerializableSupplier;

import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.exceptions.BigQueryConnectorException;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/** Collects committed GCS file paths per checkpoint and submits BigQuery load jobs. */
@Internal
public class BigQueryLoadJobOperator
        extends RichFlatMapFunction<CommittableMessage<FileSinkCommittable>, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryLoadJobOperator.class);

    private static final int EXECUTOR_PARALLELISM = 32;
    private static final long EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS = 10L;
    private static final ThreadFactory EXECUTOR_THREAD_FACTORY = r -> new Thread(r, "bq-load");

    private final BigQueryConnectOptions connectOptions;
    private final UUID uuid;
    private final SerializableSupplier<ExecutorService> executorServiceSupplier;
    private final SerializableSupplier<FileSystem> fileSystemSupplier;
    private final FormatOptions formatOptions;

    private transient BigQueryServices.QueryDataClient queryClient;
    private transient String jobIdHex;
    private transient ExecutorService executorService;
    private transient FileSystem fileSystem;

    /** Maps checkpointId to its tracking state. */
    private transient Map<Long, CheckpointState> checkpoints;

    public BigQueryLoadJobOperator(
            final BigQueryConnectOptions connectOptions,
            final UUID uuid,
            final Path gcsBasePath,
            final FormatOptions formatOptions) {
        this(
                connectOptions,
                uuid,
                () -> Executors.newFixedThreadPool(EXECUTOR_PARALLELISM, EXECUTOR_THREAD_FACTORY),
                () -> {
                    try {
                        return gcsBasePath.getFileSystem();
                    } catch (IOException e) {
                        throw new BigQueryConnectorException(
                                "Failed to resolve FileSystem for " + gcsBasePath, e);
                    }
                },
                formatOptions);
    }

    @VisibleForTesting
    BigQueryLoadJobOperator(
            final BigQueryConnectOptions connectOptions,
            final UUID uuid,
            final SerializableSupplier<ExecutorService> executorServiceSupplier,
            final SerializableSupplier<FileSystem> fileSystemSupplier,
            final FormatOptions formatOptions) {
        this.connectOptions = connectOptions;
        this.uuid = uuid;
        this.executorServiceSupplier = executorServiceSupplier;
        this.fileSystemSupplier = fileSystemSupplier;
        this.formatOptions = formatOptions;
    }

    @Override
    public void open(final OpenContext openContext) {
        this.checkpoints = new HashMap<>();
        this.queryClient = BigQueryServicesFactory.instance(connectOptions).queryClient();
        this.jobIdHex = getRuntimeContext().getJobInfo().getJobId().toHexString();
        this.executorService = executorServiceSupplier.get();
        this.fileSystem = fileSystemSupplier.get();
        LOG.info(
                "BigQueryLoadJobOperator opened: jobId={}, uuid={}, table={}.{}.{}",
                jobIdHex,
                uuid,
                connectOptions.getProjectId(),
                connectOptions.getDataset(),
                connectOptions.getTable());
    }

    @Override
    public void close() throws Exception {
        if (executorService != null) {
            executorService.shutdown();
            if (!executorService.awaitTermination(
                    EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                LOG.warn(
                        "Executor did not terminate within {}s; forcing shutdown",
                        EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS);
                executorService.shutdownNow();
            }
        }
    }

    @Override
    public void flatMap(
            final CommittableMessage<FileSinkCommittable> value, final Collector<Void> out)
            throws Exception {
        final long checkpointId = value.getCheckpointId();
        final CheckpointState state =
                checkpoints.computeIfAbsent(checkpointId, k -> new CheckpointState());
        state.add(value);

        if (state.isComplete()) {
            submitLoadJobForCheckpoint(checkpointId, state.collectFiles());
            checkpoints.remove(checkpointId);
        }
    }

    /**
     * Submits a load job for a single checkpoint's files. No-ops if {@code pendingFiles} is empty.
     */
    private void submitLoadJobForCheckpoint(
            final long checkpointId,
            final List<InProgressFileWriter.PendingFileRecoverable> pendingFiles)
            throws InterruptedException, ExecutionException {

        if (pendingFiles.isEmpty()) {
            LOG.info("No files to load for checkpoint {}; skipping load job", checkpointId);
        } else {
            final long totalBytes =
                    pendingFiles.stream()
                            .mapToLong(InProgressFileWriter.PendingFileRecoverable::getSize)
                            .sum();
            final TableId tableId =
                    TableId.of(
                            connectOptions.getProjectId(),
                            connectOptions.getDataset(),
                            connectOptions.getTable());
            final String jobId = generateJobId(checkpointId);
            final List<String> gcsUris =
                    pendingFiles.stream()
                            .map(file -> file.getPath().toUri().toString())
                            .collect(Collectors.toList());

            LOG.info(
                    "Load job {} for checkpoint {} into {}: {} files, {} bytes",
                    jobId,
                    checkpointId,
                    tableId,
                    pendingFiles.size(),
                    totalBytes);

            final JobConfiguration config =
                    LoadJobConfiguration.newBuilder(tableId, gcsUris)
                            .setFormatOptions(formatOptions)
                            // Parquet/Avro decimal logical types map to multiple BQ types
                            // depending on (precision, scale). The default order
                            // [NUMERIC, BIGNUMERIC, STRING] picks NUMERIC for any decimal with
                            // precision ≤ 38, even when scale > 9, which mismatches a target
                            // BIGNUMERIC column. Prefer BIGNUMERIC first so the column type is
                            // preserved when source decimals have scale > 9.
                            .setDecimalTargetTypes(List.of("BIGNUMERIC", "NUMERIC", "STRING"))
                            // Indirect mode requires the destination table to exist
                            // (validateIndirect rejects enableTableCreation); enforce server-side.
                            .setCreateDisposition(JobInfo.CreateDisposition.CREATE_NEVER)
                            .build();

            submitAndAwaitJob(jobId, config);

            bestEffortCleanupGcsFiles(pendingFiles);
        }
    }

    /**
     * Finds an existing job for {@code jobId} or submits a new one, then waits for completion.
     *
     * <p>Recovery semantics: if waitForJob throws (e.g. network outage past the BQ client's retry
     * window) the operator fails and Flink restarts from the last checkpoint. The replayed
     * checkpoint regenerates the same jobId (see {@link #generateJobId}), so getJob finds the
     * still-running or already-completed BQ job and we resume waiting on it rather than
     * double-submitting. Submission and completion are therefore decoupled — a successful BQ job is
     * never re-run, even if Flink's view of it failed mid-poll.
     */
    private void submitAndAwaitJob(final String jobId, final JobConfiguration config) {
        final Job job;
        final Job maybeExistingJob = queryClient.getJob(connectOptions.getProjectId(), jobId);
        if (maybeExistingJob == null) {
            LOG.info("Submitting job {}", jobId);
            job = queryClient.submitJob(connectOptions.getProjectId(), jobId, config);
        } else {
            job = maybeExistingJob;
        }

        final Job completedJob;
        try {
            completedJob = queryClient.waitForJob(job);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BigQueryConnectorException("Interrupted while waiting for job " + jobId, e);
        }
        if (completedJob.getStatus().getError() != null) {
            throw new BigQueryConnectorException(
                    String.format("Job %s failed: %s", jobId, completedJob.getStatus().getError()));
        } else {
            LOG.info("Job {} completed successfully", jobId);
        }
    }

    /**
     * Generate a job ID for load jobs.
     *
     * <p>BigQuery job IDs are globally unique and immutable: once a job ID completes, resubmitting
     * the same ID is a no-op (BigQuery returns the existing result rather than running a new job).
     * We use this to guarantee exactly-once writes in indirect mode; jobIds are generated
     * deterministically as a function of Flink job ID + UUID + checkpoint ID. The UUID is unique
     * per operator instance, so multiple indirect sinks in the same Flink job produce distinct job
     * IDs.
     */
    private String generateJobId(final long checkpointId) {
        return String.format("flink-bq-load_%s_%s_c%d", jobIdHex, uuid, checkpointId);
    }

    /** Delete GCS files after successful load. Best-effort, concurrent operation. */
    private void bestEffortCleanupGcsFiles(
            final List<InProgressFileWriter.PendingFileRecoverable> pendingFiles)
            throws ExecutionException, InterruptedException {
        final AtomicInteger cleaned = new AtomicInteger(0);
        final AtomicInteger failures = new AtomicInteger(0);

        try {
            CompletableFuture.allOf(
                            pendingFiles.stream()
                                    .map(
                                            file ->
                                                    CompletableFuture.runAsync(
                                                            () ->
                                                                    deleteSingleFile(
                                                                            fileSystem,
                                                                            file,
                                                                            cleaned,
                                                                            failures),
                                                            executorService))
                                    .toArray(CompletableFuture[]::new))
                    .get(5, TimeUnit.MINUTES);
        } catch (TimeoutException e) {
            LOG.warn("GCS cleanup timed out after 5 minutes; some files may remain.");
        }

        LOG.info(
                "GCS cleanup: {}/{} files cleaned up, {} failures",
                cleaned.get(),
                pendingFiles.size(),
                failures.get());
    }

    private void deleteSingleFile(
            final FileSystem fs,
            final InProgressFileWriter.PendingFileRecoverable file,
            final AtomicInteger cleaned,
            final AtomicInteger failures) {
        final Path filePath = file.getPath();
        try {
            final boolean deleted = fs.delete(filePath, false);
            if (deleted) {
                LOG.debug("Deleted file: {}", filePath);
            } else {
                LOG.warn("Unable to delete file: {}", filePath);
            }
            cleaned.incrementAndGet();
        } catch (IOException e) {
            LOG.warn("Error deleting file {}", filePath, e);
            failures.incrementAndGet();
        }
    }
}
