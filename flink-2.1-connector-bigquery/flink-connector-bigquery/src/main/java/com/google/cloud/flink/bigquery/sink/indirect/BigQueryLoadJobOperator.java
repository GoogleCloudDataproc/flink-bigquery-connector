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
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.PendingFileRecoverable;
import org.apache.flink.util.Collector;
import org.apache.flink.util.function.SerializableSupplier;

import com.google.cloud.bigquery.CopyJobConfiguration;
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
import java.util.ArrayList;
import java.util.Comparator;
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

    /**
     * Default maximum source URIs per BigQuery load job. See <a
     * href="https://cloud.google.com/bigquery/quotas#load_jobs">BigQuery load job quotas</a>.
     */
    static final int DEFAULT_MAX_FILES_PER_LOAD_JOB = 10_000;

    /**
     * Default maximum data per BigQuery load job (15 TB). See <a
     * href="https://cloud.google.com/bigquery/quotas#load_jobs">BigQuery load job quotas</a>.
     */
    static final long DEFAULT_MAX_BYTES_PER_LOAD_JOB = 15L * 1024 * 1024 * 1024 * 1024;

    /**
     * Format for per-checkpoint job/table IDs: {@code <prefix>_<jobIdHex>_<uuid>_c<checkpointId>}.
     */
    private static final String CHECKPOINT_ID_FORMAT = "%s_%s_%s_c%d";

    /** Per-partition variant of {@link #CHECKPOINT_ID_FORMAT}: appends {@code _p<idx>}. */
    private static final String PARTITION_ID_FORMAT = CHECKPOINT_ID_FORMAT + "_p%d";

    private static final String LOAD_JOB_ID_PREFIX = "flink-bq-load";
    private static final String TEMP_TABLE_NAME_PREFIX = "flink-bq-tmp";
    private static final String TEMP_LOAD_JOB_ID_PREFIX = "flink-bq-tmp-load";
    private static final String COPY_JOB_ID_PREFIX = "flink-bq-copy";

    private final BigQueryConnectOptions connectOptions;
    private final UUID uuid;
    private final String tempProject;
    private final String tempDataset;
    private final String jobProject;
    private final SerializableSupplier<ExecutorService> executorServiceSupplier;
    private final SerializableSupplier<FileSystem> fileSystemSupplier;
    private final FormatOptions formatOptions;
    private final int maxFilesPerLoadJob;
    private final long maxBytesPerLoadJob;

    private transient BigQueryServices.QueryDataClient queryClient;
    private transient String jobIdHex;
    private transient ExecutorService executorService;
    private transient FileSystem fileSystem;

    /** Maps checkpointId to its tracking state. */
    private transient Map<Long, CheckpointState> checkpoints;

    public BigQueryLoadJobOperator(
            final BigQueryConnectOptions connectOptions,
            final UUID uuid,
            final String tempProject,
            final String tempDataset,
            final String jobProject,
            final Path gcsBasePath,
            final FormatOptions formatOptions) {
        this(
                connectOptions,
                uuid,
                tempProject,
                tempDataset,
                jobProject,
                () -> Executors.newFixedThreadPool(EXECUTOR_PARALLELISM, EXECUTOR_THREAD_FACTORY),
                () -> {
                    try {
                        return gcsBasePath.getFileSystem();
                    } catch (IOException e) {
                        throw new BigQueryConnectorException(
                                "Failed to resolve FileSystem for " + gcsBasePath, e);
                    }
                },
                formatOptions,
                DEFAULT_MAX_FILES_PER_LOAD_JOB,
                DEFAULT_MAX_BYTES_PER_LOAD_JOB);
    }

    @VisibleForTesting
    BigQueryLoadJobOperator(
            final BigQueryConnectOptions connectOptions,
            final UUID uuid,
            final String tempProject,
            final String tempDataset,
            final String jobProject,
            final SerializableSupplier<ExecutorService> executorServiceSupplier,
            final SerializableSupplier<FileSystem> fileSystemSupplier,
            final FormatOptions formatOptions,
            final int maxFilesPerLoadJob,
            final long maxBytesPerLoadJob) {
        this.connectOptions = connectOptions;
        this.uuid = uuid;
        this.tempProject = tempProject;
        this.tempDataset = tempDataset;
        this.jobProject = jobProject;
        this.executorServiceSupplier = executorServiceSupplier;
        this.fileSystemSupplier = fileSystemSupplier;
        this.formatOptions = formatOptions;
        this.maxFilesPerLoadJob = maxFilesPerLoadJob;
        this.maxBytesPerLoadJob = maxBytesPerLoadJob;
    }

    @Override
    public void open(final OpenContext openContext) {
        this.checkpoints = new HashMap<>();
        this.queryClient = BigQueryServicesFactory.instance(connectOptions).queryClient();
        this.jobIdHex = getRuntimeContext().getJobInfo().getJobId().toHexString();
        this.executorService = executorServiceSupplier.get();
        this.fileSystem = fileSystemSupplier.get();
        LOG.info(
                "BigQueryLoadJobOperator opened: jobId={}, uuid={}, table={}.{}.{}, jobProject={}",
                jobIdHex,
                uuid,
                connectOptions.getProjectId(),
                connectOptions.getDataset(),
                connectOptions.getTable(),
                jobProject);
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
            submitLoadJobsForCheckpoint(checkpointId, state.collectFiles());
            checkpoints.remove(checkpointId);
        }
    }

    /**
     * Submits load job(s) for a single checkpoint's files. No-ops if {@code pendingFiles} is empty.
     *
     * <p>BigQuery load jobs cap at {@link #maxFilesPerLoadJob} source URIs and {@link
     * #maxBytesPerLoadJob} bytes. When a single checkpoint exceeds either limit the files are
     * partitioned, each partition is loaded into its own temp table, and a single COPY job appends
     * all temp tables atomically into the final table.
     */
    private void submitLoadJobsForCheckpoint(
            final long checkpointId, final List<PendingFileRecoverable> pendingFiles)
            throws InterruptedException, ExecutionException {

        if (pendingFiles.isEmpty()) {
            LOG.info("No files to load for checkpoint {}; skipping load job", checkpointId);
        } else {
            final long totalBytes =
                    pendingFiles.stream().mapToLong(PendingFileRecoverable::getSize).sum();
            final TableId tableId =
                    TableId.of(
                            connectOptions.getProjectId(),
                            connectOptions.getDataset(),
                            connectOptions.getTable());

            final List<List<PendingFileRecoverable>> partitions = partitionFiles(pendingFiles);

            LOG.info(
                    "Checkpoint {} into {}: {} files, {} bytes, {} partition(s)",
                    checkpointId,
                    formatTableId(tableId),
                    pendingFiles.size(),
                    totalBytes,
                    partitions.size());

            if (partitions.size() == 1) {
                submitDirectLoad(checkpointId, tableId, partitions.get(0));
            } else {
                submitMultiPartitionLoad(checkpointId, tableId, partitions);
            }

            bestEffortCleanupGcsFiles(pendingFiles);
        }
    }

    /** Splits a list of files into partitions that respect both file-count and byte-size limits. */
    private List<List<PendingFileRecoverable>> partitionFiles(
            final List<PendingFileRecoverable> files) {

        // Files are sorted by GCS path before partitioning so that retries of the same checkpoint
        // produce the same partition shape. Without a stable order, a retry could place a file into
        // a different partition than the original attempt did, and - because load jobs are
        // idempotent on jobId, not on content - the COPY job would then double-count any file the
        // first attempt had already loaded into a different partition's temp table.
        final List<PendingFileRecoverable> sortedFiles =
                files.stream()
                        .sorted(Comparator.comparing(file -> file.getPath().toUri().toString()))
                        .collect(Collectors.toList());

        // bin-pack files into partition
        final List<List<PendingFileRecoverable>> partitions = new ArrayList<>();
        List<PendingFileRecoverable> currentPartition = new ArrayList<>();
        long currentBytes = 0L;
        for (final PendingFileRecoverable file : sortedFiles) {
            if (shouldStartNewPartition(currentPartition, currentBytes, file.getSize())) {
                partitions.add(currentPartition);
                currentPartition = new ArrayList<>();
                currentBytes = 0L;
            }
            currentPartition.add(file);
            currentBytes += file.getSize();
        }
        partitions.add(currentPartition);

        return partitions;
    }

    private boolean shouldStartNewPartition(
            final List<PendingFileRecoverable> currentPartition,
            final long currentBytes,
            final long nextFileSize) {
        return !currentPartition.isEmpty()
                && (currentPartition.size() >= maxFilesPerLoadJob
                        || currentBytes + nextFileSize > maxBytesPerLoadJob);
    }

    /** Single-partition path: load all files directly into the final table. */
    private void submitDirectLoad(
            final long checkpointId,
            final TableId tableId,
            final List<PendingFileRecoverable> partitionFiles) {

        final String jobId = generateJobId(checkpointId);
        final List<String> gcsUris = toGcsUris(partitionFiles);
        final JobConfiguration config =
                applyFormatOptions(LoadJobConfiguration.newBuilder(tableId, gcsUris))
                        // Indirect mode requires the destination table to exist
                        // (validateIndirect rejects enableTableCreation); enforce server-side.
                        .setCreateDisposition(JobInfo.CreateDisposition.CREATE_NEVER)
                        .build();

        submitAndAwaitJob(jobId, config);
    }

    /**
     * Multi-partition path: load each partition into its own temp table, atomically copy all temp
     * tables into the final table, then delete temp tables.
     */
    private void submitMultiPartitionLoad(
            final long checkpointId,
            final TableId finalTableId,
            final List<List<PendingFileRecoverable>> partitions)
            throws ExecutionException, InterruptedException {

        final List<TableId> tempTableIds = new ArrayList<>(partitions.size());
        final List<CompletableFuture<Void>> tempLoadFutures = new ArrayList<>(partitions.size());

        for (int partitionIndex = 0; partitionIndex < partitions.size(); partitionIndex++) {
            final List<PendingFileRecoverable> partitionFiles = partitions.get(partitionIndex);
            final TableId tempTableId =
                    TableId.of(
                            tempProject,
                            tempDataset,
                            String.format(
                                    PARTITION_ID_FORMAT,
                                    TEMP_TABLE_NAME_PREFIX,
                                    jobIdHex,
                                    uuid,
                                    checkpointId,
                                    partitionIndex));
            final String tempLoadJobId =
                    String.format(
                            PARTITION_ID_FORMAT,
                            TEMP_LOAD_JOB_ID_PREFIX,
                            jobIdHex,
                            uuid,
                            checkpointId,
                            partitionIndex);
            final List<String> gcsUris = toGcsUris(partitionFiles);

            LOG.info(
                    "Loading partition {} ({} files) into temp table {}",
                    partitionIndex,
                    partitionFiles.size(),
                    formatTableId(tempTableId));

            final JobConfiguration tempLoadConfig =
                    applyFormatOptions(LoadJobConfiguration.newBuilder(tempTableId, gcsUris))
                            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
                            .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
                            .build();

            tempTableIds.add(tempTableId);
            tempLoadFutures.add(
                    CompletableFuture.runAsync(
                            () -> submitAndAwaitJob(tempLoadJobId, tempLoadConfig),
                            executorService));
        }

        // Submit + wait on all temp loads concurrently. If any fails, surface its
        // exception; the others continue running but their results are ignored — temp
        // loads are idempotent on jobId, so a Flink retry will re-attach to any
        // in-flight or already-completed BQ jobs without double-loading.
        CompletableFuture.allOf(tempLoadFutures.toArray(new CompletableFuture[0])).get();

        final String copyJobId =
                String.format(
                        CHECKPOINT_ID_FORMAT, COPY_JOB_ID_PREFIX, jobIdHex, uuid, checkpointId);
        final JobConfiguration copyConfig =
                CopyJobConfiguration.newBuilder(finalTableId, tempTableIds)
                        .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
                        // Indirect mode requires the destination table to exist
                        // (validateIndirect rejects enableTableCreation); enforce server-side.
                        .setCreateDisposition(JobInfo.CreateDisposition.CREATE_NEVER)
                        .build();
        submitAndAwaitJob(copyJobId, copyConfig);

        bestEffortCleanupTempTables(tempTableIds);
    }

    /**
     * Apply load options shared by all load jobs (direct + temp-table).
     *
     * <p>{@code decimalTargetTypes}: Parquet/Avro decimal logical types map to multiple BQ types
     * depending on (precision, scale). The default order [NUMERIC, BIGNUMERIC, STRING] picks
     * NUMERIC for any decimal with precision ≤38, even when scale > 9, which mismatches a target
     * BIGNUMERIC column. Prefer BIGNUMERIC first so the column type is preserved when source
     * decimals have scale > 9.
     */
    private LoadJobConfiguration.Builder applyFormatOptions(
            final LoadJobConfiguration.Builder builder) {
        return builder.setFormatOptions(formatOptions)
                .setDecimalTargetTypes(List.of("BIGNUMERIC", "NUMERIC", "STRING"));
    }

    private static List<String> toGcsUris(final List<PendingFileRecoverable> files) {
        return files.stream()
                .map(file -> file.getPath().toUri().toString())
                .collect(Collectors.toList());
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
        final Job maybeExistingJob = queryClient.getJob(jobProject, jobId);
        if (maybeExistingJob == null) {
            LOG.info("Submitting job {}", jobId);
            job = queryClient.submitJob(jobProject, jobId, config);
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
        return String.format(
                CHECKPOINT_ID_FORMAT, LOAD_JOB_ID_PREFIX, jobIdHex, uuid, checkpointId);
    }

    /** Delete GCS files after successful load. Best-effort, concurrent operation. */
    private void bestEffortCleanupGcsFiles(final List<PendingFileRecoverable> pendingFiles)
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
            final PendingFileRecoverable file,
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

    /** Delete temp tables after a successful COPY job. Best-effort, concurrent operation. */
    private void bestEffortCleanupTempTables(final List<TableId> tempTableIds)
            throws ExecutionException, InterruptedException {
        final AtomicInteger cleaned = new AtomicInteger(0);
        final AtomicInteger failures = new AtomicInteger(0);

        try {
            CompletableFuture.allOf(
                            tempTableIds.stream()
                                    .map(
                                            tempTableId ->
                                                    CompletableFuture.runAsync(
                                                            () ->
                                                                    deleteSingleTempTable(
                                                                            tempTableId,
                                                                            cleaned,
                                                                            failures),
                                                            executorService))
                                    .toArray(CompletableFuture[]::new))
                    .get(5, TimeUnit.MINUTES);
        } catch (TimeoutException e) {
            LOG.warn("Temp table cleanup timed out after 5 minutes; some tables may remain.");
        }

        LOG.info(
                "Temp table cleanup: {}/{} tables deleted, {} failures",
                cleaned.get(),
                tempTableIds.size(),
                failures.get());
    }

    private void deleteSingleTempTable(
            final TableId tempTableId, final AtomicInteger cleaned, final AtomicInteger failures) {
        try {
            final boolean deleted = queryClient.deleteTable(tempTableId);
            if (deleted) {
                LOG.debug("Deleted temp table: {}", formatTableId(tempTableId));
            } else {
                LOG.warn(
                        "Temp table {} not found for deletion (already cleaned up?)",
                        formatTableId(tempTableId));
            }
            cleaned.incrementAndGet();
        } catch (Exception e) {
            LOG.warn("Error deleting temp table {}", formatTableId(tempTableId), e);
            failures.incrementAndGet();
        }
    }

    private static String formatTableId(final TableId tableId) {
        return tableId.getProject() + "." + tableId.getDataset() + "." + tableId.getTable();
    }
}
