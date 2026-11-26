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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.util.Collector;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.CopyJobConfiguration;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.ParquetOptions;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import com.google.cloud.flink.bigquery.common.exceptions.BigQueryConnectorException;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.TablePartitionInfo;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link BigQueryLoadJobOperator}. */
public class BigQueryLoadJobOperatorTest {

    private static final String PROJECT = "p";
    private static final String DATASET = "d";
    private static final String TABLE = "t";
    private static final String TEMP_PROJECT = "temp_p";
    private static final String TEMP_DATASET = "temp_d";
    private static final String JOB_PROJECT = "job_p";

    private static class TestPendingFile implements InProgressFileWriter.PendingFileRecoverable {
        private final Path path;
        private final long size;

        TestPendingFile(Path path, long size) {
            this.path = path;
            this.size = size;
        }

        @Nullable
        @Override
        public Path getPath() {
            return path;
        }

        @Override
        public long getSize() {
            return size;
        }

        // FileSinkCommittable.equals delegates to this; replay tests need duplicate
        // pending files with the same (path, size) to compare equal.
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TestPendingFile)) {
                return false;
            }
            TestPendingFile that = (TestPendingFile) o;
            return size == that.size && java.util.Objects.equals(path, that.path);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(path, size);
        }
    }

    private static final Collector<Void> TEST_COLLECTOR =
            new Collector<>() {
                @Override
                public void collect(Void record) {
                    throw new AssertionError("collect should never be called; got: " + record);
                }

                @Override
                public void close() {}
            };

    private static final UUID FIXED_UUID = UUID.fromString("00000000-0000-0000-0000-0000abc12345");
    private static final String FIXED_JOB_ID_HEX = "fd72014d4c864993a2e5a9287b4a9c5d";
    private static final String CHECKPOINT_SUFFIX_C1 =
            "_" + FIXED_JOB_ID_HEX + "_" + FIXED_UUID + "_c1";
    private static final String EXPECTED_LOAD_JOB_ID_C1 = "flink-bq-load" + CHECKPOINT_SUFFIX_C1;
    private static final String EXPECTED_TEMP_LOAD_JOB_ID_C1_P0 =
            "flink-bq-tmp-load" + CHECKPOINT_SUFFIX_C1 + "_p0";
    private static final String EXPECTED_TEMP_LOAD_JOB_ID_C1_P1 =
            "flink-bq-tmp-load" + CHECKPOINT_SUFFIX_C1 + "_p1";
    private static final String EXPECTED_COPY_JOB_ID_C1 = "flink-bq-copy" + CHECKPOINT_SUFFIX_C1;
    private static final TableId EXPECTED_TEMP_TABLE_ID_C1_P0 =
            TableId.of(TEMP_PROJECT, TEMP_DATASET, "flink-bq-tmp" + CHECKPOINT_SUFFIX_C1 + "_p0");
    private static final TableId EXPECTED_TEMP_TABLE_ID_C1_P1 =
            TableId.of(TEMP_PROJECT, TEMP_DATASET, "flink-bq-tmp" + CHECKPOINT_SUFFIX_C1 + "_p1");

    private static final ParquetOptions EXPECTED_PARQUET_OPTIONS =
            ParquetOptions.newBuilder().setEnableListInference(true).build();

    /** Asserts the submitted load job carries the expected Parquet format options. */
    private static void assertSubmittedFormatOptions(LoadJobConfiguration config) {
        assertEquals(EXPECTED_PARQUET_OPTIONS, config.getParquetOptions());
    }

    /** Creates a mock {@link RuntimeContext} returning FIXED_JOB_ID_HEX. */
    private static RuntimeContext mockRuntimeContext() {
        RuntimeContext ctx = mock(RuntimeContext.class, RETURNS_DEEP_STUBS);
        when(ctx.getJobInfo().getJobId().toHexString()).thenReturn(FIXED_JOB_ID_HEX);
        return ctx;
    }

    /**
     * Recording fake {@link BigQueryServices.QueryDataClient}. Each test mutates the configurable
     * fields (existingJobLookup / submitJobResult / waitForJobBehavior) to script responses, and
     * asserts against the recorded lists rather than verifying mock interactions.
     */
    private static class RecordingQueryDataClient implements BigQueryServices.QueryDataClient {
        // Recording.
        final List<SubmittedJob> submittedJobs = new ArrayList<>();
        final List<String> getJobLookups = new ArrayList<>();
        final List<Job> waitForJobCalls = new ArrayList<>();
        final List<TableId> deleteTableCalls = new ArrayList<>();

        // Configurable behavior - defaults form a happy-path success.
        // existingJobLookup is keyed by jobId so multi-partition tests can script different
        // states for different temp-load / copy jobs in the same checkpoint.
        Function<String, Job> existingJobLookup = id -> null;
        Job submitJobResult = mockSuccessJob();
        Function<Job, Job> waitForJobBehavior = j -> j;
        Function<TableId, Boolean> deleteTableBehavior = id -> true;

        static final class SubmittedJob {
            final String project;
            final String jobId;
            final JobConfiguration config;

            SubmittedJob(String project, String jobId, JobConfiguration config) {
                this.project = project;
                this.jobId = jobId;
                this.config = config;
            }
        }

        @Override
        public synchronized Job submitJob(String project, String jobId, JobConfiguration config) {
            submittedJobs.add(new SubmittedJob(project, jobId, config));
            return submitJobResult;
        }

        @Override
        public synchronized Job getJob(String project, String jobId) {
            getJobLookups.add(jobId);
            return existingJobLookup.apply(jobId);
        }

        @Override
        public synchronized Job waitForJob(Job job) {
            waitForJobCalls.add(job);
            return waitForJobBehavior.apply(job);
        }

        @Override
        public List<String> retrieveTablePartitions(String project, String dataset, String table) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<TablePartitionInfo> retrievePartitionColumnInfo(
                String project, String dataset, String table) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableSchema getTableSchema(String project, String dataset, String table) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Dataset getDataset(String project, String dataset) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createDataset(String project, String dataset, String region) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Boolean tableExists(String project, String dataset, String table) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createTable(
                String project, String dataset, String table, TableDefinition tableDefinition) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Boolean isView(String project, String dataset, String table) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String materializeView(
                String project,
                String dataset,
                String table,
                List<String> selectedFields,
                String rowRestriction,
                Integer expirationHours,
                String materializationProject,
                String materializationDataset,
                String billingProject) {
            throw new UnsupportedOperationException();
        }

        @Override
        public synchronized boolean deleteTable(TableId tableId) {
            deleteTableCalls.add(tableId);
            return deleteTableBehavior.apply(tableId);
        }
    }

    /** Minimal {@link BigQueryServices} that delegates to a given {@link QueryDataClient}. */
    private static class NoOpBigQueryServices implements BigQueryServices {
        private final QueryDataClient queryDataClient;

        NoOpBigQueryServices(QueryDataClient queryDataClient) {
            this.queryDataClient = queryDataClient;
        }

        @Override
        public QueryDataClient createQueryDataClient(CredentialsOptions credentialsOptions) {
            return queryDataClient;
        }

        @Override
        public StorageReadClient createStorageReadClient(CredentialsOptions credentialsOptions) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageWriteClient createStorageWriteClient(CredentialsOptions credentialsOptions) {
            throw new UnsupportedOperationException();
        }
    }

    /** Builds a {@link BigQueryConnectOptions} whose factory returns the given client. */
    private static BigQueryConnectOptions testConnectOptions(
            final BigQueryServices.QueryDataClient client) {
        return BigQueryConnectOptions.builder()
                .setProjectId(PROJECT)
                .setDataset(DATASET)
                .setTable(TABLE)
                .setTestingBigQueryServices(() -> new NoOpBigQueryServices(client))
                .build();
    }

    /** Creates an opened operator with the given client and a fresh recording file system. */
    private static BigQueryLoadJobOperator createOpenOperator(
            final BigQueryServices.QueryDataClient client) throws Exception {
        return createOpenOperator(client, new RecordingFileSystem());
    }

    /** Creates an opened operator with the given client and an explicit file system. */
    private static BigQueryLoadJobOperator createOpenOperator(
            final BigQueryServices.QueryDataClient client, final FileSystem fs) throws Exception {
        return createOpenOperator(
                client,
                fs,
                BigQueryLoadJobOperator.DEFAULT_MAX_FILES_PER_LOAD_JOB,
                BigQueryLoadJobOperator.DEFAULT_MAX_BYTES_PER_LOAD_JOB);
    }

    /** Creates an opened operator with explicit load-job size limits. */
    private static BigQueryLoadJobOperator createOpenOperator(
            final BigQueryServices.QueryDataClient client,
            final FileSystem fs,
            final int maxFilesPerLoadJob,
            final long maxBytesPerLoadJob) {
        final BigQueryLoadJobOperator operator =
                new BigQueryLoadJobOperator(
                        testConnectOptions(client),
                        FIXED_UUID,
                        TEMP_PROJECT,
                        TEMP_DATASET,
                        JOB_PROJECT,
                        () -> Executors.newCachedThreadPool(),
                        () -> fs,
                        EXPECTED_PARQUET_OPTIONS,
                        maxFilesPerLoadJob,
                        maxBytesPerLoadJob);
        operator.setRuntimeContext(mockRuntimeContext());
        operator.open(null);
        return operator;
    }

    private static CommittableMessage<FileSinkCommittable> summary(
            int subtaskId, int numberOfSubtasks, long checkpointId, int numCommittables) {
        return new CommittableSummary<>(
                subtaskId, numberOfSubtasks, checkpointId, numCommittables, 0);
    }

    private static CommittableMessage<FileSinkCommittable> committableWithFile(
            int subtaskId, long checkpointId, String uri, long sizeBytes) {
        TestPendingFile pendingFile = new TestPendingFile(new Path(uri), sizeBytes);
        FileSinkCommittable fsc = new FileSinkCommittable("bucket", pendingFile);
        return new CommittableWithLineage<>(fsc, checkpointId, subtaskId);
    }

    /** Processes a complete checkpoint through the operator: summary (1 subtask) + files. */
    private static void processCompleteCheckpoint(
            BigQueryLoadJobOperator operator, long checkpointId, String... uris) throws Exception {
        operator.flatMap(summary(0, 1, checkpointId, uris.length), TEST_COLLECTOR);
        for (String uri : uris) {
            operator.flatMap(committableWithFile(0, checkpointId, uri, 100L), TEST_COLLECTOR);
        }
    }

    private static Job mockSuccessJob() {
        Job job = mock(Job.class);
        JobStatus status = mock(JobStatus.class);
        when(status.getState()).thenReturn(JobStatus.State.DONE);
        when(status.getError()).thenReturn(null);
        when(job.getStatus()).thenReturn(status);
        return job;
    }

    private static Job mockFailedJob() {
        Job job = mock(Job.class);
        JobStatus status = mock(JobStatus.class);
        when(status.getState()).thenReturn(JobStatus.State.DONE);
        when(status.getError()).thenReturn(new BigQueryError("error", "", "job failed"));
        when(job.getStatus()).thenReturn(status);
        return job;
    }

    private static Job mockRunningJob() {
        Job job = mock(Job.class);
        JobStatus status = mock(JobStatus.class);
        when(status.getState()).thenReturn(JobStatus.State.RUNNING);
        when(job.getStatus()).thenReturn(status);
        return job;
    }

    /**
     * Recording {@link ExecutorService} that counts {@code shutdown()} / {@code shutdownNow()}
     * calls and returns a configurable result from {@code awaitTermination}. Task-execution methods
     * are no-ops since the close-lifecycle tests never submit work.
     */
    private static class RecordingExecutorService extends AbstractExecutorService {
        private int shutdownCount;
        private int shutdownNowCount;
        private boolean awaitTerminationResult = true;

        @Override
        public void shutdown() {
            shutdownCount++;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdownNowCount++;
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            return shutdownCount > 0;
        }

        @Override
        public boolean isTerminated() {
            return shutdownCount > 0;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return awaitTerminationResult;
        }

        @Override
        public void execute(Runnable command) {}
    }

    /**
     * Recording {@link FileSystem} that captures the URIs passed to {@code delete} and lets tests
     * configure the return value or an exception. Inherits everything else from {@link
     * LocalFileSystem}. Wrapping the URI list in a synchronized list because cleanup runs in
     * parallel via the executorService.
     */
    private static class RecordingFileSystem extends LocalFileSystem {
        final List<String> deleteCalls = Collections.synchronizedList(new ArrayList<>());
        boolean deleteResult = true;
        IOException toThrow = null;

        @Override
        public boolean delete(Path f, boolean recursive) throws IOException {
            deleteCalls.add(f.toUri().toString());
            if (toThrow != null) {
                throw toThrow;
            }
            return deleteResult;
        }
    }

    // =========================================================================
    // open/close lifecycle
    // =========================================================================

    @Test
    public void openCreatesExecutorService() throws Exception {
        AtomicInteger supplierCalls = new AtomicInteger();
        RecordingExecutorService executorService = new RecordingExecutorService();
        BigQueryLoadJobOperator operator =
                new BigQueryLoadJobOperator(
                        testConnectOptions(new RecordingQueryDataClient()),
                        FIXED_UUID,
                        TEMP_PROJECT,
                        TEMP_DATASET,
                        JOB_PROJECT,
                        () -> {
                            supplierCalls.incrementAndGet();
                            return executorService;
                        },
                        RecordingFileSystem::new,
                        EXPECTED_PARQUET_OPTIONS,
                        BigQueryLoadJobOperator.DEFAULT_MAX_FILES_PER_LOAD_JOB,
                        BigQueryLoadJobOperator.DEFAULT_MAX_BYTES_PER_LOAD_JOB);
        operator.setRuntimeContext(mockRuntimeContext());

        // Construction alone must not create the executorService.
        assertEquals(0, supplierCalls.get());

        operator.open(null);

        assertEquals(1, supplierCalls.get());

        operator.close();
    }

    @Test
    public void closeShutsDownExecutorService() throws Exception {
        RecordingExecutorService executorService = new RecordingExecutorService();
        executorService.awaitTerminationResult = true;
        BigQueryLoadJobOperator operator =
                new BigQueryLoadJobOperator(
                        testConnectOptions(new RecordingQueryDataClient()),
                        FIXED_UUID,
                        TEMP_PROJECT,
                        TEMP_DATASET,
                        JOB_PROJECT,
                        () -> executorService,
                        RecordingFileSystem::new,
                        EXPECTED_PARQUET_OPTIONS,
                        BigQueryLoadJobOperator.DEFAULT_MAX_FILES_PER_LOAD_JOB,
                        BigQueryLoadJobOperator.DEFAULT_MAX_BYTES_PER_LOAD_JOB);
        operator.setRuntimeContext(mockRuntimeContext());
        operator.open(null);
        operator.close();
        assertEquals(1, executorService.shutdownCount);
        assertEquals(0, executorService.shutdownNowCount);
    }

    @Test
    public void closeForcesShutdownWhenAwaitTerminationTimesOut() throws Exception {
        RecordingExecutorService executorService = new RecordingExecutorService();
        executorService.awaitTerminationResult = false;
        BigQueryLoadJobOperator operator =
                new BigQueryLoadJobOperator(
                        testConnectOptions(new RecordingQueryDataClient()),
                        FIXED_UUID,
                        TEMP_PROJECT,
                        TEMP_DATASET,
                        JOB_PROJECT,
                        () -> executorService,
                        RecordingFileSystem::new,
                        EXPECTED_PARQUET_OPTIONS,
                        BigQueryLoadJobOperator.DEFAULT_MAX_FILES_PER_LOAD_JOB,
                        BigQueryLoadJobOperator.DEFAULT_MAX_BYTES_PER_LOAD_JOB);
        operator.setRuntimeContext(mockRuntimeContext());
        operator.open(null);
        operator.close();
        assertEquals(1, executorService.shutdownCount);
        assertEquals(1, executorService.shutdownNowCount);
    }

    @Test
    public void closeWhenExecutorNeverInitializedDoesNotThrow() throws Exception {
        // Operator constructed but open() never called - executorService stays null.
        RecordingExecutorService executorService = new RecordingExecutorService();
        BigQueryLoadJobOperator operator =
                new BigQueryLoadJobOperator(
                        testConnectOptions(new RecordingQueryDataClient()),
                        FIXED_UUID,
                        TEMP_PROJECT,
                        TEMP_DATASET,
                        JOB_PROJECT,
                        () -> executorService,
                        RecordingFileSystem::new,
                        EXPECTED_PARQUET_OPTIONS,
                        BigQueryLoadJobOperator.DEFAULT_MAX_FILES_PER_LOAD_JOB,
                        BigQueryLoadJobOperator.DEFAULT_MAX_BYTES_PER_LOAD_JOB);
        operator.close();
        assertEquals(0, executorService.shutdownCount);
        assertEquals(0, executorService.shutdownNowCount);
    }

    // =========================================================================
    // flatMap submission triggering - when/what does it submit?
    // =========================================================================

    @Test
    public void testFlatMapWithoutSummaryDoesNotSubmit() throws Exception {
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        BigQueryLoadJobOperator operator = createOpenOperator(client);

        operator.flatMap(
                committableWithFile(0, 1L, "file:///tmp/test.parquet", 100L), TEST_COLLECTOR);

        assertEquals(0, client.submittedJobs.size());
        assertEquals(0, client.getJobLookups.size());

        operator.close();
    }

    @Test
    public void testFlatMapDoesNotSubmitIncompleteCheckpoints() throws Exception {
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        BigQueryLoadJobOperator operator = createOpenOperator(client);

        // Send summary expecting 2 files, but only send 1
        operator.flatMap(summary(0, 1, 1L, 2), TEST_COLLECTOR);
        operator.flatMap(
                committableWithFile(0, 1L, "file:///tmp/f1.parquet", 100L), TEST_COLLECTOR);

        // Checkpoint is incomplete - should not submit
        assertEquals(0, client.submittedJobs.size());
        assertEquals(0, client.getJobLookups.size());

        operator.close();
    }

    @Test
    public void testFlatMapDoesNotSubmitIncompleteCheckpointsWithMultipleSubtasksSummaryFirst()
            throws Exception {
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        BigQueryLoadJobOperator operator = createOpenOperator(client);

        // Subtask 0: summary first, then file (of 2 subtasks total)
        operator.flatMap(summary(0, 2, 1L, 1), TEST_COLLECTOR);
        operator.flatMap(
                committableWithFile(0, 1L, "file:///tmp/f0.parquet", 100L), TEST_COLLECTOR);

        // Only 1 of 2 summaries received - should not submit yet
        assertEquals(0, client.submittedJobs.size());

        // Subtask 1: summary FIRST, then file
        operator.flatMap(summary(1, 2, 1L, 1), TEST_COLLECTOR);
        assertEquals(0, client.submittedJobs.size());
        operator.flatMap(
                committableWithFile(1, 1L, "file:///tmp/f1.parquet", 100L), TEST_COLLECTOR);

        // Now both summaries + all files received - should have submitted
        assertEquals(1, client.submittedJobs.size());
        assertEquals(JOB_PROJECT, client.submittedJobs.get(0).project);
        assertEquals(EXPECTED_LOAD_JOB_ID_C1, client.submittedJobs.get(0).jobId);
        // Order from the underlying HashSet isn't stable; compare as a set.
        assertEquals(
                Set.of("file:///tmp/f0.parquet", "file:///tmp/f1.parquet"),
                new HashSet<>(
                        ((LoadJobConfiguration) client.submittedJobs.get(0).config)
                                .getSourceUris()));
        assertSubmittedFormatOptions((LoadJobConfiguration) client.submittedJobs.get(0).config);

        operator.close();
    }

    @Test
    public void testFlatMapDoesNotSubmitIncompleteCheckpointsWithMultipleSubtasksFileFirst()
            throws Exception {
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        BigQueryLoadJobOperator operator = createOpenOperator(client);

        // Subtask 0: summary first, then file (of 2 subtasks total)
        operator.flatMap(summary(0, 2, 1L, 1), TEST_COLLECTOR);
        operator.flatMap(
                committableWithFile(0, 1L, "file:///tmp/f0.parquet", 100L), TEST_COLLECTOR);

        // Only 1 of 2 summaries received - should not submit yet
        assertEquals(0, client.submittedJobs.size());

        // Subtask 1: file FIRST, then summary
        operator.flatMap(
                committableWithFile(1, 1L, "file:///tmp/f1.parquet", 100L), TEST_COLLECTOR);
        assertEquals(0, client.submittedJobs.size());
        operator.flatMap(summary(1, 2, 1L, 1), TEST_COLLECTOR);

        // Now both summaries + all files received - should have submitted
        assertEquals(1, client.submittedJobs.size());
        assertEquals(JOB_PROJECT, client.submittedJobs.get(0).project);
        assertEquals(EXPECTED_LOAD_JOB_ID_C1, client.submittedJobs.get(0).jobId);
        assertEquals(
                Set.of("file:///tmp/f0.parquet", "file:///tmp/f1.parquet"),
                new HashSet<>(
                        ((LoadJobConfiguration) client.submittedJobs.get(0).config)
                                .getSourceUris()));
        assertSubmittedFormatOptions((LoadJobConfiguration) client.submittedJobs.get(0).config);

        operator.close();
    }

    @Test
    public void testFlatMapDoesNotSubmitEmptyCheckpoints() throws Exception {
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        BigQueryLoadJobOperator operator = createOpenOperator(client);

        // Summary with 0 committables
        operator.flatMap(summary(0, 1, 1L, 0), TEST_COLLECTOR);

        // No load job - checkpoint is complete but has no files
        assertEquals(0, client.submittedJobs.size());
        assertEquals(0, client.getJobLookups.size());

        operator.close();
    }

    // =========================================================================
    // flatMap submission triggering — duplicate handling for replay safety.
    // SupportsPostCommitTopology contract states: "any number of committables
    // may be replayed that have already been committed".
    // =========================================================================

    @Test
    public void duplicateSummaryFromSameSubtaskDoesNotInflateExpectedCount() throws Exception {
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        BigQueryLoadJobOperator operator = createOpenOperator(client);

        // Subtask 0 of a 1-subtask checkpoint: summary delivered twice (replay), then 1 file.
        operator.flatMap(summary(0, 1, 1L, 1), TEST_COLLECTOR);
        operator.flatMap(summary(0, 1, 1L, 1), TEST_COLLECTOR);
        operator.flatMap(committableWithFile(0, 1L, "file:///tmp/f.parquet", 100L), TEST_COLLECTOR);

        assertEquals(1, client.submittedJobs.size());
        assertEquals(
                List.of("file:///tmp/f.parquet"),
                ((LoadJobConfiguration) client.submittedJobs.get(0).config).getSourceUris());
        assertSubmittedFormatOptions((LoadJobConfiguration) client.submittedJobs.get(0).config);

        operator.close();
    }

    @Test
    public void duplicateCommittableSubmitsSingleFile() throws Exception {
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        BigQueryLoadJobOperator operator = createOpenOperator(client);

        // Summary expects 1 committable; the committable arrives twice (replay).
        operator.flatMap(summary(0, 1, 1L, 1), TEST_COLLECTOR);
        operator.flatMap(committableWithFile(0, 1L, "file:///tmp/f.parquet", 100L), TEST_COLLECTOR);
        operator.flatMap(committableWithFile(0, 1L, "file:///tmp/f.parquet", 100L), TEST_COLLECTOR);

        assertEquals(1, client.submittedJobs.size());
        assertEquals(
                List.of("file:///tmp/f.parquet"),
                ((LoadJobConfiguration) client.submittedJobs.get(0).config).getSourceUris());
        assertSubmittedFormatOptions((LoadJobConfiguration) client.submittedJobs.get(0).config);

        operator.close();
    }

    @Test
    public void testFlatMapDuplicateSummaryFromSubtaskDoesNotPrematurelyTriggerSubmission()
            throws Exception {
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        BigQueryLoadJobOperator operator = createOpenOperator(client);

        // 2-subtask checkpoint: subtask 0 sends the SAME summary twice (replay), then its file.
        CommittableMessage<FileSinkCommittable> subtask0Summary = summary(0, 2, 1L, 1);
        operator.flatMap(subtask0Summary, TEST_COLLECTOR);
        operator.flatMap(subtask0Summary, TEST_COLLECTOR);
        operator.flatMap(
                committableWithFile(0, 1L, "file:///tmp/f0.parquet", 100L), TEST_COLLECTOR);

        // Subtask 1 reports normally; submission fires only after its summary.
        operator.flatMap(summary(1, 2, 1L, 1), TEST_COLLECTOR);
        operator.flatMap(
                committableWithFile(1, 1L, "file:///tmp/f1.parquet", 100L), TEST_COLLECTOR);

        assertEquals(1, client.submittedJobs.size());
        assertEquals(EXPECTED_LOAD_JOB_ID_C1, client.submittedJobs.get(0).jobId);
        assertEquals(
                Set.of("file:///tmp/f0.parquet", "file:///tmp/f1.parquet"),
                new HashSet<>(
                        ((LoadJobConfiguration) client.submittedJobs.get(0).config)
                                .getSourceUris()));
        assertSubmittedFormatOptions((LoadJobConfiguration) client.submittedJobs.get(0).config);

        operator.close();
    }

    @Test
    public void testFlatMapDuplicateCommittableFromSubtaskDoesNotInflateFileCount()
            throws Exception {
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        BigQueryLoadJobOperator operator = createOpenOperator(client);

        // 2-subtask checkpoint: subtask 0 sends the SAME committable twice (replay).
        CommittableMessage<FileSinkCommittable> subtask0Committable =
                committableWithFile(0, 1L, "file:///tmp/f0.parquet", 100L);
        operator.flatMap(summary(0, 2, 1L, 1), TEST_COLLECTOR);
        operator.flatMap(subtask0Committable, TEST_COLLECTOR);
        operator.flatMap(subtask0Committable, TEST_COLLECTOR);
        assertEquals(0, client.submittedJobs.size());

        // Subtask 1 reports normally.
        operator.flatMap(summary(1, 2, 1L, 1), TEST_COLLECTOR);
        operator.flatMap(
                committableWithFile(1, 1L, "file:///tmp/f1.parquet", 100L), TEST_COLLECTOR);

        // Single submission with exactly 2 distinct files - the duplicate is deduped.
        assertEquals(1, client.submittedJobs.size());
        assertEquals(EXPECTED_LOAD_JOB_ID_C1, client.submittedJobs.get(0).jobId);
        assertEquals(
                Set.of("file:///tmp/f0.parquet", "file:///tmp/f1.parquet"),
                new HashSet<>(
                        ((LoadJobConfiguration) client.submittedJobs.get(0).config)
                                .getSourceUris()));
        assertSubmittedFormatOptions((LoadJobConfiguration) client.submittedJobs.get(0).config);

        operator.close();
    }

    @Test
    public void subtaskZeroFullyReplayedDoesNotPrematurelyCompleteCheckpoint() throws Exception {
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        BigQueryLoadJobOperator operator = createOpenOperator(client);

        // 2-subtask checkpoint: subtask 0's summary + committable arrive twice (replay)
        // BEFORE subtask 1 has sent anything. Old count-based code would have
        // summariesReceived == numberOfSubtasks (2 == 2) and submitted prematurely with
        // 2 copies of subtask 0's file. New code dedupes by subtaskId / lineage equality.
        operator.flatMap(summary(0, 2, 1L, 1), TEST_COLLECTOR);
        operator.flatMap(committableWithFile(0, 1L, "file:///tmp/a.parquet", 100L), TEST_COLLECTOR);
        operator.flatMap(summary(0, 2, 1L, 1), TEST_COLLECTOR);
        operator.flatMap(committableWithFile(0, 1L, "file:///tmp/a.parquet", 100L), TEST_COLLECTOR);

        // Replay alone must not trigger a submission.
        assertEquals(0, client.submittedJobs.size());

        // Subtask 1 reports - checkpoint becomes complete with 2 distinct files.
        operator.flatMap(summary(1, 2, 1L, 1), TEST_COLLECTOR);
        operator.flatMap(committableWithFile(1, 1L, "file:///tmp/b.parquet", 100L), TEST_COLLECTOR);

        assertEquals(1, client.submittedJobs.size());
        assertEquals(
                Set.of("file:///tmp/a.parquet", "file:///tmp/b.parquet"),
                new HashSet<>(
                        ((LoadJobConfiguration) client.submittedJobs.get(0).config)
                                .getSourceUris()));
        assertSubmittedFormatOptions((LoadJobConfiguration) client.submittedJobs.get(0).config);

        operator.close();
    }

    // =========================================================================
    // flatMap - new load job succeed/fail
    // =========================================================================

    @Test
    public void testFlatMapNewLoadJobSucceeds() throws Exception {
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        BigQueryLoadJobOperator operator = createOpenOperator(client);
        processCompleteCheckpoint(operator, 1L, "file:///tmp/test.parquet");

        assertEquals(1, client.submittedJobs.size());
        assertEquals(JOB_PROJECT, client.submittedJobs.get(0).project);
        assertEquals(
                JobInfo.CreateDisposition.CREATE_NEVER,
                ((LoadJobConfiguration) client.submittedJobs.get(0).config).getCreateDisposition());
        assertSubmittedFormatOptions((LoadJobConfiguration) client.submittedJobs.get(0).config);
        // submitJob returns submitJobResult; that's what waitForJob receives.
        assertEquals(List.of(client.submitJobResult), client.waitForJobCalls);

        operator.close();
    }

    @Test
    public void singlePartitionLoadUsesJobProjectButDestinationProjectForTable() throws Exception {
        // Cross-project semantics: the load job is created in JOB_PROJECT (where it's listed and
        // billed), but the destination table is in the connect-options PROJECT. These can differ.
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        BigQueryLoadJobOperator operator = createOpenOperator(client);
        processCompleteCheckpoint(operator, 1L, "file:///tmp/test.parquet");

        assertEquals(1, client.submittedJobs.size());
        assertEquals(JOB_PROJECT, client.submittedJobs.get(0).project);
        LoadJobConfiguration loadConfig = (LoadJobConfiguration) client.submittedJobs.get(0).config;
        assertEquals(TableId.of(PROJECT, DATASET, TABLE), loadConfig.getDestinationTable());
        // Recovery lookup also issued against JOB_PROJECT so jobId idempotence holds.
        assertEquals(1, client.getJobLookups.size());

        operator.close();
    }

    @Test
    public void multiPartitionLoadUsesJobProjectForAllJobsAndCorrectDestinationProjects()
            throws Exception {
        // Cross-project semantics on the multi-partition path: every submitted job (2 temp loads
        // + 1 copy) runs in JOB_PROJECT, but temp tables land in TEMP_PROJECT and the final
        // table in PROJECT.
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        BigQueryLoadJobOperator operator = createOpenOperatorForcingMultiPartition(client);
        processCompleteCheckpoint(operator, 1L, "file:///tmp/a.parquet", "file:///tmp/b.parquet");

        assertEquals(3, client.submittedJobs.size());
        for (RecordingQueryDataClient.SubmittedJob job : client.submittedJobs) {
            assertEquals(JOB_PROJECT, job.project);
            if (job.config instanceof LoadJobConfiguration) {
                assertEquals(
                        TEMP_PROJECT,
                        ((LoadJobConfiguration) job.config).getDestinationTable().getProject());
            } else {
                assertEquals(
                        PROJECT,
                        ((CopyJobConfiguration) job.config).getDestinationTable().getProject());
            }
        }

        operator.close();
    }

    @Test
    public void testFlatMapNewLoadJobFailsThrows() throws Exception {
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        client.submitJobResult = mockFailedJob();

        BigQueryLoadJobOperator operator = createOpenOperator(client);

        BigQueryConnectorException ex =
                assertThrows(
                        BigQueryConnectorException.class,
                        () -> processCompleteCheckpoint(operator, 1L, "file:///tmp/test.parquet"));
        assertTrue(ex.getMessage().contains("failed:"));

        assertEquals(1, client.submittedJobs.size());

        operator.close();
    }

    // =========================================================================
    // submitAndAwaitJob - existing load job succeed/fail/running
    // =========================================================================

    @Test
    public void testFlatMapExistingSuccessJobSkipsSubmission() throws Exception {
        Job existingJob = mockSuccessJob();
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        client.existingJobLookup = id -> existingJob;

        BigQueryLoadJobOperator operator = createOpenOperator(client);
        processCompleteCheckpoint(operator, 1L, "file:///tmp/test.parquet");

        assertEquals(0, client.submittedJobs.size());
        assertEquals(List.of(existingJob), client.waitForJobCalls);

        operator.close();
    }

    @Test
    public void testFlatMapExistingFailedJobThrows() throws Exception {
        Job existingJob = mockFailedJob();
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        client.existingJobLookup = id -> existingJob;

        BigQueryLoadJobOperator operator = createOpenOperator(client);

        BigQueryConnectorException ex =
                assertThrows(
                        BigQueryConnectorException.class,
                        () -> processCompleteCheckpoint(operator, 1L, "file:///tmp/test.parquet"));
        assertTrue(ex.getMessage().contains("failed:"));

        assertEquals(0, client.submittedJobs.size());
        assertEquals(List.of(existingJob), client.waitForJobCalls);

        operator.close();
    }

    @Test
    public void testFlatMapRunningJobSucceedsWaitsAndReturns() throws Exception {
        Job successJob = mockSuccessJob();
        Job runningJob = mockRunningJob();
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        client.existingJobLookup = id -> runningJob;
        client.waitForJobBehavior = j -> successJob;

        BigQueryLoadJobOperator operator = createOpenOperator(client);
        processCompleteCheckpoint(operator, 1L, "file:///tmp/test.parquet");

        assertEquals(0, client.submittedJobs.size());
        assertEquals(List.of(runningJob), client.waitForJobCalls);

        operator.close();
    }

    @Test
    public void testFlatMapRunningJobFailsThrows() throws Exception {
        Job failedAfterWait = mockFailedJob();
        Job runningJob = mockRunningJob();
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        client.existingJobLookup = id -> runningJob;
        client.waitForJobBehavior = j -> failedAfterWait;

        BigQueryLoadJobOperator operator = createOpenOperator(client);

        BigQueryConnectorException ex =
                assertThrows(
                        BigQueryConnectorException.class,
                        () -> processCompleteCheckpoint(operator, 1L, "file:///tmp/test.parquet"));
        assertTrue(ex.getMessage().contains("failed:"));

        assertEquals(0, client.submittedJobs.size());
        assertEquals(List.of(runningJob), client.waitForJobCalls);

        operator.close();
    }

    // =========================================================================
    // Post-load GCS cleanup
    // =========================================================================

    @Test
    public void flatMapDeletesFileAfterSuccessfulLoad() throws Exception {
        File tempDir = Files.createTempDirectory("bq-load-job-test").toFile();
        tempDir.deleteOnExit();
        File f = new File(tempDir, "part.parquet");
        assertTrue(f.createNewFile());

        RecordingQueryDataClient client = new RecordingQueryDataClient();
        BigQueryLoadJobOperator operator =
                createOpenOperator(client, LocalFileSystem.getSharedInstance());

        processCompleteCheckpoint(operator, 1L, f.toURI().toString());

        assertFalse(f.exists());
        assertEquals(1, client.submittedJobs.size());
        assertSubmittedFormatOptions((LoadJobConfiguration) client.submittedJobs.get(0).config);

        operator.close();
    }

    @Test
    public void flatMapDeletesFilesAfterSuccessfulLoad() throws Exception {
        File tempDir = Files.createTempDirectory("bq-load-job-test").toFile();
        tempDir.deleteOnExit();
        File f1 = new File(tempDir, "part-1.parquet");
        File f2 = new File(tempDir, "part-2.parquet");
        assertTrue(f1.createNewFile());
        assertTrue(f2.createNewFile());

        RecordingQueryDataClient client = new RecordingQueryDataClient();
        BigQueryLoadJobOperator operator =
                createOpenOperator(client, LocalFileSystem.getSharedInstance());

        processCompleteCheckpoint(operator, 1L, f1.toURI().toString(), f2.toURI().toString());

        assertFalse(f1.exists());
        assertFalse(f2.exists());
        assertEquals(1, client.submittedJobs.size());
        assertSubmittedFormatOptions((LoadJobConfiguration) client.submittedJobs.get(0).config);

        operator.close();
    }

    @Test
    public void flatMapIgnoresFilesThatCantBeDeleted() throws Exception {
        // delete returns false (e.g. permission denied). Operator must not propagate.
        RecordingFileSystem fs = new RecordingFileSystem();
        fs.deleteResult = false;

        RecordingQueryDataClient client = new RecordingQueryDataClient();
        BigQueryLoadJobOperator operator = createOpenOperator(client, fs);

        processCompleteCheckpoint(operator, 1L, "file:///tmp/part.parquet");

        assertEquals(1, client.submittedJobs.size());
        assertSubmittedFormatOptions((LoadJobConfiguration) client.submittedJobs.get(0).config);
        assertEquals(List.of("file:///tmp/part.parquet"), new ArrayList<>(fs.deleteCalls));

        operator.close();
    }

    @Test
    public void flatMapIgnoresAlreadyDeletedFiles() throws Exception {
        // Models a re-run after a previous attempt already submitted the load job and cleaned
        // up its files: existingJobLookup short-circuits submission, and cleanup runs against a
        // file that no longer exists (delete returns false). No exception.

        RecordingFileSystem fs = new RecordingFileSystem();
        fs.deleteResult = false;

        Job existingJob = mockSuccessJob();
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        client.existingJobLookup = id -> existingJob;
        BigQueryLoadJobOperator operator = createOpenOperator(client, fs);

        processCompleteCheckpoint(operator, 1L, "file:///tmp/part.parquet");

        assertEquals(0, client.submittedJobs.size());
        assertEquals(List.of("file:///tmp/part.parquet"), new ArrayList<>(fs.deleteCalls));

        operator.close();
    }

    @Test
    public void flatMapIgnoresIoExceptionFromDelete() throws Exception {
        // delete throws IOException (e.g. transient GCS failure). Operator must not propagate.
        RecordingFileSystem fs = new RecordingFileSystem();
        fs.toThrow = new IOException("simulated GCS failure");

        RecordingQueryDataClient client = new RecordingQueryDataClient();
        BigQueryLoadJobOperator operator = createOpenOperator(client, fs);

        processCompleteCheckpoint(operator, 1L, "file:///tmp/part.parquet");

        assertEquals(1, client.submittedJobs.size());
        assertSubmittedFormatOptions((LoadJobConfiguration) client.submittedJobs.get(0).config);
        assertEquals(List.of("file:///tmp/part.parquet"), new ArrayList<>(fs.deleteCalls));

        operator.close();
    }

    // =========================================================================
    // flatMap - multi-partition path
    // =========================================================================

    /**
     * Creates an opened operator with {@code maxFilesPerLoadJob = 1}, so any checkpoint with 2+
     * files forces a multi-partition split regardless of file size.
     */
    private static BigQueryLoadJobOperator createOpenOperatorForcingMultiPartition(
            final BigQueryServices.QueryDataClient client) {
        return createOpenOperator(
                client,
                new RecordingFileSystem(),
                1,
                BigQueryLoadJobOperator.DEFAULT_MAX_BYTES_PER_LOAD_JOB);
    }

    @Test
    public void flatMapExceedingMaxFilesPerLoadJobSubmitsTempLoadsAndCopy() throws Exception {
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        RecordingFileSystem fs = new RecordingFileSystem();
        // maxFilesPerLoadJob = 1 with 2 incoming files forces a multi-partition split.
        BigQueryLoadJobOperator operator =
                createOpenOperator(
                        client, fs, 1, BigQueryLoadJobOperator.DEFAULT_MAX_BYTES_PER_LOAD_JOB);

        processCompleteCheckpoint(operator, 1L, "file:///tmp/a.parquet", "file:///tmp/b.parquet");

        assertTwoPartitionTempLoadsAndCopy(client, fs);

        operator.close();
    }

    @Test
    public void flatMapExceedingMaxBytesPerLoadJobSubmitsTempLoadsAndCopy() throws Exception {
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        RecordingFileSystem fs = new RecordingFileSystem();
        // Each file is 100 bytes (see processCompleteCheckpoint) for a total of 200 bytes
        // So the split is byte-driven, not number-of-files
        BigQueryLoadJobOperator operator =
                createOpenOperator(
                        client, fs, BigQueryLoadJobOperator.DEFAULT_MAX_FILES_PER_LOAD_JOB, 150L);

        processCompleteCheckpoint(operator, 1L, "file:///tmp/a.parquet", "file:///tmp/b.parquet");

        assertTwoPartitionTempLoadsAndCopy(client, fs);

        operator.close();
    }

    /**
     * Verifies the multi-partition output for a 2-file checkpoint regardless of which limit (file
     * count or byte size) triggered the split: 2 temp-load jobs + 1 copy job with correct IDs,
     * destinations, and dispositions, plus cleanup of both temp tables and source files.
     */
    private static void assertTwoPartitionTempLoadsAndCopy(
            RecordingQueryDataClient client, RecordingFileSystem fs) {
        // 2 temp-load jobs + 1 copy job
        assertEquals(3, client.submittedJobs.size());

        // Copy is always last (must wait for all temp-loads); temp-loads run in parallel
        // so their relative submission order between p0 and p1 is non-deterministic.
        assertEquals(EXPECTED_COPY_JOB_ID_C1, client.submittedJobs.get(2).jobId);

        Map<String, RecordingQueryDataClient.SubmittedJob> tempLoadsByJobId = new HashMap<>();
        tempLoadsByJobId.put(client.submittedJobs.get(0).jobId, client.submittedJobs.get(0));
        tempLoadsByJobId.put(client.submittedJobs.get(1).jobId, client.submittedJobs.get(1));
        assertEquals(
                Set.of(EXPECTED_TEMP_LOAD_JOB_ID_C1_P0, EXPECTED_TEMP_LOAD_JOB_ID_C1_P1),
                tempLoadsByJobId.keySet());

        LoadJobConfiguration tempLoad0 =
                (LoadJobConfiguration) tempLoadsByJobId.get(EXPECTED_TEMP_LOAD_JOB_ID_C1_P0).config;
        assertEquals(EXPECTED_TEMP_TABLE_ID_C1_P0, tempLoad0.getDestinationTable());
        // Temp tables must land in the configured tempProject/tempDataset, not the destination.
        assertEquals(TEMP_PROJECT, tempLoad0.getDestinationTable().getProject());
        assertEquals(TEMP_DATASET, tempLoad0.getDestinationTable().getDataset());
        assertEquals(JobInfo.WriteDisposition.WRITE_TRUNCATE, tempLoad0.getWriteDisposition());
        assertEquals(JobInfo.CreateDisposition.CREATE_IF_NEEDED, tempLoad0.getCreateDisposition());
        assertEquals(1, tempLoad0.getSourceUris().size());

        LoadJobConfiguration tempLoad1 =
                (LoadJobConfiguration) tempLoadsByJobId.get(EXPECTED_TEMP_LOAD_JOB_ID_C1_P1).config;
        assertEquals(EXPECTED_TEMP_TABLE_ID_C1_P1, tempLoad1.getDestinationTable());
        assertEquals(JobInfo.WriteDisposition.WRITE_TRUNCATE, tempLoad1.getWriteDisposition());
        assertEquals(JobInfo.CreateDisposition.CREATE_IF_NEEDED, tempLoad1.getCreateDisposition());
        assertEquals(1, tempLoad1.getSourceUris().size());

        Set<String> allTempLoadUris = new HashSet<>(tempLoad0.getSourceUris());
        allTempLoadUris.addAll(tempLoad1.getSourceUris());
        assertEquals(Set.of("file:///tmp/a.parquet", "file:///tmp/b.parquet"), allTempLoadUris);

        CopyJobConfiguration copy = (CopyJobConfiguration) client.submittedJobs.get(2).config;
        assertEquals(JobInfo.WriteDisposition.WRITE_APPEND, copy.getWriteDisposition());
        assertEquals(JobInfo.CreateDisposition.CREATE_NEVER, copy.getCreateDisposition());
        assertEquals(TableId.of(PROJECT, DATASET, TABLE), copy.getDestinationTable());
        assertEquals(
                List.of(EXPECTED_TEMP_TABLE_ID_C1_P0, EXPECTED_TEMP_TABLE_ID_C1_P1),
                copy.getSourceTables());

        // Both temp tables deleted after the copy succeeded (order non-deterministic — parallel).
        assertEquals(
                Set.of(EXPECTED_TEMP_TABLE_ID_C1_P0, EXPECTED_TEMP_TABLE_ID_C1_P1),
                new HashSet<>(client.deleteTableCalls));

        // Both source files deleted after the copy succeeded (order non-deterministic — parallel).
        assertEquals(
                Set.of("file:///tmp/a.parquet", "file:///tmp/b.parquet"),
                new HashSet<>(fs.deleteCalls));
    }

    @Test
    public void flatMapMultiPartitionTempLoadFailureThrowsAndSkipsCopy() throws Exception {
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        client.submitJobResult = mockFailedJob();
        BigQueryLoadJobOperator operator = createOpenOperatorForcingMultiPartition(client);

        ExecutionException ex =
                assertThrows(
                        ExecutionException.class,
                        () ->
                                processCompleteCheckpoint(
                                        operator,
                                        1L,
                                        "file:///tmp/a.parquet",
                                        "file:///tmp/b.parquet"));
        assertTrue(ex.getCause() instanceof BigQueryConnectorException);
        assertTrue(ex.getCause().getMessage().contains("tmp-load"));

        // Both temp-loads attempted in parallel; copy never submitted; no deletes.
        assertEquals(2, client.submittedJobs.size());
        assertFalse(
                client.submittedJobs.stream()
                        .anyMatch(j -> j.jobId.equals(EXPECTED_COPY_JOB_ID_C1)));
        assertTrue(client.deleteTableCalls.isEmpty());

        operator.close();
    }

    @Test
    public void flatMapMultiPartitionCopyFailureThrowsAndSkipsDelete() throws Exception {
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        Job successJob = mockSuccessJob();
        Job failedJob = mockFailedJob();
        // Temp loads (first 2 calls) succeed, copy job (3rd) fails.
        AtomicInteger callIdx = new AtomicInteger(0);
        client.waitForJobBehavior = j -> callIdx.getAndIncrement() < 2 ? successJob : failedJob;
        client.submitJobResult = successJob;

        BigQueryLoadJobOperator operator = createOpenOperatorForcingMultiPartition(client);

        BigQueryConnectorException ex =
                assertThrows(
                        BigQueryConnectorException.class,
                        () ->
                                processCompleteCheckpoint(
                                        operator,
                                        1L,
                                        "file:///tmp/a.parquet",
                                        "file:///tmp/b.parquet"));
        assertTrue(ex.getMessage().contains("copy"));

        // Two temp loads + one copy submitted; copy threw, so no deletes attempted.
        assertEquals(3, client.submittedJobs.size());
        assertTrue(client.deleteTableCalls.isEmpty());

        operator.close();
    }

    @Test
    public void flatMapMultiPartitionToleratesDeleteTableReturningFalse() throws Exception {
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        client.deleteTableBehavior = id -> false; // delete reports the table didn't exist
        BigQueryLoadJobOperator operator = createOpenOperatorForcingMultiPartition(client);

        // Should not throw despite delete returning false.
        processCompleteCheckpoint(operator, 1L, "file:///tmp/a.parquet", "file:///tmp/b.parquet");

        assertEquals(3, client.submittedJobs.size());
        assertEquals(2, client.deleteTableCalls.size());

        operator.close();
    }

    // =========================================================================
    // flatMap - multi-partition replay: existing temp-load / copy jobs across partitions.
    // Single-partition tests already cover submitAndAwaitJob's existing/running/failed branches;
    // these tests target coordination scenarios unique to the multi-partition path, where
    // different jobs (per-partition temp-loads + copy) can be in different states on replay.
    // =========================================================================

    @Test
    public void flatMapMultiPartitionResumesExistingTempLoadAndSubmitsMissingOne()
            throws Exception {
        // p0 temp-load was submitted on a prior attempt (now existing-success); p1 wasn't.
        // Operator skips p0 submission, submits p1, then runs copy.
        Job p0Existing = mockSuccessJob();
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        client.existingJobLookup =
                id -> id.equals(EXPECTED_TEMP_LOAD_JOB_ID_C1_P0) ? p0Existing : null;
        BigQueryLoadJobOperator operator = createOpenOperatorForcingMultiPartition(client);

        processCompleteCheckpoint(operator, 1L, "file:///tmp/a.parquet", "file:///tmp/b.parquet");

        // Only p1 temp-load + copy submitted; p0 was reused.
        assertEquals(2, client.submittedJobs.size());
        Set<String> submittedIds = new HashSet<>();
        for (RecordingQueryDataClient.SubmittedJob j : client.submittedJobs) {
            submittedIds.add(j.jobId);
        }
        assertEquals(
                Set.of(EXPECTED_TEMP_LOAD_JOB_ID_C1_P1, EXPECTED_COPY_JOB_ID_C1), submittedIds);

        // Both temp tables still cleaned up.
        assertEquals(
                Set.of(EXPECTED_TEMP_TABLE_ID_C1_P0, EXPECTED_TEMP_TABLE_ID_C1_P1),
                new HashSet<>(client.deleteTableCalls));

        operator.close();
    }

    @Test
    public void flatMapMultiPartitionAllTempLoadsExistingSubmitsOnlyCopy() throws Exception {
        // Both temp-loads succeeded on a prior attempt; copy did not run.
        Job tempSuccess = mockSuccessJob();
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        client.existingJobLookup =
                id ->
                        (id.equals(EXPECTED_TEMP_LOAD_JOB_ID_C1_P0)
                                        || id.equals(EXPECTED_TEMP_LOAD_JOB_ID_C1_P1))
                                ? tempSuccess
                                : null;
        BigQueryLoadJobOperator operator = createOpenOperatorForcingMultiPartition(client);

        processCompleteCheckpoint(operator, 1L, "file:///tmp/a.parquet", "file:///tmp/b.parquet");

        // Only the copy was submitted.
        assertEquals(1, client.submittedJobs.size());
        assertEquals(EXPECTED_COPY_JOB_ID_C1, client.submittedJobs.get(0).jobId);

        // Cleanup still runs for both temp tables.
        assertEquals(
                Set.of(EXPECTED_TEMP_TABLE_ID_C1_P0, EXPECTED_TEMP_TABLE_ID_C1_P1),
                new HashSet<>(client.deleteTableCalls));

        operator.close();
    }

    @Test
    public void flatMapMultiPartitionEverythingExistingSubmitsNothingAndCleansUp()
            throws Exception {
        // Full replay after operator died with everything already submitted: both temp-loads
        // and the copy are existing-success. Operator just waits on each and cleans up.
        Job successJob = mockSuccessJob();
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        client.existingJobLookup = id -> successJob;
        BigQueryLoadJobOperator operator = createOpenOperatorForcingMultiPartition(client);

        processCompleteCheckpoint(operator, 1L, "file:///tmp/a.parquet", "file:///tmp/b.parquet");

        // No submissions at all.
        assertEquals(0, client.submittedJobs.size());
        // All three jobs (2 temp-loads + copy) were waited on.
        assertEquals(3, client.waitForJobCalls.size());
        // Both temp tables cleaned up.
        assertEquals(
                Set.of(EXPECTED_TEMP_TABLE_ID_C1_P0, EXPECTED_TEMP_TABLE_ID_C1_P1),
                new HashSet<>(client.deleteTableCalls));

        operator.close();
    }

    @Test
    public void flatMapMultiPartitionExistingFailedTempLoadThrowsAndSkipsCopy() throws Exception {
        // p0's prior temp-load attempt is recorded as failed in BQ; p1 succeeded.
        // submitAndAwaitJob short-circuits to wait + throw on p0; copy never runs;
        // no cleanup because we never reached it.
        Job successJob = mockSuccessJob();
        Job failedJob = mockFailedJob();
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        client.existingJobLookup =
                id -> {
                    if (id.equals(EXPECTED_TEMP_LOAD_JOB_ID_C1_P0)) {
                        return failedJob;
                    }
                    if (id.equals(EXPECTED_TEMP_LOAD_JOB_ID_C1_P1)) {
                        return successJob;
                    }
                    return null;
                };
        BigQueryLoadJobOperator operator = createOpenOperatorForcingMultiPartition(client);

        ExecutionException ex =
                assertThrows(
                        ExecutionException.class,
                        () ->
                                processCompleteCheckpoint(
                                        operator,
                                        1L,
                                        "file:///tmp/a.parquet",
                                        "file:///tmp/b.parquet"));
        assertTrue(ex.getCause() instanceof BigQueryConnectorException);
        assertTrue(ex.getCause().getMessage().contains("failed:"));

        // Nothing submitted (both temp-loads short-circuited via getJob); no copy; no deletes.
        assertEquals(0, client.submittedJobs.size());
        assertTrue(client.deleteTableCalls.isEmpty());

        operator.close();
    }
}
