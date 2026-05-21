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
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.ParquetOptions;
import com.google.cloud.bigquery.TableDefinition;
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
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.AbstractExecutorService;
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
    private static final String EXPECTED_LOAD_JOB_ID_C1 =
            "flink-bq-load_" + FIXED_JOB_ID_HEX + "_" + FIXED_UUID + "_c1";

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
     * fields (existingJob / submitJobResult / waitForJobBehavior) to script responses, and asserts
     * against the recorded lists rather than verifying mock interactions.
     */
    private static class RecordingQueryDataClient implements BigQueryServices.QueryDataClient {
        // Recording.
        final List<SubmittedJob> submittedJobs = new ArrayList<>();
        final List<String> getJobLookups = new ArrayList<>();
        final List<Job> waitForJobCalls = new ArrayList<>();

        // Configurable behavior - defaults form a happy-path success.
        Job existingJob = null;
        Job submitJobResult = mockSuccessJob();
        Function<Job, Job> waitForJobBehavior = j -> j;

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
        public Job submitJob(String project, String jobId, JobConfiguration config) {
            submittedJobs.add(new SubmittedJob(project, jobId, config));
            return submitJobResult;
        }

        @Override
        public Job getJob(String project, String jobId) {
            getJobLookups.add(jobId);
            return existingJob;
        }

        @Override
        public Job waitForJob(Job job) {
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
        final BigQueryLoadJobOperator operator =
                new BigQueryLoadJobOperator(
                        testConnectOptions(client),
                        FIXED_UUID,
                        () -> Executors.newCachedThreadPool(),
                        () -> fs,
                        EXPECTED_PARQUET_OPTIONS);
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
     * parallel via the cleanup executor.
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
                        () -> {
                            supplierCalls.incrementAndGet();
                            return executorService;
                        },
                        RecordingFileSystem::new,
                        EXPECTED_PARQUET_OPTIONS);
        operator.setRuntimeContext(mockRuntimeContext());

        // Construction alone must not create the executor.
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
                        () -> executorService,
                        RecordingFileSystem::new,
                        EXPECTED_PARQUET_OPTIONS);
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
                        () -> executorService,
                        RecordingFileSystem::new,
                        EXPECTED_PARQUET_OPTIONS);
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
                        () -> executorService,
                        RecordingFileSystem::new,
                        EXPECTED_PARQUET_OPTIONS);
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
        assertEquals(PROJECT, client.submittedJobs.get(0).project);
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
        assertEquals(PROJECT, client.submittedJobs.get(0).project);
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
        assertEquals(PROJECT, client.submittedJobs.get(0).project);
        assertEquals(
                JobInfo.CreateDisposition.CREATE_NEVER,
                ((LoadJobConfiguration) client.submittedJobs.get(0).config).getCreateDisposition());
        assertSubmittedFormatOptions((LoadJobConfiguration) client.submittedJobs.get(0).config);
        // submitJob returns submitJobResult; that's what waitForJob receives.
        assertEquals(List.of(client.submitJobResult), client.waitForJobCalls);

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
    // submitAndAwaitJob - existing job succeed/fail/running
    // =========================================================================

    @Test
    public void testFlatMapExistingSuccessJobSkipsSubmission() throws Exception {
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        client.existingJob = mockSuccessJob();

        BigQueryLoadJobOperator operator = createOpenOperator(client);
        processCompleteCheckpoint(operator, 1L, "file:///tmp/test.parquet");

        assertEquals(0, client.submittedJobs.size());
        assertEquals(List.of(client.existingJob), client.waitForJobCalls);

        operator.close();
    }

    @Test
    public void testFlatMapExistingFailedJobThrows() throws Exception {
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        client.existingJob = mockFailedJob();

        BigQueryLoadJobOperator operator = createOpenOperator(client);

        BigQueryConnectorException ex =
                assertThrows(
                        BigQueryConnectorException.class,
                        () -> processCompleteCheckpoint(operator, 1L, "file:///tmp/test.parquet"));
        assertTrue(ex.getMessage().contains("failed:"));

        assertEquals(0, client.submittedJobs.size());
        assertEquals(List.of(client.existingJob), client.waitForJobCalls);

        operator.close();
    }

    @Test
    public void testFlatMapRunningJobSucceedsWaitsAndReturns() throws Exception {
        Job successJob = mockSuccessJob();
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        client.existingJob = mockRunningJob();
        client.waitForJobBehavior = j -> successJob;

        BigQueryLoadJobOperator operator = createOpenOperator(client);
        processCompleteCheckpoint(operator, 1L, "file:///tmp/test.parquet");

        assertEquals(0, client.submittedJobs.size());
        assertEquals(List.of(client.existingJob), client.waitForJobCalls);

        operator.close();
    }

    @Test
    public void testFlatMapRunningJobFailsThrows() throws Exception {
        Job failedAfterWait = mockFailedJob();
        RecordingQueryDataClient client = new RecordingQueryDataClient();
        client.existingJob = mockRunningJob();
        client.waitForJobBehavior = j -> failedAfterWait;

        BigQueryLoadJobOperator operator = createOpenOperator(client);

        BigQueryConnectorException ex =
                assertThrows(
                        BigQueryConnectorException.class,
                        () -> processCompleteCheckpoint(operator, 1L, "file:///tmp/test.parquet"));
        assertTrue(ex.getMessage().contains("failed:"));

        assertEquals(0, client.submittedJobs.size());
        assertEquals(List.of(client.existingJob), client.waitForJobCalls);

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
        // up its files: existingJob short-circuits submission, and cleanup runs against a file
        // that no longer exists (delete returns false). No exception.

        RecordingFileSystem fs = new RecordingFileSystem();
        fs.deleteResult = false;

        RecordingQueryDataClient client = new RecordingQueryDataClient();
        client.existingJob = mockSuccessJob();
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
}
