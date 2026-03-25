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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.util.Collector;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import com.google.cloud.flink.bigquery.common.exceptions.BigQueryConnectorException;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import org.junit.Test;

import javax.annotation.Nullable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link BigQueryLoadJobOperator}. */
public class BigQueryLoadJobOperatorTest {

    private static final String PROJECT = "p";
    private static final String DATASET = "d";
    private static final String TABLE = "t";

    // =========================================================================
    // Test infrastructure
    // =========================================================================

    /** Simple implementation of {@link InProgressFileWriter.PendingFileRecoverable} for tests. */
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
    }

    /** A no-op collector used to satisfy the {@code flatMap} signature. */
    private static final Collector<Void> NOOP_COLLECTOR =
            new Collector<Void>() {
                @Override
                public void collect(Void record) {}

                @Override
                public void close() {}
            };

    private static final String FIXED_UUID = "abc12345";
    private static final String FIXED_JOB_NAME = "test_job";
    private static final int FIXED_SUBTASK_INDEX = 0;

    /** Creates a mock {@link RuntimeContext} returning FIXED_JOB_NAME and FIXED_SUBTASK. */
    private static RuntimeContext mockRuntimeContext() {
        RuntimeContext ctx = mock(RuntimeContext.class, RETURNS_DEEP_STUBS);
        when(ctx.getJobInfo().getJobName()).thenReturn(FIXED_JOB_NAME);
        when(ctx.getTaskInfo().getIndexOfThisSubtask()).thenReturn(FIXED_SUBTASK_INDEX);
        return ctx;
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

    /** Creates an opened operator with the given client. */
    private static BigQueryLoadJobOperator createOpenOperator(
            final BigQueryServices.QueryDataClient client) throws Exception {
        return createOpenOperator(client, FIXED_UUID);
    }

    /** Creates an opened operator with the given client and UUID. */
    private static BigQueryLoadJobOperator createOpenOperator(
            final BigQueryServices.QueryDataClient client, final String uuid) throws Exception {
        final BigQueryLoadJobOperator operator =
                new BigQueryLoadJobOperator(
                        PROJECT, DATASET, TABLE, testConnectOptions(client), uuid);
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
        operator.flatMap(summary(0, 1, checkpointId, uris.length), NOOP_COLLECTOR);
        for (String uri : uris) {
            operator.flatMap(committableWithFile(0, checkpointId, uri, 100L), NOOP_COLLECTOR);
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

    // =========================================================================
    // generateJobId tests
    // =========================================================================

    @Test
    public void testGenerateJobId_isDeterministic() throws Exception {
        BigQueryLoadJobOperator operator =
                createOpenOperator(mock(BigQueryServices.QueryDataClient.class));

        String jobId1 = operator.generateJobId(1L);
        String jobId2 = operator.generateJobId(1L);
        assertEquals(jobId1, jobId2);
    }

    @Test
    public void testGenerateJobId_hasCorrectFormat() throws Exception {
        BigQueryLoadJobOperator operator =
                createOpenOperator(mock(BigQueryServices.QueryDataClient.class));

        String jobId = operator.generateJobId(1L);
        assertEquals("flink_bq_load_test_job_abc12345_c1_s0", jobId);
    }

    @Test
    public void testGenerateJobId_includesCheckpointId() throws Exception {
        BigQueryLoadJobOperator operator =
                createOpenOperator(mock(BigQueryServices.QueryDataClient.class));

        String jobId = operator.generateJobId(42L);
        assertTrue("Job ID should contain checkpoint ID", jobId.contains("_c42_"));
    }

    @Test
    public void testGenerateJobId_differentCheckpointIds() throws Exception {
        BigQueryLoadJobOperator operator =
                createOpenOperator(mock(BigQueryServices.QueryDataClient.class));

        assertNotEquals(operator.generateJobId(1L), operator.generateJobId(2L));
    }

    @Test
    public void testGenerateJobId_differentUuidsProduceDifferentIds() throws Exception {
        BigQueryServices.QueryDataClient client = mock(BigQueryServices.QueryDataClient.class);
        BigQueryLoadJobOperator op1 = createOpenOperator(client, "uuid1");
        BigQueryLoadJobOperator op2 = createOpenOperator(client, "uuid2");

        assertNotEquals(op1.generateJobId(1L), op2.generateJobId(1L));
    }

    // =========================================================================
    // sanitizeJobName tests
    // =========================================================================

    @Test
    public void testSanitizeJobName_alphanumericUnchanged() {
        assertEquals("my_job_123", BigQueryLoadJobOperator.sanitizeJobName("my_job_123"));
    }

    @Test
    public void testSanitizeJobName_specialCharsReplaced() {
        assertEquals("my_job_v2_", BigQueryLoadJobOperator.sanitizeJobName("my job (v2)"));
    }

    @Test
    public void testSanitizeJobName_consecutiveUnderscoresCollapsed() {
        assertEquals("a_b", BigQueryLoadJobOperator.sanitizeJobName("a___b"));
    }

    @Test
    public void testSanitizeJobName_truncatedTo100Chars() {
        String longName = "a".repeat(150);
        String sanitized = BigQueryLoadJobOperator.sanitizeJobName(longName);
        assertEquals(100, sanitized.length());
    }

    @Test
    public void testSanitizeJobName_hyphenPreserved() {
        assertEquals("my-job", BigQueryLoadJobOperator.sanitizeJobName("my-job"));
    }

    // =========================================================================
    // processElement + endInput tests
    // =========================================================================

    @Test
    public void testProcessElementDoesNotSubmitJobs() throws Exception {
        BigQueryServices.QueryDataClient client = mock(BigQueryServices.QueryDataClient.class);
        BigQueryLoadJobOperator operator = createOpenOperator(client);

        operator.flatMap(committableWithFile(0, 1L, "file:///tmp/test.avro", 100L), NOOP_COLLECTOR);

        verify(client, never()).submitJob(any(), any(), any());
        verify(client, never()).getJob(any(), any());
    }

    @Test
    public void testClose_happyPath_loadJobSucceeds() throws Exception {
        Job successJob = mockSuccessJob();
        Job pendingJob = mock(Job.class);
        BigQueryServices.QueryDataClient client = mock(BigQueryServices.QueryDataClient.class);
        when(client.getJob(eq(PROJECT), anyString())).thenReturn(null);
        when(client.submitJob(eq(PROJECT), anyString(), any())).thenReturn(pendingJob);
        when(client.waitForJob(pendingJob)).thenReturn(successJob);

        BigQueryLoadJobOperator operator = createOpenOperator(client);
        processCompleteCheckpoint(operator, 1L, "file:///tmp/test.avro");
        operator.close();

        verify(client, times(1)).submitJob(eq(PROJECT), anyString(), any());
        verify(client).waitForJob(pendingJob);
    }

    @Test
    public void testClose_multipleFiles_singleLoadJob() throws Exception {
        Job successJob = mockSuccessJob();
        Job pendingJob = mock(Job.class);
        BigQueryServices.QueryDataClient client = mock(BigQueryServices.QueryDataClient.class);
        when(client.getJob(eq(PROJECT), anyString())).thenReturn(null);
        when(client.submitJob(eq(PROJECT), anyString(), any())).thenReturn(pendingJob);
        when(client.waitForJob(pendingJob)).thenReturn(successJob);

        BigQueryLoadJobOperator operator = createOpenOperator(client);
        processCompleteCheckpoint(operator, 1L, "file:///tmp/f1.avro", "file:///tmp/f2.avro");
        operator.close();

        verify(client, times(1)).submitJob(eq(PROJECT), anyString(), any());
    }

    @Test
    public void testClose_noFiles_noLoadJob() throws Exception {
        BigQueryServices.QueryDataClient client = mock(BigQueryServices.QueryDataClient.class);
        BigQueryLoadJobOperator operator = createOpenOperator(client);

        operator.close();

        verify(client, never()).submitJob(any(), any(), any());
        verify(client, never()).getJob(any(), any());
    }

    @Test
    public void testClose_existingSuccessJob_skipsSubmission() throws Exception {
        Job existingSuccess = mockSuccessJob();
        BigQueryServices.QueryDataClient client = mock(BigQueryServices.QueryDataClient.class);
        when(client.getJob(eq(PROJECT), anyString())).thenReturn(existingSuccess);
        when(client.waitForJob(existingSuccess)).thenReturn(existingSuccess);

        BigQueryLoadJobOperator operator = createOpenOperator(client);
        processCompleteCheckpoint(operator, 1L, "file:///tmp/test.avro");
        operator.close();

        verify(client, never()).submitJob(any(), any(), any());
        verify(client).waitForJob(existingSuccess);
    }

    @Test
    public void testClose_existingFailedJob_throws() throws Exception {
        Job failedJob = mockFailedJob();
        BigQueryServices.QueryDataClient client = mock(BigQueryServices.QueryDataClient.class);
        when(client.getJob(eq(PROJECT), anyString())).thenReturn(failedJob);
        when(client.waitForJob(failedJob)).thenReturn(failedJob);

        BigQueryLoadJobOperator operator = createOpenOperator(client);

        BigQueryConnectorException ex =
                assertThrows(
                        BigQueryConnectorException.class,
                        () -> processCompleteCheckpoint(operator, 1L, "file:///tmp/test.avro"));
        assertTrue(ex.getMessage().contains("failed:"));

        verify(client, never()).submitJob(any(), any(), any());
        verify(client).waitForJob(failedJob);
    }

    @Test
    public void testClose_runningJobSucceeds_waitsAndReturns() throws Exception {
        Job runningJob = mockRunningJob();
        Job successJob = mockSuccessJob();
        BigQueryServices.QueryDataClient client = mock(BigQueryServices.QueryDataClient.class);
        when(client.getJob(eq(PROJECT), anyString())).thenReturn(runningJob);
        when(client.waitForJob(runningJob)).thenReturn(successJob);

        BigQueryLoadJobOperator operator = createOpenOperator(client);
        processCompleteCheckpoint(operator, 1L, "file:///tmp/test.avro");
        operator.close();

        verify(client, never()).submitJob(any(), any(), any());
        verify(client).waitForJob(runningJob);
    }

    @Test
    public void testClose_runningJobFails_throws() throws Exception {
        Job runningJob = mockRunningJob();
        Job failedAfterWait = mockFailedJob();
        BigQueryServices.QueryDataClient client = mock(BigQueryServices.QueryDataClient.class);
        when(client.getJob(eq(PROJECT), anyString())).thenReturn(runningJob);
        when(client.waitForJob(runningJob)).thenReturn(failedAfterWait);

        BigQueryLoadJobOperator operator = createOpenOperator(client);

        BigQueryConnectorException ex =
                assertThrows(
                        BigQueryConnectorException.class,
                        () -> processCompleteCheckpoint(operator, 1L, "file:///tmp/test.avro"));
        assertTrue(ex.getMessage().contains("failed:"));

        verify(client, never()).submitJob(any(), any(), any());
        verify(client).waitForJob(runningJob);
    }

    @Test
    public void testClose_newJobFails_throws() throws Exception {
        Job failedResult = mockFailedJob();
        Job pendingJob = mock(Job.class);
        BigQueryServices.QueryDataClient client = mock(BigQueryServices.QueryDataClient.class);
        when(client.getJob(eq(PROJECT), anyString())).thenReturn(null);
        when(client.submitJob(eq(PROJECT), anyString(), any())).thenReturn(pendingJob);
        when(client.waitForJob(pendingJob)).thenReturn(failedResult);

        BigQueryLoadJobOperator operator = createOpenOperator(client);

        BigQueryConnectorException ex =
                assertThrows(
                        BigQueryConnectorException.class,
                        () -> processCompleteCheckpoint(operator, 1L, "file:///tmp/test.avro"));
        assertTrue(ex.getMessage().contains("failed:"));

        verify(client, times(1)).submitJob(eq(PROJECT), anyString(), any());
    }

    // =========================================================================
    // Eager submission tests (processElement triggers load jobs on checkpoint completion)
    // =========================================================================

    @Test
    public void testEagerSubmission_completeCheckpointTriggersSubmission() throws Exception {
        Job successJob = mockSuccessJob();
        Job pendingJob = mock(Job.class);
        BigQueryServices.QueryDataClient client = mock(BigQueryServices.QueryDataClient.class);
        when(client.getJob(eq(PROJECT), anyString())).thenReturn(null);
        when(client.submitJob(eq(PROJECT), anyString(), any())).thenReturn(pendingJob);
        when(client.waitForJob(pendingJob)).thenReturn(successJob);

        BigQueryLoadJobOperator operator = createOpenOperator(client);
        processCompleteCheckpoint(operator, 1L, "file:///tmp/test.avro");

        // No timer advance needed — flatMap should have triggered submission
        verify(client, times(1)).submitJob(eq(PROJECT), anyString(), any());
    }

    @Test
    public void testEagerSubmission_emptyCheckpoint_noSubmission() throws Exception {
        BigQueryServices.QueryDataClient client = mock(BigQueryServices.QueryDataClient.class);
        BigQueryLoadJobOperator operator = createOpenOperator(client);

        // Summary with 0 committables — complete but empty
        operator.flatMap(summary(0, 1, 1L, 0), NOOP_COLLECTOR);

        verify(client, never()).submitJob(any(), any(), any());
        verify(client, never()).getJob(any(), any());
    }

    @Test
    public void testEagerSubmission_twoCompleteCheckpoints_twoSubmissions() throws Exception {
        Job successJob = mockSuccessJob();
        Job pendingJob = mock(Job.class);
        BigQueryServices.QueryDataClient client = mock(BigQueryServices.QueryDataClient.class);
        when(client.getJob(eq(PROJECT), anyString())).thenReturn(null);
        when(client.submitJob(eq(PROJECT), anyString(), any())).thenReturn(pendingJob);
        when(client.waitForJob(pendingJob)).thenReturn(successJob);

        BigQueryLoadJobOperator operator = createOpenOperator(client);

        processCompleteCheckpoint(operator, 1L, "file:///tmp/f1.avro");
        processCompleteCheckpoint(operator, 2L, "file:///tmp/f2.avro");

        // Both submitted eagerly from flatMap
        verify(client, times(2)).submitJob(eq(PROJECT), anyString(), any());
    }

    @Test
    public void testEagerSubmission_closeIsNoOpAfterEagerSubmission() throws Exception {
        Job successJob = mockSuccessJob();
        Job pendingJob = mock(Job.class);
        BigQueryServices.QueryDataClient client = mock(BigQueryServices.QueryDataClient.class);
        when(client.getJob(eq(PROJECT), anyString())).thenReturn(null);
        when(client.submitJob(eq(PROJECT), anyString(), any())).thenReturn(pendingJob);
        when(client.waitForJob(pendingJob)).thenReturn(successJob);

        BigQueryLoadJobOperator operator = createOpenOperator(client);
        processCompleteCheckpoint(operator, 1L, "file:///tmp/test.avro");

        // close should be a no-op — checkpoint already submitted eagerly
        operator.close();

        // Only 1 submission total (from flatMap), not 2
        verify(client, times(1)).submitJob(eq(PROJECT), anyString(), any());
    }

    @Test
    public void testEagerSubmission_incompleteCheckpoint_notSubmitted() throws Exception {
        BigQueryServices.QueryDataClient client = mock(BigQueryServices.QueryDataClient.class);
        BigQueryLoadJobOperator operator = createOpenOperator(client);

        // Send summary expecting 2 files, but only send 1
        operator.flatMap(summary(0, 1, 1L, 2), NOOP_COLLECTOR);
        operator.flatMap(committableWithFile(0, 1L, "file:///tmp/f1.avro", 100L), NOOP_COLLECTOR);

        // Checkpoint is incomplete — should not submit
        verify(client, never()).submitJob(any(), any(), any());
        verify(client, never()).getJob(any(), any());
    }

    @Test
    public void testEagerSubmission_multipleSubtasks_waitsForAllSummaries() throws Exception {
        Job successJob = mockSuccessJob();
        Job pendingJob = mock(Job.class);
        BigQueryServices.QueryDataClient client = mock(BigQueryServices.QueryDataClient.class);
        when(client.getJob(eq(PROJECT), anyString())).thenReturn(null);
        when(client.submitJob(eq(PROJECT), anyString(), any())).thenReturn(pendingJob);
        when(client.waitForJob(pendingJob)).thenReturn(successJob);

        BigQueryLoadJobOperator operator = createOpenOperator(client);

        // Subtask 0 summary + file (of 2 subtasks total)
        operator.flatMap(summary(0, 2, 1L, 1), NOOP_COLLECTOR);
        operator.flatMap(committableWithFile(0, 1L, "file:///tmp/f0.avro", 100L), NOOP_COLLECTOR);

        // Only 1 of 2 summaries received — should not submit yet
        verify(client, never()).submitJob(any(), any(), any());

        // Subtask 1 summary + file
        operator.flatMap(summary(1, 2, 1L, 1), NOOP_COLLECTOR);
        operator.flatMap(committableWithFile(1, 1L, "file:///tmp/f1.avro", 100L), NOOP_COLLECTOR);

        // Now both summaries + all files received — should have submitted
        verify(client, times(1)).submitJob(eq(PROJECT), anyString(), any());
    }

    @Test
    public void testCheckpointGrouping_separateJobsPerCheckpoint() throws Exception {
        Job successJob = mockSuccessJob();
        Job pendingJob = mock(Job.class);
        BigQueryServices.QueryDataClient client = mock(BigQueryServices.QueryDataClient.class);
        when(client.getJob(eq(PROJECT), anyString())).thenReturn(null);
        when(client.submitJob(eq(PROJECT), anyString(), any())).thenReturn(pendingJob);
        when(client.waitForJob(pendingJob)).thenReturn(successJob);

        BigQueryLoadJobOperator operator = createOpenOperator(client);

        // Two complete checkpoints with different files
        processCompleteCheckpoint(operator, 1L, "file:///tmp/c1.avro");
        processCompleteCheckpoint(operator, 2L, "file:///tmp/c2.avro");

        operator.close();

        // Should be 2 separate load job submissions
        verify(client, times(2)).submitJob(eq(PROJECT), anyString(), any());
    }

    @Test
    public void testCheckpointGrouping_emptyCheckpoint_noLoadJob() throws Exception {
        BigQueryServices.QueryDataClient client = mock(BigQueryServices.QueryDataClient.class);
        BigQueryLoadJobOperator operator = createOpenOperator(client);

        // Summary with 0 committables
        operator.flatMap(summary(0, 1, 1L, 0), NOOP_COLLECTOR);

        operator.close();

        // No load job — checkpoint is complete but has no files
        verify(client, never()).submitJob(any(), any(), any());
        verify(client, never()).getJob(any(), any());
    }
}
