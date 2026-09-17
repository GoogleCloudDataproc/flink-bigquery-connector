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

package com.google.cloud.flink.bigquery.services;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.objenesis.ObjenesisStd;

import java.lang.reflect.Field;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link BigQueryServicesImpl.QueryDataClientImpl} load-job methods.
 *
 * <p>{@code QueryDataClientImpl}'s only public constructor bootstraps a real {@link BigQuery}
 * client from credentials. We use Objenesis (already on the test classpath via Mockito) to create
 * an instance without invoking the constructor, then reflect to inject a mock {@code bigQuery}
 * field.
 */
public class BigQueryServicesImplTest {

    private static BigQueryServicesImpl.QueryDataClientImpl newClientWithMockBigQuery(
            BigQuery bigQuery) throws Exception {
        BigQueryServicesImpl.QueryDataClientImpl client =
                new ObjenesisStd().newInstance(BigQueryServicesImpl.QueryDataClientImpl.class);
        Field f = BigQueryServicesImpl.QueryDataClientImpl.class.getDeclaredField("bigQuery");
        f.setAccessible(true);
        f.set(client, bigQuery);
        return client;
    }

    private static JobConfiguration newLoadJobConfig() {
        return LoadJobConfiguration.newBuilder(
                        TableId.of("p", "d", "t"), Collections.singletonList("gs://bucket/a.avro"))
                .build();
    }

    @Test
    public void materializeViewSubmitsJobUnderBillingProject() throws Exception {
        BigQuery bq = mock(BigQuery.class);
        BigQueryOptions defaultOptions = mock(BigQueryOptions.class);
        BigQueryOptions.Builder billingOptionsBuilder = mock(BigQueryOptions.Builder.class);
        BigQueryOptions billingOptions = mock(BigQueryOptions.class);
        BigQuery billingBigQuery = mock(BigQuery.class);
        when(bq.getOptions()).thenReturn(defaultOptions);
        when(defaultOptions.toBuilder()).thenReturn(billingOptionsBuilder);
        when(billingOptionsBuilder.setQuotaProjectId("billing-project"))
                .thenReturn(billingOptionsBuilder);
        when(billingOptionsBuilder.build()).thenReturn(billingOptions);
        when(billingOptions.getService()).thenReturn(billingBigQuery);
        when(billingBigQuery.create(any(JobInfo.class)))
                .thenThrow(new BigQueryException(500, "stop after job submission"));

        BigQueryServicesImpl.QueryDataClientImpl client = newClientWithMockBigQuery(bq);

        assertThrows(
                RuntimeException.class,
                () ->
                        client.materializeView(
                                "source-project",
                                "source-dataset",
                                "source-view",
                                Collections.emptyList(),
                                null,
                                24,
                                "materialization-project",
                                "materialization-dataset",
                                "billing-project"));

        // The job is submitted through the quota-project client with an explicit JobId
        // carrying the billing project.
        ArgumentCaptor<JobInfo> jobInfoCaptor = ArgumentCaptor.forClass(JobInfo.class);
        verify(billingBigQuery).create(jobInfoCaptor.capture());
        JobId jobId = jobInfoCaptor.getValue().getJobId();
        assertEquals("billing-project", jobId.getProject());
        // The job id itself stays client-generated.
        assertNotNull(jobId.getJob());

        QueryJobConfiguration config = jobInfoCaptor.getValue().getConfiguration();
        assertEquals("materialization-project", config.getDestinationTable().getProject());
        assertEquals("materialization-dataset", config.getDestinationTable().getDataset());
    }

    @Test
    public void materializeViewWithoutBillingProjectHasNoExplicitJobId() throws Exception {
        BigQuery bq = mock(BigQuery.class);
        when(bq.create(any(JobInfo.class)))
                .thenThrow(new BigQueryException(500, "stop after job submission"));

        BigQueryServicesImpl.QueryDataClientImpl client = newClientWithMockBigQuery(bq);

        assertThrows(
                RuntimeException.class,
                () ->
                        client.materializeView(
                                "source-project",
                                "source-dataset",
                                "source-view",
                                Collections.emptyList(),
                                null,
                                24,
                                "materialization-project",
                                "materialization-dataset",
                                null));

        // No explicit JobId: BigQuery#create places the job in the client's default project
        ArgumentCaptor<JobInfo> jobInfoCaptor = ArgumentCaptor.forClass(JobInfo.class);
        verify(bq).create(jobInfoCaptor.capture());
        assertNull(jobInfoCaptor.getValue().getJobId());
    }

    @Test
    public void submitJobDelegatesToBigQueryWithCorrectJobId() throws Exception {
        BigQuery bq = mock(BigQuery.class);
        Job expectedJob = mock(Job.class);
        when(bq.create((JobInfo) org.mockito.ArgumentMatchers.any())).thenReturn(expectedJob);

        BigQueryServicesImpl.QueryDataClientImpl client = newClientWithMockBigQuery(bq);
        JobConfiguration config = newLoadJobConfig();

        Job result = client.submitJob("project-x", "my_job_123", config);

        assertSame(expectedJob, result);
        ArgumentCaptor<JobInfo> jobInfoCaptor = ArgumentCaptor.forClass(JobInfo.class);
        verify(bq).create(jobInfoCaptor.capture());
        JobInfo submitted = jobInfoCaptor.getValue();
        assertEquals(JobId.of("project-x", "my_job_123"), submitted.getJobId());
        assertSame(config, submitted.getConfiguration());
    }

    @Test
    public void getJobDelegatesWithCorrectJobId() throws Exception {
        BigQuery bq = mock(BigQuery.class);
        Job expectedJob = mock(Job.class);
        when(bq.getJob(JobId.of("project-x", "lookup_job"))).thenReturn(expectedJob);

        BigQueryServicesImpl.QueryDataClientImpl client = newClientWithMockBigQuery(bq);

        Job result = client.getJob("project-x", "lookup_job");

        assertSame(expectedJob, result);
    }

    @Test
    public void getJobReturnsNullWhenNotFound() throws Exception {
        BigQuery bq = mock(BigQuery.class);
        when(bq.getJob(org.mockito.ArgumentMatchers.any(JobId.class))).thenReturn(null);

        BigQueryServicesImpl.QueryDataClientImpl client = newClientWithMockBigQuery(bq);

        assertNull(client.getJob("project-x", "missing"));
    }

    @Test
    public void waitForJobReturnsResultOfJobWaitFor() throws Exception {
        BigQuery bq = mock(BigQuery.class);
        Job input = mock(Job.class);
        Job completed = mock(Job.class);
        when(input.waitFor()).thenReturn(completed);

        BigQueryServicesImpl.QueryDataClientImpl client = newClientWithMockBigQuery(bq);

        assertSame(completed, client.waitForJob(input));
        verify(input).waitFor();
    }

    @Test
    public void waitForJobPropagatesInterrupted() throws Exception {
        BigQuery bq = mock(BigQuery.class);
        Job input = mock(Job.class);
        when(input.waitFor()).thenThrow(new InterruptedException("stopped"));

        BigQueryServicesImpl.QueryDataClientImpl client = newClientWithMockBigQuery(bq);

        assertThrows(InterruptedException.class, () -> client.waitForJob(input));
    }
}
