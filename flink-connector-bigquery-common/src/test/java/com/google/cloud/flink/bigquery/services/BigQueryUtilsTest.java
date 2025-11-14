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

package com.google.cloud.flink.bigquery.services;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.Table;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** */
public class BigQueryUtilsTest {

    @Test
    public void testRetries() throws IOException, InterruptedException {

        Bigquery client = Mockito.mock(Bigquery.class);
        Bigquery.Jobs jobs = Mockito.mock(Bigquery.Jobs.class);
        Bigquery.Jobs.Insert insert = Mockito.mock(Bigquery.Jobs.Insert.class);
        Mockito.when(client.jobs()).thenReturn(jobs);
        Mockito.when(jobs.insert(ArgumentMatchers.any(), ArgumentMatchers.any()))
                .thenReturn(insert);
        Mockito.when(insert.setPrettyPrint(false)).thenReturn(insert);
        Mockito.when(insert.execute()).thenThrow(new IOException("Expected"));

        try {
            BigQueryUtils.maxRetryCount = 2;
            BigQueryUtils.dryRunQuery(client, "", null, "");
        } catch (Exception ex) {
            // swallow the expected error
        }
        // check there was a retry because we always fail
        Mockito.verify(insert, Mockito.times(2)).execute();
    }

    @Test
    public void testNoRetriesJob() throws IOException, InterruptedException {

        Bigquery client = Mockito.mock(Bigquery.class);
        Bigquery.Jobs jobs = Mockito.mock(Bigquery.Jobs.class);
        Bigquery.Jobs.Insert insert = Mockito.mock(Bigquery.Jobs.Insert.class);

        Mockito.when(client.jobs()).thenReturn(jobs);
        Mockito.when(jobs.insert(ArgumentMatchers.any(), ArgumentMatchers.any()))
                .thenReturn(insert);
        Mockito.when(insert.setPrettyPrint(false)).thenReturn(insert);
        Mockito.when(insert.execute()).thenReturn(new Job());

        BigQueryUtils.maxRetryCount = 5;
        BigQueryUtils.runQuery(client, "", new JobConfigurationQuery(), "");

        // check there was only one request, since no errors occurred
        Mockito.verify(insert, Mockito.times(1)).execute();
    }

    @Test
    public void testNoRetriesDataset() throws IOException, InterruptedException {
        Bigquery client = Mockito.mock(Bigquery.class);

        Bigquery.Datasets datasets = Mockito.mock(Bigquery.Datasets.class);
        Bigquery.Datasets.Get got = Mockito.mock(Bigquery.Datasets.Get.class);
        Mockito.when(client.datasets()).thenReturn(datasets);
        Mockito.when(datasets.get(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(got);
        Mockito.when(got.setPrettyPrint(false)).thenReturn(got);
        Mockito.when(got.execute()).thenReturn(new Dataset());

        BigQueryUtils.maxRetryCount = 100;
        BigQueryUtils.datasetInfo(client, "", "");
        // check no retries either
        Mockito.verify(got, Mockito.times(1)).execute();
    }

    @Test
    public void testNoRetriesTable() throws IOException, InterruptedException {
        Bigquery client = Mockito.mock(Bigquery.class);

        Bigquery.Tables tables = Mockito.mock(Bigquery.Tables.class);
        Bigquery.Tables.Get got = Mockito.mock(Bigquery.Tables.Get.class);
        Mockito.when(client.tables()).thenReturn(tables);
        Mockito.when(
                        tables.get(
                                ArgumentMatchers.anyString(),
                                ArgumentMatchers.anyString(),
                                ArgumentMatchers.anyString()))
                .thenReturn(got);
        Mockito.when(got.setPrettyPrint(false)).thenReturn(got);
        Mockito.when(got.execute()).thenReturn(new Table());

        BigQueryUtils.maxRetryCount = 100;
        BigQueryUtils.tableInfo(client, "", "", "");
        // check no retries either
        Mockito.verify(got, Mockito.times(1)).execute();
    }

    @Test
    public void testGetQuotaProjectFromOptions() {
        CredentialsOptions credentialsOptions = CredentialsOptions.builder()
                .setQuotaProjectId("test")
                .build();

        String quotaProjectId = credentialsOptions.getQuotaProjectId();

        assertEquals("test", quotaProjectId);
    }

    @Test
    public void testGetQuotaProjectMissing() {
        CredentialsOptions credentialsOptions = CredentialsOptions.builder().build();

        String quotaProjectId = credentialsOptions.getQuotaProjectId();

        assertNull(quotaProjectId);
    }

    @Test
    public void testUpdateReadSessionWithQuotaProject() {
        CreateReadSessionRequest request = CreateReadSessionRequest.newBuilder()
                .setParent("projects/test-1")
                .build();

        CreateReadSessionRequest updated = BigQueryUtils.updateWithQuotaProject(request, "test");

        assertEquals("projects/test", updated.getParent());
    }

    @Test
    public void testUpdateReadSessionWithQuotaProjectMissing() {
        CreateReadSessionRequest request = CreateReadSessionRequest.newBuilder()
                .setParent("projects/test-1")
                .build();

        CreateReadSessionRequest updated = BigQueryUtils.updateWithQuotaProject(request, null);

        assertEquals("projects/test-1", updated.getParent());
    }
}
