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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.util.EnvironmentInformation;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.Table;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.RetryPolicy;
import dev.failsafe.function.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;

/** Collection of functionalities that simplify BigQuery services interactions. */
@Internal
public class BigQueryUtils {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryUtils.class);

    static final Long BACKOFF_DELAY_IN_SECONDS = 5L;
    static final Long BACKOFF_MAX_DELAY_IN_MINUTES = 5L;
    static final Double RETRY_JITTER_PROB = 0.2;
    static Integer maxRetryCount = 3;

    /** Global instance of the JSON factory. */
    private static final JsonFactory JSON_FACTORY;

    /** Global instance of the HTTP transport. */
    private static final HttpTransport HTTP_TRANSPORT;

    static {
        try {
            JSON_FACTORY = GsonFactory.getDefaultInstance();
            HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
        } catch (GeneralSecurityException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private BigQueryUtils() {}

    public static Bigquery.Builder newBigqueryBuilder(CredentialsOptions options) {

        return new Bigquery.Builder(
                        HTTP_TRANSPORT,
                        JSON_FACTORY,
                        new HttpCredentialsAdapter(options.getCredentials()))
                .setApplicationName(
                        "BigQuery Connector for Apache Flink version "
                                + EnvironmentInformation.getVersion());
    }

    public static String bqSanitizedRandomUUID() {
        return UUID.randomUUID().toString().replaceAll("-", "");
    }

    /**
     * Function to obtain a Random Descriptor Name. Descriptor name starts with a "D" and cannot
     * contain the character '-'
     *
     * @return String for descriptor name.
     */
    public static String bqSanitizedRandomUUIDForDescriptor() {
        return "D" + UUID.randomUUID().toString().replaceAll("-", "_");
    }

    static <T> FailsafeExecutor<T> buildRetriableExecutorForOperation(String operationName) {
        return Failsafe.with(
                RetryPolicy.<T>builder()
                        .handle(Arrays.asList(IOException.class))
                        .withMaxAttempts(maxRetryCount)
                        .withBackoff(
                                Duration.ofSeconds(BACKOFF_DELAY_IN_SECONDS),
                                Duration.ofMinutes(BACKOFF_MAX_DELAY_IN_MINUTES))
                        .withJitter(RETRY_JITTER_PROB)
                        .onFailedAttempt(
                                e ->
                                        LOG.error(
                                                "Execution failed for operation: " + operationName,
                                                e.getLastException()))
                        .onRetry(
                                r ->
                                        LOG.info(
                                                "Retrying operation {}, for {} time.",
                                                operationName,
                                                r.getExecutionCount()))
                        .onRetriesExceeded(
                                e ->
                                        LOG.error(
                                                "Failed to execute operation {}, retries exhausted."))
                        .build());
    }

    static <T> T executeOperation(
            FailsafeExecutor<T> failsafeExecutor, CheckedSupplier<T> operation) {
        return failsafeExecutor.get(operation);
    }

    static Job runInsertJob(Bigquery client, String projectId, Job job) throws IOException {
        return client.jobs().insert(projectId, job).setPrettyPrint(false).execute();
    }

    public static Job dryRunQuery(
            Bigquery client, String projectId, JobConfigurationQuery queryConfig, String location)
            throws InterruptedException, IOException {
        String jobId = "apacheflink_dryRun_" + bqSanitizedRandomUUID();
        JobReference jobRef =
                new JobReference().setLocation(location).setProjectId(projectId).setJobId(jobId);
        Job job =
                new Job()
                        .setJobReference(jobRef)
                        .setConfiguration(
                                new JobConfiguration().setQuery(queryConfig).setDryRun(true));

        return executeOperation(
                buildRetriableExecutorForOperation(jobId),
                () -> runInsertJob(client, projectId, job));
    }

    public static Job runQuery(
            Bigquery client, String projectId, JobConfigurationQuery queryConfig, String location)
            throws InterruptedException, IOException {
        String jobId = "apacheflink_queryjob_" + bqSanitizedRandomUUID();
        JobReference jobRef =
                new JobReference().setLocation(location).setProjectId(projectId).setJobId(jobId);
        Job job =
                new Job()
                        .setJobReference(jobRef)
                        .setConfiguration(
                                new JobConfiguration().setQuery(queryConfig).setDryRun(false));

        return executeOperation(
                buildRetriableExecutorForOperation(jobId),
                () -> runInsertJob(client, projectId, job));
    }

    public static Dataset datasetInfo(Bigquery client, String projectId, String datasetId)
            throws IOException, InterruptedException {
        return executeOperation(
                buildRetriableExecutorForOperation(
                        String.format("GetDataset - %s.%s", projectId, datasetId)),
                () -> client.datasets().get(projectId, datasetId).setPrettyPrint(false).execute());
    }

    public static Table tableInfo(
            Bigquery client, String projectId, String datasetId, String tableId)
            throws IOException, InterruptedException {
        return executeOperation(
                buildRetriableExecutorForOperation(
                        String.format("GetTable - %s.%s.%s", projectId, datasetId, tableId)),
                () ->
                        client.tables()
                                .get(projectId, datasetId, tableId)
                                .setPrettyPrint(false)
                                .execute());
    }
}
