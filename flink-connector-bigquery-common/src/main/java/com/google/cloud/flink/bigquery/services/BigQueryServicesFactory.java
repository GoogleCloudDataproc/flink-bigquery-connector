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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;

import java.io.IOException;

/**
 * A factory class to dispatch the right implementation of the BigQuery services functionalities.
 * This class can be configured to use a mock implementation of the BigQuery services, simplifying
 * testing of the library.
 */
@Internal
public class BigQueryServicesFactory {

    private static final BigQueryServicesFactory INSTANCE = new BigQueryServicesFactory();
    private static final BigQueryServices SERVICES = new BigQueryServicesImpl();

    private Boolean isTestingEnabled = false;
    private BigQueryServices testingServices;
    private BigQueryConnectOptions bqConnectOptions;

    private BigQueryServicesFactory() {}

    /**
     * Returns the factory instance, given the current factory's internal state.
     *
     * @param options The BigQuery connect options.
     * @return A factory instance.
     */
    public static BigQueryServicesFactory instance(BigQueryConnectOptions options) {
        INSTANCE.bqConnectOptions = options;
        if (options.getTestingBigQueryServices() == null) {
            return INSTANCE.defaultImplementation();
        } else {
            return INSTANCE.withTestingServices(options.getTestingBigQueryServices().get());
        }
    }

    /**
     * Returns a BigQuery storage read client, given the factory's current internal state.
     *
     * @return A BigQuery storage read client.
     */
    public BigQueryServices.StorageReadClient storageRead() throws IOException {
        if (isTestingEnabled) {
            return testingServices.createStorageReadClient(
                    bqConnectOptions.getCredentialsOptions());
        }
        return SERVICES.createStorageReadClient(bqConnectOptions.getCredentialsOptions());
    }

    /**
     * Returns a BigQuery storage write client, given the factory's current internal state.
     *
     * @return A BigQuery storage write client.
     */
    public BigQueryServices.StorageWriteClient storageWrite() throws IOException {
        if (isTestingEnabled) {
            return testingServices.createStorageWriteClient(
                    bqConnectOptions.getCredentialsOptions());
        }
        return SERVICES.createStorageWriteClient(bqConnectOptions.getCredentialsOptions());
    }

    /**
     * Returns a BigQuery query data client, given the factory's current internal state.
     *
     * @return A BigQuery query data client.
     */
    public BigQueryServices.QueryDataClient queryClient() {
        if (isTestingEnabled) {
            return testingServices.createQueryDataClient(bqConnectOptions.getCredentialsOptions());
        }
        return SERVICES.createQueryDataClient(bqConnectOptions.getCredentialsOptions());
    }

    @VisibleForTesting
    BigQueryServicesFactory withTestingServices(BigQueryServices testingServices) {
        Preconditions.checkNotNull(testingServices);
        isTestingEnabled = true;
        this.testingServices = testingServices;
        return this;
    }

    /**
     * Returns the factory instance, with its default implementation (using GCP BigQuery).
     *
     * @return A factory instance in its default state.
     */
    public BigQueryServicesFactory defaultImplementation() {
        isTestingEnabled = false;
        this.testingServices = null;
        return this;
    }

    public Boolean getIsTestingEnabled() {
        return isTestingEnabled;
    }

    public BigQueryServices getTestingServices() {
        return testingServices;
    }
}
