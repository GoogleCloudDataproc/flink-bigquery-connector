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

package org.apache.flink.connector.bigquery.services;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.bigquery.common.config.BigQueryConnectOptions;
import org.apache.flink.connector.bigquery.common.config.CredentialsOptions;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A factory class to dispatch the right implementation of the BigQuery services functionalities.
 * This class can be configured to use a mock implementation of the BigQuery services, simplifying
 * testing of the library.
 */
@Internal
public class BigQueryServicesFactory {

    private static final BigQueryServicesFactory INSTANCE = new BigQueryServicesFactory();
    private static final BigQueryServices SERVICES = new BigQueryServiceImpl();

    private Boolean isTestingEnabled = false;
    private BigQueryServices testingServices;

    private BigQueryServicesFactory() {}

    /**
     * Returns the factory instance, given the current factory's internal state.
     *
     * @param options The BigQuery connect options.
     * @return A factory instance.
     */
    public static BigQueryServicesFactory instance(BigQueryConnectOptions options) {
        if (options.getTestingBigQueryServices() == null) {
            return INSTANCE.defaultImplementation();
        } else {
            return INSTANCE.withTestingServices(options.getTestingBigQueryServices().get());
        }
    }

    /**
     * Returns a BigQuery storage read client, given the factory's current internal state.
     *
     * @param credentialsOptions The GCP auth credentials options.
     * @return A BigQuery storage read client.
     */
    public BigQueryServices.StorageReadClient storageRead(CredentialsOptions credentialsOptions)
            throws IOException {
        if (isTestingEnabled) {
            return testingServices.getStorageClient(credentialsOptions);
        }
        return SERVICES.getStorageClient(credentialsOptions);
    }

    /**
     * Returns a BigQuery query data client, given the factory's current internal state.
     *
     * @param credentialsOptions The GCP auth credentials options.
     * @return A BigQuery query data client.
     */
    public BigQueryServices.QueryDataClient queryClient(CredentialsOptions credentialsOptions) {
        if (isTestingEnabled) {
            return testingServices.getQueryDataClient(credentialsOptions);
        }
        return SERVICES.getQueryDataClient(credentialsOptions);
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
}
