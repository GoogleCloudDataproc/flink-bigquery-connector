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

import org.apache.flink.util.function.SerializableSupplier;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.io.IOException;

/** */
public class BigQueryServicesTest {
    @Test
    public void testFactoryWithTestServices() throws IOException {
        SerializableSupplier<BigQueryServices> dummyServices =
                () ->
                        new BigQueryServices() {
                            @Override
                            public BigQueryServices.QueryDataClient getQueryDataClient(
                                    CredentialsOptions credentialsOptions) {
                                return null;
                            }

                            @Override
                            public BigQueryServices.StorageReadClient getStorageClient(
                                    CredentialsOptions credentialsOptions) throws IOException {
                                return null;
                            }
                        };
        BigQueryServicesFactory original =
                BigQueryServicesFactory.instance(
                        BigQueryConnectOptions.builderForQuerySource()
                                .setTestingBigQueryServices(dummyServices)
                                .build());

        Assertions.assertThat(original.getIsTestingEnabled()).isTrue();
        Assertions.assertThat(original.getTestingServices()).isNotNull();
        Assertions.assertThat(original.queryClient()).isNull();
        Assertions.assertThat(original.storageRead()).isNull();

        original.defaultImplementation();

        Assertions.assertThat(original.getIsTestingEnabled()).isFalse();
        Assertions.assertThat(original.getTestingServices()).isNull();
    }
}
