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
import org.junit.Test;

import java.io.IOException;

import static com.google.common.truth.Truth.assertThat;

/** */
public class BigQueryServicesTest {
    @Test
    public void testFactoryWithTestServices() throws IOException {
        SerializableSupplier<BigQueryServices> dummyServices =
                () ->
                        new BigQueryServices() {
                            @Override
                            public BigQueryServices.QueryDataClient createQueryDataClient(
                                    CredentialsOptions credentialsOptions) {
                                return null;
                            }

                            @Override
                            public BigQueryServices.StorageReadClient createStorageReadClient(
                                    CredentialsOptions credentialsOptions) throws IOException {
                                return null;
                            }

                            @Override
                            public BigQueryServices.StorageWriteClient createStorageWriteClient(
                                    CredentialsOptions credentialsOptions) throws IOException {
                                return null;
                            }
                        };
        BigQueryServicesFactory original =
                BigQueryServicesFactory.instance(
                        BigQueryConnectOptions.builderForQuerySource()
                                .setTestingBigQueryServices(dummyServices)
                                .build());

        assertThat(original.getIsTestingEnabled()).isTrue();
        assertThat(original.getTestingServices()).isNotNull();
        assertThat(original.queryClient()).isNull();
        assertThat(original.storageRead()).isNull();
        assertThat(original.storageWrite()).isNull();

        original.defaultImplementation();

        assertThat(original.getIsTestingEnabled()).isFalse();
        assertThat(original.getTestingServices()).isNull();
    }
}
