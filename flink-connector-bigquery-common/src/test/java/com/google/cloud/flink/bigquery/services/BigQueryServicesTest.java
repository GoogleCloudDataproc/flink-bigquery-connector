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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** Tests for {@link BigQueryServices}. */
public class BigQueryServicesTest {
    @Test
    public void factoriesKeepTheirOwnCredentialsAndServices() throws IOException {
        BigQueryServices firstServices = mock(BigQueryServices.class);
        BigQueryServices secondServices = mock(BigQueryServices.class);
        CredentialsOptions firstCredentials =
                CredentialsOptions.builder().setAccessToken("first").build();
        CredentialsOptions secondCredentials =
                CredentialsOptions.builder().setAccessToken("second").build();
        BigQueryServicesFactory first =
                BigQueryServicesFactory.instance(
                        BigQueryConnectOptions.builderForQuerySource()
                                .setCredentialsOptions(firstCredentials)
                                .setTestingBigQueryServices(() -> firstServices)
                                .build());
        BigQueryServicesFactory second =
                BigQueryServicesFactory.instance(
                        BigQueryConnectOptions.builderForQuerySource()
                                .setCredentialsOptions(secondCredentials)
                                .setTestingBigQueryServices(() -> secondServices)
                                .build());

        first.queryClient();
        first.storageRead();
        first.storageWrite();
        second.queryClient();
        second.storageRead();
        second.storageWrite();

        verify(firstServices).createQueryDataClient(firstCredentials);
        verify(firstServices).createStorageReadClient(firstCredentials);
        verify(firstServices).createStorageWriteClient(firstCredentials);
        verify(secondServices).createQueryDataClient(secondCredentials);
        verify(secondServices).createStorageReadClient(secondCredentials);
        verify(secondServices).createStorageWriteClient(secondCredentials);
    }

    @Test
    public void defaultFactoryDoesNotResetAnotherFactory() {
        BigQueryServices services = mock(BigQueryServices.class);
        BigQueryServicesFactory testing =
                BigQueryServicesFactory.instance(
                        BigQueryConnectOptions.builderForQuerySource()
                                .setTestingBigQueryServices(() -> services)
                                .build());
        BigQueryServicesFactory normal =
                BigQueryServicesFactory.instance(
                        BigQueryConnectOptions.builderForQuerySource().build());

        assertThat(testing.getIsTestingEnabled()).isTrue();
        assertThat(testing.getTestingServices()).isSameInstanceAs(services);
        assertThat(normal.getIsTestingEnabled()).isFalse();
    }

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
