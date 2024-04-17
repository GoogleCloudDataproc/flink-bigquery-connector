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

package com.google.cloud.flink.bigquery.sink;

import org.apache.flink.connector.base.DeliveryGuarantee;

import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.sink.serializer.FakeBigQuerySerializer;
import com.google.cloud.flink.bigquery.sink.serializer.TestBigQuerySchemas;
import com.google.protobuf.ByteString;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;

/** Tests for {@link BigQuerySink}. */
public class BigQuerySinkTest {

    @Test
    public void testGetWithAtLeastOnce() throws IOException {
        BigQuerySinkConfig sinkConfig =
                BigQuerySinkConfig.newBuilder()
                        .connectOptions(StorageClientFaker.createConnectOptionsForWrite(null))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .deliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build();
        assertNotNull(BigQuerySink.get(sinkConfig, null));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetWithNoneDeliveryGuarantee() throws IOException {
        BigQuerySinkConfig sinkConfig =
                BigQuerySinkConfig.newBuilder()
                        .connectOptions(StorageClientFaker.createConnectOptionsForWrite(null))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .deliveryGuarantee(DeliveryGuarantee.NONE)
                        .build();
        assertNotNull(BigQuerySink.get(sinkConfig, null));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testExactlyOnceNotSupported() throws IOException {
        BigQuerySinkConfig sinkConfig =
                BigQuerySinkConfig.newBuilder()
                        .connectOptions(StorageClientFaker.createConnectOptionsForWrite(null))
                        .schemaProvider(TestBigQuerySchemas.getSimpleRecordSchema())
                        .serializer(new FakeBigQuerySerializer(ByteString.copyFromUtf8("foo")))
                        .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .build();
        assertNotNull(BigQuerySink.get(sinkConfig, null));
    }
}
