/*
 * Copyright 2024 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.flink.bigquery.table.config;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.Test;

import java.time.Duration;

import static com.google.cloud.flink.bigquery.RestartStrategyConfigUtils.fixedDelayRestartStrategyConfig;
import static org.junit.Assert.assertEquals;

/** Tests for {@link BigQuerySinkTableConfig}. */
public class BigQuerySinkTableConfigTest {

    @Test
    public void testConstructor_withAtLeastOnce() {
        BigQuerySinkTableConfig config =
                BigQuerySinkTableConfig.newBuilder()
                        .project("foo")
                        .dataset("bar")
                        .table("qux")
                        .build();
        assertEquals("foo", config.getProject());
        assertEquals("bar", config.getDataset());
        assertEquals("qux", config.getTable());
    }

    @Test
    public void testConstructor_withExactlyOnce() {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(
                        fixedDelayRestartStrategyConfig(5, Duration.ofSeconds(3)));
        BigQuerySinkTableConfig config =
                BigQuerySinkTableConfig.newBuilder()
                        .project("foo")
                        .dataset("bar")
                        .table("qux")
                        .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .streamExecutionEnvironment(env)
                        .build();
        assertEquals("foo", config.getProject());
        assertEquals("bar", config.getDataset());
        assertEquals("qux", config.getTable());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_withExactlyOnce_withoutStreamExecutionEnv() {
        BigQuerySinkTableConfig.newBuilder()
                .project("foo")
                .dataset("bar")
                .table("qux")
                .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();
    }
}
