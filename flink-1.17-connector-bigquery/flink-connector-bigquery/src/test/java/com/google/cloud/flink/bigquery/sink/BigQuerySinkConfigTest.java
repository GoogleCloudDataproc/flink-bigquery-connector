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

package com.google.cloud.flink.bigquery.sink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.google.cloud.flink.bigquery.sink.BigQuerySinkConfig.validateStreamExecutionEnvironment;

/** Tests for {@link BigQuerySinkConfig}. */
public class BigQuerySinkConfigTest {

    private StreamExecutionEnvironment env;

    @Before
    public void setUp() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    @After
    public void tearDown() {
        env = null;
    }

    @Test
    public void testValidation_withFixedDelayRestart_withValidConfiguration() {
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));
        validateStreamExecutionEnvironment(env);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidation_withFixedDelayRestart_withInvalidAttempts() {
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(15, Time.seconds(5)));
        validateStreamExecutionEnvironment(env);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidation_withFixedDelayRestart_withInvalidDelay() {
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.milliseconds(500)));
        validateStreamExecutionEnvironment(env);
    }

    @Test
    public void testValidation_withExponentialDelayRestart_withValidConfiguration() {
        env.setRestartStrategy(
                RestartStrategies.exponentialDelayRestart(
                        Time.seconds(5), Time.minutes(10), 3, Time.hours(2), 0));
        validateStreamExecutionEnvironment(env);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidation_withExponentialDelayRestart_withInvalidInitialBackoff() {
        env.setRestartStrategy(
                RestartStrategies.exponentialDelayRestart(
                        Time.milliseconds(500), Time.minutes(10), 3, Time.hours(2), 0));
        validateStreamExecutionEnvironment(env);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidation_withExponentialDelayRestart_withInvalidMaxBackoff() {
        env.setRestartStrategy(
                RestartStrategies.exponentialDelayRestart(
                        Time.seconds(5), Time.minutes(1), 3, Time.hours(2), 0));
        validateStreamExecutionEnvironment(env);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidation_withExponentialDelayRestart_withInvalidMultiplier() {
        env.setRestartStrategy(
                RestartStrategies.exponentialDelayRestart(
                        Time.seconds(5), Time.minutes(10), 1.5, Time.hours(2), 0));
        validateStreamExecutionEnvironment(env);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidation_withExponentialDelayRestart_withInvalidResetThreshold() {
        env.setRestartStrategy(
                RestartStrategies.exponentialDelayRestart(
                        Time.seconds(5), Time.minutes(10), 3, Time.minutes(30), 0));
        validateStreamExecutionEnvironment(env);
    }

    @Test
    public void testValidation_withFailureRateRestart_withValidConfiguration() {
        env.setRestartStrategy(
                RestartStrategies.failureRateRestart(1, Time.minutes(2), Time.seconds(5)));
        validateStreamExecutionEnvironment(env);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidation_withFailureRateRestart_withInvalidDelay() {
        env.setRestartStrategy(
                RestartStrategies.failureRateRestart(1, Time.minutes(2), Time.milliseconds(500)));
        validateStreamExecutionEnvironment(env);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidation_withFailureRateRestart_withInvalidRate() {
        env.setRestartStrategy(
                RestartStrategies.failureRateRestart(2, Time.minutes(1), Time.seconds(5)));
        validateStreamExecutionEnvironment(env);
    }

    @Test
    public void testValidation_withNoRestart() {
        env.setRestartStrategy(RestartStrategies.noRestart());
        validateStreamExecutionEnvironment(env);
    }

    @Test
    public void testValidation_withFallbackRestart() {
        env.setRestartStrategy(RestartStrategies.fallBackRestart());
        validateStreamExecutionEnvironment(env);
    }
}
