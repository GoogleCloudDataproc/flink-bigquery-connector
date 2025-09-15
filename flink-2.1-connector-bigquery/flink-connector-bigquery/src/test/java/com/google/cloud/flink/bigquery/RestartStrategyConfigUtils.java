/*
 * Copyright (C) 2024 Google Inc.
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

package com.google.cloud.flink.bigquery;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;

import java.time.Duration;

/** Helper methods for different restart strategy configs. */
public class RestartStrategyConfigUtils {

    public static Configuration fixedDelayRestartStrategyConfig(
            final int attempts, final Duration delay) {
        final Configuration config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, attempts);
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, delay);
        return config;
    }

    public static Configuration exponentialDelayRestartStrategyConfig(
            final Duration initialBackoff,
            final Duration maxBackoff,
            final double backoffMultiplier,
            final Duration resetBackoffThreshold,
            final double jitterFactor) {
        final Configuration config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "exponential-delay");
        config.set(
                RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF,
                initialBackoff);
        config.set(
                RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF, maxBackoff);
        config.set(
                RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_BACKOFF_MULTIPLIER,
                backoffMultiplier);
        config.set(
                RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_RESET_BACKOFF_THRESHOLD,
                resetBackoffThreshold);
        config.set(
                RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_JITTER_FACTOR,
                jitterFactor);
        return config;
    }

    public static Configuration failureRateRestartStrategyConfig(
            final int maxFailuresPerInterval,
            final Duration failureRateInterval,
            final Duration delay) {
        final Configuration config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "failure-rate");
        config.set(
                RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL,
                maxFailuresPerInterval);
        config.set(
                RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL,
                failureRateInterval);
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY, delay);
        return config;
    }

    public static Configuration noRestartStrategyConfig() {
        final Configuration config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        return config;
    }
}
