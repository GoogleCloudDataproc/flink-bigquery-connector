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

package com.google.cloud.flink.bigquery.sink.throttle;

import org.junit.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.Assert.assertTrue;

/** Tests for {@link WriteStreamCreationThrottler}. */
public class BigQueryWriterThrottlerTest {

    @Test
    public void testThrottle_defaultMaxParallelism() {
        Duration duration = invokeThrottle(3, 128);
        assertTrue(duration.toMillis() >= 3000L);
        assertTrue(duration.toMillis() < 4000L);
    }

    @Test
    public void testThrottle_multiRegionMaxParallelism() {
        Duration duration = invokeThrottle(171, 512);
        assertTrue(duration.toMillis() >= 1000L);
        assertTrue(duration.toMillis() < 2000L);
    }

    @Test
    public void testThrottle_withInterruptedException() {
        // Force interruption
        Thread.currentThread().interrupt();
        Duration duration = invokeThrottle(3, 128);
        assertTrue(duration.toMillis() < 3000L);
    }

    @Test
    public void testThrottle_withInvalidWriterId_expectNoThrottling() {
        Duration duration = invokeThrottle(-1, 128);
        long waitSeconds = duration.toMillis() / 1000;
        assertTrue(waitSeconds == 0);
    }

    private Duration invokeThrottle(int writerId, int maxParallelism) {
        BigQueryWriterThrottler throttler = new BigQueryWriterThrottler(writerId, maxParallelism);
        Instant start = Instant.now();
        throttler.throttle();
        Instant end = Instant.now();
        return Duration.between(start, end);
    }
}
