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

package com.google.cloud.flink.bigquery.sink.indirect;

import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link SizeBasedCheckpointRollingPolicy}. */
public class SizeBasedCheckpointRollingPolicyTest {

    @SuppressWarnings("unchecked")
    private PartFileInfo<String> mockPartFileInfo(long size) throws IOException {
        PartFileInfo<String> info = mock(PartFileInfo.class);
        when(info.getSize()).thenReturn(size);
        return info;
    }

    @Test
    public void shouldRollOnEventWhenSizeExceedsMax() throws IOException {
        SizeBasedCheckpointRollingPolicy<String, String> policy =
                new SizeBasedCheckpointRollingPolicy<>();

        assertTrue(
                policy.shouldRollOnEvent(
                        mockPartFileInfo(
                                SizeBasedCheckpointRollingPolicy.DEFAULT_MAX_FILE_SIZE + 1),
                        "element"));
    }

    @Test
    public void shouldRollOnEventAtExactBoundary() throws IOException {
        SizeBasedCheckpointRollingPolicy<String, String> policy =
                new SizeBasedCheckpointRollingPolicy<>();

        assertTrue(
                policy.shouldRollOnEvent(
                        mockPartFileInfo(SizeBasedCheckpointRollingPolicy.DEFAULT_MAX_FILE_SIZE),
                        "element"));
    }

    @Test
    public void shouldNotRollOnEventWhenSizeBelowMax() throws IOException {
        SizeBasedCheckpointRollingPolicy<String, String> policy =
                new SizeBasedCheckpointRollingPolicy<>();

        assertFalse(
                policy.shouldRollOnEvent(
                        mockPartFileInfo(
                                SizeBasedCheckpointRollingPolicy.DEFAULT_MAX_FILE_SIZE - 1),
                        "element"));
    }

    @Test
    public void shouldAlwaysRollOnCheckpoint() throws IOException {
        SizeBasedCheckpointRollingPolicy<String, String> policy =
                new SizeBasedCheckpointRollingPolicy<>();

        assertTrue(policy.shouldRollOnCheckpoint(mockPartFileInfo(0)));
    }

    @Test
    public void shouldNotRollOnProcessingTime() throws IOException {
        SizeBasedCheckpointRollingPolicy<String, String> policy =
                new SizeBasedCheckpointRollingPolicy<>();

        assertFalse(
                policy.shouldRollOnProcessingTime(mockPartFileInfo(0), System.currentTimeMillis()));
    }
}
