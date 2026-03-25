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

package com.google.cloud.flink.bigquery.sink;

import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A {@link CheckpointRollingPolicy} that also rolls files when they exceed a size threshold.
 *
 * <p>This prevents unbounded file growth in batch mode where a single checkpoint occurs at
 * end-of-input. Without size-based rolling, each subtask would produce one Avro file containing all
 * its data, which can grow very large. Rolling at a size threshold gives BigQuery load-job
 * parallelism and faster retries.
 *
 * <p>Follows the same pattern as Flink's test utility {@code PartSizeAndCheckpointRollingPolicy}.
 */
public final class SizeBasedCheckpointRollingPolicy<IN, BucketID>
        extends CheckpointRollingPolicy<IN, BucketID> {

    // 2 GB: With BigQuery's 15 TB per load job limit, 2 GB files means ~7,500 files — comfortably
    // under the 10,000 source URI limit per load job. This keeps the single-partition direct load
    // path for the common case, avoiding the temp-table + copy overhead.
    private static final long DEFAULT_MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024L; // 2 GB

    private final long maxFileSize;

    private SizeBasedCheckpointRollingPolicy(long maxFileSize) {
        Preconditions.checkArgument(
                maxFileSize > 0, "Max file size must be positive, got: %s", maxFileSize);
        this.maxFileSize = maxFileSize;
    }

    /** Creates a policy with the default max file size of 2 GB. */
    public static <IN, BucketID> SizeBasedCheckpointRollingPolicy<IN, BucketID> build() {
        return new SizeBasedCheckpointRollingPolicy<>(DEFAULT_MAX_FILE_SIZE);
    }

    /** Creates a policy with the specified max file size in bytes. */
    public static <IN, BucketID> SizeBasedCheckpointRollingPolicy<IN, BucketID> withMaxFileSize(
            long maxFileSize) {
        return new SizeBasedCheckpointRollingPolicy<>(maxFileSize);
    }

    @Override
    public boolean shouldRollOnEvent(PartFileInfo<BucketID> partFileState, IN element)
            throws IOException {
        return partFileState.getSize() >= maxFileSize;
    }

    @Override
    public boolean shouldRollOnProcessingTime(
            PartFileInfo<BucketID> partFileState, long currentTime) {
        return false;
    }

    long getMaxFileSize() {
        return maxFileSize;
    }
}
