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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;

import java.io.IOException;

/**
 * A {@link CheckpointRollingPolicy} that also rolls files when they exceed a size threshold.
 *
 * <p>This prevents unbounded file growth in batch mode where a single checkpoint occurs at
 * end-of-input. Without size-based rolling, each subtask would produce one file containing all its
 * data, which can grow very large. Rolling at a size threshold gives BigQuery load-job parallelism
 * and faster retries.
 */
@Internal
public final class SizeBasedCheckpointRollingPolicy<IN, BucketID>
        extends CheckpointRollingPolicy<IN, BucketID> {

    // 1.5 GB: the size above which a part file will roll. With BigQuery's 15 TB per load job
    // limit, that's 10,000 files, which matches the number of source URIs allowed per load job.
    // This keeps the single-partition direct load path for the common case, avoiding the temp-table
    // + copy overhead.
    @VisibleForTesting static final long DEFAULT_MAX_FILE_SIZE = 1500L * 1000 * 1000; // 1.5 GB

    @Override
    public boolean shouldRollOnEvent(PartFileInfo<BucketID> partFileState, IN element)
            throws IOException {
        return partFileState.getSize() >= DEFAULT_MAX_FILE_SIZE;
    }

    @Override
    public boolean shouldRollOnProcessingTime(
            PartFileInfo<BucketID> partFileState, long currentTime) {
        return false;
    }
}
