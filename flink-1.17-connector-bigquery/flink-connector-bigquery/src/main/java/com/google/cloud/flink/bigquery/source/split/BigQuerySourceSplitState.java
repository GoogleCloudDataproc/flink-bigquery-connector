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

package com.google.cloud.flink.bigquery.source.split;

import org.apache.flink.annotation.Internal;

import java.util.Objects;

/** BigQuery source split state for {@link BigQuerySourceSplit}. */
@Internal
public class BigQuerySourceSplitState {
    private final BigQuerySourceSplit split;
    private Long offset;

    public BigQuerySourceSplitState(BigQuerySourceSplit split) {
        this.split = split;
        offset = split.getOffset();
    }

    public BigQuerySourceSplit toBigQuerySourceSplit() {
        return new BigQuerySourceSplit(split.getStreamName(), offset);
    }

    public void updateOffset() {
        offset++;
    }

    @Override
    public String toString() {
        return "BigQuerySourceSplitState{" + "split=" + split + ", offset=" + offset + '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.split, this.offset);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final BigQuerySourceSplitState other = (BigQuerySourceSplitState) obj;
        if (!Objects.equals(this.split, other.split)) {
            return false;
        }
        return Objects.equals(this.offset, other.offset);
    }
}
