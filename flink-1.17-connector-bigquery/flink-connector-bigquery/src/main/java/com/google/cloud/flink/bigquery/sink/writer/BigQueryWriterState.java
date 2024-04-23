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

package com.google.cloud.flink.bigquery.sink.writer;

import com.google.cloud.flink.bigquery.sink.state.BigQueryStreamState;

/** State representation of a {@link BigQueryBufferedWriter}. */
public class BigQueryWriterState extends BigQueryStreamState {

    // Used for Flink metrics.
    private long totalRecordsWritten;

    public BigQueryWriterState(String streamName, long streamOffset, long totalRecordsWritten) {
        super(streamName, streamOffset);
        this.totalRecordsWritten = totalRecordsWritten;
    }

    public long getTotalRecordsWritten() {
        return totalRecordsWritten;
    }
}
