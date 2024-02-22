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

import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;

/**
 * Writer implementation for {@link DefaultSink}.
 *
 * <p>This writer appends records to the BigQuery table's default write stream. This means that
 * records are written directly to the table with no additional commit required, and available for
 * querying immediately.
 *
 * <p>In case of stream replay upon failure recovery, records will be written again, regardless of
 * appends prior to the application's failure.
 *
 * <p>Records are grouped to maximally utilize the BigQuery append request's payload.
 *
 * @param <IN> Type of records to be written to BigQuery.
 */
public class DefaultWriter<IN> implements SinkWriter<IN> {

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        throw new UnsupportedOperationException("write method is not supported");
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        throw new UnsupportedOperationException("flush method is not supported");
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("close method is not supported");
    }
}
