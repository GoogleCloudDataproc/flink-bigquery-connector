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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroWriters;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;

/**
 * Sink to write data into BigQuery indirectly by staging data in GCS and then loading via Load Job.
 *
 * @param <IN> Type of input to sink.
 */
class BigQueryIndirectSink<IN> implements Sink<IN> {

    private final BigQuerySinkConfig<IN> sinkConfig;

    BigQueryIndirectSink(BigQuerySinkConfig<IN> sinkConfig) {
        this.sinkConfig = sinkConfig;
    }

    @Override
    @SuppressWarnings("unchecked")
    public SinkWriter<IN> createWriter(WriterInitContext context) throws IOException {
        String tempGcsBucket = sinkConfig.getTemporaryGcsBucket();
        if (tempGcsBucket == null || tempGcsBucket.isEmpty()) {
            throw new IllegalArgumentException(
                    "temporaryGcsBucket option must be specified for indirect write mode.");
        }

        Path outputPath = new Path(tempGcsBucket);
        Schema schema = sinkConfig.getSchemaProvider().getAvroSchema();

        FileSink<GenericRecord> gcsAvroSink =
                FileSink.forBulkFormat(outputPath, AvroWriters.forGenericRecord(schema)).build();

        return (SinkWriter<IN>) gcsAvroSink.createWriter(context);
    }
}
