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

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.avro.AvroWriters;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.Collection;

/**
 * Sink to write data into BigQuery indirectly by staging data in GCS and then loading via Load Job.
 *
 * @param <IN> Type of input to sink.
 */
class BigQueryIndirectSink<IN>
        implements Sink<IN>, TwoPhaseCommittingSink<IN, FileSinkCommittable> {

    private final BigQuerySinkConfig<IN> sinkConfig;
    private final FileSink<GenericRecord> gcsAvroSink;

    BigQueryIndirectSink(BigQuerySinkConfig<IN> sinkConfig) {
        this.sinkConfig = sinkConfig;

        String tempGcsBucket = sinkConfig.getTemporaryGcsBucket();
        if (tempGcsBucket == null || tempGcsBucket.isEmpty()) {
            throw new IllegalArgumentException(
                    "temporaryGcsBucket option must be specified for indirect write mode.");
        }

        Path outputPath = new Path(tempGcsBucket);
        Schema schema = sinkConfig.getSchemaProvider().getAvroSchema();

        OutputFileConfig config = OutputFileConfig.builder().withPartSuffix(".avro").build();

        this.gcsAvroSink =
                FileSink.forBulkFormat(outputPath, AvroWriters.forGenericRecord(schema))
                        .withOutputFileConfig(config)
                        .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public TwoPhaseCommittingSink.PrecommittingSinkWriter<IN, FileSinkCommittable> createWriter(
            InitContext context) throws IOException {
        return new BigQueryPrecommittingSinkWriter<>(
                (SinkWriter<GenericRecord>) gcsAvroSink.createWriter(context));
    }

    @Override
    public Committer<FileSinkCommittable> createCommitter() throws IOException {
        Committer<FileSinkCommittable> fileSinkCommitter = gcsAvroSink.createCommitter();
        return new IndirectSinkCommitter(fileSinkCommitter, sinkConfig);
    }

    @Override
    public SimpleVersionedSerializer<FileSinkCommittable> getCommittableSerializer() {
        return gcsAvroSink.getCommittableSerializer();
    }

    private static class BigQueryPrecommittingSinkWriter<IN>
            implements TwoPhaseCommittingSink.PrecommittingSinkWriter<IN, FileSinkCommittable> {

        private final SinkWriter<GenericRecord> innerWriter;

        public BigQueryPrecommittingSinkWriter(SinkWriter<GenericRecord> innerWriter) {
            this.innerWriter = innerWriter;
        }

        @Override
        public void write(IN element, Context context) throws IOException, InterruptedException {
            innerWriter.write((GenericRecord) element, context);
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            innerWriter.flush(endOfInput);
        }

        @SuppressWarnings("unchecked")
        @Override
        public Collection<FileSinkCommittable> prepareCommit()
                throws IOException, InterruptedException {
            if (innerWriter instanceof TwoPhaseCommittingSink.PrecommittingSinkWriter) {
                return ((TwoPhaseCommittingSink.PrecommittingSinkWriter<?, FileSinkCommittable>)
                                innerWriter)
                        .prepareCommit();
            }
            throw new IllegalStateException(
                    "Localized FileSink writer does not implement PrecommittingSinkWriter.");
        }

        @Override
        public void close() throws Exception {
            innerWriter.close();
        }
    }
}
