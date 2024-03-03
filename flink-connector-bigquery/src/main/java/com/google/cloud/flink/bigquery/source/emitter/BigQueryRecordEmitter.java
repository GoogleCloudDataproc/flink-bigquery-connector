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

package com.google.cloud.flink.bigquery.source.emitter;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.Collector;

import com.google.cloud.flink.bigquery.source.reader.deserializer.BigQueryDeserializationSchema;
import com.google.cloud.flink.bigquery.source.split.BigQuerySourceSplitState;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link RecordEmitter} implementation for {@link BigQuerySourceReader} .We would always update
 * the last consumed message id in this emitter.
 *
 * @param <T> the emitted type.
 */
@Internal
public class BigQueryRecordEmitter<T>
        implements RecordEmitter<GenericRecord, T, BigQuerySourceSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryRecordEmitter.class);
    private final BigQueryDeserializationSchema<GenericRecord, T> deserializationSchema;
    private final SourceOutputWrapper<T> sourceOutputWrapper;

    public BigQueryRecordEmitter(
            BigQueryDeserializationSchema<GenericRecord, T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
        this.sourceOutputWrapper = new SourceOutputWrapper<>();
    }

    @Override
    public void emitRecord(
            GenericRecord record, SourceOutput<T> output, BigQuerySourceSplitState splitState)
            throws Exception {
        // Update current offset.
        splitState.updateOffset();
        // Sink the record to source output.
        sourceOutputWrapper.setSourceOutput(output);
        LOG.info("BigQueryRecordEmitter [deserialize]" + record + " -- " + sourceOutputWrapper);
        deserializationSchema.deserialize(record, sourceOutputWrapper);
    }

    private static class SourceOutputWrapper<T> implements Collector<T> {
        private SourceOutput<T> sourceOutput;

        @Override
        public void collect(T record) {
            sourceOutput.collect(record);
        }

        @Override
        public void close() {}

        private void setSourceOutput(SourceOutput<T> sourceOutput) {
            this.sourceOutput = sourceOutput;
        }
    }
}
