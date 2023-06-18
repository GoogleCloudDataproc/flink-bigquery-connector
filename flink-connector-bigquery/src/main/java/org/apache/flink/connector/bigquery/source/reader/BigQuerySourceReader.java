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

package org.apache.flink.connector.bigquery.source.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.bigquery.source.split.BigQuerySourceSplit;
import org.apache.flink.connector.bigquery.source.split.BigQuerySourceSplitState;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Supplier;

/**
 * The common BigQuery source reader for both ordered & unordered message consuming.
 *
 * @param <OUT> The output message type for Flink.
 */
@Internal
public class BigQuerySourceReader<OUT>
        extends SingleThreadMultiplexSourceReaderBase<
                GenericRecord, OUT, BigQuerySourceSplit, BigQuerySourceSplitState> {
    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySourceReader.class);

    public BigQuerySourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<GenericRecord>> elementsQueue,
            Supplier<SplitReader<GenericRecord, BigQuerySourceSplit>> splitReaderSupplier,
            RecordEmitter<GenericRecord, OUT, BigQuerySourceSplitState> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(elementsQueue, splitReaderSupplier, recordEmitter, config, context);
    }

    public BigQuerySourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<GenericRecord>> elementsQueue,
            Supplier<SplitReader<GenericRecord, BigQuerySourceSplit>> splitReaderSupplier,
            RecordEmitter<GenericRecord, OUT, BigQuerySourceSplitState> recordEmitter,
            SourceReaderContext context) {
        super(elementsQueue, splitReaderSupplier, recordEmitter, new Configuration(), context);
    }

    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, BigQuerySourceSplitState> finishedSplitIds) {
        for (BigQuerySourceSplitState splitState : finishedSplitIds.values()) {
            BigQuerySourceSplit sourceSplit = splitState.toBigQuerySourceSplit();
            LOG.info("Split {} is finished.", sourceSplit.splitId());
        }
        context.sendSplitRequest();
    }

    @Override
    protected BigQuerySourceSplitState initializedState(BigQuerySourceSplit splitt) {
        return new BigQuerySourceSplitState(splitt);
    }

    @Override
    protected BigQuerySourceSplit toSplitType(String string, BigQuerySourceSplitState sst) {
        return sst.toBigQuerySourceSplit();
    }
}
