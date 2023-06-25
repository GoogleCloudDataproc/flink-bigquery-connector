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

package org.apache.flink.connector.bigquery.source.split.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.bigquery.services.BigQueryServices.BigQueryServerStream;
import org.apache.flink.connector.bigquery.services.BigQueryServices.StorageReadClient;
import org.apache.flink.connector.bigquery.services.BigQueryServicesFactory;
import org.apache.flink.connector.bigquery.source.config.BigQueryReadOptions;
import org.apache.flink.connector.bigquery.source.reader.BigQuerySourceReaderContext;
import org.apache.flink.connector.bigquery.source.split.BigQuerySourceSplit;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.util.Preconditions;

import com.codahale.metrics.SlidingWindowReservoir;
import com.google.cloud.bigquery.storage.v1.AvroRows;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Collectors;

/** A split reader for {@link BigQuerySourceSplit}. */
@Internal
public class BigQuerySourceSplitReader implements SplitReader<GenericRecord, BigQuerySourceSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySourceSplitReader.class);

    private final BigQueryReadOptions readOptions;
    private final BigQuerySourceReaderContext readerContext;
    private final transient Optional<Histogram> readPartialTimeMetric;
    private final transient Optional<Histogram> readSplitTimeMetric;
    private final Queue<BigQuerySourceSplit> assignedSplits = new ArrayDeque<>();

    private Boolean closed = false;
    private Integer readSoFar = 0;
    private Long splitStartFetch;

    public BigQuerySourceSplitReader(
            BigQueryReadOptions readOptions, BigQuerySourceReaderContext readerContext) {
        this.readOptions = readOptions;
        this.readerContext = readerContext;
        this.readPartialTimeMetric =
                Optional.ofNullable(readerContext.metricGroup())
                        .map(
                                mgroup ->
                                        mgroup.histogram(
                                                "bq.split.read.partial.time.ms",
                                                new DropwizardHistogramWrapper(
                                                        new com.codahale.metrics.Histogram(
                                                                new SlidingWindowReservoir(500)))));
        this.readSplitTimeMetric =
                Optional.ofNullable(readerContext.metricGroup())
                        .map(
                                mgroup ->
                                        mgroup.histogram(
                                                "bq.split.read.time.ms",
                                                new DropwizardHistogramWrapper(
                                                        new com.codahale.metrics.Histogram(
                                                                new SlidingWindowReservoir(500)))));
    }

    Integer offsetToFetch(BigQuerySourceSplit split) {
        // honor what is coming as checkpointed
        if (split.getOffset() > 0) {
            readSoFar = split.getOffset();
            splitStartFetch = System.currentTimeMillis();
        } else if (readSoFar == 0) {
            // will start reading the stream from the beginning
            splitStartFetch = System.currentTimeMillis();
        }
        LOG.info(
                "[subtask #{}] Offset to fetch from {} for stream {}.",
                readerContext.getIndexOfSubtask(),
                readSoFar,
                split.getStreamName());
        return readSoFar;
    }

    @Override
    public RecordsWithSplitIds<GenericRecord> fetch() throws IOException {
        if (closed) {
            throw new IllegalStateException("Can't fetch records from a closed split reader.");
        }

        RecordsBySplits.Builder<GenericRecord> respBuilder = new RecordsBySplits.Builder<>();

        // nothing to read has been assigned
        if (assignedSplits.isEmpty()) {
            return respBuilder.build();
        }

        // return when current read count is already over limit
        if (readerContext.willItBeOverLimit(0)) {
            LOG.info(
                    "Completing reading because we are over limit (context reader count {})",
                    readerContext.currentReadCount());
            respBuilder.addFinishedSplits(
                    assignedSplits.stream()
                            .map(split -> split.splitId())
                            .collect(Collectors.toList()));
            assignedSplits.clear();
            return respBuilder.build();
        }

        BigQuerySourceSplit assignedSplit = assignedSplits.peek();
        int maxRecordsPerSplitFetch = readOptions.getMaxRecordsPerSplitFetch();
        int read = 0;
        Boolean truncated = false;

        try (StorageReadClient client =
                BigQueryServicesFactory.instance(readOptions.getBigQueryConnectOptions())
                        .storageRead(
                                readOptions.getBigQueryConnectOptions().getCredentialsOptions())) {
            ReadRowsRequest readRequest =
                    ReadRowsRequest.newBuilder()
                            .setReadStream(assignedSplit.getStreamName())
                            .setOffset(offsetToFetch(assignedSplit))
                            .build();

            BigQueryServerStream<ReadRowsResponse> stream = client.readRows(readRequest);
            Schema avroSchema = null;
            Long itStarTime = System.currentTimeMillis();
            for (ReadRowsResponse response : stream) {
                if (!response.hasAvroRows()) {
                    LOG.info(
                            "[subtask #{}] The response contained no avro records for stream {}.",
                            readerContext.getIndexOfSubtask(),
                            assignedSplit.getStreamName());
                }
                if (response.hasAvroSchema()) {
                    // this will happen only the first time we read from a particular stream
                    avroSchema = new Schema.Parser().parse(response.getAvroSchema().getSchema());
                }

                for (GenericRecord record :
                        GenericRecordReader.create(avroSchema)
                                .processRows(response.getAvroRows())) {
                    respBuilder.add(assignedSplit, record);
                    read++;
                    // check if the read count will be over the limit
                    if (readerContext.willItBeOverLimit(read)) {
                        break;
                    }
                    if (read == maxRecordsPerSplitFetch) {
                        truncated = true;
                        break;
                    }
                }
                // in case of going over any of the limits
                if (readerContext.willItBeOverLimit(read)) {
                    break;
                }
                if (read == maxRecordsPerSplitFetch) {
                    break;
                }
            }
            this.readPartialTimeMetric.ifPresent(
                    m -> m.update(System.currentTimeMillis() - itStarTime));
            readSoFar += read;
            // check if we finished to read the stream to finalize the split
            if (!truncated) {
                readerContext.updateReadCount(read);
                LOG.info(
                        "[subtask #{}] Completed reading {} records for split {}.",
                        readerContext.getIndexOfSubtask(),
                        readSoFar,
                        assignedSplit.splitId());
                readSoFar = 0;
                assignedSplits.poll();
                respBuilder.addFinishedSplit(assignedSplit.splitId());
                this.readSplitTimeMetric.ifPresent(
                        m -> m.update(System.currentTimeMillis() - splitStartFetch));
            }
            return respBuilder.build();
        } catch (Exception ex) {
            throw new IOException(
                    String.format(
                            "[subtask #%d] Problems while reading stream %s from BigQuery"
                                    + " with connection info %s. Current split offset %d,"
                                    + " reader offset %d.",
                            readerContext.getIndexOfSubtask(),
                            Optional.ofNullable(assignedSplit.getStreamName()).orElse("NA"),
                            readOptions.toString(),
                            assignedSplit.getOffset(),
                            readSoFar),
                    ex);
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<BigQuerySourceSplit> splitsChanges) {
        LOG.debug("Handle split changes {}", splitsChanges);

        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        assignedSplits.addAll(splitsChanges.splits());
    }

    @Override
    public void wakeUp() {
        LOG.info("Wake up called.");
        // do nothing, for now
    }

    @Override
    public void close() throws Exception {
        LOG.info("Close called, assigned splits {}.", assignedSplits.toString());
        if (!closed) {
            closed = true;
            readSoFar = 0;
            // complete closing with what may be needed
        }
    }

    static class GenericRecordReader {

        private final Schema schema;

        private GenericRecordReader(Schema schema) {
            Preconditions.checkNotNull(schema, "The provided avro schema reference is null.");
            this.schema = schema;
        }

        public static GenericRecordReader create(Schema schema) {
            return new GenericRecordReader(schema);
        }

        /**
         * Method for processing AVRO rows which only validates decoding.
         *
         * @param avroRows object returned from the ReadRowsResponse.
         */
        public List<GenericRecord> processRows(AvroRows avroRows) throws IOException {
            BinaryDecoder decoder =
                    DecoderFactory.get()
                            .binaryDecoder(avroRows.getSerializedBinaryRows().toByteArray(), null);
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
            List<GenericRecord> records = new ArrayList<>();
            GenericRecord row;
            while (!decoder.isEnd()) {
                // Reusing object row
                row = datumReader.read(null, decoder);
                records.add(row);
            }
            return records;
        }
    }
}
