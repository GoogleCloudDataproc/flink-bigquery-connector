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

package com.google.cloud.flink.bigquery.source.split.reader;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.util.Preconditions;

import com.codahale.metrics.SlidingWindowReservoir;
import com.google.cloud.bigquery.storage.v1.AvroRows;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.flink.bigquery.common.utils.flink.annotations.Internal;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.source.reader.BigQuerySourceReaderContext;
import com.google.cloud.flink.bigquery.source.split.BigQuerySourceSplit;
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
import java.util.Iterator;
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
    private final transient Optional<Histogram> readSplitTimeMetric;
    private final Queue<BigQuerySourceSplit> assignedSplits = new ArrayDeque<>();
    private final Configuration configuration;

    private Boolean closed = false;
    private Schema avroSchema = null;
    private Long readSoFar = 0L;
    private Long splitStartFetch;
    private Iterator<ReadRowsResponse> readStreamIterator = null;

    public BigQuerySourceSplitReader(
            BigQueryReadOptions readOptions, BigQuerySourceReaderContext readerContext) {
        this.readOptions = readOptions;
        this.readerContext = readerContext;
        this.configuration = readerContext.getConfiguration();
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

    Long offsetToFetch(BigQuerySourceSplit split) {
        // honor what is coming as checkpointed
        if (split.getOffset() > 0) {
            readSoFar = split.getOffset();
            splitStartFetch = System.currentTimeMillis();
        } else if (readSoFar == 0) {
            // will start reading the stream from the beginning
            splitStartFetch = System.currentTimeMillis();
        }
        LOG.debug(
                "[subtask #{}] Offset to fetch from {} for stream {}.",
                readerContext.getIndexOfSubtask(),
                readSoFar,
                split.getStreamName());
        return readSoFar;
    }

    BigQueryServices.BigQueryServerStream<ReadRowsResponse> retrieveReadStream(
            BigQuerySourceSplit split) throws IOException {
        try (BigQueryServices.StorageReadClient client =
                BigQueryServicesFactory.instance(readOptions.getBigQueryConnectOptions())
                        .storageRead()) {
            ReadRowsRequest readRequest =
                    ReadRowsRequest.newBuilder()
                            .setReadStream(split.getStreamName())
                            .setOffset(offsetToFetch(split))
                            .build();

            return client.readRows(readRequest);
        } catch (Exception ex) {
            throw new IOException(
                    String.format(
                            "[subtask #%d][hostname %s] Problems while opening the stream %s"
                                    + " from BigQuery with connection info %s."
                                    + " Current split offset %d, reader offset %d.",
                            readerContext.getIndexOfSubtask(),
                            readerContext.getLocalHostName(),
                            Optional.ofNullable(split.getStreamName()).orElse("NA"),
                            readOptions.toString(),
                            split.getOffset(),
                            readSoFar),
                    ex);
        }
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
        if (readerContext.willExceedLimit(0)) {
            LOG.info(
                    "Completing reading because we are over limit (context reader count {}).",
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
        Long fetchStartTime = System.currentTimeMillis();
        Boolean truncated = false;

        try {
            if (readStreamIterator == null) {
                readStreamIterator = retrieveReadStream(assignedSplit).iterator();
            }
            Long itStartTime = System.currentTimeMillis();
            while (readStreamIterator.hasNext()) {
                ReadRowsResponse response = readStreamIterator.next();
                if (!response.hasAvroRows()) {
                    LOG.info(
                            "[subtask #{}][hostname {}] The response contained"
                                    + " no avro records for stream {}.",
                            readerContext.getIndexOfSubtask(),
                            readerContext.getLocalHostName(),
                            assignedSplit.getStreamName());
                }
                if (avroSchema == null) {
                    if (response.hasAvroSchema()) {
                        // this will happen only the first time we read from a particular stream
                        avroSchema =
                                new Schema.Parser().parse(response.getAvroSchema().getSchema());
                    } else {
                        throw new IllegalArgumentException(
                                "Avro schema not initialized and not available in the response.");
                    }
                }
                Long decodeStart = System.currentTimeMillis();
                List<GenericRecord> recordList =
                        GenericRecordReader.create(avroSchema).processRows(response.getAvroRows());
                Long decodeTimeMS = System.currentTimeMillis() - decodeStart;
                LOG.debug(
                        "[subtask #{}][hostname %s] Iteration decoded records in {}ms from stream {}.",
                        readerContext.getIndexOfSubtask(),
                        decodeTimeMS,
                        assignedSplit.getStreamName());

                for (GenericRecord record : recordList) {
                    respBuilder.add(assignedSplit, record);
                    read++;
                    // check if the read count will be over the limit
                    if (readerContext.willExceedLimit(read)) {
                        break;
                    }
                }
                // check if the read count will be over the limit
                if (readerContext.willExceedLimit(read)) {
                    break;
                }
                Long itTimeMs = System.currentTimeMillis() - itStartTime;
                LOG.debug(
                        "[subtask #{}][hostname {}] Completed reading iteration in {}ms,"
                                + " so far read {} from stream {}.",
                        readerContext.getIndexOfSubtask(),
                        readerContext.getLocalHostName(),
                        itTimeMs,
                        readSoFar + read,
                        assignedSplit.getStreamName());
                itStartTime = System.currentTimeMillis();
                /**
                 * Assuming the record list from the read session have the same size (true in most
                 * cases but the last one in the response stream) we check if we will be going over
                 * the per fetch limit, in that case we break the loop and return the partial
                 * results (enabling the checkpointing of the partial retrieval if wanted by the
                 * runtime). The read response record count has been observed to have 1024 elements.
                 */
                if (read + recordList.size() > maxRecordsPerSplitFetch) {
                    truncated = true;
                    break;
                }
            }
            readSoFar += read;
            // check if we finished to read the stream to finalize the split
            if (!truncated) {
                readerContext.updateReadCount(readSoFar);
                Long splitTimeMs = System.currentTimeMillis() - splitStartFetch;
                this.readSplitTimeMetric.ifPresent(m -> m.update(splitTimeMs));
                LOG.info(
                        "[subtask #{}][hostname {}] Completed reading split, {} records in {}ms on stream {}.",
                        readerContext.getIndexOfSubtask(),
                        readerContext.getLocalHostName(),
                        readSoFar,
                        splitTimeMs,
                        assignedSplit.splitId());
                readSoFar = 0L;
                assignedSplits.poll();
                readStreamIterator = null;
                respBuilder.addFinishedSplit(assignedSplit.splitId());
            } else {
                Long fetchTimeMs = System.currentTimeMillis() - fetchStartTime;
                LOG.debug(
                        "[subtask #{}][hostname {}] Completed a partial fetch in {}ms,"
                                + " so far read {} from stream {}.",
                        readerContext.getIndexOfSubtask(),
                        readerContext.getLocalHostName(),
                        fetchTimeMs,
                        readSoFar,
                        assignedSplit.getStreamName());
            }
            return respBuilder.build();
        } catch (Exception ex) {
            LOG.error(
                    String.format(
                            "[subtask #%d][hostname %s] Problems while reading stream %s from BigQuery"
                                    + " with connection info %s. Current split offset %d,"
                                    + " reader offset %d. Flink options %s.",
                            readerContext.getIndexOfSubtask(),
                            Optional.ofNullable(readerContext.getLocalHostName()).orElse("NA"),
                            Optional.ofNullable(assignedSplit.getStreamName()).orElse("NA"),
                            readOptions.toString(),
                            assignedSplit.getOffset(),
                            readSoFar,
                            configuration.toString()),
                    ex);
            // release the iterator just in case
            readStreamIterator = null;
            /**
             * return an empty one, since we may need to re since last set offset (locally or from
             * checkpoint)
             */
            return new RecordsBySplits.Builder<GenericRecord>().build();
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<BigQuerySourceSplit> splitsChanges) {
        LOG.debug("Handle split changes {}.", splitsChanges);

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
        LOG.debug(
                "[subtask #{}][hostname %{}] Wake up called.",
                readerContext.getIndexOfSubtask(), readerContext.getLocalHostName());
        // do nothing, for now
    }

    @Override
    public void close() throws Exception {
        LOG.debug(
                "[subtask #{}][hostname {}] Close called, assigned splits {}.",
                readerContext.getIndexOfSubtask(),
                readerContext.getLocalHostName(),
                assignedSplits.toString());
        if (!closed) {
            closed = true;
            readSoFar = 0L;
            readStreamIterator = null;
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
