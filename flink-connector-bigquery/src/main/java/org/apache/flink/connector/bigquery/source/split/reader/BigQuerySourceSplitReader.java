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
import org.apache.flink.util.Preconditions;

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
import java.util.ArrayList;
import java.util.LinkedList;
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

    private Boolean closed = false;
    private final Queue<BigQuerySourceSplit> assignedSplits = new LinkedList<>();

    public BigQuerySourceSplitReader(
            BigQueryReadOptions readOptions, BigQuerySourceReaderContext readerContext) {
        this.readOptions = readOptions;
        this.readerContext = readerContext;
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

        // return when current read count is over limit
        if (readerContext.isOverLimit()) {
            respBuilder.addFinishedSplits(
                    assignedSplits.stream()
                            .map(split -> split.splitId())
                            .collect(Collectors.toList()));
            assignedSplits.clear();
            return respBuilder.build();
        }

        BigQuerySourceSplit assignedSplit = assignedSplits.peek();

        try (StorageReadClient client =
                BigQueryServicesFactory.instance(readOptions.getBigQueryConnectOptions())
                        .storageRead(
                                readOptions.getBigQueryConnectOptions().getCredentialsOptions())) {
            ReadRowsRequest readRequest =
                    ReadRowsRequest.newBuilder()
                            .setReadStream(assignedSplit.getStreamName())
                            .setOffset(assignedSplit.getOffset())
                            .build();

            BigQueryServerStream<ReadRowsResponse> stream = client.readRows(readRequest);
            int limit = readOptions.getMaxRecordsPerSplitFetch();
            int read = 0;
            Boolean truncated = false;
            GenericRecordReader reader = null;
            for (ReadRowsResponse response : stream) {
                if (!response.hasAvroRows()) {
                    LOG.debug("The response contained no avro records, finishing split.");
                }
                if (reader == null) {
                    Preconditions.checkState(
                            response.hasAvroSchema(),
                            "The response does not contain an Avro schema,"
                                    + " which is needed to decode records.");
                    reader =
                            new GenericRecordReader(
                                    new Schema.Parser()
                                            .parse(response.getAvroSchema().getSchema()));
                }

                for (GenericRecord record : reader.processRows(response.getAvroRows())) {
                    respBuilder.add(assignedSplit, record);
                    readerContext.getReadCount().incrementAndGet();
                    if (readerContext.isOverLimit()) {
                        break;
                    }
                    if (++read == limit) {
                        truncated = true;
                        break;
                    }
                }
                if (readerContext.isOverLimit()) {
                    break;
                }
                if (read == limit) {
                    break;
                }
            }
            assignedSplits.poll();
            if (!truncated) {
                LOG.info("Completing read for split: {}", assignedSplit.splitId());
                respBuilder.addFinishedSplit(assignedSplit.splitId());
            } else {
                BigQuerySourceSplit inProcess =
                        new BigQuerySourceSplit(
                                assignedSplit.getStreamName(), assignedSplit.getOffset() + read);
                assignedSplits.add(inProcess);
            }
            return respBuilder.build();
        } catch (Exception ex) {
            throw new IOException(
                    String.format(
                            "Problems while reading stream %s from BigQuery with connection"
                                    + " info %s. Current split offset %d, read session offset %d.",
                            Optional.ofNullable(assignedSplit.getStreamName()).orElse("NA"),
                            readOptions.toString(),
                            assignedSplit.getOffset(),
                            readerContext.getReadCount().get()),
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
        LOG.debug("Wake up called.");
        // do nothing, for now
    }

    @Override
    public void close() throws Exception {
        LOG.debug("Close called.");
        if (!closed) {
            closed = true;
            // complete closing with what may be needed
        }
    }

    static class GenericRecordReader {

        private final Schema schema;

        public GenericRecordReader(Schema schema) {
            Preconditions.checkNotNull(schema);
            this.schema = schema;
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
