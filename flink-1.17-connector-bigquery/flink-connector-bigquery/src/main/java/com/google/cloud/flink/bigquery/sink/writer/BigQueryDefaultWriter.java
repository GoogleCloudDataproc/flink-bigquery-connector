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

import org.apache.flink.api.connector.sink2.Sink;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQueryConnectorException;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProvider;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.ExecutionException;

/**
 * Writer implementation for {@link BigQueryDefaultSink}.
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
 * <p>Depending on the checkpointing mode, this writer offers the following consistency guarantees:
 * <li>{@link CheckpointingMode#EXACTLY_ONCE}: at-least-once write consistency.
 * <li>{@link CheckpointingMode#AT_LEAST_ONCE}: at-least-once write consistency.
 * <li>{Checkpointing disabled}: no write consistency.
 *
 * @param <IN> Type of records to be written to BigQuery.
 */
public class BigQueryDefaultWriter<IN> extends BaseWriter<IN> {

    public BigQueryDefaultWriter(
            int subtaskId,
            BigQueryConnectOptions connectOptions,
            BigQuerySchemaProvider schemaProvider,
            BigQueryProtoSerializer serializer,
            String tablePath,
            Sink.InitContext context) {
        super(subtaskId, connectOptions, schemaProvider, serializer, context);
        streamName = String.format("%s/streams/_default", tablePath);
    }

    /** Accept record for writing to BigQuery table. */
    @Override
    public void write(IN element, Context context) {
        // Increment the records seen.
        numRecordsInSinceChkptCounter.inc();
        try {
            ByteString protoRow = getProtoRow(element);
            if (!fitsInAppendRequest(protoRow)) {
                validateAppendResponses(false);
                append();
            }
            addToAppendRequest(protoRow);
        } catch (BigQuerySerializationException e) {
            logger.error(String.format("Unable to serialize record %s. Dropping it!", element), e);
        }
    }

    /** Asynchronously append to BigQuery table's default stream. */
    @Override
    ApiFuture sendAppendRequest(ProtoRows protoRows) {
        if (streamWriter == null) {
            streamWriter = createStreamWriter(true);
        }
        return streamWriter.append(protoRows);
    }

    /** Throws a RuntimeException if an error is found with append response. */
    @Override
    void validateAppendResponse(Pair<ApiFuture<AppendRowsResponse>, Long> appendResponseFuture) {
        AppendRowsResponse response;
        long expectedOffset = appendResponseFuture.getRight();
        long currentRequestRecordCount =
                expectedOffset - this.successfullyAppendedRecordsCounter.getCount();
        try {
            response = appendResponseFuture.getLeft().get();
        } catch (ExecutionException | InterruptedException e) {
            // Case 1: we did not get any response:
            this.numRecordsSendErrorCounter.inc(currentRequestRecordCount);
            logger.error(
                    String.format(
                            "Exception while retrieving AppendRowsResponse in subtask %s",
                            subtaskId),
                    e);
            throw new BigQueryConnectorException(
                    "Error getting response for BigQuery write API", e);
        }
        if (response.hasError()) {
            // Case 2: Append Fails, All the records failed. it is an atomic request.
            this.numRecordsSendErrorCounter.inc(currentRequestRecordCount);
            logger.error(
                    String.format(
                            "Request to AppendRows failed in subtask %s with error %s",
                            subtaskId, response.getError().getMessage()));
            throw new BigQueryConnectorException(
                    String.format(
                            "Exception while writing to BigQuery table: %s",
                            response.getError().getMessage()));
        }
        // Case 3: Append is successful.
        // it would arrive here only if the response was received and there were no errors.
        // the request succeeded without errors (records are in BQ)
        this.successfullyAppendedRecordsCounter.inc(currentRequestRecordCount);
        this.successfullyAppendedRecordsSinceChkptCounter.inc(currentRequestRecordCount);
    }
}
