package com.google.cloud.flink.bigquery.sink.writer;

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.flink.bigquery.common.utils.ProtobufUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** */
public class BigQueryDefaultWriter<IN> implements SinkWriter<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryBufferedWriter.class);

    private ProtoRows.Builder protoRowsBuilder;
    private final StreamWriter streamWriter;
    private int writeCount;

    public BigQueryDefaultWriter(StreamWriter streamWriter) {
        protoRowsBuilder = ProtoRows.newBuilder();
        this.streamWriter = streamWriter;
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        try {
            assert element instanceof String;
            protoRowsBuilder.addSerializedRows(
                    ProtobufUtils.createMessage(element.toString()).toByteString());
            if (protoRowsBuilder.getSerializedRowsCount() == 1000) {
                appendRows();
            }
            writeCount++;
            if (writeCount == 5000) {
                LOG.error(
                        Thread.currentThread().getId()
                                + ": Intentionally failing write for "
                                + streamWriter.getStreamName());
                throw new RuntimeException(
                        Thread.currentThread().getId()
                                + ": Intentional failure at write for "
                                + streamWriter.getStreamName());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        LOG.info("DefaultWriter.flush");
        if (protoRowsBuilder.getSerializedRowsCount() > 0) {
            appendRows();
        }
    }

    @Override
    public void close() {
        LOG.info("DefaultWriter.close");
        streamWriter.close();
        protoRowsBuilder.clear();
    }

    private void appendRows() {
        LOG.info("DefaultWriter.append");
        ProtoRows rowsToAppend = protoRowsBuilder.build();
        AppendRowsResponse response;
        try {
            response = streamWriter.append(rowsToAppend).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (response.hasError()) {
            throw new RuntimeException(response.getError().getMessage());
        }
        protoRowsBuilder = ProtoRows.newBuilder();
    }
}
