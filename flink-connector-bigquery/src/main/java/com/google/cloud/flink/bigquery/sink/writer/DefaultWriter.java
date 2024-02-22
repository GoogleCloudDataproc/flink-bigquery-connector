package com.google.cloud.flink.bigquery.sink.writer;

import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;

/** */
public class DefaultWriter<IN> implements SinkWriter<IN> {

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }
}
