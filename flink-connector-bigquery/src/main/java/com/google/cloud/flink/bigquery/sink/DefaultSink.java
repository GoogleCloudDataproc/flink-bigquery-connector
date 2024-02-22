package com.google.cloud.flink.bigquery.sink;

import org.apache.flink.api.connector.sink2.SinkWriter;

/** */
public class DefaultSink extends BaseSink {

    @Override
    public SinkWriter createWriter(InitContext context) {
        throw new UnsupportedOperationException();
    }
}
