package com.google.cloud.flink.bigquery.sink;

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import com.google.cloud.flink.bigquery.sink.writer.BigQueryDefaultWriter;

/** */
class DefaultSink<IN> extends AbstractSink<IN> {

    DefaultSink(BigQueryConnectOptions connectOptions, int maxParallelism) {
        super(connectOptions, maxParallelism);
    }

    public SinkWriter createWriter(InitContext context) {
        super.validateParallelism(context);
        try (BigQueryServices.StorageWriteClient writeClient =
                BigQueryServicesFactory.instance(connectOptions).storageWrite()) {
            String writeStreamName = String.format("%s/streams/_default", tablePath);
            StreamWriter streamWriter =
                    writeClient.createStreamWriter(
                            protoSchema, RetrySettings.newBuilder().build(), writeStreamName);
            return new BigQueryDefaultWriter(streamWriter);
        } catch (Exception e) {
            throw new RuntimeException(
                    Thread.currentThread().getId() + ": Failed to create writer", e);
        }
    }
}
