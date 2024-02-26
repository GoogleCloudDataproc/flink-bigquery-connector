package com.google.cloud.flink.bigquery.sink;

import org.apache.flink.api.connector.sink2.Sink;

import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.utils.ProtobufUtils;

/** */
abstract class AbstractSink<IN> implements Sink<IN> {

    final BigQueryConnectOptions connectOptions;
    final int maxParallelism;
    final ProtoSchema protoSchema;
    final String tablePath;

    AbstractSink(BigQueryConnectOptions connectOptions, int maxParallelism) {
        this.connectOptions = connectOptions;
        this.maxParallelism = maxParallelism;
        this.protoSchema =
                ProtoSchema.newBuilder().setProtoDescriptor(ProtobufUtils.DESCRIPTOR_PROTO).build();
        this.tablePath =
                String.format(
                        "projects/%s/datasets/%s/tables/%s",
                        connectOptions.getProjectId(),
                        connectOptions.getDataset(),
                        connectOptions.getTable());
    }

    void validateParallelism(InitContext context) {
        if (context.getNumberOfParallelSubtasks() > maxParallelism) {
            throw new IllegalStateException("Attempting to create more writers than allowed!");
        }
    }
}
