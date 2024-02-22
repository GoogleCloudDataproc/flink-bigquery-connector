package com.google.cloud.flink.bigquery.sink.serializer;

import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.protobuf.ByteString;

/** */
public interface BigQueryProtoSerializer<IN> {

    public ByteString serializeToProto(IN record) throws BigQuerySerializationException;
}
