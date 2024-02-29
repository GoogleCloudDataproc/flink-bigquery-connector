package com.google.cloud.flink.bigquery.sink.serializer;

import com.google.protobuf.DescriptorProtos;

import java.io.Serializable;

/** Javadoc. */
public abstract class BigQueryProtoSerializer implements Serializable {
    public abstract DescriptorProtos.DescriptorProto getDescriptorProto();
}
