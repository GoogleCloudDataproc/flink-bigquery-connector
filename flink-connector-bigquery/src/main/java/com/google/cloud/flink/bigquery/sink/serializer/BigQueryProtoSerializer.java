package com.google.cloud.flink.bigquery.sink.serializer;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.apache.arrow.util.VisibleForTesting;

/** Javadoc. */
public abstract class BigQueryProtoSerializer {
    @VisibleForTesting
    public abstract DescriptorProtos.DescriptorProto getDescriptorProto();

    @VisibleForTesting
    public abstract Descriptors.Descriptor getDescriptor();
}
