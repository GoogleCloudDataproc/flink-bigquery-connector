package com.google.cloud.flink.bigquery.sink.serializer;

import com.google.protobuf.DescriptorProtos;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.protobuf.Descriptors;

import java.io.Serializable;
import java.util.List;

/**
 * Interface for defining a Flink record to BigQuery proto serializer.
 *
 * @param <IN> Type of records to be written to BigQuery.
 */
public interface BigQueryProtoSerializer<IN> {
    public DescriptorProtos.DescriptorProto getDescriptorProto();

    /**
     * Function to convert the {@link DescriptorProtos.DescriptorProto} Type to {@link
     * Descriptors.Descriptor}. This is necessary as a Descriptor is needed for DynamicMessage (used
     * to write to Storage API).
     *
     * @param descriptorProto input which needs to be converted to a Descriptor.
     * @return Descriptor obtained form the input DescriptorProto
     * @throws Descriptors.DescriptorValidationException in case the conversion is not possible.
     */
    public static Descriptors.Descriptor getDescriptorFromDescriptorProto(
            DescriptorProtos.DescriptorProto descriptorProto)
            throws Descriptors.DescriptorValidationException {
        DescriptorProtos.FileDescriptorProto fileDescriptorProto =
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .addMessageType(descriptorProto)
                        .build();
        Descriptors.FileDescriptor fileDescriptor =
                Descriptors.FileDescriptor.buildFrom(
                        fileDescriptorProto, new Descriptors.FileDescriptor[0]);
        List<Descriptors.Descriptor> descriptorTypeList = fileDescriptor.getMessageTypes();
        if (descriptorTypeList.size() == 1) {
            return descriptorTypeList.get(0);
        } else {
            throw new IllegalArgumentException(
                    String.format("Expected one element but was %s", descriptorTypeList));
        }
    }

    public ByteString serialize(IN record) throws BigQuerySerializationException;

}
