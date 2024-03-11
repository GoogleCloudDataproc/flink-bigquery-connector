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

package com.google.cloud.flink.bigquery.sink.serializer;

import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;

import java.util.List;

/**
 * Interface for defining a Flink record to BigQuery proto serializer.
 *
 * @param <IN> Type of records to be written to BigQuery.
 */
public interface BigQueryProtoSerializer<IN> {

    ByteString serialize(IN record) throws BigQuerySerializationException;

    DescriptorProto getDescriptorProto();

    Descriptor getDescriptor();

    /**
     * Function to convert the {@link DescriptorProto} Type to {@link Descriptor}.This is necessary
     * as a Descriptor is needed for DynamicMessage (used to write to Storage API).
     *
     * @param descriptorProto input which needs to be converted to a Descriptor.
     * @return Descriptor obtained form the input DescriptorProto
     * @throws DescriptorValidationException in case the conversion is not possible.
     */
    static Descriptor getDescriptorFromDescriptorProto(DescriptorProto descriptorProto)
            throws DescriptorValidationException {
        FileDescriptorProto fileDescriptorProto =
                FileDescriptorProto.newBuilder().addMessageType(descriptorProto).build();
        FileDescriptor fileDescriptor =
                FileDescriptor.buildFrom(fileDescriptorProto, new FileDescriptor[0]);
        List<Descriptor> descriptorTypeList = fileDescriptor.getMessageTypes();
        if (descriptorTypeList.size() == 1) {
            return descriptorTypeList.get(0);
        } else {
            throw new IllegalArgumentException(
                    String.format("Expected one element but was %s", descriptorTypeList));
        }
    }
}
