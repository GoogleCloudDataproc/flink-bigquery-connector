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

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import org.apache.avro.Schema;

import java.io.Serializable;

/**
 * An interface to derive {@link Descriptor} for Generic Record serialization. Also provides {@link
 * Schema} and {@link DescriptorProto}.
 */
public interface BigQuerySchemaProvider extends Serializable {

    /**
     * Returns a {@link DescriptorProto} object essential for obtaining Proto Rows Builder and
     * Descriptor instances.
     *
     * @return DescriptorProto
     */
    DescriptorProto getDescriptorProto();

    /**
     * Returns a {@link Descriptor} object essential for obtaining Dynamic Message instances.
     *
     * @return Descriptor
     */
    Descriptor getDescriptor();

    /**
     * Returns a {@link Schema} object required for obtaining Descriptor and DescriptorProto
     * instances.
     *
     * @return AvroSchema
     */
    Schema getAvroSchema();
}
