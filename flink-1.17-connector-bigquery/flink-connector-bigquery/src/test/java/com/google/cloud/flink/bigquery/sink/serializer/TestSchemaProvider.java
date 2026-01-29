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

import java.util.Objects;

/**
 * Class inheriting {@link BigQuerySchemaProvider} for {@link AvroToProtoSerializerTest} and {@link
 * BigQuerySchemaProviderTest}.
 */
public class TestSchemaProvider implements BigQuerySchemaProvider {
    private final Schema schema;
    private final Descriptor descriptor;

    public TestSchemaProvider(Schema schema, Descriptor descriptor) {
        this.schema = schema;
        this.descriptor = descriptor;
    }

    @Override
    public DescriptorProto getDescriptorProto() {
        return getDescriptor().toProto();
    }

    @Override
    public Descriptor getDescriptor() {
        return descriptor;
    }

    @Override
    public Schema getAvroSchema() {
        return schema;
    }

    @Override
    public boolean schemaUnknown() {
        return schema == null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TestSchemaProvider other = (TestSchemaProvider) obj;
        return Objects.equals(schema, other.schema);
    }
}
