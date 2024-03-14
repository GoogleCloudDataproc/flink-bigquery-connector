package com.google.cloud.flink.bigquery.sink.serializer;

import com.google.protobuf.Descriptors;
import org.apache.avro.Schema;

/**
 * Class to return the {@link AvroToProtoSerializerTest} and {@link BigQuerySchemaProviderTest}
 * Results. Responsible for abstracting Descriptor and Schema.
 */
public class BigQueryAvroToProtoSerializerTestResult {
    private final Schema schema;
    private final Descriptors.Descriptor descriptor;

    BigQueryAvroToProtoSerializerTestResult(Schema schema, Descriptors.Descriptor descriptor) {
        this.schema = schema;
        this.descriptor = descriptor;
    }

    public Descriptors.Descriptor getDescriptor() {
        return this.descriptor;
    }

    public Schema getSchema() {
        return this.schema;
    }
}
