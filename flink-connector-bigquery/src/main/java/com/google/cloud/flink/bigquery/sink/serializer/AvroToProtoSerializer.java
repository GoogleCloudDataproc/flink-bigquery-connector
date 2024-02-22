package com.google.cloud.flink.bigquery.sink.serializer;

import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.protobuf.ByteString;
import org.apache.avro.generic.GenericRecord;

/** */
public class AvroToProtoSerializer implements BigQueryProtoSerializer<GenericRecord> {

    @Override
    public ByteString serializeToProto(GenericRecord record) throws BigQuerySerializationException {
        throw new UnsupportedOperationException();
    }
}
