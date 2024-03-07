package com.google.cloud.flink.bigquery.sink.serializer;

import org.apache.flink.shaded.guava30.com.google.common.primitives.Bytes;

import com.google.api.client.util.Preconditions;
import com.google.protobuf.ByteString;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.joda.time.Days;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.UUID;

/** Utilities for converting Avro Datatypes to Respective Types. */
public class AvroToProtoSerializerUtils {

    static String convertUUID(Object value) {
        if (value instanceof UUID) {
            return ((UUID) value).toString();
        } else {
            Preconditions.checkArgument(
                    value instanceof String, "Expecting a value as String type.");
            UUID.fromString((String) value);
            return (String) value;
        }
    }

    static Long convertTimestamp(Object value, boolean micros) {
        if (value instanceof ReadableInstant) {
            return ((ReadableInstant) value).getMillis() * (micros ? 1000 : 1);
        } else {
            Preconditions.checkArgument(
                    value instanceof Long, "Expecting a value as Long type (millis).");
            return (Long) value;
        }
    }

    static Integer convertDate(Object value) {
        if (value instanceof ReadableInstant) {
            return Days.daysBetween(Instant.EPOCH, (ReadableInstant) value).getDays();
        } else {
            Preconditions.checkArgument(
                    value instanceof Integer, "Expecting a value as Integer type (days).");
            return (Integer) value;
        }
    }

    static ByteString convertDecimal(LogicalType logicalType, Object value) {
        ByteBuffer byteBuffer = (ByteBuffer) value;
        BigDecimal bigDecimal =
                new Conversions.DecimalConversion()
                        .fromBytes(
                                byteBuffer.duplicate(),
                                Schema.create(Schema.Type.NULL), // dummy schema, not used
                                logicalType);
        return serializeBigDecimalToNumeric(bigDecimal);
    }

    static ByteString serializeBigDecimalToNumeric(BigDecimal o) {
        final int numericScale = 9;
        // Maximum and minimum allowed values for the NUMERIC data type.
        final BigDecimal maxNumericValue =
                new BigDecimal("99999999999999999999999999999.999999999");
        final BigDecimal minNumericValue =
                new BigDecimal("-99999999999999999999999999999.999999999");
        return serializeBigDecimal(o, numericScale, maxNumericValue, minNumericValue, "Numeric");
    }

    private static ByteString serializeBigDecimal(
            BigDecimal v, int scale, BigDecimal maxValue, BigDecimal minValue, String typeName) {
        if (v.scale() > scale) {
            throw new IllegalArgumentException(
                    typeName + " scale cannot exceed " + scale + ": " + v.toPlainString());
        }
        if (v.compareTo(maxValue) > 0 || v.compareTo(minValue) < 0) {
            throw new IllegalArgumentException(typeName + " overflow: " + v.toPlainString());
        }

        byte[] bytes = v.setScale(scale).unscaledValue().toByteArray();
        // NUMERIC/BIGNUMERIC values are serialized as scaled integers in two's complement form in
        // little endian
        // order. BigInteger requires the same encoding but in big endian order, therefore we must
        // reverse the bytes that come from the proto.
        // TODO: Remove guava dependency.
        Bytes.reverse(bytes);
        return ByteString.copyFrom(bytes);
    }
}
