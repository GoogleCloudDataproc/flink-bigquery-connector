package com.google.cloud.flink.bigquery.sink.serializer;

import org.apache.flink.shaded.guava30.com.google.common.primitives.Bytes;

import com.google.api.client.util.Preconditions;
import com.google.protobuf.ByteString;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;
import org.json.JSONException;
import org.json.JSONObject;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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

    private static long validateTimestamp(long timestamp) {
        // Since bigquery requires the timestamp to be in Microseconds since epoch.
        // But UNIX considers it in Milliseconds since the epoch.
        long timestampMillis = TimeUnit.MICROSECONDS.toMillis(timestamp);
        Timestamp minTs = Timestamp.valueOf("0001-01-01 00:00:00");
        Timestamp maxTs = Timestamp.valueOf("9999-12-31 23:59:59.999999");
        Timestamp ts = null;
        try {
            ts = new Timestamp(timestampMillis);

            if (ts.before(minTs) || ts.after(maxTs)) {
                throw new IllegalArgumentException(
                        String.format("Invalid Timestamp '%s' Provided", ts));
            }
            return timestamp;
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid Timestamp '%s' Provided."
                                    + "\nShould be a long value indicating microseconds since Epoch (1970-01-01 00:00:00) "
                                    + "between %s and %s",
                            ts, minTs, maxTs));
        }
    }

    // BigQuery inputs timestamps as microseconds since EPOCH,
    // So if we have TIMESTAMP as micros - we convert as it is.
    // If the TIMESTAMP is in millis - we convert to Micros and then add.
    static Long convertTimestamp(Object value, boolean micros, String type) {
        long timestamp;
        if (value instanceof ReadableInstant) {
            timestamp = TimeUnit.MILLISECONDS.toMicros(((ReadableInstant) value).getMillis());
        } else {
            Preconditions.checkArgument(
                    value instanceof Long,
                    String.format(
                            "Expecting a value as Long type " + "%s. Instead %s was obtained",
                            type, value.getClass()));
            timestamp = (micros ? (Long) value : TimeUnit.MILLISECONDS.toMicros((Long) value));
        }
        return validateTimestamp(timestamp);
    }

    static Integer validateDate(Integer date) {
        if (date > 2932896 || date < -719162) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid Date '%s' Provided."
                                    + "\nShould be a Integer value indicating days since Epoch (1970-01-01 00:00:00) "
                                    + "between %s and %s",
                            LocalDate.ofEpochDay(date), "0001-01-01", "9999-12-31"));
        }
        return date;
    }

    static Integer convertDate(Object value) {
        // The value is the number of days since the Unix epoch (1970-01-01).
        // The valid range is `-719162` (0001-01-01) to `2932896` (9999-12-31).
        int date;
        if (value instanceof ReadableInstant) {
            date = Days.daysBetween(Instant.EPOCH, (ReadableInstant) value).getDays();
        } else {
            Preconditions.checkArgument(
                    value instanceof Integer, "Expecting a value as Integer type (days).");
            date = (Integer) value;
        }
        return validateDate(date);
    }

    private static LocalDateTime convertDatetimeAndTime(Object value, boolean micros, String type) {
        // Get number of microseconds since epoch.
        long timestamp = convertTimestamp(value, micros, type);
        // joda-time offers millisecond precision.
        // So, extracting time (millisecond precision) and then forming the microsecond precision
        // time (java.time).
        DateTime time = Instant.EPOCH.plus(TimeUnit.MICROSECONDS.toMillis(timestamp)).toDateTime();
        return LocalDateTime.of(
                time.getYear(),
                time.getMonthOfYear(),
                time.getDayOfMonth(),
                time.hourOfDay().get(),
                time.minuteOfHour().get(),
                time.getSecondOfMinute(),
                (int) (timestamp % 1000000) * 1000);
    }

    static String convertDateTime(Object value, boolean micros, String type) {
        LocalDateTime datetime = convertDatetimeAndTime(value, micros, type);
        return datetime.toString();
    }

    static String convertTime(Object value, boolean micros, String type) {
        LocalDateTime datetime = convertDatetimeAndTime(value, micros, type);
        return datetime.toLocalTime().toString();
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

    static String convertGeography(Object value) {
        Preconditions.checkArgument(
                value instanceof String,
                "Expecting a value as String type (geography_wkt or geojson format).");
        String geographyString = (String) value;
        // TODO: add validations to check if a valid GEO-WKT or GEO-JSON Instance.
        //        throw new IllegalArgumentException(String.format("The input string %s is not in
        // GeoJSON or GEO-WKT Format.", geographyString));
        return geographyString;
    }

    static String convertJson(Object value) {
        Preconditions.checkArgument(
                value instanceof String, "Expecting a value as String type (json format).");
        String jsonString = (String) value;
        try {
            new JSONObject(jsonString);
        } catch (JSONException e) {
            throw new IllegalArgumentException(
                    String.format("The input string %s is not in valid JSON Format.", jsonString));
        }
        return jsonString;
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
