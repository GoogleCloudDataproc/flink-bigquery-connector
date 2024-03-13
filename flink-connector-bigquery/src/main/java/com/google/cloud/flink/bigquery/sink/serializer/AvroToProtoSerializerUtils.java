package com.google.cloud.flink.bigquery.sink.serializer;

import com.google.api.client.util.Preconditions;
import com.google.cloud.bigquery.storage.v1.BigDecimalByteStringEncoder;
import com.google.cloud.bigquery.storage.v1.CivilTimeEncoder;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.ArrayUtils;
import org.joda.time.Days;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;
import org.json.JSONException;
import org.json.JSONObject;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDate;
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

    // BigQuery inputs timestamp as microseconds since EPOCH,
    // So if we have TIMESTAMP in micros - we convert as it is.
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

    static String convertDateTime(Object value, boolean micros) {
        if (value instanceof String) {
            return (String) value;
        }
        if (value instanceof Long) {
            /**
             * Assume that it is provided in {@link CivilTimeEncoder} Encoding Microsecond Precision
             * format
             */
            return CivilTimeEncoder.decodePacked64DatetimeMicros((long) value).toString();
        }
        throw new UnsupportedOperationException(
                "Local Timestamp(micros/millis) value not provided in String format");
    }

    static String convertTime(Object value, boolean micros) {
        if (value instanceof String) {
            return (String) value;
        }
        if (value instanceof Long) {
            /**
             * Assume that it is provided in {@link CivilTimeEncoder} Encoding Microsecond Precision
             * format
             */
            return CivilTimeEncoder.decodePacked64TimeMicros((long) value).toString();
        }
        throw new UnsupportedOperationException(
                "Time(micros/millis) value not provided in String/Long format");
    }

    // 1. There is no way to check the precision and scale of NUMERIC/BIGNUMERIC fields,
    // so we can just check by the maximum value.
    // 2. decimal() logical type is mapped to BIGNUMERIC bigquery field.
    // So in case we attempt to add decimal with precision >9 to a NUMERIC BQ field.
    // The .append() method would be responsible for the error.
    // .serialise() would successfully serialise it without any error indications.
    static ByteString convertBigDecimal(Object value) {
        // Assuming decimal (value) comes in big-endian encoding.
        ByteBuffer byteBuffer = (ByteBuffer) value;
        // Reverse before sending to big endian convertor.
        // decodeBigNumericByteString() assumes string to be provided in little-endian.
        byte[] byteArray = byteBuffer.array();
        ArrayUtils.reverse(byteArray); // Converted to little-endian.
        BigDecimal bigDecimal =
                BigDecimalByteStringEncoder.decodeBigNumericByteString(
                        ByteString.copyFrom(byteArray));
        return BigDecimalByteStringEncoder.encodeToBigNumericByteString(bigDecimal);
    }

    static String convertGeography(Object value) {
        Preconditions.checkArgument(
                value instanceof String,
                "Expecting a value as String type (geography_wkt or geojson format).");
        return (String) value;
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
}
