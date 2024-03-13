package com.google.cloud.flink.bigquery.sink.serializer;

import org.joda.time.Instant;
import org.junit.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

/** Tests for Utils - conversion of Object types to respective values. */
public class AvroToProtoSerializerUtilsTest {
    @Test
    public void testConvertJson() {
        Object value = "{\"name\": \"test_value\", \"value\": \"value\"}";
        String convertedValue = AvroToProtoSerializerUtils.convertJson(value);
        assertThat(convertedValue).isEqualTo((String) value);
    }

    @Test
    public void testErrorConvertJson() {
        Object value = "invalid_json_string";
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> AvroToProtoSerializerUtils.convertJson(value));
        assertThat(exception).hasMessageContaining(" is not in valid JSON Format.");
    }

    @Test
    public void testConvertGeography() {
        Object value = "POINT(-121 41)";
        String convertedValue = AvroToProtoSerializerUtils.convertGeography(value);
        assertThat(convertedValue).isEqualTo((String) value);
    }

    @Test
    public void testErrorConvertGeography() {
        Object value = 5678;
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> AvroToProtoSerializerUtils.convertGeography(value));
        assertThat(exception)
                .hasMessageContaining(
                        "Expecting a value as String type (geography_wkt or geojson format).");
    }

    @Test
    public void testConvertUUID() {
        Object value = UUID.randomUUID();
        String convertedValue = AvroToProtoSerializerUtils.convertUUID(value);
        assertThat(convertedValue).isEqualTo((String) value.toString());
    }

    @Test
    public void testConvertUUIDString() {
        Object value = UUID.randomUUID().toString();
        String convertedValue = AvroToProtoSerializerUtils.convertUUID(value);
        assertThat(convertedValue).isEqualTo((String) value);
    }

    @Test
    public void testErrorConvertUUID() {
        Object value = 5678;
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> AvroToProtoSerializerUtils.convertUUID(value));
        assertThat(exception).hasMessageContaining("Expecting a value as String type.");
    }

    @Test
    public void testErrorConvertBigNumeric() {
        Object value = "A random string";
        assertThrows(
                ClassCastException.class,
                () -> AvroToProtoSerializerUtils.convertBigDecimal(value));
    }

    @Test
    public void testConvertTimeStringType() {
        Object value = "00:22:59.700440";
        String convertedValue = AvroToProtoSerializerUtils.convertTime(value, true);
        assertThat(convertedValue).isEqualTo((String) value);
    }

    @Test
    public void testConvertTimeLongType() {
        Object value = 2494194728L;
        String convertedValue = AvroToProtoSerializerUtils.convertTime(value, true);
        assertThat(convertedValue).isEqualTo("00:37:10.681");
    }

    @Test
    public void testConvertErrorTimeLongType() {
        Object invalidValueLong = 142441321387821952L;
        assertThrows(
                IllegalArgumentException.class,
                () -> AvroToProtoSerializerUtils.convertTime(invalidValueLong, true));
        Object floatValue = 1234.56;
        assertThrows(
                UnsupportedOperationException.class,
                () -> AvroToProtoSerializerUtils.convertTime(floatValue, true));
    }

    @Test
    public void testConvertTimestampMillis() {
        Object value = 1710270870L;
        Long convertedValue =
                AvroToProtoSerializerUtils.convertTimestamp(value, false, "Timestamp (millis)");
        assertThat(convertedValue).isEqualTo(1710270870000L);
    }

    @Test
    public void testConvertTimestampMillisTimeInstant() {
        Object value = Instant.ofEpochMilli(1710270870123L);
        Long convertedValue =
                AvroToProtoSerializerUtils.convertTimestamp(value, false, "Timestamp (millis)");
        assertThat(convertedValue).isEqualTo(1710270870123000L);
    }

    @Test
    public void testConvertTimestampMicrosTimeInstant() {
        Object value = Instant.parse("2024-03-13T01:01:16.579501+00:00");
        Long convertedValue =
                AvroToProtoSerializerUtils.convertTimestamp(value, true, "Timestamp (micros)");
        assertThat(convertedValue).isEqualTo(1710291676579000L);
    }

    @Test
    public void testConvertTimestampMicros() {
        Object value = 1710270870123L;
        Long convertedValue =
                AvroToProtoSerializerUtils.convertTimestamp(value, true, "Timestamp (micros)");
        assertThat(convertedValue).isEqualTo(1710270870123L);
    }

    @Test
    public void testConvertErrorTimestamp() {
        Object invalidValue = "not_a_timestamp";
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                AvroToProtoSerializerUtils.convertTimestamp(
                                        invalidValue, true, "Timestamp (micros)"));
        assertThat(exception).hasMessageContaining("Expecting a value as Long type");

        Object invalidLongValue = 1123456789101112131L;
        exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                AvroToProtoSerializerUtils.convertTimestamp(
                                        invalidLongValue, true, "Timestamp (micros)"));
        assertThat(exception)
                .hasMessageContaining(
                        "Should be a long value indicating microseconds since Epoch (1970-01-01 00:00:00) between 0001-01-01 00:00:00.0 and 9999-12-31 23:59:59.999999");
    }

    @Test
    public void testConvertDateInstant() {
        Object value = Instant.parse("2024-03-13T00:00:00.000000+00:00");
        Integer convertedValue = AvroToProtoSerializerUtils.convertDate(value);
        assertThat(convertedValue).isEqualTo(19795);
    }

    @Test
    public void testConvertDate() {
        Object value = 19794;
        Integer convertedValue = AvroToProtoSerializerUtils.convertDate(value);
        assertThat(convertedValue).isEqualTo(19794);
    }

    @Test
    public void testConvertErrorDate() {
        Object invalidValue = 29328967;
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> AvroToProtoSerializerUtils.convertDate(invalidValue));
        assertThat(exception)
                .hasMessageContaining(
                        "Should be a Integer value indicating days since Epoch (1970-01-01 00:00:00)");

        Object floatValue = 1234.56;
        assertThrows(
                IllegalArgumentException.class,
                () -> AvroToProtoSerializerUtils.convertDate(floatValue));
    }

    @Test
    public void testConvertDateTimeStringType() {
        Object value = "2024-01-01 00:22:59.700440";
        String convertedValue = AvroToProtoSerializerUtils.convertDateTime(value, true);
        assertThat(convertedValue).isEqualTo((String) value);
    }

    @Test
    public void testConvertDateTimeLongType() {
        Object value = 142441321937062776L;
        String convertedValue = AvroToProtoSerializerUtils.convertDateTime(value, true);
        assertThat(convertedValue).isEqualTo("2024-03-13T00:42:54.347");
    }

    @Test
    public void testConvertErrorTimeDateLongType() {
        Object value = 1424413213878251952L;
        assertThrows(
                IllegalArgumentException.class,
                () -> AvroToProtoSerializerUtils.convertDateTime(value, true));
        Object floatValue = 1234.56;
        assertThrows(
                UnsupportedOperationException.class,
                () -> AvroToProtoSerializerUtils.convertDateTime(floatValue, true));
    }
}
