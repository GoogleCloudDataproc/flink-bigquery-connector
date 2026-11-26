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

package com.google.cloud.flink.bigquery.common.utils;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.Year;
import java.time.format.DateTimeParseException;
import java.util.Objects;

/**
 * Parses BigQuery DATETIME strings with the same result as {@code LocalDateTime.parse(text,
 * DateTimeFormatterPatterns.DATETIME_FORMATTER)}.
 *
 * <p>The formatter has many optional sections, so it is slow. The Storage Read API sends DATETIME
 * values as {@code YYYY-MM-DDTHH:MM:SS} with up to six fractional digits, so this class parses that
 * shape directly. Any other text, and any value that the shape cannot represent exactly, goes to
 * the formatter, so the accepted values and the errors do not change.
 *
 * <p>{@link #format(LocalDateTime)} writes values the way {@code
 * DateTimeFormatterPatterns.DATETIME_OUTPUT_FORMATTER} does, also without the formatter.
 */
public final class BigQueryDatetimeParser {

    private static final int[] FRACTION_SCALE = {
        1, 100_000_000, 10_000_000, 1_000_000, 100_000, 10_000, 1_000
    };

    private BigQueryDatetimeParser() {}

    /**
     * Parses a DATETIME string.
     *
     * @param text the DATETIME text
     * @return the parsed date and time
     * @throws NullPointerException if {@code text} is null
     * @throws DateTimeParseException if the formatter cannot parse the text
     */
    public static LocalDateTime parse(String text) {
        Objects.requireNonNull(text, "text");
        LocalDateTime value = parseCanonical(text);
        if (value != null) {
            return value;
        }
        return LocalDateTime.parse(text, DateTimeFormatterPatterns.DATETIME_FORMATTER);
    }

    /**
     * Parses a DATETIME string from the first {@code length} bytes of its UTF-8 encoding.
     *
     * @param utf8 the UTF-8 bytes; bytes after {@code length} are ignored
     * @param length the number of bytes to parse, from zero through {@code utf8.length}
     * @return the parsed date and time
     * @throws NullPointerException if {@code utf8} is null
     * @throws IndexOutOfBoundsException if {@code length} is negative or exceeds the array length
     * @throws DateTimeParseException if the formatter cannot parse the text
     */
    public static LocalDateTime parse(byte[] utf8, int length) {
        Objects.requireNonNull(utf8, "utf8");
        if (length < 0 || length > utf8.length) {
            throw new IndexOutOfBoundsException(
                    "length must be between 0 and " + utf8.length + ": " + length);
        }
        LocalDateTime value = parseCanonical(utf8, length);
        if (value != null) {
            return value;
        }
        return LocalDateTime.parse(
                new String(utf8, 0, length, StandardCharsets.UTF_8),
                DateTimeFormatterPatterns.DATETIME_FORMATTER);
    }

    /**
     * Formats a value as {@code uuuu-MM-dd'T'HH:mm:ss.SSSSSS}, with the same result as {@code
     * value.format(DateTimeFormatterPatterns.DATETIME_OUTPUT_FORMATTER)}.
     */
    public static String format(LocalDateTime value) {
        int year = value.getYear();
        if (year < 0 || year > 9999) {
            // the formatter adds a sign to these years
            return value.format(DateTimeFormatterPatterns.DATETIME_OUTPUT_FORMATTER);
        }
        char[] text = new char[26];
        put(text, 0, year, 4);
        text[4] = '-';
        put(text, 5, value.getMonthValue(), 2);
        text[7] = '-';
        put(text, 8, value.getDayOfMonth(), 2);
        text[10] = 'T';
        put(text, 11, value.getHour(), 2);
        text[13] = ':';
        put(text, 14, value.getMinute(), 2);
        text[16] = ':';
        put(text, 17, value.getSecond(), 2);
        text[19] = '.';
        put(text, 20, value.getNano() / 1_000, 6);
        return new String(text);
    }

    /** Writes {@code value} as {@code count} decimal digits, with leading zeros. */
    private static void put(char[] text, int start, int value, int count) {
        for (int i = start + count - 1; i >= start; i--) {
            text[i] = (char) ('0' + value % 10);
            value /= 10;
        }
    }

    /** Returns the value of the canonical shape, or null for any other text. */
    static LocalDateTime parseCanonical(String text) {
        int length = text.length();
        if (length != 19 && (length < 21 || length > 26)) {
            return null;
        }
        if (text.charAt(4) != '-'
                || text.charAt(7) != '-'
                || text.charAt(13) != ':'
                || text.charAt(16) != ':') {
            return null;
        }
        char separator = text.charAt(10);
        if (separator != 'T' && separator != ' ' && separator != 't') {
            return null;
        }
        int year = digits(text, 0, 4);
        int month = digits(text, 5, 2);
        int day = digits(text, 8, 2);
        int hour = digits(text, 11, 2);
        int minute = digits(text, 14, 2);
        int second = digits(text, 17, 2);
        if (year < 1 || month < 1 || month > 12 || day < 1 || hour < 0 || hour > 23) {
            return null;
        }
        if (minute < 0 || minute > 59 || second < 0 || second > 59) {
            return null;
        }
        // the formatter moves a day past the end of the month back to the last day
        if (day > 28 && day > Month.of(month).length(Year.isLeap(year))) {
            return null;
        }
        int nanos = 0;
        if (length > 19) {
            if (text.charAt(19) != '.') {
                return null;
            }
            int fractionDigits = length - 20;
            int fraction = digits(text, 20, fractionDigits);
            if (fraction < 0) {
                return null;
            }
            nanos = fraction * FRACTION_SCALE[fractionDigits];
        }
        return LocalDateTime.of(year, month, day, hour, minute, second, nanos);
    }

    /** Returns the decimal value of {@code count} ASCII digits, or -1 if one is not a digit. */
    private static int digits(String text, int start, int count) {
        int value = 0;
        for (int i = start; i < start + count; i++) {
            int digit = text.charAt(i) - '0';
            if (digit < 0 || digit > 9) {
                return -1;
            }
            value = value * 10 + digit;
        }
        return value;
    }

    /** Returns the value of the canonical shape, or null for any other text. */
    static LocalDateTime parseCanonical(byte[] text, int length) {
        if (length != 19 && (length < 21 || length > 26)) {
            return null;
        }
        if (text[4] != '-' || text[7] != '-' || text[13] != ':' || text[16] != ':') {
            return null;
        }
        byte separator = text[10];
        if (separator != 'T' && separator != ' ' && separator != 't') {
            return null;
        }
        int year = digits(text, 0, 4);
        int month = digits(text, 5, 2);
        int day = digits(text, 8, 2);
        int hour = digits(text, 11, 2);
        int minute = digits(text, 14, 2);
        int second = digits(text, 17, 2);
        if (year < 1 || month < 1 || month > 12 || day < 1 || hour < 0 || hour > 23) {
            return null;
        }
        if (minute < 0 || minute > 59 || second < 0 || second > 59) {
            return null;
        }
        // the formatter moves a day past the end of the month back to the last day
        if (day > 28 && day > Month.of(month).length(Year.isLeap(year))) {
            return null;
        }
        int nanos = 0;
        if (length > 19) {
            if (text[19] != '.') {
                return null;
            }
            int fractionDigits = length - 20;
            int fraction = digits(text, 20, fractionDigits);
            if (fraction < 0) {
                return null;
            }
            nanos = fraction * FRACTION_SCALE[fractionDigits];
        }
        return LocalDateTime.of(year, month, day, hour, minute, second, nanos);
    }

    /** Returns the decimal value of {@code count} ASCII digits, or -1 if one is not a digit. */
    private static int digits(byte[] text, int start, int count) {
        int value = 0;
        for (int i = start; i < start + count; i++) {
            int digit = text[i] - '0';
            if (digit < 0 || digit > 9) {
                return -1;
            }
            value = value * 10 + digit;
        }
        return value;
    }
}
