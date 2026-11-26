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

import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.assertThrows;

/** Tests for {@link BigQueryDatetimeParser}. */
public class BigQueryDatetimeParserTest {

    @Test
    public void parsesTheCanonicalShapeDirectly() {
        assertThat(parseCanonical("2024-01-15T10:30:45"))
                .isEqualTo(LocalDateTime.of(2024, 1, 15, 10, 30, 45));
        assertThat(parseCanonical("2024-01-15 10:30:45.1"))
                .isEqualTo(LocalDateTime.of(2024, 1, 15, 10, 30, 45, 100_000_000));
        assertThat(parseCanonical("0001-01-01t00:00:00.000001"))
                .isEqualTo(LocalDateTime.of(1, 1, 1, 0, 0, 0, 1_000));
        assertThat(parseCanonical("9999-12-31T23:59:59.999999"))
                .isEqualTo(LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999_999_000));
        assertThat(parseCanonical("2024-02-29T00:00:00"))
                .isEqualTo(LocalDateTime.of(2024, 2, 29, 0, 0));
    }

    @Test
    public void leavesOtherTextToTheFormatter() {
        for (String text :
                Arrays.asList(
                        "2024-01-15",
                        "2024-1-15T10:30:45",
                        "2024-01-15T1:30:45",
                        "2024-01-15 T10:30:45",
                        "2024-01-15T10:30:45.",
                        "2024-01-15T10:30:45.1234567",
                        "2023-02-29T00:00:00",
                        "2024-04-31T00:00:00",
                        "2024-01-15T24:00:00",
                        "2024-01-15T10:60:00",
                        "2024-01-15T10:30:60",
                        "0000-01-01T00:00:00",
                        "2024-00-01T00:00:00",
                        "2024-13-01T00:00:00",
                        "2024-01-00T00:00:00",
                        "+024-01-15T10:30:45",
                        "2024-01-15T10:30:4é")) {
            assertWithMessage(text).that(parseCanonical(text)).isNull();
        }
    }

    @Test
    public void matchesTheFormatterForEdgeCases() {
        for (String text :
                Arrays.asList(
                        "2024-01-15T10:30:45",
                        "2024-01-15 10:30:45.123",
                        "2024-01-15t10:30:45.123456",
                        "2024-1-5 9:5:3",
                        "2024-01-15",
                        "2024-01-15 T10:30:45",
                        "2024-01-15T10:30:45.1234567",
                        "2023-02-29T00:00:00",
                        "2024-04-31T00:00:00",
                        "2024-01-15T24:00:00",
                        "2024-01-15T10:30:60",
                        "0000-01-01T00:00:00",
                        "2024-01-15T10:30:45Z",
                        "",
                        "2024-01-15T10:30:4é")) {
            assertMatchesFormatter(text);
        }
    }

    @Test
    public void matchesTheFormatterForMutatedCanonicalText() {
        Random random = new Random(20261126L);
        String alphabet = "0123456789-: Tt.Z+/é";
        for (int i = 0; i < 20_000; i++) {
            StringBuilder text =
                    new StringBuilder(
                            String.format(
                                    "%04d-%02d-%02dT%02d:%02d:%02d",
                                    random.nextInt(10_000),
                                    random.nextInt(14),
                                    random.nextInt(33),
                                    random.nextInt(26),
                                    random.nextInt(62),
                                    random.nextInt(62)));
            int fractionDigits = random.nextInt(9) - 1;
            if (fractionDigits >= 0) {
                text.append('.');
                for (int d = 0; d < fractionDigits; d++) {
                    text.append((char) ('0' + random.nextInt(10)));
                }
            }
            if (random.nextInt(4) == 0) {
                text.setCharAt(
                        random.nextInt(text.length()),
                        alphabet.charAt(random.nextInt(alphabet.length())));
            }
            assertMatchesFormatter(text.toString());
        }
    }

    @Test
    public void readsOnlyTheGivenLengthOfTheBytes() {
        byte[] bytes = "2024-01-15T10:30:45.123456 and more".getBytes(StandardCharsets.UTF_8);
        assertThat(BigQueryDatetimeParser.parse(bytes, 26))
                .isEqualTo(LocalDateTime.of(2024, 1, 15, 10, 30, 45, 123_456_000));
        assertThat(BigQueryDatetimeParser.parse(bytes, 19))
                .isEqualTo(LocalDateTime.of(2024, 1, 15, 10, 30, 45));
        // text the fast path does not handle goes to the formatter with only the given length
        byte[] nonCanonical = "2024-1-5 9:5:3 and more".getBytes(StandardCharsets.UTF_8);
        assertThat(BigQueryDatetimeParser.parse(nonCanonical, 14))
                .isEqualTo(LocalDateTime.of(2024, 1, 5, 9, 5, 3));
    }

    @Test
    public void formatsLikeTheOutputFormatter() {
        List<LocalDateTime> values =
                new ArrayList<>(
                        Arrays.asList(
                                LocalDateTime.of(2024, 3, 13, 0, 42),
                                LocalDateTime.of(1, 1, 1, 0, 0, 0, 1),
                                LocalDateTime.of(0, 1, 1, 0, 0),
                                LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999_999_999),
                                LocalDateTime.of(10_000, 1, 1, 0, 0),
                                LocalDateTime.of(-1, 6, 15, 12, 0, 0, 500_000_000)));
        Random random = new Random(20261126L);
        for (int i = 0; i < 20_000; i++) {
            values.add(
                    LocalDateTime.of(
                            random.nextInt(10_000),
                            1 + random.nextInt(12),
                            1 + random.nextInt(28),
                            random.nextInt(24),
                            random.nextInt(60),
                            random.nextInt(60),
                            random.nextInt(1_000_000_000)));
        }
        for (LocalDateTime value : values) {
            assertWithMessage(value.toString())
                    .that(BigQueryDatetimeParser.format(value))
                    .isEqualTo(value.format(DateTimeFormatterPatterns.DATETIME_OUTPUT_FORMATTER));
        }
    }

    @Test
    public void rejectsInvalidByteRangesBeforeParsing() {
        byte[] bytes = "2024-01-15T10:30:45".getBytes(StandardCharsets.UTF_8);
        for (int length : new int[] {-1, 20, 26, Integer.MAX_VALUE}) {
            IndexOutOfBoundsException error =
                    assertThrows(
                            IndexOutOfBoundsException.class,
                            () -> BigQueryDatetimeParser.parse(bytes, length));
            assertThat(error.getClass()).isEqualTo(IndexOutOfBoundsException.class);
            assertThat(error).hasMessageThat().contains("length");
        }
        assertThrows(
                NullPointerException.class, () -> BigQueryDatetimeParser.parse((byte[]) null, 0));
        assertThrows(NullPointerException.class, () -> BigQueryDatetimeParser.parse((String) null));
        assertThrows(
                DateTimeParseException.class, () -> BigQueryDatetimeParser.parse(new byte[0], 0));
    }

    @Test
    public void matchesTheFormatterForEveryMonthAndFractionLength() {
        for (int year : new int[] {1, 1900, 2000, 2023, 2024, 9999}) {
            for (int month = 1; month <= 12; month++) {
                for (int day : new int[] {28, 29, 30, 31}) {
                    for (String separator : Arrays.asList("T", "t", " ")) {
                        for (int digits = 0; digits <= 9; digits++) {
                            String text =
                                    String.format(
                                            "%04d-%02d-%02d%s23:59:59",
                                            year, month, day, separator);
                            if (digits > 0) {
                                text += "." + "123456789".substring(0, digits);
                            }
                            assertMatchesFormatter(text);
                        }
                    }
                }
            }
        }
        for (String text :
                Arrays.asList(
                        "２０２４-01-15T10:30:45",
                        "2024-01-15T10:30:4😀",
                        "2024-01-15T10:30:45.é",
                        "2024/01/15T10:30:45",
                        "2024-01-15X10:30:45")) {
            assertMatchesFormatter(text);
        }
    }

    private static LocalDateTime parseCanonical(String text) {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        return BigQueryDatetimeParser.parseCanonical(bytes, bytes.length);
    }

    private static void assertMatchesFormatter(String text) {
        Object expected = parseWithFormatter(text);
        List<Object> actual = new ArrayList<>();
        actual.add(outcome(() -> BigQueryDatetimeParser.parse(text)));
        byte[] utf8 = text.getBytes(StandardCharsets.UTF_8);
        actual.add(outcome(() -> BigQueryDatetimeParser.parse(utf8, utf8.length)));
        for (Object result : actual) {
            assertWithMessage(text).that(result).isEqualTo(expected);
        }
    }

    private static Object parseWithFormatter(String text) {
        return outcome(
                () -> LocalDateTime.parse(text, DateTimeFormatterPatterns.DATETIME_FORMATTER));
    }

    /** The parsed value, or the exception class, so values and failures compare alike. */
    private static Object outcome(java.util.function.Supplier<LocalDateTime> parse) {
        try {
            return parse.get();
        } catch (DateTimeParseException e) {
            return DateTimeParseException.class;
        }
    }
}
