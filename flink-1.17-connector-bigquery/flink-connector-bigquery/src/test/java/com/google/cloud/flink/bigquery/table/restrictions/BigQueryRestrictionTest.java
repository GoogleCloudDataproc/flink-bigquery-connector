/*
 * Copyright (C) 2023 Google Inc.
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

package com.google.cloud.flink.bigquery.table.restrictions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.types.DataType;

import org.junit.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Optional;

import static com.google.common.truth.Truth8.assertThat;

/** Tests for the {@link BigQueryRestriction} utility class. */
public class BigQueryRestrictionTest {
    @Test
    public void testLocalDateTimeWithTrailingZerosInSeconds() {
        assertTemporalConversion(
                LocalDateTime.of(2025, 1, 1, 0, 0, 0, 0),
                DataTypes.TIMESTAMP(6).notNull(),
                "timestamp_field",
                "(timestamp_field = '2025-01-01T00:00:00.000000')");
    }

    @Test
    public void testLocalDateTimeWithTrailingZerosInMilliseconds() {
        assertTemporalConversion(
                LocalDateTime.of(2025, 1, 1, 15, 30, 0, 0),
                DataTypes.TIMESTAMP(6).notNull(),
                "timestamp_field",
                "(timestamp_field = '2025-01-01T15:30:00.000000')");
    }

    @Test
    public void testInstantWithTrailingZerosInSeconds() {
        assertTemporalConversion(
                Instant.ofEpochMilli(0),
                DataTypes.TIMESTAMP_LTZ(6).notNull(),
                "instant_field",
                "(instant_field = '1970-01-01T00:00:00.000000Z')");
    }

    @Test
    public void testInstantWithTrailingZerosInMilliseconds() {
        assertTemporalConversion(
                Instant.parse("2025-01-01T12:30:45.100Z"),
                DataTypes.TIMESTAMP_LTZ(6).notNull(),
                "instant_field",
                "(instant_field = '2025-01-01T12:30:45.100000Z')");
    }

    @Test
    public void testLocalTimeWithTrailingZerosInSeconds() {
        assertTemporalConversion(
                LocalTime.of(0, 0, 0, 0),
                DataTypes.TIME(6).notNull(),
                "time_field",
                "(time_field = '00:00:00.000000')");
    }

    @Test
    public void testLocalTimeWithTrailingZerosInMilliseconds() {
        assertTemporalConversion(
                LocalTime.of(15, 30, 0, 0),
                DataTypes.TIME(6).notNull(),
                "time_field",
                "(time_field = '15:30:00.000000')");
    }

    @Test
    public void testLocalTimeWithPartialMilliseconds() {
        assertTemporalConversion(
                LocalTime.of(10, 20, 30, 500_000_000),
                DataTypes.TIME(6).notNull(),
                "time_field",
                "(time_field = '10:20:30.500000')");
    }

    @Test
    public void testLocalDateFormatting() {
        assertTemporalConversion(
                LocalDate.of(2025, 1, 1),
                DataTypes.DATE().notNull(),
                "date_field",
                "(date_field = '2025-01-01')");
    }

    private FieldReferenceExpression createField(String name, DataType type) {
        return new FieldReferenceExpression(name, type, 0, 0);
    }

    private <T> ValueLiteralExpression createLiteral(T value, DataType type) {
        return new ValueLiteralExpression(value, type);
    }

    private CallExpression createEqualsCallExpression(
            ResolvedExpression left, ResolvedExpression right) {
        return new CallExpression(
                false,
                FunctionIdentifier.of("equals"),
                BuiltInFunctionDefinitions.EQUALS,
                Arrays.asList(left, right),
                DataTypes.BOOLEAN());
    }

    private void assertTemporalConversion(
            Object temporalValue, DataType dataType, String fieldName, String expected) {
        ValueLiteralExpression literal = createLiteral(temporalValue, dataType);
        FieldReferenceExpression field = createField(fieldName, dataType);
        CallExpression callExpression = createEqualsCallExpression(field, literal);

        Optional<String> result = BigQueryRestriction.convert(callExpression);

        assertThat(result).hasValue(expected);
    }
}
