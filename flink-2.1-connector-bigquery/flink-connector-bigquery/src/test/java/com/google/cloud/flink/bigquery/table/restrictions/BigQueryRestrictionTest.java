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
import org.apache.flink.table.expressions.NestedFieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
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

    @Test
    public void testNestedFieldEqualsLiteral() {
        NestedFieldReferenceExpression field =
                createNestedField(
                        new String[] {"address", "city"}, new int[] {1, 0}, DataTypes.STRING());
        ValueLiteralExpression literal =
                this.<String>createLiteral("Toronto", DataTypes.STRING().notNull());

        Optional<String> result =
                BigQueryRestriction.convert(createEqualsCallExpression(field, literal));

        assertThat(result).hasValue("(address.city = 'Toronto')");
    }

    @Test
    public void testNestedFieldEqualsLiteralWithLiteralOnLeft() {
        ValueLiteralExpression literal = createLiteral("Toronto", DataTypes.STRING().notNull());
        NestedFieldReferenceExpression field =
                createNestedField(
                        new String[] {"address", "city"}, new int[] {1, 0}, DataTypes.STRING());

        Optional<String> result =
                BigQueryRestriction.convert(createEqualsCallExpression(literal, field));

        assertThat(result).hasValue("('Toronto' = address.city)");
    }

    @Test
    public void testNestedFieldIsNull() {
        NestedFieldReferenceExpression field =
                createNestedField(
                        new String[] {"address", "city"}, new int[] {1, 0}, DataTypes.STRING());

        Optional<String> result =
                BigQueryRestriction.convert(
                        createCallExpression(
                                "isNull",
                                BuiltInFunctionDefinitions.IS_NULL,
                                DataTypes.BOOLEAN(),
                                field));

        assertThat(result).hasValue("address.city IS NULL");
    }

    @Test
    public void testNestedFieldIsNotNull() {
        NestedFieldReferenceExpression field =
                createNestedField(
                        new String[] {"address", "city"}, new int[] {1, 0}, DataTypes.STRING());

        Optional<String> result =
                BigQueryRestriction.convert(
                        createCallExpression(
                                "isNotNull",
                                BuiltInFunctionDefinitions.IS_NOT_NULL,
                                DataTypes.BOOLEAN(),
                                field));

        assertThat(result).hasValue("NOT address.city IS NULL");
    }

    @Test
    public void testNestedFieldGreaterThanLiteral() {
        NestedFieldReferenceExpression field =
                createNestedField(
                        new String[] {"address", "zip"}, new int[] {1, 1}, DataTypes.INT());
        ValueLiteralExpression literal = createLiteral(10000, DataTypes.INT().notNull());

        Optional<String> result =
                BigQueryRestriction.convert(createGreaterThanCallExpression(field, literal));

        assertThat(result).hasValue("(address.zip > 10000)");
    }

    @Test
    public void testNestedAnd() {
        NestedFieldReferenceExpression city =
                createNestedField(
                        new String[] {"address", "city"}, new int[] {1, 0}, DataTypes.STRING());
        NestedFieldReferenceExpression zip =
                createNestedField(
                        new String[] {"address", "zip"}, new int[] {1, 1}, DataTypes.INT());

        Optional<String> result =
                BigQueryRestriction.convert(
                        createAndCallExpression(
                                createEqualsCallExpression(
                                        city,
                                        createLiteral("Toronto", DataTypes.STRING().notNull())),
                                createGreaterThanCallExpression(
                                        zip, createLiteral(10000, DataTypes.INT().notNull()))));

        assertThat(result).hasValue("((address.city = 'Toronto') AND (address.zip > 10000))");
    }

    @Test
    public void testNestedOr() {
        NestedFieldReferenceExpression city =
                createNestedField(
                        new String[] {"address", "city"}, new int[] {1, 0}, DataTypes.STRING());
        NestedFieldReferenceExpression country =
                createNestedField(
                        new String[] {"address", "country"}, new int[] {1, 1}, DataTypes.STRING());

        Optional<String> result =
                BigQueryRestriction.convert(
                        createOrCallExpression(
                                createEqualsCallExpression(
                                        city,
                                        createLiteral("Toronto", DataTypes.STRING().notNull())),
                                createEqualsCallExpression(
                                        country,
                                        createLiteral("CA", DataTypes.STRING().notNull()))));

        assertThat(result).hasValue("((address.city = 'Toronto') OR (address.country = 'CA'))");
    }

    @Test
    public void testMultiLevelNestedFieldEqualsLiteral() {
        NestedFieldReferenceExpression field =
                createNestedField(
                        new String[] {"customer", "address", "city"},
                        new int[] {3, 1, 0},
                        DataTypes.STRING());
        ValueLiteralExpression literal = createLiteral("Toronto", DataTypes.STRING().notNull());

        Optional<String> result =
                BigQueryRestriction.convert(createEqualsCallExpression(field, literal));

        assertThat(result).hasValue("(customer.address.city = 'Toronto')");
    }

    @Test
    public void testNotNestedFieldEqualsLiteral() {
        NestedFieldReferenceExpression field =
                createNestedField(
                        new String[] {"address", "city"}, new int[] {1, 0}, DataTypes.STRING());
        ValueLiteralExpression literal = createLiteral("Toronto", DataTypes.STRING().notNull());
        CallExpression equalsCall = createEqualsCallExpression(field, literal);

        Optional<String> result =
                BigQueryRestriction.convert(
                        createCallExpression(
                                "not",
                                BuiltInFunctionDefinitions.NOT,
                                DataTypes.BOOLEAN(),
                                equalsCall));

        assertThat(result).hasValue("NOT (address.city = 'Toronto')");
    }

    @Test
    public void testTopLevelFieldEqualsLiteral() {
        FieldReferenceExpression field = createField("city", DataTypes.STRING());
        ValueLiteralExpression literal = createLiteral("Toronto", DataTypes.STRING().notNull());

        Optional<String> result =
                BigQueryRestriction.convert(createEqualsCallExpression(field, literal));

        assertThat(result).hasValue("(city = 'Toronto')");
    }

    @Test
    public void testTopLevelFieldIsNull() {
        FieldReferenceExpression field = createField("city", DataTypes.STRING());

        Optional<String> result =
                BigQueryRestriction.convert(
                        createCallExpression(
                                "isNull",
                                BuiltInFunctionDefinitions.IS_NULL,
                                DataTypes.BOOLEAN(),
                                field));

        assertThat(result).hasValue("city IS NULL");
    }

    private FieldReferenceExpression createField(String name, DataType type) {
        return new FieldReferenceExpression(name, type, 0, 0);
    }

    private NestedFieldReferenceExpression createNestedField(
            String[] fieldNames, int[] fieldIndices, DataType type) {
        return new NestedFieldReferenceExpression(fieldNames, fieldIndices, type);
    }

    private <T> ValueLiteralExpression createLiteral(T value, DataType type) {
        return new ValueLiteralExpression(value, type);
    }

    private CallExpression createEqualsCallExpression(
            ResolvedExpression left, ResolvedExpression right) {
        return createCallExpression(
                "equals", BuiltInFunctionDefinitions.EQUALS, DataTypes.BOOLEAN(), left, right);
    }

    private CallExpression createGreaterThanCallExpression(
            ResolvedExpression left, ResolvedExpression right) {
        return createCallExpression(
                "greaterThan",
                BuiltInFunctionDefinitions.GREATER_THAN,
                DataTypes.BOOLEAN(),
                left,
                right);
    }

    private CallExpression createLikeCallExpression(
            ResolvedExpression left, ResolvedExpression right) {
        return createCallExpression(
                "like", BuiltInFunctionDefinitions.LIKE, DataTypes.BOOLEAN(), left, right);
    }

    private CallExpression createAndCallExpression(
            ResolvedExpression left, ResolvedExpression right) {
        return createCallExpression(
                "and", BuiltInFunctionDefinitions.AND, DataTypes.BOOLEAN(), left, right);
    }

    private CallExpression createOrCallExpression(
            ResolvedExpression left, ResolvedExpression right) {
        return createCallExpression(
                "or", BuiltInFunctionDefinitions.OR, DataTypes.BOOLEAN(), left, right);
    }

    private CallExpression createCallExpression(
            String name,
            FunctionDefinition functionDefinition,
            DataType outputDataType,
            ResolvedExpression... children) {
        return new CallExpression(
                false,
                FunctionIdentifier.of(name),
                functionDefinition,
                Arrays.asList(children),
                outputDataType);
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
