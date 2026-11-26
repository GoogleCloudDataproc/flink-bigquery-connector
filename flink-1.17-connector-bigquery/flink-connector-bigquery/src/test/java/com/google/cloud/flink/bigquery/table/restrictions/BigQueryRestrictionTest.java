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
                "(`timestamp_field` = '2025-01-01T00:00:00.000000')");
    }

    @Test
    public void testLocalDateTimeWithTrailingZerosInMilliseconds() {
        assertTemporalConversion(
                LocalDateTime.of(2025, 1, 1, 15, 30, 0, 0),
                DataTypes.TIMESTAMP(6).notNull(),
                "timestamp_field",
                "(`timestamp_field` = '2025-01-01T15:30:00.000000')");
    }

    @Test
    public void testInstantWithTrailingZerosInSeconds() {
        assertTemporalConversion(
                Instant.ofEpochMilli(0),
                DataTypes.TIMESTAMP_LTZ(6).notNull(),
                "instant_field",
                "(`instant_field` = '1970-01-01T00:00:00.000000Z')");
    }

    @Test
    public void testInstantWithTrailingZerosInMilliseconds() {
        assertTemporalConversion(
                Instant.parse("2025-01-01T12:30:45.100Z"),
                DataTypes.TIMESTAMP_LTZ(6).notNull(),
                "instant_field",
                "(`instant_field` = '2025-01-01T12:30:45.100000Z')");
    }

    @Test
    public void testLocalTimeWithTrailingZerosInSeconds() {
        assertTemporalConversion(
                LocalTime.of(0, 0, 0, 0),
                DataTypes.TIME(6).notNull(),
                "time_field",
                "(`time_field` = '00:00:00.000000')");
    }

    @Test
    public void testLocalTimeWithTrailingZerosInMilliseconds() {
        assertTemporalConversion(
                LocalTime.of(15, 30, 0, 0),
                DataTypes.TIME(6).notNull(),
                "time_field",
                "(`time_field` = '15:30:00.000000')");
    }

    @Test
    public void testLocalTimeWithPartialMilliseconds() {
        assertTemporalConversion(
                LocalTime.of(10, 20, 30, 500_000_000),
                DataTypes.TIME(6).notNull(),
                "time_field",
                "(`time_field` = '10:20:30.500000')");
    }

    @Test
    public void testLocalDateFormatting() {
        assertTemporalConversion(
                LocalDate.of(2025, 1, 1),
                DataTypes.DATE().notNull(),
                "date_field",
                "(`date_field` = '2025-01-01')");
    }

    @Test
    public void testLikePrefixPatternIsPushedDown() {
        // A pure "starts with" pattern must be pushed down, keeping the trailing '%' so the
        // BigQuery restriction stays equivalent to the original prefix match.
        assertLikeConversion("name", "foo%", Optional.of("`name` LIKE 'foo%'"));
    }

    @Test
    public void testLikePrefixIsEscaped() {
        assertLikeConversion("name", "it's%", Optional.of("`name` LIKE 'it\\'s%'"));
        // a backslash is escaped for LIKE first, then for the string literal
        assertLikeConversion("name", "a\\b%", Optional.of("`name` LIKE 'a\\\\\\\\b%'"));
    }

    @Test
    public void testLikeBackslashPrefixIsLiteral() {
        assertLikeConversion("name", "a\\%", Optional.of("`name` LIKE 'a\\\\\\\\%'"));
    }

    @Test
    public void testLikeWithExplicitEscapeIsNotPushedDown() {
        assertThat(
                        BigQueryRestriction.convert(
                                createCallExpression(
                                        "like",
                                        BuiltInFunctionDefinitions.LIKE,
                                        DataTypes.BOOLEAN(),
                                        createField("name", DataTypes.STRING()),
                                        createLiteral("a\\%", DataTypes.STRING().notNull()),
                                        createLiteral("\\", DataTypes.STRING().notNull()))))
                .isEmpty();
    }

    @Test
    public void testQuotedColumnNames() {
        for (String name : Arrays.asList("set", "hash", "struct", "a.b", "a b", "a`b", "a\\b")) {
            String quoted = "`" + name.replace("\\", "\\\\").replace("`", "\\`") + "`";
            assertTemporalConversion(
                    LocalDate.of(2025, 1, 1),
                    DataTypes.DATE().notNull(),
                    name,
                    "(" + quoted + " = '2025-01-01')");
        }
    }

    @Test
    public void testLikePrefixPatternWithSpacesIsPushedDown() {
        assertLikeConversion("name", "foo bar%", Optional.of("`name` LIKE 'foo bar%'"));
    }

    @Test
    public void testLikeWithoutWildcardIsNotPushedDown() {
        // No trailing '%' means it is not a prefix match, so nothing is pushed down and Flink
        // applies the filter itself.
        assertLikeConversion("name", "foo", Optional.empty());
    }

    @Test
    public void testLikeWithInfixWildcardIsNotPushedDown() {
        // "foo%bar%" is "starts with foo AND contains bar", not a pure prefix match.
        assertLikeConversion("name", "foo%bar%", Optional.empty());
    }

    @Test
    public void testLikeWithSuffixWildcardIsNotPushedDown() {
        // "%foo" is an "ends with" pattern, not a prefix match.
        assertLikeConversion("name", "%foo", Optional.empty());
    }

    @Test
    public void testLikeWithLeadingAndTrailingWildcardIsNotPushedDown() {
        // "%foo%" is a "contains" pattern, not a prefix match.
        assertLikeConversion("name", "%foo%", Optional.empty());
    }

    @Test
    public void testLikeMatchAllIsNotPushedDown() {
        // "%" matches everything and has no usable prefix.
        assertLikeConversion("name", "%", Optional.empty());
    }

    @Test
    public void testLikeWithUnderscoreWildcardIsNotPushedDown() {
        // '_' is the single-character wildcard of SQL LIKE, which BigQuery's prefix restriction
        // cannot represent, so we must not push it down.
        assertLikeConversion("name", "fo_o%", Optional.empty());
    }

    @Test
    public void testLikeWithTrailingUnderscoreIsNotPushedDown() {
        assertLikeConversion("name", "foo_", Optional.empty());
    }

    private CallExpression createLikeCallExpression(
            ResolvedExpression left, ResolvedExpression right) {
        return new CallExpression(
                false,
                FunctionIdentifier.of(BuiltInFunctionDefinitions.LIKE.getName()),
                BuiltInFunctionDefinitions.LIKE,
                Arrays.asList(left, right),
                DataTypes.BOOLEAN());
    }

    private void assertLikeConversion(String fieldName, String pattern, Optional<String> expected) {
        DataType stringType = DataTypes.STRING().notNull();
        FieldReferenceExpression field = createField(fieldName, stringType);
        ValueLiteralExpression literal = createLiteral(pattern, stringType);
        CallExpression callExpression = createLikeCallExpression(field, literal);

        Optional<String> result = BigQueryRestriction.convert(callExpression);

        if (expected.isPresent()) {
            assertThat(result).hasValue(expected.get());
        } else {
            assertThat(result).isEmpty();
        }
    }

    @Test
    public void testUnsupportedTopLevelFunctionReturnsEmpty() {
        FieldReferenceExpression field = createField("flag", DataTypes.BOOLEAN());
        ValueLiteralExpression fallback = createLiteral(false, DataTypes.BOOLEAN().notNull());

        Optional<String> result =
                BigQueryRestriction.convert(
                        createCallExpression(
                                "coalesce",
                                BuiltInFunctionDefinitions.COALESCE,
                                DataTypes.BOOLEAN(),
                                field,
                                fallback));

        assertThat(result).isEmpty();
    }

    @Test
    public void testNotOfUnsupportedFunctionReturnsEmpty() {
        FieldReferenceExpression field = createField("flag", DataTypes.BOOLEAN());
        ValueLiteralExpression fallback = createLiteral(false, DataTypes.BOOLEAN().notNull());
        CallExpression coalesceCall =
                createCallExpression(
                        "coalesce",
                        BuiltInFunctionDefinitions.COALESCE,
                        DataTypes.BOOLEAN(),
                        field,
                        fallback);

        Optional<String> result =
                BigQueryRestriction.convert(
                        createCallExpression(
                                "not",
                                BuiltInFunctionDefinitions.NOT,
                                DataTypes.BOOLEAN(),
                                coalesceCall));

        assertThat(result).isEmpty();
    }

    @Test
    public void testAndOfSupportedAndUnsupportedReturnsEmpty() {
        FieldReferenceExpression city = createField("city", DataTypes.STRING());
        ValueLiteralExpression toronto = createLiteral("Toronto", DataTypes.STRING().notNull());
        FieldReferenceExpression flag = createField("flag", DataTypes.BOOLEAN());
        ValueLiteralExpression fallback = createLiteral(false, DataTypes.BOOLEAN().notNull());

        CallExpression supportedSide = createEqualsCallExpression(city, toronto);
        CallExpression unsupportedSide =
                createCallExpression(
                        "coalesce",
                        BuiltInFunctionDefinitions.COALESCE,
                        DataTypes.BOOLEAN(),
                        flag,
                        fallback);

        Optional<String> result =
                BigQueryRestriction.convert(
                        createAndCallExpression(supportedSide, unsupportedSide));

        // The whole AND collapses to empty (mixed pushability is not partially expressible as a
        // single row restriction); applyFilters then leaves the AND as a remaining Flink filter.
        assertThat(result).isEmpty();
    }

    private FieldReferenceExpression createField(String name, DataType type) {
        return new FieldReferenceExpression(name, type, 0, 0);
    }

    private <T> ValueLiteralExpression createLiteral(T value, DataType type) {
        return new ValueLiteralExpression(value, type);
    }

    private CallExpression createEqualsCallExpression(
            ResolvedExpression left, ResolvedExpression right) {
        return createCallExpression(
                "equals", BuiltInFunctionDefinitions.EQUALS, DataTypes.BOOLEAN(), left, right);
    }

    private CallExpression createAndCallExpression(
            ResolvedExpression left, ResolvedExpression right) {
        return createCallExpression(
                "and", BuiltInFunctionDefinitions.AND, DataTypes.BOOLEAN(), left, right);
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
