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

package org.apache.flink.connector.bigquery.table.restrictions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class to transform a flink {@link ResolvedExpression} in a string restriction that can be
 * send to BigQuery as a row restriction. Heavily based in the Iceberg expression translation
 * implementation.
 */
@Internal
public class BigQueryRestriction {

    private BigQueryRestriction() {}

    private static final Pattern STARTS_WITH_PATTERN = Pattern.compile("([^%]+)%");

    /** Represents the possible BQ expressions supported for the correspondent flink ones. */
    public enum Operation {
        EQ,
        NOT_EQ,
        GT,
        GT_EQ,
        LT,
        LT_EQ,
        IS_NULL,
        NOT_NULL,
        AND,
        OR,
        NOT,
        STARTS_WITH
    }

    private static final Map<FunctionDefinition, Operation> FILTERS =
            ImmutableMap.<FunctionDefinition, Operation>builder()
                    .put(BuiltInFunctionDefinitions.EQUALS, Operation.EQ)
                    .put(BuiltInFunctionDefinitions.NOT_EQUALS, Operation.NOT_EQ)
                    .put(BuiltInFunctionDefinitions.GREATER_THAN, Operation.GT)
                    .put(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, Operation.GT_EQ)
                    .put(BuiltInFunctionDefinitions.LESS_THAN, Operation.LT)
                    .put(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, Operation.LT_EQ)
                    .put(BuiltInFunctionDefinitions.IS_NULL, Operation.IS_NULL)
                    .put(BuiltInFunctionDefinitions.IS_NOT_NULL, Operation.NOT_NULL)
                    .put(BuiltInFunctionDefinitions.AND, Operation.AND)
                    .put(BuiltInFunctionDefinitions.OR, Operation.OR)
                    .put(BuiltInFunctionDefinitions.NOT, Operation.NOT)
                    .put(BuiltInFunctionDefinitions.LIKE, Operation.STARTS_WITH)
                    .build();

    /**
     * Convert a flink expression into a BigQuery row restriction.
     *
     * <p>the BETWEEN, NOT_BETWEEN, IN expression will be converted by flink automatically. the
     * BETWEEN will be converted to (GT_EQ AND LT_EQ), the NOT_BETWEEN will be converted to (LT_EQ
     * OR GT_EQ), the IN will be converted to OR, so we do not add the conversion here
     *
     * @param flinkExpression the flink expression
     * @return An {@link Optional} potentially containing the resolved row restriction for BigQuery.
     */
    public static Optional<String> convert(Expression flinkExpression) {
        if (!(flinkExpression instanceof CallExpression)) {
            return Optional.empty();
        }

        CallExpression call = (CallExpression) flinkExpression;
        Operation op = FILTERS.get(call.getFunctionDefinition());
        if (op != null) {
            switch (op) {
                case IS_NULL:
                    return onlyChildAs(call, FieldReferenceExpression.class)
                            .map(FieldReferenceExpression::getName)
                            .map(field -> field + " IS NULL");

                case NOT_NULL:
                    return onlyChildAs(call, FieldReferenceExpression.class)
                            .map(FieldReferenceExpression::getName)
                            .map(field -> "NOT " + field + " IS NULL");

                case LT:
                    return convertFieldAndLiteral(
                            (left, right) -> "(" + left + " < " + right + ")",
                            (left, right) -> "(" + left + " > " + right + ")",
                            call);

                case LT_EQ:
                    return convertFieldAndLiteral(
                            (left, right) -> "(" + left + " <= " + right + ")",
                            (left, right) -> "(" + left + " >= " + right + ")",
                            call);

                case GT:
                    return convertFieldAndLiteral(
                            (left, right) -> "(" + left + " > " + right + ")",
                            (left, right) -> "(" + left + " < " + right + ")",
                            call);

                case GT_EQ:
                    return convertFieldAndLiteral(
                            (left, right) -> "(" + left + " >= " + right + ")",
                            (left, right) -> "(" + left + " <= " + right + ")",
                            call);

                case EQ:
                    return convertFieldAndLiteral((ref, lit) -> ref + " = " + lit, call);

                case NOT_EQ:
                    return convertFieldAndLiteral((ref, lit) -> ref + " <> " + lit, call);

                case NOT:
                    return onlyChildAs(call, CallExpression.class)
                            .flatMap(BigQueryRestriction::convert)
                            .map(field -> "NOT " + field);

                case AND:
                    return convertLogicExpression(
                            (left, right) -> "(" + left + " AND " + right + ")", call);

                case OR:
                    return convertLogicExpression(
                            (left, right) -> "(" + left + " OR " + right + ")", call);

                case STARTS_WITH:
                    return convertLike(call);
            }
        }

        return Optional.empty();
    }

    private static <T extends ResolvedExpression> Optional<T> onlyChildAs(
            CallExpression call, Class<T> expectedChildClass) {
        List<ResolvedExpression> children = call.getResolvedChildren();
        if (children.size() != 1) {
            return Optional.empty();
        }

        ResolvedExpression child = children.get(0);
        if (!expectedChildClass.isInstance(child)) {
            return Optional.empty();
        }

        return Optional.of(expectedChildClass.cast(child));
    }

    private static Optional<String> convertLike(CallExpression call) {
        List<ResolvedExpression> args = call.getResolvedChildren();
        if (args.size() != 2) {
            return Optional.empty();
        }

        Expression left = args.get(0);
        Expression right = args.get(1);

        if (left instanceof FieldReferenceExpression && right instanceof ValueLiteralExpression) {
            String name = ((FieldReferenceExpression) left).getName();
            return convertLiteral((ValueLiteralExpression) right)
                    .flatMap(
                            lit -> {
                                if (lit instanceof String) {
                                    String pattern = (String) lit;
                                    Matcher matcher = STARTS_WITH_PATTERN.matcher(pattern);
                                    // exclude special char of LIKE
                                    // '_' is the wildcard of the SQL LIKE
                                    if (!pattern.contains("_") && matcher.matches()) {
                                        return Optional.of(
                                                name + " LIKE '" + matcher.group(1) + "'");
                                    }
                                }

                                return Optional.empty();
                            });
        }

        return Optional.empty();
    }

    private static Optional<String> convertLogicExpression(
            BiFunction<String, String, String> function, CallExpression call) {
        List<ResolvedExpression> args = call.getResolvedChildren();
        if (args == null || args.size() != 2) {
            return Optional.empty();
        }

        Optional<String> left = convert(args.get(0));
        Optional<String> right = convert(args.get(1));
        if (left.isPresent() && right.isPresent()) {
            return Optional.of(function.apply(left.get(), right.get()));
        }

        return Optional.empty();
    }

    private static String addSingleQuotes(String input) {
        return "'" + input + "'";
    }

    private static Optional<Object> convertLiteral(ValueLiteralExpression expression) {
        Optional<?> value =
                expression.getValueAs(
                        expression.getOutputDataType().getLogicalType().getDefaultConversion());
        return value.map(
                o -> {
                    if (o instanceof LocalDateTime) {
                        return addSingleQuotes(((LocalDateTime) o).toString());
                    } else if (o instanceof Instant) {
                        return addSingleQuotes(((Instant) o).toString());
                    } else if (o instanceof LocalTime) {
                        return addSingleQuotes(((LocalTime) o).toString());
                    } else if (o instanceof LocalDate) {
                        return addSingleQuotes(((LocalDate) o).toString());
                    } else if (o instanceof String) {
                        return addSingleQuotes((String) o);
                    }

                    return o;
                });
    }

    private static Optional<String> convertFieldAndLiteral(
            BiFunction<String, Object, String> expr, CallExpression call) {
        return convertFieldAndLiteral(expr, expr, call);
    }

    private static Optional<String> convertFieldAndLiteral(
            BiFunction<String, Object, String> convertLR,
            BiFunction<String, Object, String> convertRL,
            CallExpression call) {
        List<ResolvedExpression> args = call.getResolvedChildren();
        if (args.size() != 2) {
            return Optional.empty();
        }

        Expression left = args.get(0);
        Expression right = args.get(1);

        if (left instanceof FieldReferenceExpression && right instanceof ValueLiteralExpression) {
            String name = ((FieldReferenceExpression) left).getName();
            Optional<Object> lit = convertLiteral((ValueLiteralExpression) right);
            if (lit.isPresent()) {
                return Optional.of(convertLR.apply(name, lit.get()));
            }
        } else if (left instanceof ValueLiteralExpression
                && right instanceof FieldReferenceExpression) {
            Optional<Object> lit = convertLiteral((ValueLiteralExpression) left);
            String name = ((FieldReferenceExpression) right).getName();
            if (lit.isPresent()) {
                return Optional.of(convertRL.apply(name, lit.get()));
            }
        }

        return Optional.empty();
    }
}
