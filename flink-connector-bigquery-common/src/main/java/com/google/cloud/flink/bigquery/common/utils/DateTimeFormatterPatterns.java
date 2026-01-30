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

import java.time.format.DateTimeFormatter;

/**
 * Static {@link DateTimeFormatter} instances for BigQuery date/time parsing.
 *
 * <p>DateTimeFormatter.ofPattern() compiles the pattern on each call, so these are cached as static
 * constants to avoid repeated compilation in hot paths.
 */
public final class DateTimeFormatterPatterns {

    private DateTimeFormatterPatterns() {}

    /**
     * Formatter for BigQuery DATETIME/TIMESTAMP strings.
     *
     * <p>Supports formats like:
     *
     * <ul>
     *   <li>2024-01-15
     *   <li>2024-1-5
     *   <li>2024-01-15 10:30:45
     *   <li>2024-01-15T10:30:45
     *   <li>2024-01-15T10:30:45.123456
     * </ul>
     */
    public static final DateTimeFormatter DATETIME_FORMATTER =
            DateTimeFormatter.ofPattern(
                    "yyyy-M[M]-d[d][[' ']['T']['t']H[H]':'m[m]':'s[s]['.'SSSSSS]['.'SSSSS]['.'SSSS]['.'SSS]['.'SS]['.'S]]");

    /**
     * Formatter for BigQuery TIME strings.
     *
     * <p>Supports formats like:
     *
     * <ul>
     *   <li>10:30:45
     *   <li>9:5:3
     *   <li>10:30:45.123456
     * </ul>
     */
    public static final DateTimeFormatter TIME_FORMATTER =
            DateTimeFormatter.ofPattern(
                    "H[H]':'m[m]':'s[s]['.'SSSSSS]['.'SSSSS]['.'SSSS]['.'SSS]['.'SS]['.'S]");
}
