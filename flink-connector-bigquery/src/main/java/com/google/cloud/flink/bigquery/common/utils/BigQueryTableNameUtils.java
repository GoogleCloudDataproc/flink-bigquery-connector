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

package com.google.cloud.flink.bigquery.common.utils;

import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.flink.bigquery.common.exceptions.BigQueryConnectorException;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Collection of utility methods to simplify the parsing of a BigQuery. */
public class BigQueryTableNameUtils {
    /**
     * Project IDs must contain 6-63 lowercase letters, digits, or dashes. IDs must start with a
     * letter and may not end with a dash. This regex isn't exact - this allows for patterns that
     * would be rejected by the service, but this is sufficient for basic parsing of table
     * references.
     */
    private static final String PROJECT_ID_REGEXP = "[a-z][-a-z0-9:.]{4,61}[a-z0-9]";

    /** Regular expression that matches Dataset IDs. */
    private static final String DATASET_REGEXP = "[-\\w.]{1,1024}";

    /** Regular expression that matches Table IDs. */
    private static final String TABLE_REGEXP = "[-\\w$@]{1,1024}";

    /**
     * Matches table specifications in the form {@code "[project_id]:[dataset_id].[table_id]"} or
     * {@code "[dataset_id].[table_id]"}.
     */
    private static final String PROJECT_DATASET_TABLE_REGEXP =
            String.format(
                    "((?<PROJECT>%s)[:\\.])?(?<DATASET>%s)\\.(?<TABLE>%s)",
                    PROJECT_ID_REGEXP, DATASET_REGEXP, TABLE_REGEXP);

    static final Pattern TABLE_SPEC_PATTERN = Pattern.compile(PROJECT_DATASET_TABLE_REGEXP);

    /**
     * Matches table specifications in the form {@code
     * "projects/[project_id]/datasets/[dataset_id]/tables/[table_id]}".
     */
    private static final String TABLE_URN_REGEXP =
            String.format(
                    "projects/(?<PROJECT>%s)/datasets/(?<DATASET>%s)/tables/(?<TABLE>%s)",
                    PROJECT_ID_REGEXP, DATASET_REGEXP, TABLE_REGEXP);

    static final Pattern TABLE_URN_SPEC_PATTERN = Pattern.compile(TABLE_URN_REGEXP);

    static Optional<TableReference> maybeTableReferenceFromMatcher(Matcher matcher) {
        if (!matcher.matches()) {
            return Optional.empty();
        }

        return Optional.of(
                new TableReference()
                        .setProjectId(matcher.group("PROJECT"))
                        .setDatasetId(matcher.group("DATASET"))
                        .setTableId(matcher.group("TABLE")));
    }

    /**
     * Parses a BigQUery {@link TableReference} from the provided string specification. It supports
     * BigQuery table URN or simple project, dataset, table terms.
     *
     * @param bigQueryTableSpec The BigQuery table specification.
     * @return A BigQuery table reference.
     */
    public static TableReference parseTableReference(String bigQueryTableSpec) {
        return maybeTableReferenceFromMatcher(TABLE_URN_SPEC_PATTERN.matcher(bigQueryTableSpec))
                .orElseGet(
                        () ->
                                maybeTableReferenceFromMatcher(
                                                TABLE_SPEC_PATTERN.matcher(bigQueryTableSpec))
                                        .orElseThrow(
                                                () ->
                                                        new BigQueryConnectorException(
                                                                String.format(
                                                                        "The provided BigQuery table specification [%s] is not one of the expected formats ("
                                                                                + " projects/[project_id]/datasets/[dataset_id]/tables/[table_id],"
                                                                                + " [project_id]:[dataset_id].[table_id],"
                                                                                + " [project_id].[dataset_id].[table_id],"
                                                                                + " [dataset_id].[table_id]).",
                                                                        bigQueryTableSpec))));
    }
}
