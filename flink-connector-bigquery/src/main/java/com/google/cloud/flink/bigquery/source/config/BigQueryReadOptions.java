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

package com.google.cloud.flink.bigquery.source.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import com.google.auto.value.AutoValue;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import org.threeten.bp.Instant;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** The options available to read data from BigQuery. */
@AutoValue
@PublicEvolving
public abstract class BigQueryReadOptions implements Serializable {

    public abstract ImmutableList<String> getColumnNames();

    public abstract String getRowRestriction();

    @Nullable
    public abstract Long getSnapshotTimestampInMillis();

    @Nullable
    public abstract String getQuery();

    @Nullable
    public abstract String getQueryExecutionProject();

    public abstract Integer getMaxStreamCount();

    public abstract Integer getMaxRecordsPerSplitFetch();

    public abstract BigQueryConnectOptions getBigQueryConnectOptions();

    @Override
    public final int hashCode() {
        int hash = 5;
        hash = 61 * hash + Objects.hashCode(getColumnNames());
        hash = 61 * hash + Objects.hashCode(getRowRestriction());
        hash = 61 * hash + Objects.hashCode(getSnapshotTimestampInMillis());
        hash = 61 * hash + Objects.hashCode(getMaxStreamCount());
        hash = 61 * hash + Objects.hashCode(getBigQueryConnectOptions());
        return hash;
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final BigQueryReadOptions other = (BigQueryReadOptions) obj;
        if (!Objects.equals(this.getColumnNames(), other.getColumnNames())) {
            return false;
        }
        if (!Objects.equals(this.getRowRestriction(), other.getRowRestriction())) {
            return false;
        }
        if (!Objects.equals(
                this.getSnapshotTimestampInMillis(), other.getSnapshotTimestampInMillis())) {
            return false;
        }
        if (!Objects.equals(this.getMaxStreamCount(), other.getMaxStreamCount())) {
            return false;
        }
        return Objects.equals(this.getBigQueryConnectOptions(), other.getBigQueryConnectOptions());
    }

    /**
     * Transforms the instance into a builder instance for property modification.
     *
     * @return A {@link Builder} instance for the type.
     */
    public abstract Builder toBuilder();

    /**
     * Creates a builder instance with all the default values set.
     *
     * @return A {@link Builder} for the type.
     */
    public static Builder builder() {
        return new AutoValue_BigQueryReadOptions.Builder()
                .setRowRestriction("")
                .setColumnNames(new ArrayList<>())
                .setMaxStreamCount(0)
                .setMaxRecordsPerSplitFetch(10000)
                .setSnapshotTimestampInMillis(null);
    }

    /** Builder class for {@link BigQueryReadOptions}. */
    @AutoValue.Builder
    public abstract static class Builder {

        /**
         * Prepares this builder to execute a query driven read using the default credentials
         * configuration.
         *
         * @param query A BigQuery standard SQL query.
         * @param projectId A GCP project where the query will run.
         * @return This {@link Builder} instance.
         * @throws IOException
         */
        public Builder setQueryAndExecutionProject(String query, String projectId)
                throws IOException {
            return setQueryWithExecutionProjectAndCredentialsOptions(
                    query, projectId, CredentialsOptions.builder().build());
        }

        /**
         * Prepares this builder to execute a query driven read.
         *
         * @param query A BigQuery standard SQL query.
         * @param projectId A GCP project where the query will run.
         * @param credentialsOptions The GCP credentials options.
         * @return This {@link Builder} instance.
         * @throws IOException
         */
        public Builder setQueryWithExecutionProjectAndCredentialsOptions(
                String query, String projectId, CredentialsOptions credentialsOptions)
                throws IOException {
            this.setQuery(query);
            this.setQueryExecutionProject(projectId);
            this.setBigQueryConnectOptions(
                    BigQueryConnectOptions.builderForQuerySource()
                            .setCredentialsOptions(credentialsOptions)
                            .build());
            return this;
        }

        /**
         * Sets a BigQuery query which will be run first, storing its result in a temporary table,
         * and Flink will read the query results from that temporary table. This is an optional
         * argument.
         *
         * @param query A BigQuery standard SQL query.
         * @return This {@link Builder} instance.
         */
        public abstract Builder setQuery(String query);

        /**
         * Sets the GCP project where the configured query will be run. In case the query
         * configuration is not set this configuration is discarded.
         *
         * @param projectId A GCP project.
         * @return This {@link Builder} instance.
         */
        public abstract Builder setQueryExecutionProject(String projectId);

        /**
         * Sets the restriction the rows in the BigQuery table must comply to be returned by the
         * source.
         *
         * @param rowRestriction A {@link String} containing the row restrictions.
         * @return This {@link Builder} instance.
         */
        public abstract Builder setRowRestriction(String rowRestriction);

        /**
         * Sets the column names that will be projected from the table's retrieved data.
         *
         * @param colNames The names of the columns as a list of strings.
         * @return This {@link Builder} instance.
         */
        public abstract Builder setColumnNames(List<String> colNames);

        /**
         * Sets the snapshot time (in milliseconds since epoch) for the BigQuery table, if not set
         * {@code now()} is used.
         *
         * @param snapshotTs The snapshot's time in milliseconds since epoch.
         * @return This {@link Builder} instance.
         */
        public abstract Builder setSnapshotTimestampInMillis(Long snapshotTs);

        /**
         * Sets the maximum number of read streams that BigQuery should create to retrieve data from
         * the table. BigQuery can return a lower number than the specified.
         *
         * @param maxStreamCount The maximum number of read streams.
         * @return This {@link Builder} instance.
         */
        public abstract Builder setMaxStreamCount(Integer maxStreamCount);

        /**
         * Sets the maximum number of records to read from a streams once a fetch has been requested
         * from a particular split. Configuring this number too high may cause memory pressure in
         * the task manager, depending on the BigQuery record's size and total rows on the stream.
         *
         * @param maxStreamCount The maximum number records to read from a split at a time.
         * @return This {@link Builder} instance.
         */
        public abstract Builder setMaxRecordsPerSplitFetch(Integer maxStreamCount);

        /**
         * Sets the {@link BigQueryConnectOptions} instance.
         *
         * @param connect The {@link BigQueryConnectOptions} instance.
         * @return This {@link Builder} instance.
         */
        public abstract Builder setBigQueryConnectOptions(BigQueryConnectOptions connect);

        abstract BigQueryReadOptions autoBuild();

        /**
         * A fully initialized {@link BigQueryReadOptions} instance.
         *
         * @return A {@link BigQueryReadOptions} instance.
         */
        public final BigQueryReadOptions build() {
            BigQueryReadOptions readOptions = autoBuild();
            Preconditions.checkState(
                    readOptions.getMaxStreamCount() >= 0,
                    "The max number of streams should be zero or positive.");
            Preconditions.checkState(
                    !Optional.ofNullable(readOptions.getSnapshotTimestampInMillis())
                            // see if the value is lower than the epoch
                            .filter(timeInMillis -> timeInMillis < Instant.EPOCH.toEpochMilli())
                            // if present, then fail
                            .isPresent(),
                    "The oldest timestamp should be equal or bigger than epoch.");
            Preconditions.checkState(
                    !Optional.ofNullable(readOptions.getQuery())
                            // if the project was not configured
                            .filter(q -> readOptions.getQueryExecutionProject() == null)
                            // if present fail
                            .isPresent(),
                    "If a query is configured, then a GCP projec should be provided.");

            return readOptions;
        }
    }
}
