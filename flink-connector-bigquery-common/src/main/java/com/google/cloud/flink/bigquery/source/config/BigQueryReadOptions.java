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

import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.serializable.SerializableAutoValue;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import org.threeten.bp.Instant;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** The options available to read data from BigQuery. */
@AutoValue
@SerializableAutoValue
@PublicEvolving
public abstract class BigQueryReadOptions implements Serializable {

    public abstract List<String> getColumnNames();

    public abstract String getRowRestriction();

    public abstract Optional<Long> getSnapshotTimestampInMillis();

    public abstract Optional<Integer> getLimit();

    public abstract Integer getMaxStreamCount();

    public abstract Integer getMaxRecordsPerSplitFetch();

    public abstract BigQueryConnectOptions getBigQueryConnectOptions();

    @Override
    public final int hashCode() {
        return Objects.hash(
                getColumnNames(),
                getRowRestriction(),
                getSnapshotTimestampInMillis(),
                getMaxStreamCount(),
                getBigQueryConnectOptions());
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
        return Objects.equals(this.getColumnNames(), other.getColumnNames())
                && Objects.equals(this.getRowRestriction(), other.getRowRestriction())
                && Objects.equals(
                        this.getSnapshotTimestampInMillis(), other.getSnapshotTimestampInMillis())
                && Objects.equals(this.getMaxStreamCount(), other.getMaxStreamCount())
                && Objects.equals(
                        this.getBigQueryConnectOptions(), other.getBigQueryConnectOptions());
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
                .setColumnNames(Arrays.asList())
                .setMaxStreamCount(0)
                .setMaxRecordsPerSplitFetch(10000)
                .setSnapshotTimestampInMillis(null);
    }

    /** Builder class for {@link BigQueryReadOptions}. */
    @AutoValue.Builder
    public abstract static class Builder {

        /**
         * Sets the max element count that should be read.
         *
         * @param limit The max element count returned by the source.
         * @return This {@link Builder} instance.
         */
        public abstract Builder setLimit(@Nullable Integer limit);

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
        public abstract Builder setSnapshotTimestampInMillis(@Nullable Long snapshotTs);

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
         * @param maxRecordsPerSplitFetch The maximum number records to read from a split at a time.
         * @return This {@link Builder} instance.
         */
        public abstract Builder setMaxRecordsPerSplitFetch(Integer maxRecordsPerSplitFetch);

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
                    !readOptions
                            .getSnapshotTimestampInMillis()
                            // see if the value is lower than the epoch
                            .filter(timeInMillis -> timeInMillis < Instant.EPOCH.toEpochMilli())
                            // if present, then fail
                            .isPresent(),
                    "The oldest timestamp should be equal or bigger than epoch.");

            return readOptions;
        }
    }
}
