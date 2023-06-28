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

package com.google.cloud.flink.bigquery.common.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.function.SerializableSupplier;

import com.google.auto.value.AutoValue;
import com.google.cloud.flink.bigquery.services.BigQueryServices;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

/** BigQuery client connection configuration. */
@AutoValue
@PublicEvolving
public abstract class BigQueryConnectOptions implements Serializable {

    public abstract String getProjectId();

    public abstract String getDataset();

    public abstract String getTable();

    @Nullable
    public abstract CredentialsOptions getCredentialsOptions();

    @Nullable
    public abstract SerializableSupplier<BigQueryServices> getTestingBigQueryServices();

    @Override
    public String toString() {
        return String.format(
                "[project : %s, dataset : %s, table : %s]",
                getProjectId(), getDataset(), getTable());
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 61 * hash + Objects.hashCode(getProjectId());
        hash = 61 * hash + Objects.hashCode(getDataset());
        hash = 61 * hash + Objects.hashCode(getTable());
        hash = 61 * hash + Objects.hashCode(getCredentialsOptions());
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final BigQueryConnectOptions other = (BigQueryConnectOptions) obj;
        if (!Objects.equals(this.getProjectId(), other.getProjectId())) {
            return false;
        }
        if (!Objects.equals(this.getDataset(), other.getDataset())) {
            return false;
        }
        if (!Objects.equals(this.getTable(), other.getTable())) {
            return false;
        }
        return Objects.equals(this.getCredentialsOptions(), other.getCredentialsOptions());
    }

    /**
     * Creates a builder for the instance.
     *
     * @return A Builder instance.
     * @throws java.io.IOException
     */
    public static Builder builder() throws IOException {
        return new AutoValue_BigQueryConnectOptions.Builder()
                .setCredentialsOptions(CredentialsOptions.builder().build());
    }

    public static Builder builderForQuerySource() throws IOException {
        return new AutoValue_BigQueryConnectOptions.Builder()
                .setCredentialsOptions(CredentialsOptions.builder().build())
                .setProjectId("")
                .setDataset("")
                .setTable("");
    }

    /**
     * Transforms the instance into a builder instance for property modification.
     *
     * @return A {@link Builder} instance for the type.
     */
    public abstract Builder toBuilder();

    /** Builder class for BigQueryConnectOptions. */
    @AutoValue.Builder
    public abstract static class Builder {
        /**
         * Sets the project for the BigQuery resource.
         *
         * @param projectId A GCP project name
         * @return A BigQueryConnectOptions builder instance
         */
        public abstract Builder setProjectId(String projectId);

        /**
         * Sets the dataset for the BigQuery resource.
         *
         * @param dataset A BigQuery dataset name
         * @return A BigQueryConnectOptions builder instance
         */
        public abstract Builder setDataset(String dataset);

        /**
         * Sets the BigQuery table resource name.
         *
         * @param table A BigQuery table name
         * @return A BigQueryConnectOptions builder instance
         */
        public abstract Builder setTable(String table);

        /**
         * Sets the GCP credentials options.
         *
         * @param options A credentials option instance.
         * @return A BigQueryConnectOptions builder instance
         */
        public abstract Builder setCredentialsOptions(CredentialsOptions options);

        /**
         * Sets a testing implementation for the BigQuery services, needs to be supplied in runtime
         * to avoid serialization problems.
         *
         * @param bqServices An test instance of the {@link BigQueryServices} class.
         * @return A BigQueryConnectOptions builder instance
         */
        public abstract Builder setTestingBigQueryServices(
                SerializableSupplier<BigQueryServices> bqServices);

        /**
         * Creates the BigQueryConnectOptions object.
         *
         * @return the options instance.
         */
        public abstract BigQueryConnectOptions build();
    }
}
