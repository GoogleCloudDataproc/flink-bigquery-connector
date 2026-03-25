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

package com.google.cloud.flink.bigquery.sink;

import org.apache.flink.api.common.serialization.BulkWriter;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;

import java.util.Objects;

/**
 * Configuration for BigQuery indirect writes (GCS staging + load jobs).
 *
 * <p>Uses static inner builder to initialize new instances.
 *
 * @param <IN> Type of input to sink.
 */
public class BigQueryIndirectSinkConfig<IN> {

    private final BigQueryConnectOptions connectOptions;
    private final String gcsTempPath;
    private final BulkWriter.Factory<IN> bulkWriterFactory;

    public static <IN> Builder<IN> newBuilder() {
        return new Builder<>();
    }

    private BigQueryIndirectSinkConfig(
            BigQueryConnectOptions connectOptions,
            String gcsTempPath,
            BulkWriter.Factory<IN> bulkWriterFactory) {
        this.connectOptions = connectOptions;
        this.gcsTempPath = gcsTempPath;
        this.bulkWriterFactory = bulkWriterFactory;
    }

    public BigQueryConnectOptions getConnectOptions() {
        return connectOptions;
    }

    public String getGcsTempPath() {
        return gcsTempPath;
    }

    public BulkWriter.Factory<IN> getBulkWriterFactory() {
        return bulkWriterFactory;
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectOptions, gcsTempPath, bulkWriterFactory);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        BigQueryIndirectSinkConfig<?> other = (BigQueryIndirectSinkConfig<?>) obj;
        return Objects.equals(connectOptions, other.connectOptions)
                && Objects.equals(gcsTempPath, other.gcsTempPath)
                && Objects.equals(bulkWriterFactory, other.bulkWriterFactory);
    }

    /** Builder for BigQueryIndirectSinkConfig. */
    public static class Builder<IN> {

        private BigQueryConnectOptions connectOptions;
        private String gcsTempPath;
        private BulkWriter.Factory<IN> bulkWriterFactory;

        public Builder<IN> connectOptions(BigQueryConnectOptions connectOptions) {
            this.connectOptions = connectOptions;
            return this;
        }

        public Builder<IN> gcsTempPath(String gcsTempPath) {
            this.gcsTempPath = gcsTempPath;
            return this;
        }

        public Builder<IN> bulkWriterFactory(BulkWriter.Factory<IN> bulkWriterFactory) {
            this.bulkWriterFactory = bulkWriterFactory;
            return this;
        }

        public BigQueryIndirectSinkConfig<IN> build() {
            if (gcsTempPath == null || gcsTempPath.isEmpty()) {
                throw new IllegalArgumentException(
                        "gcsTempPath is required for INDIRECT write mode");
            }
            if (!gcsTempPath.startsWith("gs://")) {
                throw new IllegalArgumentException(
                        "gcsTempPath must start with gs:// for INDIRECT write mode");
            }
            if (bulkWriterFactory == null) {
                throw new IllegalArgumentException(
                        "bulkWriterFactory is required for INDIRECT write mode");
            }
            return new BigQueryIndirectSinkConfig<>(connectOptions, gcsTempPath, bulkWriterFactory);
        }
    }
}
