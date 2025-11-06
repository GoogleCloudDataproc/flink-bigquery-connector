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

package com.google.cloud.flink.bigquery.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.auto.value.AutoValue;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.utils.SchemaTransform;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.source.emitter.BigQueryRecordEmitter;
import com.google.cloud.flink.bigquery.source.enumerator.BigQuerySourceEnumState;
import com.google.cloud.flink.bigquery.source.enumerator.BigQuerySourceEnumStateSerializer;
import com.google.cloud.flink.bigquery.source.enumerator.BigQuerySourceEnumerator;
import com.google.cloud.flink.bigquery.source.reader.BigQuerySourceReader;
import com.google.cloud.flink.bigquery.source.reader.BigQuerySourceReaderContext;
import com.google.cloud.flink.bigquery.source.reader.deserializer.AvroDeserializationSchema;
import com.google.cloud.flink.bigquery.source.reader.deserializer.BigQueryDeserializationSchema;
import com.google.cloud.flink.bigquery.source.split.BigQuerySourceSplit;
import com.google.cloud.flink.bigquery.source.split.BigQuerySourceSplitSerializer;
import com.google.cloud.flink.bigquery.source.split.reader.BigQuerySourceSplitReader;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * The DataStream {@link Source} implementation for Google BigQuery. It reads data directly from a
 * BigQuery table. Only bounded source is supported right now.
 *
 * <p>Review the option classes and their builders for more details on the configurable options.
 *
 * <p>The following example demonstrates how to create a bounded source to read {@link
 * GenericRecord} data from a BigQuery table.
 *
 * <pre>{@code
 * BigQuerySource<GenericRecord> source =
 *       BigQuerySource.readAvros(
 *           BigQueryReadOptions.builder()
 *             .setBigQueryConnectOptions(
 *               BigQueryConnectOptions.builder()
 *                 .setProjectId("some-gcp-project")
 *                 .setDataset("some-bq-dataset")
 *                 .setTable("some-bq-table")
 *                 .build())
 *             .build());
 * }</pre>
 *
 * @param <OUT> The type of the data returned by this source implementation.
 */
@AutoValue
@PublicEvolving
public abstract class BigQuerySource<OUT>
        implements Source<OUT, BigQuerySourceSplit, BigQuerySourceEnumState>,
                ResultTypeQueryable<OUT> {
    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySource.class);

    public abstract BigQueryDeserializationSchema<GenericRecord, OUT> getDeserializationSchema();

    public abstract BigQueryReadOptions getReadOptions();

    public abstract Boundedness getSourceBoundedness();

    @Override
    public Boundedness getBoundedness() {
        return getSourceBoundedness();
    }

    @Override
    public SimpleVersionedSerializer<BigQuerySourceSplit> getSplitSerializer() {
        return BigQuerySourceSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<BigQuerySourceEnumState> getEnumeratorCheckpointSerializer() {
        return BigQuerySourceEnumStateSerializer.INSTANCE;
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return getDeserializationSchema().getProducedType();
    }

    @Override
    public SourceReader<OUT, BigQuerySourceSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        BigQuerySourceReaderContext bqReaderContext =
                new BigQuerySourceReaderContext(
                        readerContext, getReadOptions().getLimit().orElse(-1));

        Supplier<SplitReader<GenericRecord, BigQuerySourceSplit>> splitReaderSupplier =
                () -> new BigQuerySourceSplitReader(getReadOptions(), bqReaderContext);

        return new BigQuerySourceReader<>(
                splitReaderSupplier,
                new BigQueryRecordEmitter<>(getDeserializationSchema()),
                bqReaderContext);
    }

    @Override
    public SplitEnumerator<BigQuerySourceSplit, BigQuerySourceEnumState> createEnumerator(
            SplitEnumeratorContext<BigQuerySourceSplit> enumContext) throws Exception {
        return new BigQuerySourceEnumerator(
                getBoundedness(),
                enumContext,
                getReadOptions(),
                BigQuerySourceEnumState.initialState());
    }

    @Override
    public SplitEnumerator<BigQuerySourceSplit, BigQuerySourceEnumState> restoreEnumerator(
            SplitEnumeratorContext<BigQuerySourceSplit> enumContext,
            BigQuerySourceEnumState checkpoint)
            throws Exception {
        LOG.debug("Restoring enumerator with state {}", checkpoint);
        return new BigQuerySourceEnumerator(
                getBoundedness(), enumContext, getReadOptions(), checkpoint);
    }

    /**
     * Transforms the instance into a builder instance for property modification.
     *
     * @return A {@link Builder} instance for the type.
     */
    public abstract Builder<OUT> toBuilder();

    /**
     * Creates an instance of this class builder.
     *
     * @param <OUT> The expected return type of this source.
     * @return the BigQuerySource builder instance.
     */
    public static <OUT> Builder<OUT> builder() {
        return new AutoValue_BigQuerySource.Builder<OUT>()
                .setSourceBoundedness(Boundedness.BOUNDED);
    }

    /**
     * Creates an instance of the source, setting Avro {@link GenericRecord} as the return type for
     * the data (mimicking the table's schema). In case of projecting the columns of the table a new
     * de-serialization schema should be provided (considering the new result projected schema).
     *
     * @param readOptions The read options for this source
     * @return A fully initialized instance of the source, ready to read {@link GenericRecord} from
     *     the underlying table.
     */
    public static BigQuerySource<GenericRecord> readAvros(BigQueryReadOptions readOptions) {
        BigQueryConnectOptions connectOptions = readOptions.getBigQueryConnectOptions();
        TableSchema tableSchema =
                BigQueryServicesFactory.instance(connectOptions)
                        .queryClient()
                        .getTableSchema(
                                connectOptions.getProjectId(),
                                connectOptions.getDataset(),
                                connectOptions.getTable());
        return BigQuerySource.<GenericRecord>builder()
                .setDeserializationSchema(
                        new AvroDeserializationSchema(
                                SchemaTransform.toGenericAvroSchema(
                                                String.format(
                                                        "%s.%s.%s",
                                                        sanitizeAvroSchemaName(
                                                                connectOptions.getProjectId()),
                                                        sanitizeAvroSchemaName(
                                                                connectOptions.getDataset()),
                                                        sanitizeAvroSchemaName(
                                                                connectOptions.getTable())),
                                                tableSchema.getFields())
                                        .toString()))
                .setReadOptions(readOptions)
                .build();
    }

    /** Replaces invalid characters with underscore. */
    private static String sanitizeAvroSchemaName(String name) {
        if (name == null) {
            return name;
        }
        int length = name.length();
        if (length == 0) {
            return name;
        }
        String newName = "";
        char first = name.charAt(0);
        if (Character.isLetter(first) || first == '_') {
            newName += first;
        } else {
            newName += '_';
        }
        for (int i = 1; i < length; i++) {
            char c = name.charAt(i);
            if (Character.isLetterOrDigit(c) || c == '_') {
                newName += c;
            } else {
                newName += '_';
            }
        }
        return newName;
    }

    /**
     * Builder class for {@link BigQuerySource}.
     *
     * @param <OUT> The type of the data returned by this source implementation.
     */
    @AutoValue.Builder
    public abstract static class Builder<OUT> {
        /**
         * Sets the de-serialization schema for BigQuery returned data (AVRO by default).
         *
         * @param deserSchema the de-serialization schema for BigQuery return type.
         * @return the BigQuerySource builder instance.
         */
        public abstract Builder<OUT> setDeserializationSchema(
                BigQueryDeserializationSchema<GenericRecord, OUT> deserSchema);

        /**
         * Sets the BigQuery read options that configures the source's instance.
         *
         * @param options The instance of the BigQuery read options.
         * @return the BigQuerySource builder instance.
         */
        public abstract Builder<OUT> setReadOptions(BigQueryReadOptions options);

        /**
         * Sets how the source will scan and read data from BigQuery, in batch fashion (once from a
         * table, partition or query results) or continuously (reading new completed partitions as
         * they appear).
         *
         * @param boundedness The boundedness of the source
         * @return the BigQuerySource builder instance.
         */
        public abstract Builder<OUT> setSourceBoundedness(Boundedness boundedness);

        /**
         * Creates an instance of the {@link BigQuerySource}.
         *
         * @return A fully initialized instance of the source.
         */
        public abstract BigQuerySource<OUT> build();
    }
}
