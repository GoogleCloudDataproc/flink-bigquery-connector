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
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
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
import com.google.cloud.flink.bigquery.source.split.BigQuerySourceSplitAssigner;
import com.google.cloud.flink.bigquery.source.split.BigQuerySourceSplitSerializer;
import com.google.cloud.flink.bigquery.source.split.reader.BigQuerySourceSplitReader;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * The DataStream {@link Source} implementation for Google BigQuery. It can be used to read data
 * directly from a BigQuery table or read data from a BigQuery query execution.
 *
 * <p>The following example demonstrates how to configure the source to read {@link GenericRecord}
 * data from a BigQuery table.
 *
 * <pre>{@code
 * BigQuerySource<GenericRecord> source =
 *       BigQuerySource.readAvros(
 *           BigQueryReadOptions.builder()
 *             .setColumnNames(Lists.newArrayList("col1", "col2"))
 *             .setRowRestriction(
 *               "col2 BETWEEN '2023-06-01' AND '2023-06-02'")
 *             .setBigQueryConnectOptions(
 *               BigQueryConnectOptions.builder()
 *                 .setProjectId("some-gcp-project")
 *                 .setDataset("some-bq-dataset")
 *                 .setTable("some-bq-table")
 *                 .build())
 *             .build(), 1000);
 * }</pre>
 *
 * <p>Review the option classes and their builders for more details on the configurable options.
 *
 * <p>The following example demonstrates how to configure the Source to read {@link GenericRecord}
 * data from the results of a BigQuery query execution.
 *
 * <pre>{@code
 * BigQuerySource<GenericRecord> bqSource =
 *         BigQuerySource.readAvrosFromQuery(
 *                 "SELECT * FROM some_dataset.INFORMATION_SCHEMA.PARTITIONS",
 *                 "some_gcp_project");
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

    @Nullable
    public abstract Integer getLimit();

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
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
        FutureCompletingBlockingQueue<RecordsWithSplitIds<GenericRecord>> elementsQueue =
                new FutureCompletingBlockingQueue<>();

        BigQuerySourceReaderContext bqReaderContext =
                new BigQuerySourceReaderContext(readerContext, getLimit());

        Supplier<SplitReader<GenericRecord, BigQuerySourceSplit>> splitReaderSupplier =
                () -> new BigQuerySourceSplitReader(getReadOptions(), bqReaderContext);

        return new BigQuerySourceReader<>(
                elementsQueue,
                splitReaderSupplier,
                new BigQueryRecordEmitter<>(getDeserializationSchema()),
                bqReaderContext);
    }

    @Override
    public SplitEnumerator<BigQuerySourceSplit, BigQuerySourceEnumState> createEnumerator(
            SplitEnumeratorContext<BigQuerySourceSplit> enumContext) throws Exception {
        BigQuerySourceEnumState initialState = BigQuerySourceEnumState.initialState();
        BigQuerySourceSplitAssigner assigner =
                new BigQuerySourceSplitAssigner(getReadOptions(), initialState);
        return new BigQuerySourceEnumerator(getBoundedness(), enumContext, assigner);
    }

    @Override
    public SplitEnumerator<BigQuerySourceSplit, BigQuerySourceEnumState> restoreEnumerator(
            SplitEnumeratorContext<BigQuerySourceSplit> enumContext,
            BigQuerySourceEnumState checkpoint)
            throws Exception {
        LOG.debug("Restoring enumerator with state {}", checkpoint);
        BigQuerySourceSplitAssigner splitAssigner =
                new BigQuerySourceSplitAssigner(getReadOptions(), checkpoint);
        return new BigQuerySourceEnumerator(getBoundedness(), enumContext, splitAssigner);
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
        return new AutoValue_BigQuerySource.Builder<OUT>().setLimit(-1);
    }

    /**
     * Creates an instance of the source, setting Avro {@link GenericRecord} as the return type for
     * the data (mimicking the table's schema), limiting the record retrieval to the provided limit
     * and reading data from the provided query which will be executed using the provided GCP
     * project.
     *
     * @param query A BigQuery standard SQL query.
     * @param gcpProject The GCP project where the provided query will execute.
     * @param limit the max quantity of records to be returned.
     * @return A fully initialized instance of the source, ready to read {@link GenericRecord} from
     *     the BigQuery query results.
     * @throws IOException
     */
    public static BigQuerySource<GenericRecord> readAvrosFromQuery(
            String query, String gcpProject, Integer limit) throws IOException {
        BigQueryReadOptions readOptions =
                BigQueryReadOptions.builder()
                        .setQueryAndExecutionProject(query, gcpProject)
                        .build();

        BigQueryConnectOptions connectOptions = readOptions.getBigQueryConnectOptions();
        TableSchema tableSchema =
                BigQueryServicesFactory.instance(connectOptions)
                        .queryClient()
                        .dryRunQuery(readOptions.getQueryExecutionProject(), readOptions.getQuery())
                        .getStatistics()
                        .getQuery()
                        .getSchema();
        return BigQuerySource.<GenericRecord>builder()
                .setDeserializationSchema(
                        new AvroDeserializationSchema(
                                SchemaTransform.toGenericAvroSchema(
                                                "queryresultschema", tableSchema.getFields())
                                        .toString()))
                .setLimit(limit)
                .setReadOptions(readOptions)
                .build();
    }

    /**
     * Creates an instance of the source, setting Avro {@link GenericRecord} as the return type for
     * the data (mimicking the table's schema) and reading data from the provided query which will
     * be executed using the provided GCP project.
     *
     * @param query A BigQuery standard SQL query.
     * @param gcpProject The GCP project where the provided query will execute.
     * @return A fully initialized instance of the source, ready to read {@link GenericRecord} from
     *     the BigQuery query results.
     * @throws IOException
     */
    public static BigQuerySource<GenericRecord> readAvrosFromQuery(String query, String gcpProject)
            throws IOException {
        return readAvrosFromQuery(query, gcpProject, -1);
    }

    /**
     * Creates an instance of the source, limiting the record retrieval to the provided limit and
     * setting Avro {@link GenericRecord} as the return type for the data (mimicking the table's
     * schema). In case of projecting the columns of the table a new de-serialization schema should
     * be provided (considering the new result projected schema).
     *
     * @param readOptions The read options for this source
     * @param limit the max quantity of records to be returned.
     * @return A fully initialized instance of the source, ready to read {@link GenericRecord} from
     *     the underlying table.
     */
    public static BigQuerySource<GenericRecord> readAvros(
            BigQueryReadOptions readOptions, Integer limit) {
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
                                                        connectOptions.getProjectId(),
                                                        connectOptions.getDataset(),
                                                        connectOptions.getTable()),
                                                tableSchema.getFields())
                                        .toString()))
                .setLimit(limit)
                .setReadOptions(readOptions)
                .build();
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
        return readAvros(readOptions, -1);
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
         * Sets the max element count returned by this source.
         *
         * @param limit The max element count returned by the source.
         * @return the BigQuerySource builder instance.
         */
        public abstract Builder<OUT> setLimit(Integer limit);

        /**
         * Creates an instance of the {@link BigQuerySource}.
         *
         * @return A fully initialized instance of the source.
         */
        public abstract BigQuerySource<OUT> build();
    }
}
