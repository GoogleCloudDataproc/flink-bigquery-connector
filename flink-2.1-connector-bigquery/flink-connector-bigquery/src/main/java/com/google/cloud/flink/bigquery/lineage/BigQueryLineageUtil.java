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

package com.google.cloud.flink.bigquery.lineage;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.lineage.DatasetConfigFacet;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Builds the FLIP-314 {@link LineageDataset} shared by the BigQuery source and sink.
 *
 * <p>The dataset's {@code (namespace, name)} pair is {@code ("bigquery", "project.dataset.table")}:
 * {@code namespace} identifies the storage system and {@code name} is the fully-qualified table.
 * This is the identity a FLIP-314 {@link org.apache.flink.core.execution.JobStatusChangedListener}
 * consumes to resolve the BigQuery table.
 *
 * <p>For DataStream jobs that pair is sufficient. For FlinkSQL/Table jobs it is <em>not</em>:
 * Flink's Table planner wraps the connector-provided dataset in {@code TableLineageDatasetImpl},
 * whose constructor <em>overrides</em> {@code name()} with the Flink object identifier ({@code
 * catalog.database.table}) while <em>preserving</em> {@code namespace()} and {@code facets()}. The
 * clobbered name (e.g. {@code my_catalog.my_database.my_table}) no longer reflects the real
 * BigQuery table. To survive that, the real BigQuery coordinates are also published in a {@link
 * DatasetConfigFacet}; a downstream consumer that finds the facet can recover {@code
 * project.dataset.table} even when {@code name()} has been overridden.
 */
public final class BigQueryLineageUtil {

    /** Lineage namespace for BigQuery datasets. */
    public static final String NAMESPACE = "bigquery";

    /** Facet key under which the BigQuery coordinates are published. */
    public static final String CONFIG_FACET_NAME = "bigquery";

    /** Config-facet key holding the GCP project id. */
    public static final String CONFIG_PROJECT = "project";

    /** Config-facet key holding the BigQuery dataset. */
    public static final String CONFIG_DATASET = "dataset";

    /** Config-facet key holding the BigQuery table. */
    public static final String CONFIG_TABLE = "table";

    private BigQueryLineageUtil() {}

    /**
     * The FLIP-314 lineage vertex for a BigQuery <em>source</em> reading the table addressed by
     * {@code options}. Returns a {@link SourceLineageVertex} (not a plain {@link LineageVertex})
     * because Flink's {@code LineageGraphUtils.processSource} hard-casts source vertices to that
     * type, so a plain vertex would fail at graph-build time.
     */
    public static SourceLineageVertex sourceVertexOf(
            BigQueryConnectOptions options, Boundedness boundedness) {
        Objects.requireNonNull(options, "options must not be null");
        Objects.requireNonNull(boundedness, "boundedness must not be null");
        List<LineageDataset> datasets = Collections.singletonList(datasetOf(options));
        return new SourceLineageVertex() {
            @Override
            public List<LineageDataset> datasets() {
                return datasets;
            }

            @Override
            public Boundedness boundedness() {
                return boundedness;
            }
        };
    }

    /**
     * The FLIP-314 lineage vertex for a BigQuery <em>sink</em> writing the table addressed by
     * {@code options}.
     */
    public static LineageVertex sinkVertexOf(BigQueryConnectOptions options) {
        Objects.requireNonNull(options, "options must not be null");
        List<LineageDataset> datasets = Collections.singletonList(datasetOf(options));
        return () -> datasets;
    }

    /**
     * The FLIP-314 lineage dataset for the BigQuery table addressed by {@code options}. The
     * BigQuery coordinates are carried both in {@code name()} (for DataStream jobs) and in a {@link
     * DatasetConfigFacet} (so they survive the Table planner overriding {@code name()}).
     */
    private static LineageDataset datasetOf(BigQueryConnectOptions options) {
        String project =
                Objects.requireNonNull(options.getProjectId(), "projectId must not be null");
        String dataset = Objects.requireNonNull(options.getDataset(), "dataset must not be null");
        String table = Objects.requireNonNull(options.getTable(), "table must not be null");
        String name = String.join(".", project, dataset, table);

        Map<String, String> config = new LinkedHashMap<>();
        config.put(CONFIG_PROJECT, project);
        config.put(CONFIG_DATASET, dataset);
        config.put(CONFIG_TABLE, table);
        Map<String, LineageDatasetFacet> facets =
                Collections.singletonMap(CONFIG_FACET_NAME, new BigQueryConfigFacet(config));

        return new LineageDataset() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public String namespace() {
                return NAMESPACE;
            }

            @Override
            public Map<String, LineageDatasetFacet> facets() {
                return facets;
            }
        };
    }

    /** A {@link DatasetConfigFacet} carrying the BigQuery project/dataset/table coordinates. */
    private static final class BigQueryConfigFacet implements DatasetConfigFacet {
        private final Map<String, String> config;

        BigQueryConfigFacet(Map<String, String> config) {
            this.config = Collections.unmodifiableMap(config);
        }

        @Override
        public Map<String, String> config() {
            return config;
        }

        @Override
        public String name() {
            return CONFIG_FACET_NAME;
        }
    }
}
