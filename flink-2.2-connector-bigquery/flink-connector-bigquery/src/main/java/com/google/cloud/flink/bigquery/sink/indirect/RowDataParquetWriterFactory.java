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

package com.google.cloud.flink.bigquery.sink.indirect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.bigquery.ParquetOptions;
import org.apache.hadoop.conf.Configuration;

import java.util.Objects;

/**
 * Factory for Parquet bulk writers used by the indirect-write path.
 *
 * <p>Configures Parquet to encode timestamps as INT64 with microsecond precision so BigQuery's
 * Parquet load reads them back as {@code TIMESTAMP} with full TIMESTAMP(6) fidelity. The default
 * INT96 encoding would lose precision and is treated as a legacy Hive type by BigQuery.
 *
 * <p><b>Known limitation:</b> Flink's {@code ParquetSchemaConverter} emits identical {@code
 * timestamp(isAdjustedToUTC=false, MICROS)} schemas for both Flink {@code
 * TIMESTAMP_WITHOUT_TIME_ZONE} and {@code TIMESTAMP_WITH_LOCAL_TIME_ZONE}. BigQuery's Parquet
 * reader interprets that pair the same way for both - so loading into a target table that has a
 * {@code DATETIME} column from Flink {@code TIMESTAMP_WITHOUT_TIME_ZONE} fails with a schema
 * mismatch ("changed type from DATETIME to TIMESTAMP"). DATETIME target columns aren't supported.
 */
@Internal
public final class RowDataParquetWriterFactory extends ParquetWriterFactory<RowData> {

    private static final long serialVersionUID = 1L;

    // Load-side options paired with the writer config above: enableListInference=true tells
    // BigQuery to map Parquet repeated groups (how this writer encodes ARRAY columns) to
    // BigQuery REPEATED columns rather than nested STRUCTs.
    public static final ParquetOptions PARQUET_FORMAT_OPTIONS =
            ParquetOptions.newBuilder().setEnableListInference(true).build();

    private final RowType rowType;

    private RowDataParquetWriterFactory(RowType rowType) {
        // utcTimestamp=true: TIMESTAMP_LTZ values are written relative to UTC, matching BigQuery
        // TIMESTAMP semantics.
        super(new ParquetRowDataBuilder.FlinkParquetBuilder(rowType, buildConf(), true));
        this.rowType = rowType;
    }

    public static RowDataParquetWriterFactory create(RowType rowType) {
        return new RowDataParquetWriterFactory(rowType);
    }

    private static Configuration buildConf() {
        Configuration conf = new Configuration();
        // Force INT64-based timestamp encoding so logical types (TIMESTAMP_MICROS,
        // LOCAL_TIMESTAMP_MICROS) are emitted instead of legacy INT96.
        conf.setBoolean("parquet.write.int64.timestamp", true);
        // Microsecond resolution matches BigQuery TIMESTAMP precision (6).
        conf.set("parquet.timestamp.time.unit", "micros");
        conf.set("parquet.compression", "SNAPPY");
        return conf;
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowType);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof RowDataParquetWriterFactory)) {
            return false;
        }
        return Objects.equals(this.rowType, ((RowDataParquetWriterFactory) obj).rowType);
    }
}
