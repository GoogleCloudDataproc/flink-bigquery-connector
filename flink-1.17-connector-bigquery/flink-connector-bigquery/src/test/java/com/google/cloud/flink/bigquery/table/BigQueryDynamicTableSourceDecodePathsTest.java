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

package com.google.cloud.flink.bigquery.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.bigquery.storage.v1.AvroRows;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;
import com.google.cloud.flink.bigquery.source.BigQuerySource;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import com.google.cloud.flink.bigquery.source.reader.ProjectedAvroReader;
import com.google.cloud.flink.bigquery.source.reader.deserializer.AvroToRowDataConverters;
import com.google.protobuf.ByteString;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

/** Decoding array elements with the Avro paths of the declared ROW type. */
public class BigQueryDynamicTableSourceDecodePathsTest {

    // Shaped like a Storage Read session schema: nullable leaves, arrays of records, and an
    // array nested inside the array's element.
    private static final Schema WRITER =
            new Schema.Parser()
                    .parse(
                            "{\"type\":\"record\",\"name\":\"__root__\",\"fields\":["
                                    + "{\"name\":\"user_id\",\"type\":[\"null\",\"long\"]},"
                                    + "{\"name\":\"recs\",\"type\":{\"type\":\"array\",\"items\":"
                                    + "{\"type\":\"record\",\"name\":\"rec\",\"fields\":["
                                    + "{\"name\":\"product_id\",\"type\":[\"null\",\"long\"]},"
                                    + "{\"name\":\"broker_id\",\"type\":[\"null\",\"long\"]},"
                                    + "{\"name\":\"score\",\"type\":[\"null\",\"double\"]},"
                                    + "{\"name\":\"cg_sources\",\"type\":{\"type\":\"array\",\"items\":"
                                    + "{\"type\":\"record\",\"name\":\"cg\",\"fields\":["
                                    + "{\"name\":\"name\",\"type\":[\"null\",\"string\"]},"
                                    + "{\"name\":\"version\",\"type\":[\"null\",\"string\"]},"
                                    + "{\"name\":\"cg_score\",\"type\":[\"null\",\"double\"]}"
                                    + "]}}}]}}}]}");

    // Declares a subset of the element fields, in a different order than the writer.
    private static final DataType DECLARED =
            DataTypes.ROW(
                    DataTypes.FIELD("user_id", DataTypes.BIGINT()),
                    DataTypes.FIELD(
                            "recs",
                            DataTypes.ARRAY(
                                    DataTypes.ROW(
                                            DataTypes.FIELD("score", DataTypes.DOUBLE()),
                                            DataTypes.FIELD("product_id", DataTypes.BIGINT()),
                                            DataTypes.FIELD(
                                                    "cg_sources",
                                                    DataTypes.ARRAY(
                                                            DataTypes.ROW(
                                                                    DataTypes.FIELD(
                                                                            "cg_score",
                                                                            DataTypes.DOUBLE()),
                                                                    DataTypes.FIELD(
                                                                            "name",
                                                                            DataTypes
                                                                                    .STRING()))))))));

    private static final String[][] COLUMNS = {{"user_id"}, {"recs"}};

    @Test
    public void decodePathsReachDeclaredLeavesInsideArrays() {
        assertThat(paths(DECLARED))
                .containsExactly(
                        "user_id",
                        "recs.score",
                        "recs.product_id",
                        "recs.cg_sources.cg_score",
                        "recs.cg_sources.name")
                .inOrder();
    }

    @Test
    public void scanRuntimeProviderPassesDecodePathsToTheSource() throws Exception {
        BigQueryDynamicTableSource source = new BigQueryDynamicTableSource(readOptions(), DECLARED);
        BigQuerySource<?> bqSource =
                (BigQuerySource<?>)
                        ((SourceProvider)
                                        source.getScanRuntimeProvider(
                                                ScanRuntimeProviderContext.INSTANCE))
                                .createSource();
        assertThat(bqSource.getReadOptions().getColumnNames())
                .containsExactly("user_id", "recs")
                .inOrder();
        assertThat(bqSource.getReadOptions().getProjectedAvroPaths()).isEqualTo(paths(DECLARED));
    }

    @Test
    public void undeclaredElementFieldsAreSkippedAndDeclaredOnesDecode() throws Exception {
        ProjectedAvroReader reader = new ProjectedAvroReader(WRITER, paths(DECLARED), rows());
        AvroToRowDataConverters.AvroToRowDataConverter converter =
                AvroToRowDataConverters.createRowConverter(rowType(DECLARED), COLUMNS);
        for (long i = 0; i < 3; i++) {
            GenericRecord record = reader.read();
            Schema element = record.getSchema().getField("recs").schema().getElementType();
            assertThat(element.getFields().stream().map(Schema.Field::name))
                    .containsExactly("product_id", "score", "cg_sources")
                    .inOrder();
            assertThat(
                            element.getField("cg_sources").schema().getElementType().getFields()
                                    .stream()
                                    .map(Schema.Field::name))
                    .containsExactly("name", "cg_score")
                    .inOrder();

            RowData row = (RowData) converter.convert(record);
            assertThat(row.getLong(0)).isEqualTo(i);
            ArrayData recs = row.getArray(1);
            assertThat(recs.size()).isEqualTo(2);
            for (int j = 0; j < 2; j++) {
                RowData rec = recs.getRow(j, 3);
                assertThat(rec.getDouble(0)).isEqualTo(i + j / 10.0);
                assertThat(rec.getLong(1)).isEqualTo(100 * i + j);
                ArrayData cgs = rec.getArray(2);
                assertThat(cgs.size()).isEqualTo(j + 1);
                for (int k = 0; k <= j; k++) {
                    RowData cg = cgs.getRow(k, 2);
                    assertThat(cg.getDouble(0)).isEqualTo(k / 100.0);
                    assertThat(cg.getString(1).toString()).isEqualTo("cg" + k);
                }
            }
        }
        assertThat(reader.hasNext()).isFalse();
    }

    @Test
    public void declaredElementFieldMissingFromTheWriterFails() throws Exception {
        DataType declared =
                DataTypes.ROW(
                        DataTypes.FIELD("user_id", DataTypes.BIGINT()),
                        DataTypes.FIELD(
                                "recs",
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("product_id", DataTypes.BIGINT()),
                                                DataTypes.FIELD("missing", DataTypes.STRING())))));
        // the column paths keep the whole element, so the converter finds the missing field
        GenericRecord record =
                new ProjectedAvroReader(WRITER, Arrays.asList("user_id", "recs"), rows()).read();
        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                AvroToRowDataConverters.createRowConverter(
                                                rowType(declared), COLUMNS)
                                        .convert(record));
        assertThat(error).hasMessageThat().contains("Field 'missing' not found");
        // the decode paths name the missing field, so the projection already fails
        error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> ProjectedAvroReader.projectSchema(WRITER, paths(declared)));
        assertThat(error).hasMessageThat().contains("[recs.missing]");
    }

    @Test
    public void rowsWithoutFieldsKeepTheWholeField() throws Exception {
        DataType declared =
                DataTypes.ROW(
                        DataTypes.FIELD("user_id", DataTypes.BIGINT()),
                        DataTypes.FIELD("recs", DataTypes.ARRAY(DataTypes.ROW())));
        assertThat(paths(declared)).containsExactly("user_id", "recs").inOrder();
        GenericRecord record = new ProjectedAvroReader(WRITER, paths(declared), rows()).read();
        RowData row =
                (RowData)
                        AvroToRowDataConverters.createRowConverter(rowType(declared), COLUMNS)
                                .convert(record);
        assertThat(row.getArray(1).size()).isEqualTo(2);

        DataType nested =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                "outer",
                                DataTypes.ROW(
                                        DataTypes.FIELD("keep", DataTypes.BIGINT()),
                                        DataTypes.FIELD("empty_inner", DataTypes.ROW()))));
        assertThat(
                        BigQueryDynamicTableSource.avroDecodePaths(
                                new String[][] {{"outer"}}, rowType(nested)))
                .containsExactly("outer.keep", "outer.empty_inner")
                .inOrder();
    }

    private static List<String> paths(DataType declared) {
        return BigQueryDynamicTableSource.avroDecodePaths(COLUMNS, rowType(declared));
    }

    private static RowType rowType(DataType type) {
        return (RowType) type.getLogicalType();
    }

    private static BigQueryReadOptions readOptions() {
        return BigQueryReadOptions.builder()
                .setBigQueryConnectOptions(
                        BigQueryConnectOptions.builder()
                                .setDataset("dataset")
                                .setProjectId("project")
                                .setTable("table")
                                .setCredentialsOptions(CredentialsOptions.builder().build())
                                .build())
                .build();
    }

    private static AvroRows rows() throws Exception {
        Schema rec = WRITER.getField("recs").schema().getElementType();
        Schema cg = rec.getField("cg_sources").schema().getElementType();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bytes, null);
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(WRITER);
        for (long i = 0; i < 3; i++) {
            GenericRecord record = new GenericData.Record(WRITER);
            record.put("user_id", i);
            GenericRecord[] recs = new GenericRecord[2];
            for (int j = 0; j < 2; j++) {
                recs[j] = new GenericData.Record(rec);
                recs[j].put("product_id", 100 * i + j);
                recs[j].put("broker_id", -1L);
                recs[j].put("score", i + j / 10.0);
                GenericRecord[] cgs = new GenericRecord[j + 1];
                for (int k = 0; k <= j; k++) {
                    cgs[k] = new GenericData.Record(cg);
                    cgs[k].put("name", "cg" + k);
                    cgs[k].put("version", String.join("", Collections.nCopies(100, "v")));
                    cgs[k].put("cg_score", k / 100.0);
                }
                recs[j].put("cg_sources", Arrays.asList(cgs));
            }
            record.put("recs", Arrays.asList(recs));
            writer.write(record, encoder);
        }
        encoder.flush();
        return AvroRows.newBuilder()
                .setSerializedBinaryRows(ByteString.copyFrom(bytes.toByteArray()))
                .setRowCount(3)
                .build();
    }
}
