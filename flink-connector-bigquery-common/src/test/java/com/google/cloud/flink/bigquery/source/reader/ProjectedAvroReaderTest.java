/*
 * Copyright (C) 2026 Shopify Inc.
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

package com.google.cloud.flink.bigquery.source.reader;

import com.google.cloud.bigquery.storage.v1.AvroRows;
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

import static com.google.common.truth.Truth.assertThat;

/** Tests for {@link ProjectedAvroReader}. */
public class ProjectedAvroReaderTest {
    private static final Schema SCHEMA =
            new Schema.Parser()
                    .parse(
                            "{\"type\":\"record\",\"name\":\"event\",\"fields\":["
                                    + "{\"name\":\"id\",\"type\":\"long\"},"
                                    + "{\"name\":\"unused\",\"type\":\"string\"},"
                                    + "{\"name\":\"items\",\"type\":[\"null\",{\"type\":\"array\",\"items\":"
                                    + "{\"type\":\"record\",\"name\":\"item\",\"fields\":["
                                    + "{\"name\":\"keep\",\"type\":\"long\"},"
                                    + "{\"name\":\"drop\",\"type\":\"string\"}]}}]}]}");

    private AvroRows rows() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bytes, null);
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(SCHEMA);
        Schema itemSchema = SCHEMA.getField("items").schema().getTypes().get(1).getElementType();
        for (long i = 0; i < 7; i++) {
            GenericRecord record = new GenericData.Record(SCHEMA);
            record.put("id", i);
            record.put("unused", String.join("", Collections.nCopies(10000, "x")));
            GenericRecord item = new GenericData.Record(itemSchema);
            item.put("keep", i);
            item.put("drop", "discard");
            record.put("items", i == 0 ? null : Arrays.asList(item));
            writer.write(record, encoder);
        }
        encoder.flush();
        return AvroRows.newBuilder()
                .setSerializedBinaryRows(ByteString.copyFrom(bytes.toByteArray()))
                .setRowCount(7)
                .build();
    }

    @Test
    public void projectsBeforeMaterializingAndPreservesRecordOrder() throws Exception {
        ProjectedAvroReader reader =
                new ProjectedAvroReader(SCHEMA, Arrays.asList("id", "items.keep"), rows());
        for (long i = 0; i < 7; i++) {
            assertThat(reader.hasNext()).isTrue();
            GenericRecord record = reader.read();
            assertThat(record.get("id")).isEqualTo(i);
            assertThat(record.getSchema().getField("unused")).isNull();
            if (i == 0) {
                assertThat(record.get("items")).isNull();
            } else {
                GenericRecord item = ((java.util.List<GenericRecord>) record.get("items")).get(0);
                assertThat(item.get("keep")).isEqualTo(i);
                assertThat(item.getSchema().getField("drop")).isNull();
            }
        }
        assertThat(reader.hasNext()).isFalse();
    }

    @Test
    public void emptyProjectionKeepsAllFields() throws Exception {
        ProjectedAvroReader reader =
                new ProjectedAvroReader(SCHEMA, Collections.emptyList(), rows());
        assertThat(reader.read().getSchema()).isEqualTo(SCHEMA);
    }

    @Test
    public void wholeParentProjectionKeepsAllChildren() throws Exception {
        ProjectedAvroReader reader =
                new ProjectedAvroReader(SCHEMA, Arrays.asList("items", "items.keep"), rows());
        assertThat(reader.read().getSchema().getField("items").schema())
                .isEqualTo(SCHEMA.getField("items").schema());
    }
}
