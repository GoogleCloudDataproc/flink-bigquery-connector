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
import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

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
    public void reorderedDuplicatePathsPreserveSchemaOrder() throws Exception {
        Schema projected =
                ProjectedAvroReader.projectSchema(
                        SCHEMA,
                        Arrays.asList("items.drop", "items.keep", "items.drop", "id", "id"));
        assertThat(projected.getFields().stream().map(Schema.Field::name))
                .containsExactly("id", "items")
                .inOrder();
        Schema items = projected.getField("items").schema();
        assertThat(items.getTypes().stream().map(Schema::getType))
                .containsExactly(Schema.Type.NULL, Schema.Type.ARRAY)
                .inOrder();
        assertThat(
                        items.getTypes().get(1).getElementType().getFields().stream()
                                .map(Schema.Field::name))
                .containsExactly("keep", "drop")
                .inOrder();
        assertThat(projected)
                .isEqualTo(
                        ProjectedAvroReader.projectSchema(
                                SCHEMA, Arrays.asList("id", "items.keep", "items.drop")));
    }

    @Test
    public void reorderedDuplicateNestedPathsDecodeSelectedFields() throws Exception {
        ProjectedAvroReader reader =
                new ProjectedAvroReader(
                        SCHEMA, Arrays.asList("items.keep", "id", "items.keep", "id"), rows());
        for (long i = 0; i < 7; i++) {
            assertThat(reader.hasNext()).isTrue();
            GenericRecord record = reader.read();
            assertThat(record.get("id")).isEqualTo(i);
            assertThat(record.getSchema().getFields().stream().map(Schema.Field::name))
                    .containsExactly("id", "items")
                    .inOrder();
            if (i == 0) {
                assertThat(record.get("items")).isNull();
            } else {
                GenericRecord item = ((java.util.List<GenericRecord>) record.get("items")).get(0);
                assertThat(item.getSchema().getFields().stream().map(Schema.Field::name))
                        .containsExactly("keep");
                assertThat(item.get("keep")).isEqualTo(i);
            }
        }
        assertThat(reader.hasNext()).isFalse();
    }

    @Test
    public void duplicateParentPathsKeepAllChildrenRegardlessOfPathOrder() {
        Schema projected =
                ProjectedAvroReader.projectSchema(
                        SCHEMA, Arrays.asList("items.keep", "items", "items.keep", "items"));
        assertThat(projected.getFields().stream().map(Schema.Field::name)).containsExactly("items");
        assertThat(projected.getField("items").schema())
                .isEqualTo(SCHEMA.getField("items").schema());
    }

    @Test
    public void matchesSelectedPathsCaseInsensitively() throws Exception {
        ProjectedAvroReader reader =
                new ProjectedAvroReader(SCHEMA, Arrays.asList("ID", "Items.KEEP"), rows());
        reader.read();
        GenericRecord record = reader.read();
        assertThat(record.getSchema().getFields().stream().map(Schema.Field::name))
                .containsExactly("id", "items")
                .inOrder();
        assertThat(record.get("id")).isEqualTo(1L);
        GenericRecord item = ((java.util.List<GenericRecord>) record.get("items")).get(0);
        assertThat(item.getSchema().getFields().stream().map(Schema.Field::name))
                .containsExactly("keep");
        assertThat(item.get("keep")).isEqualTo(1L);
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

    @Test
    public void trimsPathsAndIgnoresBlankPaths() {
        assertThat(ProjectedAvroReader.projectSchema(SCHEMA, Arrays.asList("", "  ")))
                .isEqualTo(SCHEMA);
        Schema projected =
                ProjectedAvroReader.projectSchema(SCHEMA, Arrays.asList(" id", "", "items.keep "));
        assertThat(projected.getFields().stream().map(Schema.Field::name))
                .containsExactly("id", "items")
                .inOrder();
    }

    @Test
    public void rejectsNullPaths() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ProjectedAvroReader.projectSchema(SCHEMA, null));
        assertThrows(
                IllegalArgumentException.class,
                () -> ProjectedAvroReader.projectSchema(SCHEMA, Arrays.asList("id", null)));
    }

    @Test
    public void rejectsPathsThatMatchNoField() {
        IllegalArgumentException topLevel =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                ProjectedAvroReader.projectSchema(
                                        SCHEMA, Arrays.asList("id", "nonexistent")));
        assertThat(topLevel).hasMessageThat().contains("[nonexistent]");
        IllegalArgumentException nested =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                ProjectedAvroReader.projectSchema(
                                        SCHEMA, Arrays.asList("items.keep", "items.missing")));
        assertThat(nested).hasMessageThat().contains("[items.missing]");
    }

    @Test
    public void rejectsPathsInsideFieldsThatAreNotRecords() {
        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> ProjectedAvroReader.projectSchema(SCHEMA, Arrays.asList("id.sub")));
        assertThat(ex).hasMessageThat().contains("'id.sub'");
    }

    @Test
    public void checksSubPathsOfAFieldThatIsAlsoSelectedWhole() {
        IllegalArgumentException notRecord =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                ProjectedAvroReader.projectSchema(
                                        SCHEMA, Arrays.asList("id", "id.sub")));
        assertThat(notRecord).hasMessageThat().contains("'id.sub'");
        IllegalArgumentException missing =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                ProjectedAvroReader.projectSchema(
                                        SCHEMA, Arrays.asList("items", "items.missing")));
        assertThat(missing).hasMessageThat().contains("[items.missing]");
    }

    @Test
    public void keepsArrayProperties() {
        Schema schema =
                new Schema.Parser()
                        .parse(
                                "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
                                        + "{\"name\":\"xs\",\"type\":{\"type\":\"array\","
                                        + "\"prop\":\"value\",\"items\":{\"type\":\"record\","
                                        + "\"name\":\"x\",\"fields\":["
                                        + "{\"name\":\"a\",\"type\":\"long\"},"
                                        + "{\"name\":\"b\",\"type\":\"long\"}]}}}]}");
        Schema projected = ProjectedAvroReader.projectSchema(schema, Arrays.asList("xs.a"));
        assertThat(projected.getField("xs").schema().getProp("prop")).isEqualTo("value");
    }

    @Test
    public void namesEachDifferentProjectionOfASharedRecord() throws Exception {
        Schema shared = sharedAddressSchema();
        Schema projected =
                ProjectedAvroReader.projectSchema(
                        shared, Arrays.asList("billing.street", "shipping.city"));
        // the projected schema must stay a valid Avro schema
        Schema reparsed = new Schema.Parser().parse(projected.toString());
        assertThat(reparsed).isEqualTo(projected);
        Schema billing = projected.getField("billing").schema().getTypes().get(1);
        Schema shipping = projected.getField("shipping").schema().getTypes().get(1);
        assertThat(billing.getFullName()).isNotEqualTo(shipping.getFullName());
        assertThat(billing.getFields().stream().map(Schema.Field::name)).containsExactly("street");
        assertThat(shipping.getFields().stream().map(Schema.Field::name)).containsExactly("city");

        GenericRecord record =
                new ProjectedAvroReader(shared, reparsed, addressRows(shared)).read();
        assertThat(((GenericRecord) record.get("billing")).get("street").toString())
                .isEqualTo("billing street");
        assertThat(((GenericRecord) record.get("shipping")).get("city").toString())
                .isEqualTo("shipping city");
    }

    @Test
    public void reusesIdenticalProjectionsOfASharedRecord() {
        Schema projected =
                ProjectedAvroReader.projectSchema(
                        sharedAddressSchema(), Arrays.asList("billing.street", "shipping.street"));
        assertThat(projected.getField("shipping").schema().getTypes().get(1))
                .isSameInstanceAs(projected.getField("billing").schema().getTypes().get(1));
        assertThat(new Schema.Parser().parse(projected.toString())).isEqualTo(projected);
    }

    @Test
    public void namesAProjectionOfARecordThatIsAlsoSelectedWhole() throws Exception {
        Schema shared = sharedAddressSchema();
        for (List<String> columns :
                Arrays.asList(
                        Arrays.asList("billing", "shipping.city"),
                        Arrays.asList("billing.city", "shipping.city", "shipping.street"))) {
            Schema projected = ProjectedAvroReader.projectSchema(shared, columns);
            Schema reparsed = new Schema.Parser().parse(projected.toString());
            assertThat(reparsed).isEqualTo(projected);
            GenericRecord record =
                    new ProjectedAvroReader(shared, reparsed, addressRows(shared)).read();
            assertThat(((GenericRecord) record.get("billing")).get("city").toString())
                    .isEqualTo("billing city");
            assertThat(((GenericRecord) record.get("shipping")).get("city").toString())
                    .isEqualTo("shipping city");
        }
    }

    @Test
    public void resetReusesTheReaderAndClearDropsTheRemainingRows() throws Exception {
        ProjectedAvroReader reader =
                new ProjectedAvroReader(
                        SCHEMA, ProjectedAvroReader.projectSchema(SCHEMA, Arrays.asList("id")));
        assertThat(reader.hasNext()).isFalse();
        reader.reset(rows());
        assertThat(reader.read().get("id")).isEqualTo(0L);
        assertThat(reader.read().get("id")).isEqualTo(1L);
        reader.clear();
        assertThat(reader.hasNext()).isFalse();
        reader.reset(rows());
        for (long i = 0; i < 7; i++) {
            assertThat(reader.read().get("id")).isEqualTo(i);
        }
        assertThat(reader.hasNext()).isFalse();
    }

    private static Schema sharedAddressSchema() {
        return new Schema.Parser()
                .parse(
                        "{\"type\":\"record\",\"name\":\"order\",\"namespace\":\"ns\","
                                + "\"fields\":[{\"name\":\"billing\",\"type\":[\"null\","
                                + "{\"type\":\"record\",\"name\":\"Address\",\"fields\":["
                                + "{\"name\":\"street\",\"type\":\"string\"},"
                                + "{\"name\":\"city\",\"type\":\"string\"}]}]},"
                                + "{\"name\":\"shipping\",\"type\":[\"null\",\"Address\"]}]}");
    }

    private static AvroRows addressRows(Schema schema) throws Exception {
        Schema address = schema.getField("billing").schema().getTypes().get(1);
        GenericRecord billing = new GenericData.Record(address);
        billing.put("street", "billing street");
        billing.put("city", "billing city");
        GenericRecord shipping = new GenericData.Record(address);
        shipping.put("street", "shipping street");
        shipping.put("city", "shipping city");
        GenericRecord order = new GenericData.Record(schema);
        order.put("billing", billing);
        order.put("shipping", shipping);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bytes, null);
        new GenericDatumWriter<GenericRecord>(schema).write(order, encoder);
        encoder.flush();
        return AvroRows.newBuilder()
                .setSerializedBinaryRows(ByteString.copyFrom(bytes.toByteArray()))
                .setRowCount(1)
                .build();
    }
}
