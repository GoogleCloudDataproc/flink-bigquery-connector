/*
 * Copyright (C) 2026 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.flink.bigquery.source.split.reader.deserializer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import com.google.cloud.flink.bigquery.common.exceptions.BigQueryConnectorException;
import com.google.cloud.flink.bigquery.source.reader.deserializer.AvroToRowDataDeserializationSchema;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Tests temporal units through the public source deserializer. */
public class AvroTemporalLogicalTypesTest {
    @Test
    public void readsTimestampUnitsIndependentlyOfTargetPrecision() throws Exception {
        for (boolean localZone : new boolean[] {false, true}) {
            for (int precision = 0; precision <= 6; precision++) {
                LogicalType type =
                        localZone
                                ? new LocalZonedTimestampType(precision)
                                : new TimestampType(precision);
                for (String unit :
                        new String[] {
                            "timestamp-micros", "local-timestamp-micros", "timestamp-millis"
                        }) {
                    Schema field = temporalSchema(unit);
                    Schema schema = record("root", field);
                    AvroToRowDataDeserializationSchema deserializer = deserializer(type);
                    for (long value : new long[] {1_000_000L, 1_234_567L, -1L, -1_234_567L, 0L}) {
                        GenericRecord input = new GenericData.Record(schema);
                        input.put("value", value);
                        long micros = unit.equals("timestamp-millis") ? value * 1000 : value;
                        long quantum = (long) Math.pow(10, 6 - precision);
                        long truncated = Math.floorDiv(micros, quantum) * quantum;
                        RowData result = deserializer.deserialize(roundTrip(input));
                        TimestampData actual = result.getTimestamp(0, precision);
                        assertEquals(
                                unit + " precision " + precision,
                                Math.floorDiv(truncated, 1000),
                                actual.getMillisecond());
                        assertEquals(
                                unit + " precision " + precision,
                                Math.floorMod(truncated, 1000) * 1000,
                                actual.getNanoOfMillisecond());
                    }
                }
            }
        }
    }

    @Test
    public void keepsPlainLongUnits() throws Exception {
        Schema schema = record("plain", Schema.create(Schema.Type.LONG));
        GenericRecord input = new GenericData.Record(schema);
        input.put("value", 1_234_567L);
        assertEquals(
                TimestampData.fromEpochMillis(1_234_567L),
                deserializer(new TimestampType(3))
                        .deserialize(roundTrip(input))
                        .getTimestamp(0, 3));
        assertEquals(
                TimestampData.fromEpochMillis(1234, 567000),
                deserializer(new TimestampType(6))
                        .deserialize(roundTrip(input))
                        .getTimestamp(0, 6));
    }

    @Test
    public void readsNullableRowsArraysMapsAndProjectedLeaves() throws Exception {
        Schema timestamp = temporalSchema("timestamp-micros");
        Schema nested =
                record("nested", Schema.createUnion(timestamp, Schema.create(Schema.Type.NULL)));
        Schema root = Schema.createRecord("containers", null, null, false);
        root.setFields(
                Arrays.asList(
                        new Schema.Field(
                                "row",
                                Schema.createUnion(Schema.create(Schema.Type.NULL), nested),
                                null,
                                (Object) null),
                        new Schema.Field(
                                "array",
                                Schema.createUnion(
                                        Schema.create(Schema.Type.NULL),
                                        Schema.createArray(
                                                Schema.createUnion(
                                                        Schema.create(Schema.Type.NULL), nested))),
                                null,
                                (Object) null),
                        new Schema.Field(
                                "map",
                                Schema.createMap(
                                        Schema.createUnion(
                                                Schema.create(Schema.Type.NULL), timestamp)),
                                null,
                                (Object) null),
                        new Schema.Field(
                                "timestamps",
                                Schema.createArray(
                                        Schema.createUnion(
                                                timestamp, Schema.create(Schema.Type.NULL))),
                                null,
                                (Object) null)));
        RowType nestedType =
                RowType.of(new LogicalType[] {new TimestampType(3)}, new String[] {"value"});
        RowType type =
                RowType.of(
                        new LogicalType[] {
                            nestedType,
                            new ArrayType(nestedType),
                            new MapType(new VarCharType(), new TimestampType(3)),
                            new ArrayType(new TimestampType(3))
                        },
                        new String[] {"row", "array", "map", "timestamps"});
        AvroToRowDataDeserializationSchema deserializer =
                new AvroToRowDataDeserializationSchema(
                        type,
                        TypeInformation.of(RowData.class),
                        new String[][] {{"row"}, {"array"}, {"map"}, {"timestamps"}});
        GenericRecord input = new GenericData.Record(root);
        input.put("row", null);
        input.put("array", Collections.emptyList());
        input.put("map", Collections.emptyMap());
        input.put("timestamps", Collections.emptyList());
        RowData empty = deserializer.deserialize(roundTrip(input));
        assertNull(empty.getRow(0, 1));
        assertEquals(0, empty.getArray(1).size());
        GenericRecord child = new GenericData.Record(nested);
        child.put("value", 1_234_999L);
        input.put("row", child);
        input.put("array", Arrays.asList(null, child));
        input.put("map", Collections.singletonMap("key", 1_234_999L));
        input.put("timestamps", Arrays.asList(1_234_999L, null));
        RowData result = deserializer.deserialize(roundTrip(input));
        TimestampData expected = TimestampData.fromEpochMillis(1234);
        assertEquals(expected, result.getRow(0, 1).getTimestamp(0, 3));
        assertTrue(result.getArray(1).isNullAt(0));
        assertEquals(expected, result.getArray(1).getRow(1, 1).getTimestamp(0, 3));
        assertEquals(expected, result.getMap(2).valueArray().getTimestamp(0, 3));
        assertEquals(expected, result.getArray(3).getTimestamp(0, 3));
        assertTrue(result.getArray(3).isNullAt(1));
        RowType projected =
                RowType.of(
                        new LogicalType[] {new LocalZonedTimestampType(3)},
                        new String[] {"selected"});
        AvroToRowDataDeserializationSchema projection =
                new AvroToRowDataDeserializationSchema(
                        projected,
                        TypeInformation.of(RowData.class),
                        new String[][] {{"row", "value"}});
        assertEquals(expected, projection.deserialize(roundTrip(input)).getTimestamp(0, 3));
        child.put("value", null);
        assertNull(deserializer.deserialize(roundTrip(input)).getRow(0, 1).getTimestamp(0, 3));
    }

    @Test
    public void readsMicrosTimeIntoMillisWithoutChangingHigherPrecision() throws Exception {
        Schema schema =
                record(
                        "time",
                        LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)));
        GenericRecord input = new GenericData.Record(schema);
        input.put("value", 1_234_999L);
        for (int precision = 0; precision <= 3; precision++) {
            int quantum = (int) Math.pow(10, 3 - precision);
            assertEquals(
                    (1234 / quantum) * quantum,
                    deserializer(new TimeType(precision)).deserialize(roundTrip(input)).getInt(0));
        }
        assertEquals(
                1_234_999L, deserializer(new TimeType(6)).deserialize(roundTrip(input)).getLong(0));
    }

    @Test
    public void preservesNonLongInputsAndInvalidValueErrors() throws Exception {
        Schema schema = record("instant", temporalSchema("timestamp-micros"));
        GenericRecord input = new GenericData.Record(schema);
        Instant instant = Instant.ofEpochSecond(1, 234567000);
        input.put("value", instant);
        AvroToRowDataDeserializationSchema deserializer = deserializer(new TimestampType(3));
        assertEquals(
                TimestampData.fromInstant(instant),
                deserializer.deserialize(input).getTimestamp(0, 3));
        input.put("value", true);
        BigQueryConnectorException error =
                assertThrows(
                        BigQueryConnectorException.class, () -> deserializer.deserialize(input));
        assertTrue(error.getCause() instanceof IllegalArgumentException);
        assertTrue(error.getCause().getMessage().contains("TIMESTAMP/DATETIME"));
        Schema time =
                record(
                        "timeFallback",
                        LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)));
        GenericRecord timeInput = new GenericData.Record(time);
        timeInput.put("value", 1234);
        assertEquals(1234, deserializer(new TimeType(3)).deserialize(timeInput).getInt(0));
        timeInput.put("value", true);
        assertThrows(
                BigQueryConnectorException.class,
                () -> deserializer(new TimeType(3)).deserialize(timeInput));
    }

    private static Schema temporalSchema(String unit) {
        Schema schema = Schema.create(Schema.Type.LONG);
        switch (unit) {
            case "timestamp-micros":
                return LogicalTypes.timestampMicros().addToSchema(schema);
            case "local-timestamp-micros":
                return LogicalTypes.localTimestampMicros().addToSchema(schema);
            case "timestamp-millis":
                return LogicalTypes.timestampMillis().addToSchema(schema);
            default:
                throw new IllegalArgumentException(unit);
        }
    }

    private static Schema record(String name, Schema field) {
        Schema schema = Schema.createRecord(name, null, null, false);
        schema.setFields(
                Collections.singletonList(new Schema.Field("value", field, null, (Object) null)));
        return schema;
    }

    private static AvroToRowDataDeserializationSchema deserializer(LogicalType type) {
        return new AvroToRowDataDeserializationSchema(
                RowType.of(new LogicalType[] {type}, new String[] {"value"}),
                TypeInformation.of(RowData.class),
                new String[][] {{"value"}});
    }

    private static GenericRecord roundTrip(GenericRecord record) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(bytes, null);
        new GenericDatumWriter<GenericRecord>(record.getSchema()).write(record, encoder);
        encoder.flush();
        return new GenericDatumReader<GenericRecord>(record.getSchema())
                .read(null, DecoderFactory.get().binaryDecoder(bytes.toByteArray(), null));
    }
}
