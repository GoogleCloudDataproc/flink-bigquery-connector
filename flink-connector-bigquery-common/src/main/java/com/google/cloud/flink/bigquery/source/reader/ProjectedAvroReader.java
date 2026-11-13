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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Avoids retaining decoded rows and fields that the source does not need. */
public final class ProjectedAvroReader {
    private final GenericDatumReader<GenericRecord> datumReader;
    private final BinaryDecoder decoder;

    public ProjectedAvroReader(Schema writerSchema, List<String> columns, AvroRows rows)
            throws IOException {
        this(writerSchema, projectSchema(writerSchema, columns), rows);
    }

    public ProjectedAvroReader(Schema writerSchema, Schema readerSchema, AvroRows rows)
            throws IOException {
        datumReader = new GenericDatumReader<>(writerSchema, readerSchema);
        decoder =
                DecoderFactory.get().binaryDecoder(rows.getSerializedBinaryRows().newInput(), null);
    }

    public static Schema projectSchema(Schema writerSchema, List<String> columns) {
        return columns.isEmpty() ? writerSchema : project(writerSchema, new HashSet<>(columns));
    }

    public boolean hasNext() throws IOException {
        return !decoder.isEnd();
    }

    public GenericRecord read() throws IOException {
        return datumReader.read(null, decoder);
    }

    private static Schema project(Schema schema, Set<String> paths) {
        switch (schema.getType()) {
            case UNION:
                return Schema.createUnion(
                        schema.getTypes().stream()
                                .map(branch -> project(branch, paths))
                                .collect(Collectors.toList()));
            case ARRAY:
                return Schema.createArray(project(schema.getElementType(), paths));
            case RECORD:
                Schema projected =
                        Schema.createRecord(
                                schema.getName(),
                                schema.getDoc(),
                                schema.getNamespace(),
                                schema.isError());
                schema.getAliases().forEach(projected::addAlias);
                schema.getObjectProps().forEach(projected::addProp);
                List<Schema.Field> fields = new ArrayList<>();
                for (Schema.Field field : schema.getFields()) {
                    if (paths.contains(field.name())) {
                        fields.add(new Schema.Field(field, field.schema()));
                    } else {
                        String prefix = field.name() + ".";
                        Set<String> children =
                                paths.stream()
                                        .filter(path -> path.startsWith(prefix))
                                        .map(path -> path.substring(prefix.length()))
                                        .collect(Collectors.toSet());
                        if (!children.isEmpty()) {
                            fields.add(new Schema.Field(field, project(field.schema(), children)));
                        }
                    }
                }
                projected.setFields(fields);
                return projected;
            default:
                return schema;
        }
    }
}
