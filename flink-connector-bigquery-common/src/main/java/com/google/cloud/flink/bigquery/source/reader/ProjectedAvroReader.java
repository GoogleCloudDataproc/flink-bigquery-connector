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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/** Avoids retaining decoded rows and fields that the source does not need. */
public final class ProjectedAvroReader {
    private static final byte[] NO_ROWS = new byte[0];

    private final GenericDatumReader<GenericRecord> datumReader;
    private BinaryDecoder decoder;

    public ProjectedAvroReader(Schema writerSchema, List<String> columns, AvroRows rows)
            throws IOException {
        this(writerSchema, projectSchema(writerSchema, columns), rows);
    }

    public ProjectedAvroReader(Schema writerSchema, Schema readerSchema, AvroRows rows)
            throws IOException {
        this(writerSchema, readerSchema);
        reset(rows);
    }

    /** Creates a reader without rows. Call {@link #reset(AvroRows)} to start decoding rows. */
    public ProjectedAvroReader(Schema writerSchema, Schema readerSchema) {
        datumReader = new GenericDatumReader<>(writerSchema, readerSchema);
        decoder = DecoderFactory.get().binaryDecoder(NO_ROWS, null);
    }

    /** Starts decoding the given rows, reusing this reader's datum reader and decoder. */
    public void reset(AvroRows rows) {
        decoder =
                DecoderFactory.get()
                        .binaryDecoder(rows.getSerializedBinaryRows().newInput(), decoder);
    }

    /** Drops the rows that are not decoded yet, and the reference to the response that has them. */
    public void clear() {
        decoder = DecoderFactory.get().binaryDecoder(NO_ROWS, decoder);
    }

    public boolean hasNext() throws IOException {
        return !decoder.isEnd();
    }

    public GenericRecord read() throws IOException {
        return datumReader.read(null, decoder);
    }

    /**
     * Returns the reader schema that keeps only the given column paths of the writer schema.
     *
     * <p>Paths use "." to select fields inside records, including records in nullable and repeated
     * fields. Paths match field names case-insensitively, are trimmed, and blank paths are ignored.
     * No paths keep all fields.
     *
     * @throws IllegalArgumentException if the list or a path is null, if a path does not match a
     *     field, or if a path selects a field inside a field that is not a record.
     */
    public static Schema projectSchema(Schema writerSchema, List<String> columns) {
        if (columns == null) {
            throw new IllegalArgumentException("The projected column paths must not be null.");
        }
        // BigQuery column names are case-insensitive, and the Storage API honours that in
        // selected fields, so match paths against the writer schema case-insensitively too.
        Set<String> paths = new HashSet<>();
        for (String column : columns) {
            if (column == null) {
                throw new IllegalArgumentException(
                        "The projected column paths must not contain null: " + columns);
            }
            String path = column.trim();
            if (!path.isEmpty()) {
                paths.add(lowerCase(path));
            }
        }
        if (paths.isEmpty()) {
            return writerSchema;
        }
        return new Projection(writerSchema, paths).project();
    }

    private static String lowerCase(String name) {
        return name.toLowerCase(Locale.ROOT);
    }

    /**
     * Builds one reader schema, giving each different projection of a named record its own name.
     */
    private static final class Projection {
        private final Schema writerSchema;
        private final Set<String> paths;
        private final Set<String> writerNames = new HashSet<>();
        // writer records that the reader schema keeps unchanged, under their own names
        private final Set<String> unchangedNames = new HashSet<>();
        private final Set<String> projectedNames = new HashSet<>();
        private final Map<String, List<Schema>> projectionsByWriterName = new HashMap<>();

        private Projection(Schema writerSchema, Set<String> paths) {
            this.writerSchema = writerSchema;
            this.paths = paths;
        }

        private Schema project() {
            collectNames(writerSchema, writerNames, newIdentitySet());
            // a whole selected field keeps the writer's records under their own names, so a
            // projected subset of one of them must take another name
            reserveWholeRecords(writerSchema, paths);
            return project(writerSchema, "", paths);
        }

        private void reserveWholeRecords(Schema schema, Set<String> paths) {
            switch (schema.getType()) {
                case UNION:
                    schema.getTypes().forEach(branch -> reserveWholeRecords(branch, paths));
                    return;
                case ARRAY:
                    reserveWholeRecords(schema.getElementType(), paths);
                    return;
                case RECORD:
                    for (Schema.Field field : schema.getFields()) {
                        String name = lowerCase(field.name());
                        if (paths.contains(name)) {
                            collectNames(field.schema(), unchangedNames, newIdentitySet());
                        } else {
                            Set<String> children = children(name, paths);
                            if (!children.isEmpty()) {
                                reserveWholeRecords(field.schema(), children);
                            }
                        }
                    }
                    return;
                default:
                    return;
            }
        }

        private Schema project(Schema schema, String prefix, Set<String> paths) {
            switch (schema.getType()) {
                case UNION:
                    return Schema.createUnion(
                            schema.getTypes().stream()
                                    .map(branch -> project(branch, prefix, paths))
                                    .collect(Collectors.toList()));
                case ARRAY:
                    Schema array =
                            Schema.createArray(project(schema.getElementType(), prefix, paths));
                    schema.getObjectProps().forEach(array::addProp);
                    return array;
                case RECORD:
                    return projectRecord(schema, prefix, paths);
                default:
                    return schema;
            }
        }

        private Schema projectRecord(Schema schema, String prefix, Set<String> paths) {
            Set<String> unmatched = new TreeSet<>(paths);
            List<Schema.Field> fields = new ArrayList<>();
            for (Schema.Field field : schema.getFields()) {
                String name = lowerCase(field.name());
                String childPrefix = name + ".";
                if (paths.contains(name)) {
                    unmatched.remove(name);
                    unmatched.removeIf(path -> path.startsWith(childPrefix));
                    fields.add(new Schema.Field(field, field.schema()));
                    continue;
                }
                Set<String> children = children(name, paths);
                if (children.isEmpty()) {
                    continue;
                }
                unmatched.removeIf(path -> path.startsWith(childPrefix));
                if (!containsRecord(field.schema())) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "The projected column path '%s%s%s' selects a field inside"
                                            + " '%s%s', which is not a record.",
                                    prefix, childPrefix, children.iterator().next(), prefix, name));
                }
                fields.add(
                        new Schema.Field(
                                field, project(field.schema(), prefix + childPrefix, children)));
            }
            if (!unmatched.isEmpty()) {
                throw new IllegalArgumentException(
                        String.format(
                                "The projected column paths %s do not match any field of the"
                                        + " Avro record '%s'.",
                                unmatched.stream()
                                        .map(path -> prefix + path)
                                        .collect(Collectors.toList()),
                                schema.getFullName()));
            }
            boolean unchanged = fields.equals(schema.getFields());
            if (unchanged && !projectedNames.contains(schema.getFullName())) {
                unchangedNames.add(schema.getFullName());
                return schema;
            }
            List<Schema> projections =
                    projectionsByWriterName.computeIfAbsent(
                            schema.getFullName(), name -> new ArrayList<>());
            for (Schema projection : projections) {
                if (projection.getFields().equals(fields)) {
                    return projection;
                }
            }
            Schema projected =
                    Schema.createRecord(
                            projectedName(schema, unchanged),
                            schema.getDoc(),
                            schema.getNamespace(),
                            schema.isError());
            schema.getAliases().forEach(projected::addAlias);
            if (!projected.getFullName().equals(schema.getFullName())) {
                // lets the resolving decoder match this record to the writer's record
                projected.addAlias(schema.getFullName());
            }
            schema.getObjectProps().forEach(projected::addProp);
            projected.setFields(fields);
            projections.add(projected);
            return projected;
        }

        private String projectedName(Schema schema, boolean unchanged) {
            if (!unchanged
                    && !unchangedNames.contains(schema.getFullName())
                    && projectedNames.add(schema.getFullName())) {
                return schema.getName();
            }
            for (int suffix = 1; ; suffix++) {
                String name = schema.getName() + "_projection" + suffix;
                String fullName =
                        schema.getNamespace() == null || schema.getNamespace().isEmpty()
                                ? name
                                : schema.getNamespace() + "." + name;
                if (!writerNames.contains(fullName) && projectedNames.add(fullName)) {
                    return name;
                }
            }
        }

        private static Set<String> children(String name, Set<String> paths) {
            String prefix = name + ".";
            return paths.stream()
                    .filter(path -> path.startsWith(prefix))
                    .map(path -> path.substring(prefix.length()))
                    .collect(Collectors.toCollection(TreeSet::new));
        }

        private static boolean containsRecord(Schema schema) {
            switch (schema.getType()) {
                case RECORD:
                    return true;
                case UNION:
                    return schema.getTypes().stream().anyMatch(Projection::containsRecord);
                case ARRAY:
                    return containsRecord(schema.getElementType());
                default:
                    return false;
            }
        }

        private static void collectNames(Schema schema, Set<String> names, Set<Schema> visited) {
            if (!visited.add(schema)) {
                return;
            }
            switch (schema.getType()) {
                case RECORD:
                    names.add(schema.getFullName());
                    schema.getFields().forEach(f -> collectNames(f.schema(), names, visited));
                    return;
                case ENUM:
                case FIXED:
                    names.add(schema.getFullName());
                    return;
                case UNION:
                    schema.getTypes().forEach(branch -> collectNames(branch, names, visited));
                    return;
                case ARRAY:
                    collectNames(schema.getElementType(), names, visited);
                    return;
                case MAP:
                    collectNames(schema.getValueType(), names, visited);
                    return;
                default:
                    return;
            }
        }

        private static Set<Schema> newIdentitySet() {
            return Collections.newSetFromMap(new IdentityHashMap<>());
        }
    }
}
