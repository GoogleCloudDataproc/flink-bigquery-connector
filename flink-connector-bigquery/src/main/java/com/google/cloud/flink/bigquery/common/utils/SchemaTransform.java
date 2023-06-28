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

package com.google.cloud.flink.bigquery.common.utils;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableCollection;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMultimap;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.FieldList;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A utility class that helps on the transformation of BigQuery {@link TableSchema} into Avro {@link
 * Schema}. Some methods are heavily influenced on the Apache Beam implementation (not externally
 * accessible methods).
 */
public class SchemaTransform {

    static final String NAMESPACE = "org.apache.flink.connector.bigquery";
    /**
     * Defines the valid mapping between BigQuery types and native Avro types.
     *
     * <p>Some BigQuery types are duplicated here since slightly different Avro records are produced
     * when exporting data in Avro format and when reading data directly using the read API.
     */
    static final ImmutableMultimap<String, Schema.Type> BIG_QUERY_TO_AVRO_TYPES =
            ImmutableMultimap.<String, Schema.Type>builder()
                    .put("STRING", Schema.Type.STRING)
                    .put("GEOGRAPHY", Schema.Type.STRING)
                    .put("BYTES", Schema.Type.BYTES)
                    .put("INTEGER", Schema.Type.LONG)
                    .put("INT64", Schema.Type.LONG)
                    .put("FLOAT", Schema.Type.DOUBLE)
                    .put("FLOAT64", Schema.Type.DOUBLE)
                    .put("NUMERIC", Schema.Type.BYTES)
                    .put("BIGNUMERIC", Schema.Type.BYTES)
                    .put("BOOLEAN", Schema.Type.BOOLEAN)
                    .put("BOOL", Schema.Type.BOOLEAN)
                    .put("TIMESTAMP", Schema.Type.LONG)
                    .put("RECORD", Schema.Type.RECORD)
                    .put("STRUCT", Schema.Type.RECORD)
                    .put("DATE", Schema.Type.STRING)
                    .put("DATE", Schema.Type.INT)
                    .put("DATETIME", Schema.Type.STRING)
                    .put("TIME", Schema.Type.STRING)
                    .put("TIME", Schema.Type.LONG)
                    .put("JSON", Schema.Type.STRING)
                    .build();

    public static Schema toGenericAvroSchema(
            String schemaName, List<TableFieldSchema> fieldSchemas, String namespace) {

        String nextNamespace =
                namespace == null ? null : String.format("%s.%s", namespace, schemaName);

        List<Schema.Field> avroFields = new ArrayList<>();
        for (TableFieldSchema bigQueryField : fieldSchemas) {
            avroFields.add(convertField(bigQueryField, nextNamespace));
        }
        return Schema.createRecord(
                schemaName,
                "Translated Avro Schema for " + schemaName,
                namespace == null ? NAMESPACE : namespace,
                false,
                avroFields);
    }

    public static Schema toGenericAvroSchema(
            String schemaName, List<TableFieldSchema> fieldSchemas) {
        return toGenericAvroSchema(
                schemaName, fieldSchemas, hasNamespaceCollision(fieldSchemas) ? NAMESPACE : null);
    }

    // To maintain backwards compatibility we only disambiguate collisions in the field namespaces
    // as these never worked with this piece of code.
    private static boolean hasNamespaceCollision(List<TableFieldSchema> fieldSchemas) {
        Set<String> recordTypeFieldNames = new HashSet<>();

        List<TableFieldSchema> fieldsToCheck = new ArrayList<>();
        for (fieldsToCheck.addAll(fieldSchemas); !fieldsToCheck.isEmpty(); ) {
            TableFieldSchema field = fieldsToCheck.remove(0);
            if (field.getType().equals("STRUCT") || field.getType().equals("RECORD")) {
                if (recordTypeFieldNames.contains(field.getName())) {
                    return true;
                }
                recordTypeFieldNames.add(field.getName());
                fieldsToCheck.addAll(field.getFields());
            }
        }

        // No collisions present
        return false;
    }

    @SuppressWarnings({
        "nullness" // Avro library not annotated
    })
    private static Schema.Field convertField(TableFieldSchema bigQueryField, String namespace) {
        ImmutableCollection<Schema.Type> avroTypes =
                BIG_QUERY_TO_AVRO_TYPES.get(bigQueryField.getType());
        if (avroTypes.isEmpty()) {
            throw new IllegalArgumentException(
                    "Unable to map BigQuery field type "
                            + bigQueryField.getType()
                            + " to avro type.");
        }

        Schema.Type avroType = avroTypes.iterator().next();
        Schema elementSchema;
        if (avroType == Schema.Type.RECORD) {
            elementSchema =
                    toGenericAvroSchema(
                            bigQueryField.getName(), bigQueryField.getFields(), namespace);
        } else {
            elementSchema = handleAvroLogicalTypes(bigQueryField, avroType);
        }
        Schema fieldSchema;
        if (bigQueryField.getMode() == null || bigQueryField.getMode().equals("NULLABLE")) {
            fieldSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), elementSchema);
        } else if (Objects.equals(bigQueryField.getMode(), "REQUIRED")) {
            fieldSchema = elementSchema;
        } else if (bigQueryField.getMode().equals("REPEATED")) {
            fieldSchema = Schema.createArray(elementSchema);
        } else {
            throw new IllegalArgumentException(
                    String.format("Unknown BigQuery Field Mode: %s", bigQueryField.getMode()));
        }
        return new Schema.Field(
                bigQueryField.getName(),
                fieldSchema,
                bigQueryField.getDescription(),
                (Object) null /* Cast to avoid deprecated JsonNode constructor. */);
    }

    private static Schema handleAvroLogicalTypes(
            TableFieldSchema bigQueryField, Schema.Type avroType) {
        String bqType = bigQueryField.getType();
        switch (bqType) {
            case "NUMERIC":
                // Default value based on
                // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
                int precision =
                        Optional.ofNullable(bigQueryField.getPrecision()).orElse(38L).intValue();
                int scale = Optional.ofNullable(bigQueryField.getScale()).orElse(9L).intValue();
                return LogicalTypes.decimal(precision, scale)
                        .addToSchema(Schema.create(Schema.Type.BYTES));
            case "BIGNUMERIC":
                // Default value based on
                // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
                int precisionBigNumeric =
                        Optional.ofNullable(bigQueryField.getPrecision()).orElse(77L).intValue();
                int scaleBigNumeric =
                        Optional.ofNullable(bigQueryField.getScale()).orElse(38L).intValue();
                return LogicalTypes.decimal(precisionBigNumeric, scaleBigNumeric)
                        .addToSchema(Schema.create(Schema.Type.BYTES));
            case "TIMESTAMP":
                return LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
            case "GEOGRAPHY":
                Schema geoSchema = Schema.create(Schema.Type.STRING);
                geoSchema.addProp(LogicalType.LOGICAL_TYPE_PROP, "geography_wkt");
                return geoSchema;
            default:
                return Schema.create(avroType);
        }
    }

    static List<TableFieldSchema> fieldListToListOfTableFieldSchema(FieldList fieldList) {
        return Optional.ofNullable(fieldList)
                .map(
                        fList ->
                                fList.stream()
                                        .map(
                                                field ->
                                                        new TableFieldSchema()
                                                                .setName(field.getName())
                                                                .setDescription(
                                                                        field.getDescription())
                                                                .setDefaultValueExpression(
                                                                        field
                                                                                .getDefaultValueExpression())
                                                                .setCollation(field.getCollation())
                                                                .setMode(
                                                                        Optional.ofNullable(
                                                                                        field
                                                                                                .getMode())
                                                                                .map(m -> m.name())
                                                                                .orElse(null))
                                                                .setType(field.getType().name())
                                                                .setFields(
                                                                        fieldListToListOfTableFieldSchema(
                                                                                field
                                                                                        .getSubFields())))
                                        .collect(Collectors.toList()))
                .orElse(Lists.newArrayList());
    }

    /**
     * Transforms a BigQuery {@link com.google.cloud.bigquery.Schema} into a {@link TableSchema}.
     *
     * @param schema the schema from the API.
     * @return a TableSchema instance.
     */
    public static TableSchema bigQuerySchemaToTableSchema(com.google.cloud.bigquery.Schema schema) {
        return new TableSchema().setFields(fieldListToListOfTableFieldSchema(schema.getFields()));
    }
}
