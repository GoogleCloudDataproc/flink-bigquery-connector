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

package org.apache.flink.connector.bigquery.common.utils;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableCollection;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMultimap;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.FieldList;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** */
public class SchemaTransform {

    static final String NAMESPACE = "org.apache.flink.connector.bigquery";
    /**
     * Defines the valid mapping between BigQuery types and native Avro types.
     *
     * <p>Some BigQuery types are duplicated here since slightly different Avro records are produced
     * when exporting data in Avro format and when reading data directly using the read API.
     */
    static final ImmutableMultimap<String, Type> BIG_QUERY_TO_AVRO_TYPES =
            ImmutableMultimap.<String, Type>builder()
                    .put("STRING", Type.STRING)
                    .put("GEOGRAPHY", Type.STRING)
                    .put("BYTES", Type.BYTES)
                    .put("INTEGER", Type.LONG)
                    .put("INT64", Type.LONG)
                    .put("FLOAT", Type.DOUBLE)
                    .put("FLOAT64", Type.DOUBLE)
                    .put("NUMERIC", Type.BYTES)
                    .put("BIGNUMERIC", Type.BYTES)
                    .put("BOOLEAN", Type.BOOLEAN)
                    .put("BOOL", Type.BOOLEAN)
                    .put("TIMESTAMP", Type.LONG)
                    .put("RECORD", Type.RECORD)
                    .put("STRUCT", Type.RECORD)
                    .put("DATE", Type.STRING)
                    .put("DATE", Type.INT)
                    .put("DATETIME", Type.STRING)
                    .put("TIME", Type.STRING)
                    .put("TIME", Type.LONG)
                    .put("JSON", Type.STRING)
                    .build();

    public static Schema toGenericAvroSchema(
            String schemaName, List<TableFieldSchema> fieldSchemas, String namespace) {

        String nextNamespace =
                namespace == null ? null : String.format("%s.%s", namespace, schemaName);

        List<Field> avroFields = new ArrayList<>();
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
            if ("STRUCT".equals(field.getType()) || "RECORD".equals(field.getType())) {
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
    private static Field convertField(TableFieldSchema bigQueryField, String namespace) {
        ImmutableCollection<Type> avroTypes = BIG_QUERY_TO_AVRO_TYPES.get(bigQueryField.getType());
        if (avroTypes.isEmpty()) {
            throw new IllegalArgumentException(
                    "Unable to map BigQuery field type "
                            + bigQueryField.getType()
                            + " to avro type.");
        }

        Type avroType = avroTypes.iterator().next();
        Schema elementSchema;
        if (avroType == Type.RECORD) {
            elementSchema =
                    toGenericAvroSchema(
                            bigQueryField.getName(), bigQueryField.getFields(), namespace);
        } else {
            elementSchema = handleAvroLogicalTypes(bigQueryField, avroType);
        }
        Schema fieldSchema;
        if (bigQueryField.getMode() == null || "NULLABLE".equals(bigQueryField.getMode())) {
            fieldSchema = Schema.createUnion(Schema.create(Type.NULL), elementSchema);
        } else if ("REQUIRED".equals(bigQueryField.getMode())) {
            fieldSchema = elementSchema;
        } else if ("REPEATED".equals(bigQueryField.getMode())) {
            fieldSchema = Schema.createArray(elementSchema);
        } else {
            throw new IllegalArgumentException(
                    String.format("Unknown BigQuery Field Mode: %s", bigQueryField.getMode()));
        }
        return new Field(
                bigQueryField.getName(),
                fieldSchema,
                bigQueryField.getDescription(),
                (Object) null /* Cast to avoid deprecated JsonNode constructor. */);
    }

    private static Schema handleAvroLogicalTypes(TableFieldSchema bigQueryField, Type avroType) {
        String bqType = bigQueryField.getType();
        switch (bqType) {
            case "NUMERIC":
                // Default value based on
                // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
                int precision =
                        Optional.ofNullable(bigQueryField.getPrecision()).orElse(38L).intValue();
                int scale = Optional.ofNullable(bigQueryField.getScale()).orElse(9L).intValue();
                return LogicalTypes.decimal(precision, scale)
                        .addToSchema(Schema.create(Type.BYTES));
            case "BIGNUMERIC":
                // Default value based on
                // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
                int precisionBigNumeric =
                        Optional.ofNullable(bigQueryField.getPrecision()).orElse(77L).intValue();
                int scaleBigNumeric =
                        Optional.ofNullable(bigQueryField.getScale()).orElse(38L).intValue();
                return LogicalTypes.decimal(precisionBigNumeric, scaleBigNumeric)
                        .addToSchema(Schema.create(Type.BYTES));
            case "TIMESTAMP":
                return LogicalTypes.timestampMicros().addToSchema(Schema.create(Type.LONG));
            case "GEOGRAPHY":
                Schema geoSchema = Schema.create(Type.STRING);
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
                                                                .setMode(field.getMode().name())
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
