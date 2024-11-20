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

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import org.apache.avro.LogicalTypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Utility class for transforming Avro {@link org.apache.avro.Schema} to BigQuery {@link Schema}.
 */
public class AvroToBigQuerySchemaTransform {
    /**
     * Maximum nesting level for BigQuery schemas (15 levels). See <a
     * href="https://cloud.google.com/bigquery/docs/nested-repeated#limitations">BigQuery nested and
     * repeated field limitations</a>.
     */
    private static final int MAX_NESTED_LEVEL = 15;

    /**
     * A list of Avro schema TYPES that have a "name" property. We can only create BigQuery Fields
     * for these
     */
    private static final List<org.apache.avro.Schema.Type> AVRO_TYPES_WITH_NAME =
            new ArrayList<>(
                    Arrays.asList(
                            org.apache.avro.Schema.Type.RECORD,
                            org.apache.avro.Schema.Type.ENUM,
                            org.apache.avro.Schema.Type.FIXED));

    /**
     * Converts an Avro schema to a BigQuery schema.
     *
     * <p>This method transforms an Avro {@link org.apache.avro.Schema} into a BigQuery {@link
     * Schema}. It iterates through the fields of the Avro schema, converts each field to its
     * BigQuery equivalent, and constructs a BigQuery schema with the resulting fields.
     *
     * <p>For each Avro field, the method extracts the field's name, documentation (if available),
     * and default value (if available), and uses this information to build the corresponding
     * BigQuery field.
     *
     * <p>The Avro schema **must** define a name along with the datatype to be converted to a
     * BigQuery schema
     *
     * @param avroSchema The Avro schema to convert.
     * @return The converted BigQuery {@link Schema}.
     */
    public static Schema getBigQuerySchema(org.apache.avro.Schema avroSchema) {

        org.apache.avro.Schema avroSchemaWithFields = avroSchema;
        if (!AVRO_TYPES_WITH_NAME.contains(avroSchema.getType())) {
            throw new IllegalArgumentException(
                    "The Avro Schema must have named fields to convert to BigQuery columns. Found Schema: "
                            + avroSchema);
        }

        // Wrap the named schema without fields in a field as fields are iterated to generate BQ
        // Schema
        if (avroSchema.getType() != org.apache.avro.Schema.Type.RECORD) {
            List<org.apache.avro.Schema.Field> avroFields = new ArrayList<>();
            org.apache.avro.Schema avroSchemaWithWrappedNamedField =
                    org.apache.avro.Schema.createRecord(
                            "fieldContainingGenericRecordAvroSchema",
                            avroSchema.getDoc(),
                            avroSchema.getNamespace(),
                            false);
            avroFields.add(
                    new org.apache.avro.Schema.Field(
                            avroSchema.getName(), avroSchema, avroSchema.getDoc(), null));
            avroSchemaWithWrappedNamedField.setFields(avroFields);
            avroSchemaWithFields = avroSchemaWithWrappedNamedField;
        }

        List<Field> fields =
                avroSchemaWithFields.getFields().stream()
                        .map(
                                avroField -> {
                                    Field.Builder bigQueryFieldBuilder =
                                            convertAvroFieldToBigQueryField(
                                                            avroField.schema(), avroField.name(), 0)
                                                    .toBuilder();
                                    if (avroField.doc() != null) {
                                        bigQueryFieldBuilder.setDescription(avroField.doc());
                                    }
                                    if (avroField.hasDefaultValue()) {
                                        bigQueryFieldBuilder.setDefaultValueExpression(
                                                avroField.defaultVal().toString());
                                    }
                                    return bigQueryFieldBuilder.build();
                                })
                        .collect(Collectors.toList());
        return Schema.of(fields);
    }

    private static Field convertAvroFieldToBigQueryField(
            org.apache.avro.Schema avroSchema, String name, int nestedLevel) {
        org.apache.avro.Schema.Type type = avroSchema.getType();
        switch (type) {
            case ARRAY:
                return convertAvroRepeatedFieldToBigQueryField(
                        avroSchema.getElementType(), name, nestedLevel);
            case UNION:
                return convertAvroNullableFieldToBigQueryField(avroSchema, name, nestedLevel);
            case MAP:
                throw new UnsupportedOperationException(getErrorMessage(type.toString(), name));
            default:
                return convertAvroRequiredFieldToBigQueryField(avroSchema, name, nestedLevel);
        }
    }

    /**
     * Handles Avro UNION schema types for BigQuery compatibility.
     *
     * <p>BigQuery supports nullable fields but not unions with multiple non-null types. This method
     * validates the Avro UNION schema to ensure it conforms to BigQuery's requirements.
     *
     * <p>Valid UNION schemas are:
     *
     * <ul>
     *   <li>["null", datatype] - Represents a nullable field.
     *   <li>[datatype] - Represents a non-nullable field.
     * </ul>
     *
     * <p>Invalid UNION schemas include:
     *
     * <ul>
     *   <li>["null"] - Only null type is not supported.
     *   <li>[datatype1, datatype2, ...] - Unions without a null type are not supported.
     * </ul>
     *
     * <p>If the UNION schema is valid, this method returns a BigQuery field with the schema of the
     * non-null datatype. Otherwise, it throws an {@link UnsupportedOperationException}.
     *
     * @param avroSchema The Avro UNION schema to process.
     * @return The BigQuery field corresponding to the non-null type in the UNION.
     * @throws UnsupportedOperationException if the UNION schema is invalid for BigQuery.
     */
    private static Field convertAvroNullableFieldToBigQueryField(
            org.apache.avro.Schema avroSchema, String name, int nestedLevel)
            throws UnsupportedOperationException {
        List<org.apache.avro.Schema> unionTypes = avroSchema.getTypes();

        // Case, when there is only a single type in UNION.
        // Can be ['null'] - ERROR
        // [Valid-Datatype] - Not Nullable, element type
        // Then it is essentially the same as not having a UNION.
        if (unionTypes.size() == 1
                && unionTypes.get(0).getType() != org.apache.avro.Schema.Type.NULL) {
            return convertAvroFieldToBigQueryField(unionTypes.get(0), name, nestedLevel);
        }

        if (unionTypes.size() == 2) {
            // Extract all the nonNull Datatypes.
            // ['datatype, 'null'] and ['null', datatype] are only valid cases.
            org.apache.avro.Schema actualSchema = null;
            if (unionTypes.get(0).getType() != org.apache.avro.Schema.Type.NULL
                    && unionTypes.get(1).getType() == org.apache.avro.Schema.Type.NULL) {
                actualSchema = unionTypes.get(0);
            } else if (unionTypes.get(0).getType() == org.apache.avro.Schema.Type.NULL
                    && unionTypes.get(1).getType() != org.apache.avro.Schema.Type.NULL) {
                actualSchema = unionTypes.get(1);
            }

            if (actualSchema != null
                    && actualSchema.getType() == org.apache.avro.Schema.Type.ARRAY) {
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported Nullable Avro Field: %s of type ARRAY. BigQuery does not support ARRAY of Nullable type.",
                                name));
            }

            if (actualSchema != null) {
                return convertAvroFieldToBigQueryField(actualSchema, name, nestedLevel)
                        .toBuilder()
                        .setMode(Field.Mode.NULLABLE)
                        .build();
            }
        }
        throw new UnsupportedOperationException(
                String.format(
                        "Unsupported Avro Field: %s of type UNION. Only supported types are \"['datatype'] or ['null', 'datatype']\"",
                        name));
    }

    /**
     * Converts an Avro ARRAY field to a BigQuery REPEATED field.
     *
     * <p>This method handles the conversion of an Avro schema representing a repeated field to a
     * corresponding BigQuery {@link Field}. It enforces the following restrictions imposed by
     * BigQuery's schema definition:
     *
     * <ul>
     *   <li>Arrays of arrays are not supported.
     *   <li>Repeated fields cannot have nullable elements.
     *   <li>Repeated fields cannot be unions (i.e., have multiple data types).
     * </ul>
     *
     * <p>If any of these restrictions are violated, an {@link UnsupportedOperationException} is
     * thrown.
     *
     * @param avroSchema The Avro schema of the repeated field.
     * @param name The name of the field.
     * @param nestedLevel The nesting level of the field.
     * @return The converted BigQuery {@link Field} with mode set to {@link Field.Mode#REPEATED}.
     * @throws UnsupportedOperationException if the Avro schema violates any of the restrictions for
     *     BigQuery repeated fields.
     */
    private static Field convertAvroRepeatedFieldToBigQueryField(
            org.apache.avro.Schema avroSchema, String name, int nestedLevel) {
        if (avroSchema.getType() == org.apache.avro.Schema.Type.ARRAY) {
            throw new UnsupportedOperationException(
                    String.format(
                            "BigQuery ARRAY cannot have recursive ARRAY fields. Found recursive Array field: %s",
                            name));
        }
        return convertAvroFieldToBigQueryField(avroSchema, name, nestedLevel)
                .toBuilder()
                .setMode(Field.Mode.REPEATED)
                .build();
    }

    private static Field convertAvroRequiredFieldToBigQueryField(
            org.apache.avro.Schema avroSchema, String name, int nestedLevel)
            throws UnsupportedOperationException, IllegalArgumentException {
        if (nestedLevel > MAX_NESTED_LEVEL) {
            throw new UnsupportedOperationException(
                    String.format(
                            "BigQuery RECORD type can only be nested 15 times. Found nesting level of: %s",
                            nestedLevel));
        }
        StandardSQLTypeName dataType;
        org.apache.avro.Schema.Type type = avroSchema.getType();

        // handle logical type
        if (avroSchema.getProp(org.apache.avro.LogicalType.LOGICAL_TYPE_PROP) != null) {
            dataType = convertAvroLogicalTypeToBigQueryType(avroSchema, name);
            Field logicalBigQueryField =
                    Field.newBuilder(name, dataType).setMode(Field.Mode.REQUIRED).build();
            // Add precision to the "decimal" type data
            if (dataType == StandardSQLTypeName.NUMERIC
                    || dataType == StandardSQLTypeName.BIGNUMERIC) {
                long precision =
                        ((LogicalTypes.Decimal) avroSchema.getLogicalType()).getPrecision();
                return logicalBigQueryField.toBuilder().setPrecision(precision).build();
            }
            return logicalBigQueryField;
        }

        // handle  record type
        if (Objects.requireNonNull(type) == org.apache.avro.Schema.Type.RECORD) {
            List<Field> fields = new ArrayList<>();
            for (int i = 0; i < avroSchema.getFields().size(); i++) {
                org.apache.avro.Schema nestedAvroSchema =
                        avroSchema.getField(avroSchema.getFields().get(i).name()).schema();
                fields.add(
                        convertAvroFieldToBigQueryField(
                                nestedAvroSchema,
                                avroSchema.getFields().get(i).name(),
                                nestedLevel + 1));
            }
            FieldList nestedBigQueryFields = FieldList.of(fields);
            return Field.newBuilder(name, LegacySQLTypeName.RECORD, nestedBigQueryFields)
                    .setDescription((avroSchema.getDoc() != null) ? avroSchema.getDoc() : null)
                    .setMode(Field.Mode.REQUIRED)
                    .build();
        } else {
            dataType = convertAvroTypeToBigQueryType(type, name);
        }

        return Field.newBuilder(name, dataType).setMode(Field.Mode.REQUIRED).build();
    }

    private static StandardSQLTypeName convertAvroTypeToBigQueryType(
            org.apache.avro.Schema.Type type, String name) throws UnsupportedOperationException {
        switch (type) {
            case STRING:
            case ENUM:
                return StandardSQLTypeName.STRING;
            case BOOLEAN:
                return StandardSQLTypeName.BOOL;
            case INT:
            case LONG:
                return StandardSQLTypeName.INT64;
            case FLOAT:
            case DOUBLE:
                return StandardSQLTypeName.FLOAT64;
            case BYTES:
            case FIXED:
                return StandardSQLTypeName.BYTES;
            default:
                throw new UnsupportedOperationException(getErrorMessage(type.toString(), name));
        }
    }

    private static StandardSQLTypeName convertAvroLogicalTypeToBigQueryType(
            org.apache.avro.Schema schema, String name) {
        String logicalTypeString = schema.getProp(org.apache.avro.LogicalType.LOGICAL_TYPE_PROP);
        switch (logicalTypeString) {
            case "date":
                return StandardSQLTypeName.DATE;
            case "time-millis":
            case "time-micros":
                return StandardSQLTypeName.TIME;
            case "timestamp-millis":
            case "timestamp-micros":
                return StandardSQLTypeName.TIMESTAMP;
            case "local-timestamp-millis":
            case "local-timestamp-micros":
                return StandardSQLTypeName.DATETIME;
            case "duration":
                return StandardSQLTypeName.BYTES;
            case "decimal":
                long precision = validatePrecisionAndScale(schema, name);
                if (precision > 0 && precision <= 38) {
                    return StandardSQLTypeName.NUMERIC;
                } else if (precision > 38 && precision <= 77) {
                    return StandardSQLTypeName.BIGNUMERIC;
                }
                return StandardSQLTypeName.STRING;
            case "geography_wkt":
                return StandardSQLTypeName.GEOGRAPHY;
            case "uuid":
                return StandardSQLTypeName.STRING;
            case "Json":
                return StandardSQLTypeName.JSON;
            default:
                throw new UnsupportedOperationException(getErrorMessage(logicalTypeString, name));
        }
    }

    private static long validatePrecisionAndScale(org.apache.avro.Schema schema, String name) {
        long precision = ((LogicalTypes.Decimal) schema.getLogicalType()).getPrecision();
        long scale = ((LogicalTypes.Decimal) schema.getLogicalType()).getScale();
        if (precision <= 0) {
            throw new IllegalArgumentException(
                    "Precision of decimal field"
                            + name
                            + "must be non-negative. Saw: "
                            + precision);
        }
        if (scale < 0) {
            throw new IllegalArgumentException(
                    "Scale of decimal field" + name + "must be non-negative. Saw: " + scale);
        }
        if (precision < scale) {
            throw new IllegalArgumentException(
                    "Scale of the field "
                            + name
                            + "cannot exceed precision. Saw scale: "
                            + scale
                            + ", precision: "
                            + precision);
        }
        return precision;
    }

    private static String getErrorMessage(String unsupportedAvroType, String fieldName) {
        return String.format(
                "The avro type: %s of field: %s is not supported by BigQuery",
                unsupportedAvroType, fieldName);
    }
}
