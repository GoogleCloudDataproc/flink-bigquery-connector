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

import org.apache.flink.annotation.VisibleForTesting;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Utility class for transforming Avro {@link org.apache.avro.Schema} to BigQuery {@link Schema}.
 */
public class AvroToBigQuerySchemaTransform {

    // Private Constructor to ensure no instantiation.
    private AvroToBigQuerySchemaTransform() {}

    private static final Logger LOG = LoggerFactory.getLogger(AvroToBigQuerySchemaTransform.class);

    /**
     * Maximum nesting level for BigQuery schemas (15 levels). See <a
     * href="https://cloud.google.com/bigquery/docs/nested-repeated#limitations">BigQuery nested and
     * repeated field limitations</a>.
     */
    private static final int MAX_NESTED_LEVEL = 15;

    private static final Map<org.apache.avro.Schema.Type, StandardSQLTypeName>
            AVRO_TYPES_TO_BQ_TYPES;
    private static final Map<String, StandardSQLTypeName> LOGICAL_AVRO_TYPES_TO_BQ_TYPES;

    // ----------- Initialize Maps between Avro Schema to BigQuery schema -------------
    static {
        /*
         * Map Avro Schema Type to StandardSQLTypeName which converts AvroSchema
         * Primitive Type to StandardSQLTypeName.
         * AVRO_TYPES_TO_BQ_TYPES: containing mapping from Primitive Avro Schema Type to StandardSQLTypeName.
         */
        AVRO_TYPES_TO_BQ_TYPES = new EnumMap<>(org.apache.avro.Schema.Type.class);
        AVRO_TYPES_TO_BQ_TYPES.put(org.apache.avro.Schema.Type.INT, StandardSQLTypeName.INT64);
        AVRO_TYPES_TO_BQ_TYPES.put(org.apache.avro.Schema.Type.FIXED, StandardSQLTypeName.BYTES);
        AVRO_TYPES_TO_BQ_TYPES.put(org.apache.avro.Schema.Type.LONG, StandardSQLTypeName.INT64);
        AVRO_TYPES_TO_BQ_TYPES.put(org.apache.avro.Schema.Type.FLOAT, StandardSQLTypeName.FLOAT64);
        AVRO_TYPES_TO_BQ_TYPES.put(org.apache.avro.Schema.Type.DOUBLE, StandardSQLTypeName.FLOAT64);
        AVRO_TYPES_TO_BQ_TYPES.put(org.apache.avro.Schema.Type.STRING, StandardSQLTypeName.STRING);
        AVRO_TYPES_TO_BQ_TYPES.put(org.apache.avro.Schema.Type.BOOLEAN, StandardSQLTypeName.BOOL);
        AVRO_TYPES_TO_BQ_TYPES.put(org.apache.avro.Schema.Type.ENUM, StandardSQLTypeName.STRING);
        AVRO_TYPES_TO_BQ_TYPES.put(org.apache.avro.Schema.Type.BYTES, StandardSQLTypeName.BYTES);

        /*
         * Map Logical Avro Schema Type to StandardSQLTypeName Type, which converts
         * AvroSchema Logical Type to StandardSQLTypeName.
         * LOGICAL_AVRO_TYPES_TO_BQ_TYPES: Map containing mapping from Logical Avro Schema Type to StandardSQLTypeName.
         */
        LOGICAL_AVRO_TYPES_TO_BQ_TYPES = new HashMap<>();
        LOGICAL_AVRO_TYPES_TO_BQ_TYPES.put(LogicalTypes.date().getName(), StandardSQLTypeName.DATE);
        LOGICAL_AVRO_TYPES_TO_BQ_TYPES.put(
                LogicalTypes.timestampMicros().getName(), StandardSQLTypeName.TIMESTAMP);
        LOGICAL_AVRO_TYPES_TO_BQ_TYPES.put(
                LogicalTypes.timestampMillis().getName(), StandardSQLTypeName.TIMESTAMP);
        LOGICAL_AVRO_TYPES_TO_BQ_TYPES.put(
                LogicalTypes.uuid().getName(), StandardSQLTypeName.STRING);
        LOGICAL_AVRO_TYPES_TO_BQ_TYPES.put(
                LogicalTypes.timeMillis().getName(), StandardSQLTypeName.TIME);
        LOGICAL_AVRO_TYPES_TO_BQ_TYPES.put(
                LogicalTypes.timeMicros().getName(), StandardSQLTypeName.TIME);
        LOGICAL_AVRO_TYPES_TO_BQ_TYPES.put(
                LogicalTypes.localTimestampMillis().getName(), StandardSQLTypeName.DATETIME);
        LOGICAL_AVRO_TYPES_TO_BQ_TYPES.put(
                LogicalTypes.localTimestampMicros().getName(), StandardSQLTypeName.DATETIME);
        LOGICAL_AVRO_TYPES_TO_BQ_TYPES.put("geography_wkt", StandardSQLTypeName.GEOGRAPHY);
        LOGICAL_AVRO_TYPES_TO_BQ_TYPES.put("Json", StandardSQLTypeName.JSON);
    }

    /**
     * Converts an Avro schema to a BigQuery schema.
     *
     * <p>This method transforms an Avro {@link org.apache.avro.Schema} into a BigQuery {@link
     * Schema}. It iterates through the fields of the Avro schema, converts each field to its
     * BigQuery equivalent, and constructs a BigQuery schema with the resulting fields.
     *
     * <p>For each Avro field, the method extracts the field's name and documentation (if
     * available), and uses this information to build the corresponding BigQuery field.
     *
     * <p>The Avro schema must be of type Avro RECORD
     *
     * @param avroSchema The Avro schema to convert.
     * @return The converted BigQuery {@link Schema}.
     */
    public static Schema getBigQuerySchema(org.apache.avro.Schema avroSchema) {
        if (avroSchema == null) {
            throw new IllegalArgumentException(
                    "Could not convert avro schema of expected output to BigQuery table schema. The avro schema of the expected output record cannot be null");
        }
        if (avroSchema.getType() == null) {
            throw new IllegalArgumentException(
                    "Could not convert avro schema of expected output to BigQuery table schema. The avro schema of the output record must have a type");
        }
        if (avroSchema.getType() != org.apache.avro.Schema.Type.RECORD) {
            throw new IllegalArgumentException(
                    String.format(
                            "Could not convert avro schema of expected output to BigQuery table schema. Avro schema must be of type RECORD, found %s",
                            avroSchema.getType()));
        }
        if (avroSchema.getFields().isEmpty()) {
            throw new IllegalArgumentException(
                    "Could not convert avro schema of expected output to BigQuery table schema. The avro schema of the output record must have at least one field.");
        }
        // Iterate over each record field and add them to the BigQuery schema.
        List<Field> fields =
                avroSchema.getFields().stream()
                        .map(
                                avroField -> {
                                    if (avroField.name() == null) {
                                        throw new IllegalArgumentException(
                                                String.format(
                                                        "Could not convert avro field: %s of expected output to BigQuery table schema. The output avro field must have a name attribute to convert to a BigQuery table schema.",
                                                        avroField.toString()));
                                    }
                                    Field.Builder bigQueryFieldBuilder =
                                            convertAvroFieldToBigQueryField(
                                                            avroField.schema(), avroField.name(), 0)
                                                    .toBuilder();
                                    bigQueryFieldBuilder.setName(avroField.name());
                                    if (avroField.doc() != null) {
                                        bigQueryFieldBuilder.setDescription(avroField.doc());
                                    }
                                    return bigQueryFieldBuilder.build();
                                })
                        .collect(Collectors.toList());
        return Schema.of(fields);
    }

    /**
     * Converts an Avro field to a BigQuery field.
     *
     * @param avroField the Avro field to convert
     * @param name the name of the field
     * @param nestedLevel the current nesting level of the field
     */
    private static Field convertAvroFieldToBigQueryField(
            org.apache.avro.Schema avroField, String name, int nestedLevel) {
        if (nestedLevel > MAX_NESTED_LEVEL) {
            throw new IllegalArgumentException(
                    String.format(
                            "Could not convert avro field: %s of expected output to BigQuery table schema. Field nesting in expected output's schema exceeds 15. BigQuery allows nesting up to a depth of 15.",
                            name));
        }
        if (avroField == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "Could not convert avro field: %s of expected output to BigQuery table schema. The avro schema of the expected output record cannot be null.",
                            name));
        }
        if (avroField.getType() == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "Could not convert avro field: %s of expected output to BigQuery table schema. The avro schema of the output record must have a type",
                            name));
        }
        switch (avroField.getType()) {
            case RECORD:
                return convertAvroRecordFieldToBigQueryField(avroField, name, nestedLevel);
            case ARRAY:
                return convertAvroRepeatedFieldToBigQueryField(avroField, name, nestedLevel);
            case UNION:
                return convertAvroUnionFieldToBigQueryField(avroField, name, nestedLevel);
            case MAP:
                throw new UnsupportedOperationException(
                        getUnsupportedTypeErrorMessage(avroField.getType().toString(), name));
            default:
                return convertAvroRequiredFieldToBigQueryField(avroField, name);
        }
    }
    // --------------- Helper Functions to convert AvroSchema to BigQuerySchema ---------------

    /**
     * Converts an Avro record field to a BigQuery record field. This method iterates through each
     * field in the Avro record schema, recursively converts them to BigQuery fields, adding them as
     * sub-fields, and then constructs a BigQuery record field with the converted fields.
     *
     * @param avroSchema the Avro schema of the record field
     * @param name the name of the field
     * @param nestedLevel the current nesting level of the field
     * @return the converted BigQuery record field with nested fields.
     */
    private static Field convertAvroRecordFieldToBigQueryField(
            org.apache.avro.Schema avroSchema, String name, int nestedLevel) {
        List<Field> fields = new ArrayList<>();
        if (avroSchema.getFields().isEmpty()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Could not convert avro field: %s of expected output to BigQuery table schema. The avro schema of the output record must have at least one field.",
                            name));
        }
        // Iterate over each record field and obtain the nested record fields.
        for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
            org.apache.avro.Schema nestedAvroSchema = field.schema();
            fields.add(
                    convertAvroFieldToBigQueryField(
                            nestedAvroSchema, field.name(), nestedLevel + 1));
        }
        FieldList nestedBigQueryFields = FieldList.of(fields);
        return Field.newBuilder(name, LegacySQLTypeName.RECORD, nestedBigQueryFields)
                .setDescription((avroSchema.getDoc() != null) ? avroSchema.getDoc() : null)
                .setMode(Field.Mode.REQUIRED)
                .build();
    }

    /**
     * Helper function to convert the UNION type schema field to a BigQuery Field.
     *
     * <p>BigQuery supports nullable fields but not unions with multiple non-null types. This method
     * validates the Avro UNION schema to ensure it conforms to BigQuery's requirements.
     *
     * <p>Valid UNION schemas are:
     *
     * <ul>
     *   <li>["null", datatype] - Represents a nullable field.
     *   <li>["datatype", null] - Represents a nullable field.
     *   <li>[datatype] - Represents a non-nullable field.
     * </ul>
     *
     * <p>Invalid UNION schemas include:
     *
     * <ul>
     *   <li>["null"] - Only null type is not supported.
     *   <li>[datatype1, datatype2, ...] - Unions without a null type are not supported.
     *   <li>["null", ARRAY] Arrays in BigQuery cannot be nullable
     *   <li>[ARRAY, "null"] Arrays in BigQuery cannot be nullable
     * </ul>
     *
     * <p>If the UNION schema is valid, this method returns a BigQuery field with the schema of the
     * non-null datatype. Otherwise, it throws an {@link IllegalArgumentException}.
     *
     * @param avroSchema The Avro UNION schema to process.
     * @return The BigQuery field corresponding to the non-null type in the UNION.
     * @throws IllegalArgumentException if the UNION schema is invalid for BigQuery.
     */
    private static Field convertAvroUnionFieldToBigQueryField(
            org.apache.avro.Schema avroSchema, String name, int nestedLevel) {
        List<org.apache.avro.Schema> unionTypes = avroSchema.getTypes();

        // Case, when there is only a single type in UNION.
        // Can be ['null'] - ERROR
        // [Valid-Datatype] - Not Nullable, element type
        // Then it is essentially the same as not having a UNION.
        if (unionTypes.size() == 1
                && unionTypes.get(0).getType() != org.apache.avro.Schema.Type.NULL) {
            return convertAvroFieldToBigQueryField(unionTypes.get(0), name, nestedLevel);
        }

        // Extract all the nonNull Datatypes.
        // ['datatype, 'null'] and ['null', datatype] are only valid cases.
        if (unionTypes.size() == 2
                && ((unionTypes.get(0).getType() == org.apache.avro.Schema.Type.NULL
                                && unionTypes.get(1).getType() != org.apache.avro.Schema.Type.ARRAY)
                        || (unionTypes.get(1).getType() == org.apache.avro.Schema.Type.NULL
                                && unionTypes.get(0).getType()
                                        != org.apache.avro.Schema.Type.ARRAY))) {
            org.apache.avro.Schema actualDataType =
                    unionTypes.get(0).getType() == org.apache.avro.Schema.Type.NULL
                            ? unionTypes.get(1)
                            : unionTypes.get(0);
            return convertAvroNullableFieldToBigQueryField(actualDataType, name, nestedLevel);
        }

        /* UNION of type ARRAY is not supported.
        ARRAY is mapped to REPEATED type in Bigquery, which cannot be NULLABLE.
        If we have the datatype is ["null", "ARRAY"] or ["ARRAY", "null],
        IllegalArgumentException is thrown.
        For any other unsupported union types, exception is thrown. */
        throw new IllegalArgumentException(
                String.format(
                        "Could not convert schema of avro field: %s to BigQuery table schema. Found unsupported Avro Field of type UNION. Only supported types for BigQuery table schema are non-array types of \"['datatype'], ['null', 'datatype'] or ['datatype', 'null']\"",
                        name));
    }

    private static Field convertAvroNullableFieldToBigQueryField(
            org.apache.avro.Schema avroSchema, String name, int nestedLevel) {
        return convertAvroFieldToBigQueryField(avroSchema, name, nestedLevel)
                .toBuilder()
                .setMode(Field.Mode.NULLABLE)
                .build();
    }

    /**
     * Converts an Avro ARRAY field to a BigQuery REPEATED field.
     *
     * <p>The following restrictions imposed by BigQuery's schema definition:
     *
     * <ul>
     *   <li>Arrays of arrays are not supported.
     *   <li>Array cannot have a NULLABLE element.
     * </ul>
     *
     * <p>If any of these restrictions are violated, an {@link IllegalArgumentException} is thrown.
     *
     * @param avroSchema The Avro schema of the repeated field.
     * @param name The name of the field.
     * @param nestedLevel The nesting level of the field.
     * @return The converted BigQuery {@link Field} with mode set to {@link Field.Mode#REPEATED}.
     * @throws IllegalArgumentException if the Avro schema violates any of the restrictions for
     *     BigQuery repeated fields.
     */
    private static Field convertAvroRepeatedFieldToBigQueryField(
            org.apache.avro.Schema avroSchema, String name, int nestedLevel) {
        org.apache.avro.Schema arrayAvroSchema = avroSchema.getElementType();
        if (arrayAvroSchema.getType() == org.apache.avro.Schema.Type.ARRAY) {
            throw new IllegalArgumentException(
                    String.format(
                            "Could not convert schema of avro field: %s to BigQuery table schema. BigQuery ARRAY cannot have recursive ARRAY fields.",
                            name));
        }
        Field innerArrayField = convertAvroFieldToBigQueryField(arrayAvroSchema, name, nestedLevel);
        if (innerArrayField.getMode() != Field.Mode.REQUIRED) {
            throw new IllegalArgumentException(
                    String.format(
                            "Could not convert schema of avro field: %s to BigQuery table schema. Array cannot have a NULLABLE element in BigQuery",
                            name));
        }
        return innerArrayField.toBuilder().setMode(Field.Mode.REPEATED).build();
    }

    /**
     * Helper function convert Avro Field to BigQuery Field for Primitive and Logical Datatypes.
     *
     * <p><i>LOGICAL</i>: Use elementType.getProp() to obtain the string for the property name and
     * search for its corresponding mapping in the LOGICAL_AVRO_TYPES_TO_BQ_TYPES map.
     *
     * <p><i>PRIMITIVE</i>: If there is no match for the logical type (or there is no logical type
     * present), the field data type is attempted to be mapped to a PRIMITIVE type map.
     *
     * @param avroSchema the Avro schema of the required field
     * @param name the name of the field
     * @return the converted BigQuery field with the appropriate data type and mode set to REQUIRED
     * @throws UnsupportedOperationException if the Avro type is not supported
     * @throws IllegalArgumentException if the Avro schema is invalid for a decimal logical type
     */
    private static Field convertAvroRequiredFieldToBigQueryField(
            org.apache.avro.Schema avroSchema, String name)
            throws UnsupportedOperationException, IllegalArgumentException {
        @Nullable StandardSQLTypeName dataType;

        // Handle decimal logical types by extracting precision and setting the appropriate
        // StandardSQLTypeName.
        // The conversions are according to
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
        if ("decimal".equals(avroSchema.getProp(LogicalType.LOGICAL_TYPE_PROP))) {
            dataType = handleDecimalLogicalType(avroSchema, name);
            if (dataType == StandardSQLTypeName.NUMERIC
                    || dataType == StandardSQLTypeName.BIGNUMERIC) {
                // The precision and scale is also set in the BigQuery Field
                long precision =
                        ((LogicalTypes.Decimal) avroSchema.getLogicalType()).getPrecision();
                long scale = ((LogicalTypes.Decimal) avroSchema.getLogicalType()).getScale();
                return Field.newBuilder(name, dataType)
                        .setMode(Field.Mode.REQUIRED)
                        .setPrecision(precision)
                        .setScale(scale)
                        .build();
            }
        } else {
            dataType =
                    Optional.ofNullable(avroSchema.getProp(LogicalType.LOGICAL_TYPE_PROP))
                            .map(LOGICAL_AVRO_TYPES_TO_BQ_TYPES::get)
                            .orElse(AVRO_TYPES_TO_BQ_TYPES.get(avroSchema.getType()));
        }
        if (dataType == null) {
            throw new UnsupportedOperationException(
                    getUnsupportedTypeErrorMessage(avroSchema.getType().toString(), name));
        }

        return Field.newBuilder(name, dataType).setMode(Field.Mode.REQUIRED).build();
    }

    /**
     * Helper method to handle Avro decimal logical types by determining the appropriate BigQuery
     * data type based on the precision of the decimal.
     *
     * @param avroSchema the Avro schema of the decimal field
     * @param name the name of the field
     */
    private static StandardSQLTypeName handleDecimalLogicalType(
            org.apache.avro.Schema avroSchema, String name) {
        validatePrecisionAndScale(avroSchema, name);
        long precision = ((LogicalTypes.Decimal) avroSchema.getLogicalType()).getPrecision();
        long scale = ((LogicalTypes.Decimal) avroSchema.getLogicalType()).getScale();

        if (precision > 0 && precision <= (scale + 29) && scale <= 9) {
            return StandardSQLTypeName.NUMERIC;
        } else if (precision > 0 && precision <= (scale + 38) && scale <= 38) {
            return StandardSQLTypeName.BIGNUMERIC;
        }
        LOG.warn(
                "The precision {} and scale {} of decimal field {} is not supported by BigQuery. Converting the field to type STRING.",
                precision,
                scale,
                name);
        return StandardSQLTypeName.STRING;
    }

    /**
     * Validates the precision and scale of an Avro decimal logical type. Ensures precision and
     * scale are non-negative and that scale does not exceed precision.
     *
     * @param schema the Avro schema of the decimal field
     * @param name the name of the field
     * @throws IllegalArgumentException if precision or scale are invalid
     */
    private static void validatePrecisionAndScale(org.apache.avro.Schema schema, String name) {
        LogicalTypes.Decimal decimalLogicalSchema =
                ((LogicalTypes.Decimal) schema.getLogicalType());
        // The Avro schema of logical type "decimal" has "precision" and "scale" attributes
        // inherently defined so null checks for these attributes are not required.
        long precision = decimalLogicalSchema.getPrecision();
        long scale = decimalLogicalSchema.getScale();
        if (precision <= 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "Could not convert avro field: %s of expected output to BigQuery table schema.. Precision of decimal avro field must be non-negative. Saw: %d",
                            name, precision));
        }
        if (scale < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "Could not convert avro field: %s of expected output to BigQuery table schema. Scale of decimal avro field must be non-negative. Saw: %d",
                            name, scale));
        }
        if (precision < scale) {
            throw new IllegalArgumentException(
                    String.format(
                            "Could not convert avro field: %s of expected output to BigQuery table schema. Scale of the decimal avro field cannot exceed precision. Saw scale: %d, precision: %d",
                            name, scale, precision));
        }
    }

    @VisibleForTesting
    public static String getUnsupportedTypeErrorMessage(
            String unsupportedAvroType, String fieldName) {
        return String.format(
                "Could not convert avro field: %s of expected output to BigQuery table schema. The avro type: %s of the field is not supported by BigQuery",
                fieldName, unsupportedAvroType);
    }
}
