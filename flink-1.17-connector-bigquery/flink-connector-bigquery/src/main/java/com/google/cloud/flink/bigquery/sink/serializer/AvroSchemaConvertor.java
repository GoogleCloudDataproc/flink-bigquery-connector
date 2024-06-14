/*
 * Copyright (C) 2024 Google Inc.
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

package com.google.cloud.flink.bigquery.sink.serializer;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.util.Preconditions;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Source: <a href =
 * "https://github.com/apache/flink/blob/master/flink-formats/flink-avro/src/main/java/org/apache/flink/formats/avro/typeutils/AvroSchemaConverter.java">Source</a>
 * <br>
 * Modified for special BigQuery Types. Class to convert Avro{@link Schema} to {@link DataType}
 * Schema which could be further converted to Table API {@link org.apache.flink.table.api.Schema}.
 */
public class AvroSchemaConvertor {

    private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaConvertor.class);

    /**
     * Converts an Avro schema string into a nested row structure with deterministic field order and
     * data types that are compatible with Flink's Table & SQL API.
     *
     * @param avroSchemaString Avro schema definition string
     * @return data type matching the schema
     */
    public static DataType convertToDataType(String avroSchemaString) {
        Preconditions.checkNotNull(avroSchemaString, "Avro schema must not be null.");
        final Schema schema;
        try {
            schema = new Schema.Parser().parse(avroSchemaString);
        } catch (SchemaParseException e) {
            throw new IllegalArgumentException("Could not parse Avro schema string.", e);
        }
        return convertToDataType(schema);
    }

    private static DataType convertToDataType(Schema schema) {
        switch (schema.getType()) {
            case RECORD:
                final List<Schema.Field> schemaFields = schema.getFields();

                final DataTypes.Field[] fields = new DataTypes.Field[schemaFields.size()];
                for (int i = 0; i < schemaFields.size(); i++) {
                    final Schema.Field field = schemaFields.get(i);
                    fields[i] = DataTypes.FIELD(field.name(), convertToDataType(field.schema()));
                }
                return DataTypes.ROW(fields).notNull();
            case ARRAY:
                return DataTypes.ARRAY(convertToDataType(schema.getElementType())).notNull();
            case UNION:
                final Schema actualSchema;
                final boolean nullable;
                if (schema.getTypes().size() == 2) {
                    // UNION (Size 2) is of the type [datatype, `NULL`] or [`NULL`, datatype],
                    // actual datatype is derived from index 0 or 1 depending on the
                    // placement of `NULL`.
                    actualSchema =
                            schema.getTypes().get(0).getType() == Schema.Type.NULL
                                    ? schema.getTypes().get(1)
                                    : schema.getTypes().get(0);
                    nullable = true;
                } else if (schema.getTypes().size() == 1) {
                    actualSchema = schema.getTypes().get(0);
                    nullable = false;
                } else {
                    // use Kryo for serialization
                    return new AtomicDataType(
                            new TypeInformationRawType<>(false, Types.GENERIC(Object.class)));
                }
                DataType converted = convertToDataType(actualSchema);
                return nullable ? converted.nullable() : converted;
            case MAP:
                return DataTypes.MAP(
                                DataTypes.STRING().notNull(),
                                convertToDataType(schema.getValueType()))
                        .notNull();
            case STRING:
                DataType logicalDataType = handleLogicalTypeSchema(schema);
                if (logicalDataType != null) {
                    return logicalDataType;
                }
                return DataTypes.STRING().notNull();
            case ENUM:
                return DataTypes.STRING().notNull();
            case FIXED:
                // logical decimal type
                logicalDataType = handleLogicalTypeSchema(schema);
                if (logicalDataType != null) {
                    return logicalDataType;
                }
                // convert fixed size binary data to primitive byte arrays
                return DataTypes.VARBINARY(schema.getFixedSize()).notNull();
            case BYTES:
                // logical decimal type
                logicalDataType = handleLogicalTypeSchema(schema);
                if (logicalDataType != null) {
                    return logicalDataType;
                }
                return DataTypes.BYTES().notNull();
            case INT:
                // logical date and time type
                logicalDataType = handleLogicalTypeSchema(schema);
                if (logicalDataType != null) {
                    return logicalDataType;
                }
                return DataTypes.INT().notNull();
            case LONG:
                // logical timestamp type
                logicalDataType = handleLogicalTypeSchema(schema);
                if (logicalDataType != null) {
                    return logicalDataType;
                }
                return DataTypes.BIGINT().notNull();
            case FLOAT:
                return DataTypes.FLOAT().notNull();
            case DOUBLE:
                return DataTypes.DOUBLE().notNull();
            case BOOLEAN:
                return DataTypes.BOOLEAN().notNull();
            case NULL:
                return DataTypes.NULL();
        }
        LOG.info(
                "Unsupported Avro type '"
                        + schema.getType()
                        + "'. \nSupported types are NULL,"
                        + " BOOLEAN, DOUBLE, FLOAT, LONG, INT, BYTES, FIXED,"
                        + " ENUM, STRING, ENUM, MAP, UNION, ARRAY, RECORD ");
        throw new IllegalArgumentException("Unsupported Avro type '" + schema.getType() + "'.");
    }

    /**
     * Function to convert Avro Schema Field value to Data Type (Tale API Schema).
     *
     * @param fieldSchema Avro Schema describing the schema for the value.
     * @return Converted {@link DataType} value if supported logical type exists, NULL Otherwise.
     */
    private static DataType handleLogicalTypeSchema(Schema fieldSchema) {
        String logicalTypeString = fieldSchema.getProp(LogicalType.LOGICAL_TYPE_PROP);
        if (logicalTypeString != null) {
            // 1. In case, the Schema has a Logical Type.
            if (logicalTypeString.equals(LogicalTypes.date().getName())) {
                return DataTypes.DATE().notNull();
            } else if (logicalTypeString.equals(LogicalTypes.decimal(1).getName())) {
                final LogicalTypes.Decimal decimalType =
                        (LogicalTypes.Decimal) fieldSchema.getLogicalType();
                int precision = decimalType.getPrecision();
                // BIGNUMERIC type field from BigQuery does not contain the "is_numeric" tag.
                if (fieldSchema.getProp("isNumeric") != null && precision < 38 && precision > 0) {
                    return DataTypes.DECIMAL(precision, decimalType.getScale()).notNull();
                } else {
                    return DataTypes.BYTES().notNull();
                }
            } else if (logicalTypeString.equals(LogicalTypes.timestampMicros().getName())) {
                return DataTypes.TIMESTAMP(6).notNull();
            } else if (logicalTypeString.equals(LogicalTypes.timestampMillis().getName())) {
                return DataTypes.TIMESTAMP(3).notNull();
            } else if (logicalTypeString.equals(LogicalTypes.uuid().getName())) {
                return DataTypes.STRING().notNull();
            } else if (logicalTypeString.equals(LogicalTypes.timeMillis().getName())) {
                return DataTypes.TIME(3).notNull();
            } else if (logicalTypeString.equals((LogicalTypes.timeMicros().getName()))) {
                return DataTypes.TIME(6).notNull();
            } else if (logicalTypeString.equals(LogicalTypes.localTimestampMillis().getName())) {
                return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull();
            } else if (logicalTypeString.equals(LogicalTypes.localTimestampMicros().getName())) {
                /// TIMESTAMP_LTZ() => has precision 6 by default.
                return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE().notNull();
            } else if (logicalTypeString.equals("geography_wkt")
                    || logicalTypeString.equals("Json")) {
                return DataTypes.STRING().notNull();
            }
        }
        return null;
    }
}
