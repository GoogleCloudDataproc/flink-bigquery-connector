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
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.util.Preconditions;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Source: <a href =
 * "https://github.com/apache/flink/blob/master/flink-formats/flink-avro/src/main/java/org/apache/flink/formats/avro/typeutils/AvroSchemaConverter.java">Source</a>
 * <br>
 * Modified for special BigQuery Types. Class to convert Avro{@link Schema} to {@link DataType}
 * Schema which could be further converted to Table API {@link org.apache.flink.table.api.Schema}.
 */
public class AvroSchemaConvertor {

    private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaConvertor.class);

    private final ConcurrentMap<Schema, DataType> avroToDataTypeConversionMemorizationMap =
            new ConcurrentHashMap<>();

    private final ConcurrentMap<LogicalType, Schema> logicalToAvroTypeConversionMemorizationMap =
            new ConcurrentHashMap<>();

    /**
     * Converts an Avro schema string into a nested row structure with deterministic field order and
     * data types that are compatible with Flink's Table & SQL API.
     *
     * @param avroSchemaString Avro schema definition string
     * @return data type matching the schema
     */
    public DataType convertToDataType(String avroSchemaString) {
        Preconditions.checkNotNull(avroSchemaString, "Avro schema must not be null.");
        final Schema schema;
        try {
            schema = new Schema.Parser().parse(avroSchemaString);
        } catch (SchemaParseException e) {
            throw new IllegalArgumentException("Could not parse Avro schema string.", e);
        }
        return this.convertToDataType(schema);
    }

    private DataType convertToDataType(Schema schema) {
        DataType convertedDataType = this.getFromAvroToDataTypeConversionMap(schema);
        if (convertedDataType != null) {
            return convertedDataType;
        }
        DataType dataType;
        switch (schema.getType()) {
            case RECORD:
                final List<Schema.Field> schemaFields = schema.getFields();
                final DataTypes.Field[] fields = new DataTypes.Field[schemaFields.size()];
                for (int i = 0; i < schemaFields.size(); i++) {
                    final Schema.Field field = schemaFields.get(i);
                    fields[i] = DataTypes.FIELD(field.name(), convertToDataType(field.schema()));
                }
                dataType = DataTypes.ROW(fields).notNull();
                break;
            case ARRAY:
                dataType = DataTypes.ARRAY(convertToDataType(schema.getElementType())).notNull();
                break;
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
                dataType = nullable ? converted.nullable() : converted;
                break;
            case MAP:
                dataType =
                        DataTypes.MAP(
                                        DataTypes.STRING().notNull(),
                                        convertToDataType(schema.getValueType()))
                                .notNull();
                break;
            case STRING:
                DataType logicalDataType = handleLogicalTypeSchema(schema);
                if (logicalDataType != null) {
                    return logicalDataType;
                }
                dataType = DataTypes.STRING().notNull();
                break;
            case ENUM:
                dataType = DataTypes.STRING().notNull();
                break;
            case FIXED:
                // logical decimal type
                logicalDataType = handleLogicalTypeSchema(schema);
                if (logicalDataType != null) {
                    addToAvroToDataTypeConversionMap(schema, logicalDataType);
                    return logicalDataType;
                }
                // convert fixed size binary data to primitive byte arrays
                dataType = DataTypes.VARBINARY(schema.getFixedSize()).notNull();
                break;
            case BYTES:
                // logical decimal type
                logicalDataType = handleLogicalTypeSchema(schema);
                if (logicalDataType != null) {
                    addToAvroToDataTypeConversionMap(schema, logicalDataType);
                    return logicalDataType;
                }
                dataType = DataTypes.BYTES().notNull();
                break;
            case INT:
                // logical date and time type
                logicalDataType = handleLogicalTypeSchema(schema);
                if (logicalDataType != null) {
                    addToAvroToDataTypeConversionMap(schema, logicalDataType);
                    return logicalDataType;
                }
                dataType = DataTypes.INT().notNull();
                break;
            case LONG:
                // logical timestamp type
                logicalDataType = handleLogicalTypeSchema(schema);
                if (logicalDataType != null) {
                    addToAvroToDataTypeConversionMap(schema, logicalDataType);
                    return logicalDataType;
                }
                dataType = DataTypes.BIGINT().notNull();
                break;
            case FLOAT:
                dataType = DataTypes.FLOAT().notNull();
                break;
            case DOUBLE:
                dataType = DataTypes.DOUBLE().notNull();
                break;
            case BOOLEAN:
                dataType = DataTypes.BOOLEAN().notNull();
                break;
            case NULL:
                dataType = DataTypes.NULL();
                break;
            default:
                LOG.info(
                        "Unsupported Avro type '"
                                + schema.getType()
                                + "'. \nSupported types are NULL,"
                                + " BOOLEAN, DOUBLE, FLOAT, LONG, INT, BYTES, FIXED,"
                                + " ENUM, STRING, ENUM, MAP, UNION, ARRAY, RECORD ");
                throw new IllegalArgumentException(
                        "Unsupported Avro type '" + schema.getType() + "'.");
        }
        this.addToAvroToDataTypeConversionMap(schema, dataType);
        return dataType;
    }

    /**
     * Wrapper to add to map. Converted to a function to enable cache efficiency testing.
     *
     * @param schema {@link Schema} object being converted.
     * @param dataType Converted {@link DataType} object.
     */
    private void addToAvroToDataTypeConversionMap(Schema schema, DataType dataType) {
        this.avroToDataTypeConversionMemorizationMap.putIfAbsent(schema, dataType);
    }

    /**
     * Wrapper to get from a map. Converted to a function to enable cache efficiency testing.
     *
     * @param schema {@link Schema} object being converted.
     * @return Converted {@link DataType} object.
     */
    @Nullable
    private DataType getFromAvroToDataTypeConversionMap(Schema schema) {
        return this.avroToDataTypeConversionMemorizationMap.getOrDefault(schema, null);
    }

    /**
     * Function to convert Avro Schema Field value to Data Type (Tale API Schema).
     *
     * @param fieldSchema Avro Schema describing the schema for the value.
     * @return Converted {@link DataType} value if supported logical type exists, NULL Otherwise.
     */
    private static DataType handleLogicalTypeSchema(Schema fieldSchema) {
        String logicalTypeString =
                fieldSchema.getProp(org.apache.avro.LogicalType.LOGICAL_TYPE_PROP);
        if (logicalTypeString != null) {
            // 1. In case, the Schema has a Logical Type.
            if (logicalTypeString.equals(LogicalTypes.date().getName())) {
                return DataTypes.DATE().notNull();
            } else if (logicalTypeString.equals(LogicalTypes.decimal(1).getName())) {
                final LogicalTypes.Decimal decimalType =
                        (LogicalTypes.Decimal) fieldSchema.getLogicalType();
                int precision = decimalType.getPrecision();
                // BIGNUMERIC type field from BigQuery does not contain the "is_numeric" tag.
                if (fieldSchema.getObjectProp("isNumeric") != null
                        && precision < 39
                        && precision > 0) {
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

    /**
     * Converts Flink SQL {@link LogicalType} (can be nested) into an Avro schema.
     *
     * <p>Use "org.apache.flink.avro.generated.record" as the type name.
     *
     * @param schema the schema type, usually it should be the top level record type, e.g. not a
     *     nested type
     * @return Avro's {@link Schema} matching this logical type.
     */
    public Schema convertToSchema(LogicalType schema) {
        return convertToSchema(schema, "org.apache.flink.avro.generated.record");
    }

    /**
     * Converts Flink SQL {@link LogicalType} (can be nested) into an Avro schema.
     *
     * <p>The "{rowName}_" is used as the nested row type name prefix in order to generate the right
     * schema. Nested record type that only differs with type name is still compatible.
     *
     * @param logicalType logical type
     * @param rowName the record name
     * @return Avro's {@link Schema} matching this logical type.
     */
    public Schema convertToSchema(LogicalType logicalType, String rowName) {
        Schema convertedSchema = getFromLogicalToAvroTypeConversionMap(logicalType);
        if (convertedSchema != null) {
            return convertedSchema;
        }
        Schema avroSchema;
        boolean nullable = logicalType.isNullable();
        switch (logicalType.getTypeRoot()) {
            case NULL:
                avroSchema = SchemaBuilder.builder().nullType();
                break;
            case BOOLEAN:
                Schema booleanType = SchemaBuilder.builder().booleanType();
                avroSchema = nullable ? nullableSchema(booleanType) : booleanType;
                break;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                Schema intType = SchemaBuilder.builder().intType();
                avroSchema = nullable ? nullableSchema(intType) : intType;
                break;
            case BIGINT:
                Schema longType = SchemaBuilder.builder().longType();
                avroSchema = nullable ? nullableSchema(longType) : longType;
                break;
            case FLOAT:
                Schema floatType = SchemaBuilder.builder().floatType();
                avroSchema = nullable ? nullableSchema(floatType) : floatType;
                break;
            case DOUBLE:
                Schema doubleType = SchemaBuilder.builder().doubleType();
                avroSchema = nullable ? nullableSchema(doubleType) : doubleType;
                break;
            case CHAR:
            case VARCHAR:
                Schema stringType = SchemaBuilder.builder().stringType();
                avroSchema = nullable ? nullableSchema(stringType) : stringType;
                break;
            case BINARY:
            case VARBINARY:
                Schema binaryType = SchemaBuilder.builder().bytesType();
                avroSchema = nullable ? nullableSchema(binaryType) : binaryType;
                break;
            case DATE:
                // use int to represent Date
                Schema dateType =
                        LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType());
                avroSchema = nullable ? nullableSchema(dateType) : dateType;
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIME_WITHOUT_TIME_ZONE:
                Schema schema = getAvroLogicalType(logicalType);
                avroSchema = nullable ? nullableSchema(schema) : schema;
                break;
            case DECIMAL:
                Schema decimalSchema = getDecimalSchema(logicalType);
                avroSchema = nullable ? nullableSchema(decimalSchema) : decimalSchema;
                break;
            case ROW:
                Schema rowSchema = getRowSchema(logicalType, rowName);
                avroSchema = nullable ? nullableSchema(rowSchema) : rowSchema;
                break;
            case MULTISET:
            case MAP:
                Schema mapType =
                        SchemaBuilder.builder()
                                .map()
                                .values(
                                        convertToSchema(
                                                extractValueTypeToAvroMap(logicalType), rowName));
                avroSchema = nullable ? nullableSchema(mapType) : mapType;
                break;
            case ARRAY:
                ArrayType arrayType = (ArrayType) logicalType;
                Schema array =
                        SchemaBuilder.builder()
                                .array()
                                .items(convertToSchema(arrayType.getElementType(), rowName));
                avroSchema = nullable ? nullableSchema(array) : array;
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported to derive Schema for type: " + logicalType);
        }
        addToLogicalToAvroTypeConversionMap(logicalType, avroSchema);
        return avroSchema;
    }

    // -------------------- Helper Methods to handle various schema types --------------------------
    /**
     * Method to convert {@link RowType} Schema to {@link Schema}.
     *
     * @param logicalType {@link LogicalType} Schema to be converted.
     * @return {@link Schema} of the converted avro Type.
     */
    private Schema getRowSchema(LogicalType logicalType, String rowName) {
        RowType rowType = (RowType) logicalType;
        // we have to make sure the record name is different in a Schema
        SchemaBuilder.FieldAssembler<Schema> builder =
                SchemaBuilder.builder().record(rowName).fields();

        for (RowField field : rowType.getFields()) {
            String fieldName = field.getName();
            LogicalType fieldType = field.getType();
            SchemaBuilder.GenericDefault<Schema> fieldBuilder =
                    builder.name(fieldName)
                            .type(this.convertToSchema(fieldType, rowName + "_" + fieldName));
            builder = fieldBuilder.noDefault();
        }
        return builder.endRecord();
    }

    /**
     * Method to convert {@link DecimalType} Schema to {@link Schema}.
     *
     * @param logicalType {@link LogicalType} Schema to be converted.
     * @return {@link Schema} of the converted avro Type.
     */
    private static Schema getDecimalSchema(LogicalType logicalType) {
        DecimalType decimalType = (DecimalType) logicalType;
        int precision = decimalType.getPrecision();
        // store BigDecimal as byte[]
        Schema decimal =
                LogicalTypes.decimal(precision, decimalType.getScale())
                        .addToSchema(SchemaBuilder.builder().bytesType());
        if (precision <= 38 && precision > 0) {
            decimal.addProp("isNumeric", true);
        }
        return decimal;
    }

    /**
     * Method to convert Time, Local Timestamp and Timestamp Datatypes to Avro schema. It checks the
     * underlying type and invokes their respective handling methods.
     *
     * @param logicalType {@link LogicalType} Schema to be converted.
     * @return {@link Schema} of the converted avro Type.
     */
    private static Schema getAvroLogicalType(LogicalType logicalType) {
        if (logicalType instanceof TimestampType) {
            // use long to represent Timestamp
            int precision = ((TimestampType) logicalType).getPrecision();
            org.apache.avro.LogicalType avroLogicalType = getAvroLogicalTimestampType(precision);
            return avroLogicalType.addToSchema(SchemaBuilder.builder().longType());
        } else if (logicalType instanceof TimeType) {
            int precision = ((TimeType) logicalType).getPrecision();
            return getTimeType(precision);
        } else if (logicalType instanceof LocalZonedTimestampType) {
            int precision = ((LocalZonedTimestampType) logicalType).getPrecision();
            org.apache.avro.LogicalType avroLogicalDateTimeType =
                    getAvroLogicalDateTimeType(precision);
            // use int to represent Datetime, we only support millisecond/microsecond when
            // deserialization
            return avroLogicalDateTimeType.addToSchema(SchemaBuilder.builder().longType());
        } else {
            throw new IllegalArgumentException("Invalid logical type obtained!. Cannot convert.");
        }
    }

    /**
     * Method to convert {@link LocalZonedTimestampType} Schema to {@link Schema}.
     *
     * @param precision Precision of the passed Datetime(Local Timestamp) Type.
     * @return {@link Schema} of the converted avro Type.
     */
    private static org.apache.avro.LogicalType getAvroLogicalDateTimeType(int precision) {
        org.apache.avro.LogicalType avroLogicalDateTimeType;
        if (precision <= 3) {
            avroLogicalDateTimeType = LogicalTypes.localTimestampMillis();
        } else if (precision <= 6) {
            avroLogicalDateTimeType = LogicalTypes.localTimestampMicros();
        } else {
            throw new IllegalArgumentException(
                    "Avro does not support DATETIME type with precision: "
                            + precision
                            + ", it only supports precision less than equal to 6.");
        }
        return avroLogicalDateTimeType;
    }

    /**
     * Method to convert {@link TimestampType} Schema to {@link Schema}.
     *
     * @param precision Precision of the passed Timestamp Type.
     * @return {@link Schema} of the converted avro Type.
     */
    private static org.apache.avro.LogicalType getAvroLogicalTimestampType(int precision) {
        org.apache.avro.LogicalType avroLogicalType;
        if (precision <= 3) {
            avroLogicalType = LogicalTypes.timestampMillis();
        } else if (precision <= 6) {
            avroLogicalType = LogicalTypes.timestampMicros();
        } else {
            throw new IllegalArgumentException(
                    "Avro does not support TIMESTAMP type "
                            + "with precision: "
                            + precision
                            + ", it only supports precision less than equal to 6.");
        }
        return avroLogicalType;
    }

    /**
     * Method to convert {@link TimeType} Schema to {@link Schema}.
     *
     * @param precision Precision of the passed Time Type.
     * @return {@link Schema} of the converted avro Type.
     */
    private static Schema getTimeType(int precision) {
        org.apache.avro.LogicalType avroLogicalTimeType;
        Schema avroSchemaTimeType;
        if (precision <= 3) {
            avroLogicalTimeType = LogicalTypes.timeMillis();
            avroSchemaTimeType = SchemaBuilder.builder().intType();
        } else if (precision <= 6) {
            avroLogicalTimeType = LogicalTypes.timeMicros();
            avroSchemaTimeType = SchemaBuilder.builder().longType();
        } else {
            throw new IllegalArgumentException(
                    "Avro does not support TIME type with precision: "
                            + precision
                            + ", it only supports precision less than equal to 6.");
        }
        return avroLogicalTimeType.addToSchema(avroSchemaTimeType);
    }

    public static LogicalType extractValueTypeToAvroMap(LogicalType type) {
        LogicalType keyType;
        LogicalType valueType;
        if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            keyType = mapType.getKeyType();
            valueType = mapType.getValueType();
        } else {
            MultisetType multisetType = (MultisetType) type;
            keyType = multisetType.getElementType();
            valueType = new IntType();
        }
        if (!keyType.is(LogicalTypeFamily.CHARACTER_STRING)) {
            throw new UnsupportedOperationException(
                    "Avro format doesn't support non-string as key type of map. "
                            + "The key type is: "
                            + keyType.asSummaryString());
        }
        return valueType;
    }

    /**
     * Wrapper to get from a map. Converted to a function to enable cache efficiency testing.
     *
     * @param logicalType {@link LogicalType} object being converted.
     * @return Converted {@link Schema} object.
     */
    @Nullable
    private Schema getFromLogicalToAvroTypeConversionMap(LogicalType logicalType) {
        return this.logicalToAvroTypeConversionMemorizationMap.getOrDefault(logicalType, null);
    }

    /**
     * Wrapper to add to map. Converted to a function to enable cache efficiency testing.
     *
     * @param logicalType {@link LogicalType} object being converted.
     * @param schema Converted {@link Schema} object.
     */
    private void addToLogicalToAvroTypeConversionMap(LogicalType logicalType, Schema schema) {
        this.logicalToAvroTypeConversionMemorizationMap.putIfAbsent(logicalType, schema);
    }

    /** Returns schema with nullable true. */
    private static Schema nullableSchema(Schema schema) {
        return schema.isNullable()
                ? schema
                : Schema.createUnion(SchemaBuilder.builder().nullType(), schema);
    }
}
