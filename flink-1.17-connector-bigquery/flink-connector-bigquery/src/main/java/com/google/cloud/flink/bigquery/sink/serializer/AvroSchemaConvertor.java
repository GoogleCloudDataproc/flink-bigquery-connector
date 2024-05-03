package com.google.cloud.flink.bigquery.sink.serializer;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.util.Preconditions;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;

import java.util.List;

/**
 * Class to convert Avro Schema to Data Type Schema which could be further converted to Table API
 * Schema.
 */
public class AvroSchemaConvertor {

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
            case ENUM:
                return DataTypes.STRING().notNull();
            case ARRAY:
                return DataTypes.ARRAY(convertToDataType(schema.getElementType())).notNull();
            case MAP:
                return DataTypes.MAP(
                                DataTypes.STRING().notNull(),
                                convertToDataType(schema.getValueType()))
                        .notNull();
            case UNION:
                final Schema actualSchema;
                final boolean nullable;
                if (schema.getTypes().size() == 2
                        && schema.getTypes().get(0).getType() == Schema.Type.NULL) {
                    actualSchema = schema.getTypes().get(1);
                    nullable = true;
                } else if (schema.getTypes().size() == 2
                        && schema.getTypes().get(1).getType() == Schema.Type.NULL) {
                    actualSchema = schema.getTypes().get(0);
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
            case FIXED:
                // logical decimal type
                if (schema.getLogicalType() instanceof LogicalTypes.Decimal) {
                    final LogicalTypes.Decimal decimalType =
                            (LogicalTypes.Decimal) schema.getLogicalType();
                    return DataTypes.DECIMAL(decimalType.getPrecision(), decimalType.getScale())
                            .notNull();
                }
                // convert fixed size binary data to primitive byte arrays
                return DataTypes.VARBINARY(schema.getFixedSize()).notNull();
            case STRING:
                // convert Avro's Utf8/CharSequence to String
                return DataTypes.STRING().notNull();
            case BYTES:
                // logical decimal type
                if (schema.getLogicalType() instanceof LogicalTypes.Decimal) {
                    final LogicalTypes.Decimal decimalType =
                            (LogicalTypes.Decimal) schema.getLogicalType();
                    return DataTypes.DECIMAL(decimalType.getPrecision(), decimalType.getScale())
                            .notNull();
                }
                return DataTypes.BYTES().notNull();
            case INT:
                // logical date and time type
                final org.apache.avro.LogicalType logicalType = schema.getLogicalType();
                if (logicalType == LogicalTypes.date()) {
                    return DataTypes.DATE().notNull();
                } else if (logicalType == LogicalTypes.timeMillis()) {
                    return DataTypes.TIME(3).notNull();
                }
                return DataTypes.INT().notNull();
            case LONG:
                // logical timestamp type
                if (schema.getLogicalType() == LogicalTypes.timestampMillis()) {
                    return DataTypes.TIMESTAMP_WITH_TIME_ZONE(3).notNull();
                } else if (schema.getLogicalType() == LogicalTypes.timestampMicros()) {
                    return DataTypes.TIMESTAMP_WITH_TIME_ZONE(6).notNull();
                } else if (schema.getLogicalType() == LogicalTypes.timeMillis()) {
                    return DataTypes.TIME(3).notNull();
                } else if (schema.getLogicalType() == LogicalTypes.timeMicros()) {
                    return DataTypes.TIME(6).notNull();
                } else if (schema.getLogicalType() == LogicalTypes.localTimestampMillis()) {
                    return DataTypes.TIMESTAMP(3).notNull();
                } else if (schema.getLogicalType() == LogicalTypes.localTimestampMicros()) {
                    return DataTypes.TIMESTAMP(6).notNull();
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
        throw new IllegalArgumentException("Unsupported Avro type '" + schema.getType() + "'.");
    }
}
