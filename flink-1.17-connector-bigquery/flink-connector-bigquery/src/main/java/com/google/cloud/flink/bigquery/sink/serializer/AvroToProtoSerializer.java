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

import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.client.util.Preconditions;
import com.google.cloud.Timestamp;
import com.google.cloud.bigquery.storage.v1.BigDecimalByteStringEncoder;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.joda.time.Days;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Serializer for converting Avro's {@link GenericRecord} to BigQuery proto. */
public class AvroToProtoSerializer extends BigQueryProtoSerializer<GenericRecord> {

    private Descriptor descriptor;

    /**
     * Prepares the serializer before its serialize method can be called. It allows contextual
     * preprocessing after constructor and before serialize. The Sink will internally call this
     * method when initializing itself.
     *
     * @param bigQuerySchemaProvider {@link BigQuerySchemaProvider} for the destination table.
     */
    @Override
    public void init(BigQuerySchemaProvider bigQuerySchemaProvider) {
        Preconditions.checkNotNull(
                bigQuerySchemaProvider,
                "BigQuerySchemaProvider not found while initializing AvroToProtoSerializer");
        Descriptor derivedDescriptor = bigQuerySchemaProvider.getDescriptor();
        Preconditions.checkNotNull(
                derivedDescriptor, "Destination BigQuery table's Proto Schema could not be found.");
        this.descriptor = derivedDescriptor;
    }

    @Override
    public Schema getAvroSchema(GenericRecord record) {
        return record.getSchema();
    }

    @Override
    public ByteString serialize(GenericRecord record) throws BigQuerySerializationException {
        try {
            return getDynamicMessageFromGenericRecord(record, this.descriptor).toByteString();
        } catch (Exception e) {
            throw new BigQuerySerializationException(e.getMessage());
        }
    }

    /**
     * Function to convert a Generic Avro Record to Dynamic Message to write using the Storage Write
     * API.
     *
     * @param element {@link GenericRecord} Object to convert to {@link DynamicMessage}
     * @param descriptor {@link Descriptor} describing the schema of the sink table.
     * @return {@link DynamicMessage} Object converted from the Generic Avro Record.
     */
    public static DynamicMessage getDynamicMessageFromGenericRecord(
            GenericRecord element, Descriptor descriptor) {
        Schema recordSchema = element.getSchema();
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        // Get a record's field schema and find the field descriptor for each field one by one.
        for (Schema.Field field : recordSchema.getFields()) {
            // In case no field descriptor exists for the field, throw an error as we have
            // incompatible schemas.
            FieldDescriptor fieldDescriptor =
                    Preconditions.checkNotNull(
                            descriptor.findFieldByName(field.name().toLowerCase()));
            // Get the value for a field.
            // Check if the value is null.
            @Nullable Object value = element.get(field.name());
            if (value == null) {
                // Do nothing in case value == null and fieldDescriptor != "REQUIRED"
                if (fieldDescriptor.isRequired()) {
                    throw new IllegalArgumentException(
                            "Received null value for non-nullable field "
                                    + fieldDescriptor.getName());
                }
            } else {
                // Convert to Dynamic Message.
                value = toProtoValue(fieldDescriptor, field.schema(), value);
                builder.setField(fieldDescriptor, value);
            }
        }
        return builder.build();
    }

    /**
     * Function to convert a value of an AvroSchemaField value to required Dynamic Message value.
     *
     * @param fieldDescriptor {@link com.google.protobuf.Descriptors.FieldDescriptor} Object
     *     describing the sink table field to which given value needs to be converted to.
     * @param avroSchema {@link Schema} Object describing the value of Avro Schema Field.
     * @param value Value of the Avro Schema Field.
     * @return Converted Object.
     */
    private static Object toProtoValue(
            FieldDescriptor fieldDescriptor, Schema avroSchema, Object value) {
        switch (avroSchema.getType()) {
            case RECORD:
                return getDynamicMessageFromGenericRecord(
                        (GenericRecord) value, fieldDescriptor.getMessageType());
            case ARRAY:
                return AvroSchemaHandler.handleArraySchema(fieldDescriptor, avroSchema, value);
            case UNION:
                Schema type = AvroSchemaHandler.handleUnionSchema(avroSchema).getLeft();
                // Get the schema of the field.
                return toProtoValue(fieldDescriptor, type, value);
            case MAP:
                throw new UnsupportedOperationException("MAP type not supported yet");
            case STRING:
                /*
                Logical Types currently supported are of the following underlying types:
                    DATE - INT
                    DECIMAL - BYTES
                    TIMESTAMP-MICROS - LONG
                    TIMESTAMP-MILLIS - LONG
                    UUID - STRING
                    TIME-MILLIS - INT/STRING
                    TIME-MICROS - LONG/STRING
                    LOCAL-TIMESTAMP-MICROS - LONG/STRING
                    LOCAL-TIMESTAMP-MILLIS - LONG/STRING
                    GEOGRAPHY - STRING
                    JSON - STRING
                */
                Object convertedValue =
                        AvroSchemaHandler.handleLogicalTypeSchema(avroSchema, value);
                if (convertedValue != value) {
                    return convertedValue;
                }
                return value.toString();
            case LONG:
                convertedValue = AvroSchemaHandler.handleLogicalTypeSchema(avroSchema, value);
                if (convertedValue != value) {
                    return convertedValue;
                }
                return Long.parseLong(value.toString());
            case INT:
                // Return the converted value.
                convertedValue = AvroSchemaHandler.handleLogicalTypeSchema(avroSchema, value);
                if (convertedValue != value) {
                    return convertedValue;
                }
                return Integer.parseInt(value.toString());
            case BYTES:
                // Find if it is of decimal Type.
                convertedValue = AvroSchemaHandler.handleLogicalTypeSchema(avroSchema, value);
                if (convertedValue != value) {
                    return convertedValue;
                }
                return ByteString.copyFrom(((ByteBuffer) value).array());
            case ENUM:
                return value.toString();
            case FIXED:
                return ByteString.copyFrom(((GenericData.Fixed) value).bytes());
            case BOOLEAN:
                return (boolean) value;
            case FLOAT:
                return Float.parseFloat(String.valueOf((float) value));
            case DOUBLE:
                return (double) value;
            case NULL:
                throw new IllegalArgumentException("Null Type Field not supported in BigQuery!");
            default:
                throw new IllegalArgumentException("Unexpected Avro type" + avroSchema);
        }
    }

    /** Class to handle Specific Avro Proto Schema Types (Logical and Union). */
    static class AvroSchemaHandler {
        private AvroSchemaHandler() {}

        private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaHandler.class);

        /**
         * Function to convert a value of an <b>ARRAY</b> Type AvroSchemaField value to required
         * Dynamic Message value.
         *
         * @param fieldDescriptor {@link com.google.protobuf.Descriptors.FieldDescriptor} Object
         *     describes the destination field in the sink table. Given value must be converted to a
         *     format compatible with this field.
         * @param avroSchema {@link Schema} Object describing the value of Avro Schema Field.
         * @param value Value of the Avro Schema Field.
         * @return Converted List Object
         */
        public static List<Object> handleArraySchema(
                FieldDescriptor fieldDescriptor, Schema avroSchema, Object value) {
            Iterable<Object> iterable;
            if (value instanceof Iterable) {
                iterable = (Iterable<Object>) value;
            } else {
                LOG.error(getLogErrorMessage("Iterable", "ARRAY", value.getClass().toString()));
                throw new IllegalArgumentException(
                        "Expecting the value as Iterable type for type ARRAY.");
            }
            // Get the inner element type.
            @Nullable Schema arrayElementType = avroSchema.getElementType();
            if (arrayElementType.isNullable()) {
                throw new IllegalArgumentException("Array cannot have NULLABLE datatype");
            }
            if (arrayElementType.isUnion()) {
                throw new IllegalArgumentException(
                        "ARRAY cannot have multiple datatypes in BigQuery.");
            }
            // Convert each value one by one.
            return StreamSupport.stream(iterable.spliterator(), false)
                    .map(v -> toProtoValue(fieldDescriptor, arrayElementType, v))
                    .collect(Collectors.toList());
        }

        /**
         * Helper function to handle the UNION Schema Type. We only consider the union schema valid
         * when it is of the form ["null", datatype]. All other forms such as ["null"],["null",
         * datatype1, datatype2, ...], and [datatype1, datatype2, ...] Are considered as invalid (as
         * there is no such support in BQ) So we throw an error in all such cases. For the valid
         * case of ["null", datatype] or [datatype] we set the Schema as the schema of the <b>not
         * null</b> datatype.
         *
         * @param schema of type UNION to check and derive.
         * @return Schema of the OPTIONAL field.
         * @throws IllegalArgumentException If multiple non-null datatypes or only null is observed.
         */
        public static ImmutablePair<Schema, Boolean> handleUnionSchema(Schema schema)
                throws IllegalArgumentException {
            List<Schema> types = schema.getTypes();
            if (types.size() == 1) {
                // Can be ['null'] - ERROR
                // [Valid-Datatype] - Not Nullable, element type
                if (types.get(0).getType() != Schema.Type.NULL) {
                    // Case, when there is only a single type in UNION.
                    // Then it is essentially the same as not having a UNION.
                    return new ImmutablePair<>(types.get(0), false);
                }
            } else if (types.size() == 2) {
                // don't need recursion because nested unions aren't supported in AVRO
                // Extract all the nonNull Datatypes.
                // ['datatype, 'null'] and ['null', datatype] are only valid cases.
                if (types.get(0).getType() != Schema.Type.NULL
                        && types.get(1).getType() == Schema.Type.NULL) {
                    return new ImmutablePair<>(types.get(0), true);
                } else if (types.get(0).getType() == Schema.Type.NULL
                        && types.get(1).getType() != Schema.Type.NULL) {
                    return new ImmutablePair<>(types.get(1), true);
                }
            }
            LOG.error(
                    getLogErrorMessage(
                            "['datatype'] or ['null', 'datatype']",
                            "UNION",
                            "Multiple not-null types: " + types));
            throw new IllegalArgumentException("Multiple non-null union types are not supported.");
        }

        /**
         * Function to convert Avro Schema Field value to Dynamic Message value (for Logical Types).
         *
         * @param fieldSchema Avro Schema describing the schema for the value.
         * @param value Avro Schema Field value to convert to {@link DynamicMessage} value.
         * @return Converted {@link DynamicMessage} value if a supported logical types exists, param
         *     value otherwise.
         */
        private static Object handleLogicalTypeSchema(Schema fieldSchema, Object value) {
            String logicalTypeString = fieldSchema.getProp(LogicalType.LOGICAL_TYPE_PROP);
            if (logicalTypeString != null) {
                // 1. In case, the Schema has a Logical Type.
                @Nullable
                UnaryOperator<Object> encoder = getLogicalEncoder(fieldSchema, logicalTypeString);
                // 2. Check if this is supported, Return the value
                if (encoder != null) {
                    return encoder.apply(value);
                }
            }
            // Otherwise, return the value as it is.
            return value;
        }

        /**
         * Function to obtain the Encoder Function responsible for encoding AvroSchemaField to
         * Dynamic Message.
         *
         * @param logicalTypeString String containing the name for Logical Schema Type.
         * @return Encoder Function which converts AvroSchemaField to {@link DynamicMessage}
         */
        private static UnaryOperator<Object> getLogicalEncoder(
                Schema fieldSchema, String logicalTypeString) {
            Map<String, UnaryOperator<Object>> mapping = new HashMap<>();
            mapping.put(LogicalTypes.date().getName(), AvroSchemaHandler::convertDate);
            mapping.put(
                    LogicalTypes.decimal(1).getName(),
                    value -> AvroSchemaHandler.convertBigDecimal(value, fieldSchema));
            mapping.put(
                    LogicalTypes.timestampMicros().getName(),
                    value -> convertTimestamp(value, true, "Timestamp(micros/millis)"));
            mapping.put(
                    LogicalTypes.timestampMillis().getName(),
                    value -> convertTimestamp(value, false, "Timestamp(micros/millis)"));
            mapping.put(LogicalTypes.uuid().getName(), AvroSchemaHandler::convertUUID);
            mapping.put(LogicalTypes.timeMillis().getName(), value -> convertTime(value, false));
            mapping.put(LogicalTypes.timeMicros().getName(), value -> convertTime(value, true));
            mapping.put(
                    LogicalTypes.localTimestampMillis().getName(),
                    value -> convertDateTime(value, false));
            mapping.put(
                    LogicalTypes.localTimestampMicros().getName(),
                    value -> convertDateTime(value, true));
            mapping.put("geography_wkt", AvroSchemaHandler::convertGeography);
            mapping.put("Json", AvroSchemaHandler::convertJson);
            return mapping.get(logicalTypeString);
        }

        // ---- Utilities to enable Conversions for Logical Types ------------
        @VisibleForTesting
        static String convertUUID(Object value) {
            if (value instanceof UUID) {
                return ((UUID) value).toString();
            } else if (value instanceof String) {
                UUID.fromString((String) value);
                return (String) value;
            }
            LOG.error(getLogErrorMessage("String/UUID", "UUID", value.getClass().toString()));
            throw new IllegalArgumentException(getErrorMessage("String/UUID", "UUID"));
        }

        private static void validateTimestamp(long timestamp) {
            try {
                // Validate timestamp from microseconds since EPOCH.
                Timestamp.ofTimeMicroseconds(timestamp);
            } catch (IllegalArgumentException e) {
                LOG.error(
                        String.format(
                                "Invalid Timestamp '%s' Provided."
                                        + "\nShould be a long value indicating microseconds since "
                                        + "Epoch between %s and %s",
                                timestamp, Timestamp.MIN_VALUE, Timestamp.MAX_VALUE));
                throw new IllegalArgumentException(
                        String.format("Invalid Timestamp '%s' Provided.", timestamp));
            }
        }

        // BigQuery inputs timestamp as microseconds since EPOCH,
        // So if we have TIMESTAMP in micros - we convert as it is.
        // If the TIMESTAMP is in millis - we convert to Micros and then add.
        static Long convertTimestamp(Object value, boolean micros, String type) {
            long timestamp;
            if (value instanceof ReadableInstant) {
                timestamp = TimeUnit.MILLISECONDS.toMicros(((ReadableInstant) value).getMillis());
            } else if (value instanceof Long) {
                timestamp = (micros ? (Long) value : TimeUnit.MILLISECONDS.toMicros((Long) value));
            } else {
                LOG.error(
                        getLogErrorMessage(
                                "LONG/ReadableInstant",
                                "TIMESTAMP (" + type + ")",
                                value.getClass().toString()));
                throw new IllegalArgumentException(
                        getErrorMessage("LONG/ReadableInstant", "TIMESTAMP"));
            }
            validateTimestamp(timestamp);
            return timestamp;
        }

        private static void validateDate(Integer date) {
            // The value is the number of days since the Unix epoch (1970-01-01).
            // The valid range is `-719162` (0001-01-01) to `2932896` (9999-12-31).
            int maxDateValue = 2932896;
            int minDateValue = -719162;
            if (date > maxDateValue || date < minDateValue) {
                LOG.error(
                        String.format(
                                "Invalid Date '%s' Provided."
                                        + "\nShould be a Integer value indicating days since Epoch (1970-01-01 00:00:00) "
                                        + "between %s and %s",
                                LocalDate.ofEpochDay(date), "0001-01-01", "9999-12-31"));
                throw new IllegalArgumentException("Invalid date Provided.");
            }
        }

        @VisibleForTesting
        static Integer convertDate(Object value) {
            // The value is the number of days since the Unix epoch (1970-01-01).
            // The valid range is `-719162` (0001-01-01) to `2932896` (9999-12-31).
            int date;
            if (value instanceof ReadableInstant) {
                date = Days.daysBetween(Instant.EPOCH, (ReadableInstant) value).getDays();
            } else if (value instanceof Integer) {
                date = (Integer) value;
            } else {
                LOG.error(
                        getLogErrorMessage(
                                "ReadableInstant/Integer",
                                "Days(micros/millis)",
                                value.getClass().toString()));
                throw new IllegalArgumentException(getErrorMessage("Integer", "DATE"));
            }
            validateDate(date);
            return date;
        }

        static String convertDateTime(Object value, boolean micros) {
            if (value instanceof Long) {
                // Convert to Microseconds if provided in millisecond precision.
                /* We follow the same steps as that of Timestamp conversion
                because essentially we have a timestamp - we just need to strip the timezone before insertion.
                 */
                Long convertedTs =
                        convertTimestamp(value, micros, "Local Timestamp(millis/micros)");
                Timestamp convertedTimestamp = Timestamp.ofTimeMicroseconds(convertedTs);
                return LocalDateTime.ofEpochSecond(
                                convertedTimestamp.getSeconds(),
                                convertedTimestamp.getNanos(),
                                ZoneOffset.UTC)
                        .toString();
            }
            String obtainedValue;
            if (value instanceof String) {
                obtainedValue = (String) value;
            } else if (value instanceof Utf8) {
                obtainedValue = ((Utf8) value).toString();
            } else {
                LOG.error(
                        getLogErrorMessage(
                                "String/Long/UTF-8",
                                "Local-Timestamp(micros/millis)",
                                value.getClass().toString()));
                throw new IllegalArgumentException(
                        getErrorMessage("String/LONG/UTF-8", "Local-Timestamp(micros/millis)"));
            }
            // LocalDateTime.parse is also responsible for validating the string passed.
            // If the text cannot be parsed DateTimeParseException is thrown.
            // Formatting,
            // according to
            // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#datetime_type.
            try {
                return LocalDateTime.parse(
                                obtainedValue,
                                DateTimeFormatter.ofPattern(
                                        "yyyy-M[M]-d[d][[' ']['T']['t']H[H]':'m[m]':'s[s]['.'SSSSSS]['.'SSSSS]['.'SSSS]['.'SSS]['.'SS]['.'S]]"))
                        .toString();
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException(
                        String.format(
                                "The datetime string obtained %s, is of invalid format.",
                                (String) value));
            }
        }

        private static void validateTime(long value) {
            long minTimeMicros = ChronoUnit.MICROS.between(LocalTime.MIDNIGHT, LocalTime.MIN);
            long maxTimeMicros = ChronoUnit.MICROS.between(LocalTime.MIDNIGHT, LocalTime.MAX);
            if (value < minTimeMicros || value > maxTimeMicros) {
                LOG.error(
                        String.format(
                                "Input Time should be between %s and %s.%n Found %s instead.",
                                LocalTime.MIN,
                                LocalTime.MIN,
                                LocalTime.MIDNIGHT.plusNanos(
                                        TimeUnit.MICROSECONDS.toNanos(value))));
                throw new IllegalArgumentException("Invalid time value obtained.");
            }
        }

        @VisibleForTesting
        static String convertTime(Object value, boolean micros) {
            if (value instanceof String) {
                // LocalTime.parse is also responsible for validating the string passed.
                // If the text cannot be parsed DateTimeParseException is thrown.
                // Formatting,
                // according to
                // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#time_type.
                try {
                    return LocalTime.parse(
                                    (String) value,
                                    DateTimeFormatter.ofPattern(
                                            "H[H]':'m[m]':'s[s]['.'SSSSSS]['.'SSSSS]['.'SSSS]['.'SSS]['.'SS]['.'S]"))
                            .toString();
                } catch (DateTimeParseException e) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "The datetime string obtained %s, is of invalid format.",
                                    (String) value));
                }
            }
            Long microSecondsSinceMidnight = null;
            /* Check if time-millis is in int and time-micros are provided in long*/
            if (micros) {
                if (value instanceof Long) {
                    microSecondsSinceMidnight = (long) value;
                } else {
                    LOG.error(
                            getLogErrorMessage(
                                    "LONG", "Time(micros)", value.getClass().toString()));
                    throw new IllegalArgumentException(getErrorMessage("LONG", "Time(micros)"));
                }
            } else {
                if (value instanceof Integer) {
                    microSecondsSinceMidnight = TimeUnit.MILLISECONDS.toMicros((int) value);
                } else {
                    LOG.error(
                            getLogErrorMessage(
                                    "INTEGER", "Time(millis)", value.getClass().toString()));
                    throw new IllegalArgumentException(getErrorMessage("INTEGER", "Time(millis)"));
                }
            }
            // Either microSecondsSinceMidnight would be set or error would be thrown.
            validateTime(microSecondsSinceMidnight);
            LocalTime time =
                    LocalTime.MIDNIGHT.plusNanos(
                            TimeUnit.MICROSECONDS.toNanos(microSecondsSinceMidnight));
            return time.format(DateTimeFormatter.ISO_TIME);
        }

        /**
         * Function to convert ByteBuffer value to ByteString for Bignumeric Field.
         *
         * <ol>
         *   <li>If scale and precision is set in the BQ Table
         *       <ul>
         *         <li>Precision > BQ table and Scale > BQ Table - ERROR while inserting to Storage
         *             Write API
         *         <li>Precision > BQ table and Scale <= BQ Table - ERROR while inserting to Storage
         *             Write API
         *         <li>Precision <= BQ table and Scale > BQ Table - ROUND OFF
         *             <ul>
         *               <li>Example: 1.234567(Precision 7, Scale = 6) value is converted to 1.23457
         *                   for a table of type BIGNUMERIC(9, 5)
         *             </ul>
         *         <li>Precision <= BQ table and Scale <= BQ Table - AS IT IS
         *       </ul>
         *   <li>If No scale is set in the BQ Table
         *       <ul>
         *         <li>The value is inserted as it is (should be within the limits of BIG NUMERIC
         *             Field of a BigQuery Table)
         *       </ul>
         * </ol>
         *
         * @param value {@link ByteBuffer} object containing the 2's-complement binary
         *     representation of a BigInteger (in big-endian byte order).
         * @param fieldSchema {@link Schema} object of the value to be converted.
         * @return {@link ByteString} object of the converted BigDecimal
         */
        @VisibleForTesting
        static ByteString convertBigDecimal(Object value, Schema fieldSchema) {
            // Check if a ByteBuffer instance is obtained.\
            if (!(value instanceof ByteBuffer)) {
                LOG.error(
                        getLogErrorMessage(
                                "ByteBuffer", "BigDecimal", value.getClass().toString()));
                throw new IllegalArgumentException(getErrorMessage("ByteBuffer", "BigDecimal"));
            }
            byte[] byteArray = ((ByteBuffer) value).array(); // This is in big-endian format.

            // Extract the scale from the Record Schema.
            Preconditions.checkNotNull(
                    fieldSchema.getLogicalType(), "Invalid decimal type obtained");
            LogicalTypes.Decimal decimalFieldSchema =
                    (LogicalTypes.Decimal) fieldSchema.getLogicalType();
            int scale = decimalFieldSchema.getScale();

            BigInteger scaledValue = new BigInteger(byteArray); // requires big-endian.
            BigDecimal decimalValue = new BigDecimal(scaledValue, scale);

            try {
                if (fieldSchema.getObjectProp("isNumeric") != null
                        && (boolean) fieldSchema.getObjectProp("isNumeric")) {
                    // Pass the BigDecimal Object to be converted to ByteString (with Numeric
                    // Scale).
                    return BigDecimalByteStringEncoder.encodeToNumericByteString(decimalValue);
                }
                // Pass the BigDecimal Object to be converted to ByteString (with Bignumeric scale).
                return BigDecimalByteStringEncoder.encodeToBigNumericByteString(decimalValue);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(e);
            }
        }

        @VisibleForTesting
        static String convertGeography(Object value) {
            if (value instanceof Utf8) {
                return ((Utf8) value).toString();
            }
            if (value instanceof String) {
                return (String) value;
            }
            LOG.error(
                    getLogErrorMessage(
                            "STRING/UTF-8",
                            "geography_wkt or geojson format",
                            value.getClass().toString()));
            throw new IllegalArgumentException(getErrorMessage("STRING/UTF-8", "GEOGRAPHY"));
        }

        @VisibleForTesting
        static String convertJson(Object value) {
            String jsonString;
            if (value instanceof Utf8) {
                jsonString = ((Utf8) value).toString();
            } else if (value instanceof String) {
                jsonString = (String) value;
            } else {
                LOG.error(getLogErrorMessage("UTF-8/STRING", "JSON", value.getClass().toString()));
                throw new IllegalArgumentException(getErrorMessage("UTF-8/STRING", "JSON"));
            }
            try {
                new JSONObject(jsonString);
            } catch (JSONException e) {
                throw new IllegalArgumentException(
                        String.format(
                                "The input string %s is not in valid JSON Format.", jsonString));
            }
            return jsonString;
        }

        // --------- Utilities for maintaining consistency across error messages ------------------
        private static String getLogErrorMessage(
                String expectedType, String actualType, String foundInstead) {
            return String.format(
                    "Expecting the value as %s type for %s.%nFound %s instead.",
                    expectedType, actualType, foundInstead);
        }

        private static String getErrorMessage(String expectedType, String actualType) {
            return String.format(
                    "Expecting the value as %s type for type %s.", expectedType, actualType);
        }
    }
}
