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
import org.apache.commons.lang3.ArrayUtils;
import org.joda.time.Days;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Serializer for converting Avro's {@link GenericRecord} to BigQuery proto. */
public class AvroToProtoSerializer implements BigQueryProtoSerializer<GenericRecord> {

    private Descriptor descriptor;
    private static BigQuerySchemaProvider bigQuerySchemaProvider;
    private static final Map<Schema.Type, UnaryOperator<Object>> PRIMITIVE_TYPE_ENCODERS;

    /*
     * Map Avro Schema Type to an Encoding function which converts AvroSchema Primitive
     * Type to Dynamic Message.
     *
     * PRIMITIVE_TYPE_ENCODERS: A Map containing mapping from Primitive Avro Schema Type with encoder function.
     */
    static {
        PRIMITIVE_TYPE_ENCODERS = new EnumMap<>(Schema.Type.class);
        PRIMITIVE_TYPE_ENCODERS.put(Schema.Type.INT, UnaryOperator.identity()); // INT -> INT
        PRIMITIVE_TYPE_ENCODERS.put(Schema.Type.LONG, UnaryOperator.identity());
        PRIMITIVE_TYPE_ENCODERS.put(Schema.Type.DOUBLE, UnaryOperator.identity());
        PRIMITIVE_TYPE_ENCODERS.put(Schema.Type.BOOLEAN, UnaryOperator.identity());
        PRIMITIVE_TYPE_ENCODERS.put(
                Schema.Type.FLOAT,
                o -> Float.parseFloat(String.valueOf((float) o))); // FLOAT -> FLOAT
        PRIMITIVE_TYPE_ENCODERS.put(Schema.Type.STRING, Object::toString);
        PRIMITIVE_TYPE_ENCODERS.put(Schema.Type.ENUM, Object::toString);
        PRIMITIVE_TYPE_ENCODERS.put(
                Schema.Type.FIXED, o -> ByteString.copyFrom(((GenericData.Fixed) o).bytes()));
        PRIMITIVE_TYPE_ENCODERS.put(
                Schema.Type.BYTES, o -> ByteString.copyFrom(((ByteBuffer) o).array()));
    }

    /**
     * Initializer for the Serializer.
     *
     * @param bigQuerySchemaProvider BigQuerySchemaProvider for the Sink Table ({@link
     *     BigQuerySchemaProvider} object)
     */
    public void init(BigQuerySchemaProvider bigQuerySchemaProvider) {
        Preconditions.checkNotNull(
                bigQuerySchemaProvider,
                "BigQuerySchemaProvider not initialized before initializing Serializer.");
        this.descriptor = bigQuerySchemaProvider.getDescriptor();
        AvroToProtoSerializer.bigQuerySchemaProvider = bigQuerySchemaProvider;
    }

    @Override
    public ByteString serialize(GenericRecord record) throws BigQuerySerializationException {
        try {
            return getDynamicMessageFromGenericRecord(record, this.descriptor).toByteString();
        } catch (Exception e) {
            throw new BigQuerySerializationException(e.getMessage(), e);
        }
    }

    public Descriptor getDescriptor() {
        Preconditions.checkNotNull(this.descriptor, "Descriptor not initialized!");
        return this.descriptor;
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
        Schema schema = element.getSchema();
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        // Get the record's schema and find the field descriptor for each field one by one.
        for (Schema.Field field : schema.getFields()) {
            // In case no field descriptor exists for the field, throw an error as we have
            // incompatible schemas.
            FieldDescriptor fieldDescriptor =
                    Preconditions.checkNotNull(
                            descriptor.findFieldByName(field.name().toLowerCase()));
            // Get the value for a field.
            // Check if the value is null.
            @Nullable Object value = element.get(field.name());
            if (value == null) {
                // If the field required, throw an error.
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
     * Function to convert a value of an AvroSchemaField value to required DynamicMessage value.
     *
     * @param fieldDescriptor {@link com.google.protobuf.Descriptors.FieldDescriptor} Object
     *     describing the sink table field to which given value needs to be converted to.
     * @param avroSchema {@link Schema} Object describing the value of Avro Schema Field.
     * @param value Value of the Avro Schema Field.
     * @return Converted Object.
     * @throws UnsupportedOperationException In case schema is of type MAP.
     */
    private static Object toProtoValue(
            FieldDescriptor fieldDescriptor, Schema avroSchema, Object value)
            throws UnsupportedOperationException {
        switch (avroSchema.getType()) {
            case RECORD:
                // Recursion
                return getDynamicMessageFromGenericRecord(
                        (GenericRecord) value, fieldDescriptor.getMessageType());
            case ARRAY:
                // Get an Iterable of all the values in the array.
                Iterable<Object> iterable = (Iterable<Object>) value;
                // Get the inner element type.
                @Nullable Schema arrayElementType = avroSchema.getElementType();
                if (arrayElementType == null) {
                    throw new IllegalArgumentException("Unexpected null element type!");
                }
                if (arrayElementType.isUnion()) {
                    throw new IllegalArgumentException(
                            "ARRAY cannot have multiple datatypes in BigQuery.");
                }
                // Convert each value one by one.
                return StreamSupport.stream(iterable.spliterator(), false)
                        .map(v -> toProtoValue(fieldDescriptor, arrayElementType, v))
                        .collect(Collectors.toList());
            case UNION:
                Schema type = BigQuerySchemaProviderImpl.handleUnionSchema(avroSchema).getLeft();
                // Get the schema of the field.
                return toProtoValue(fieldDescriptor, type, value);
            case MAP:
                throw new UnsupportedOperationException("Not supported yet");
            default:
                return scalarToProtoValue(avroSchema, value);
        }
    }

    /**
     * Function to convert Avro Schema Field value to Dynamic Message value (for Primitive and
     * Logical Types).
     *
     * @param fieldSchema Avro Schema describing the schema for the value.
     * @param value Avro Schema Field value to convert to Dynamic Message value.
     * @return Converted Dynamic Message value.
     */
    private static Object scalarToProtoValue(Schema fieldSchema, Object value) {
        String logicalTypeString = fieldSchema.getProp(LogicalType.LOGICAL_TYPE_PROP);
        @Nullable UnaryOperator<Object> encoder;
        String errorMessage;
        if (logicalTypeString != null) {
            // 1. In case, the Schema has a Logical Type.
            encoder = getLogicalEncoder(logicalTypeString, fieldSchema.getName());
            errorMessage = "Unsupported logical type " + logicalTypeString;
        } else {
            // 2. For all the other Primitive types.
            encoder = PRIMITIVE_TYPE_ENCODERS.get(fieldSchema.getType());
            errorMessage = "Unexpected Avro type " + fieldSchema;
        }
        if (encoder == null) {
            throw new IllegalArgumentException(errorMessage);
        }
        return encoder.apply(value);
    }

    /**
     * Function to obtain the Encoder Function responsible for encoding AvroSchemaField to
     * DynamicMessage.
     *
     * @param logicalTypeString String containing the name for Logical Schema Type.
     * @return Encoder Function which converts AvroSchemaField to DynamicMessage
     */
    private static UnaryOperator<Object> getLogicalEncoder(
            String logicalTypeString, String fieldName) {
        Map<String, UnaryOperator<Object>> mapping = new HashMap<>();
        mapping.put(LogicalTypes.date().getName(), AvroToProtoSerializer::convertDate);
        mapping.put(
                LogicalTypes.decimal(1).getName(), value -> convertBigDecimal(value, fieldName));
        mapping.put(
                LogicalTypes.timestampMicros().getName(),
                value -> convertTimestamp(value, true, "Timestamp(micros/millis)"));
        mapping.put(
                LogicalTypes.timestampMillis().getName(),
                value -> convertTimestamp(value, false, "Timestamp(micros/millis)"));
        mapping.put(LogicalTypes.uuid().getName(), AvroToProtoSerializer::convertUUID);
        mapping.put(LogicalTypes.timeMillis().getName(), value -> convertTime(value, false));
        mapping.put(LogicalTypes.timeMicros().getName(), value -> convertTime(value, true));
        mapping.put(
                LogicalTypes.localTimestampMillis().getName(),
                value -> convertDateTime(value, false));
        mapping.put(
                LogicalTypes.localTimestampMicros().getName(),
                value -> convertDateTime(value, true));
        mapping.put("geography_wkt", AvroToProtoSerializer::convertGeography);
        mapping.put("Json", AvroToProtoSerializer::convertJson);
        return mapping.get(logicalTypeString);
    }

    // ---- Utilities to enable Conversions for Logical Types ------------
    @VisibleForTesting
    static String convertUUID(Object value) {
        if (value instanceof UUID) {
            return ((UUID) value).toString();
        } else {
            Preconditions.checkArgument(
                    value instanceof String, "Expecting a value as String type.");
            UUID.fromString((String) value);
            return (String) value;
        }
    }

    private static long validateTimestamp(long timestamp) {
        Timestamp minTs = Timestamp.parseTimestamp("0001-01-01T00:00:00.000000+00:00");
        Timestamp maxTs = Timestamp.parseTimestamp("9999-12-31T23:59:59.999999+00:00");
        Timestamp ts = null;
        try {
            // Obtain timestamp from microseconds since EPOCH.
            ts = Timestamp.ofTimeMicroseconds(timestamp);
            // Validate the ts formed.
            if (ts.compareTo(minTs) < 0 || ts.compareTo(maxTs) > 0) {
                throw new IllegalArgumentException(
                        String.format("Invalid Timestamp '%s' Provided", ts));
            }
            return timestamp;
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid Timestamp '%s' Provided."
                                    + "\nShould be a long value indicating microseconds since Epoch (1970-01-01 00:00:00.000000+00:00) "
                                    + "between %s and %s",
                            ts, minTs, maxTs));
        }
    }

    // BigQuery inputs timestamp as microseconds since EPOCH,
    // So if we have TIMESTAMP in micros - we convert as it is.
    // If the TIMESTAMP is in millis - we convert to Micros and then add.
    @VisibleForTesting
    static Long convertTimestamp(Object value, boolean micros, String type) {
        long timestamp;
        if (value instanceof ReadableInstant) {
            timestamp = TimeUnit.MILLISECONDS.toMicros(((ReadableInstant) value).getMillis());
        } else {
            Preconditions.checkArgument(
                    value instanceof Long,
                    String.format(
                            "Expecting a value as Long type %s. Instead %s was obtained",
                            type, value.getClass()));
            timestamp = (micros ? (Long) value : TimeUnit.MILLISECONDS.toMicros((Long) value));
        }
        return validateTimestamp(timestamp);
    }

    private static Integer validateDate(Integer date) {
        if (date > 2932896 || date < -719162) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid Date '%s' Provided."
                                    + "\nShould be a Integer value indicating days since Epoch (1970-01-01 00:00:00) "
                                    + "between %s and %s",
                            LocalDate.ofEpochDay(date), "0001-01-01", "9999-12-31"));
        }
        return date;
    }

    @VisibleForTesting
    static Integer convertDate(Object value) {
        // The value is the number of days since the Unix epoch (1970-01-01).
        // The valid range is `-719162` (0001-01-01) to `2932896` (9999-12-31).
        int date;
        if (value instanceof ReadableInstant) {
            date = Days.daysBetween(Instant.EPOCH, (ReadableInstant) value).getDays();
        } else {
            Preconditions.checkArgument(
                    value instanceof Integer, "Expecting a value as Integer type (days).");
            date = (Integer) value;
        }
        return validateDate(date);
    }

    @VisibleForTesting
    static String convertDateTime(Object value, boolean micros) {
        if (value instanceof String) {
            return (String) value;
        }
        if (value instanceof Utf8) {
            return ((Utf8) value).toString();
        }
        Preconditions.checkArgument(
                value instanceof Long,
                String.format(
                        "Expecting a value as Long type " + "%s.%n Instead %s was obtained",
                        "Local-Timestamp(micros/millis)", value.getClass()));
        // Convert to Microseconds if provided in millisecond precision.
        /* We follow the same steps as that of Timestamp conversion
        because essentially we have a timestamp - we just need to strip the timezone before insertion.
         */
        Long convertedTs = convertTimestamp(value, micros, "Local Timestamp(millis/micros)");
        Timestamp convertedTimestamp = Timestamp.ofTimeMicroseconds(convertedTs);
        return LocalDateTime.ofEpochSecond(
                        convertedTimestamp.getSeconds(),
                        convertedTimestamp.getNanos(),
                        ZoneOffset.UTC)
                .toString();
    }

    private static void validateTime(long value) {
        LocalTime minTime = LocalTime.parse("00:00:00");
        LocalTime maxTime = LocalTime.parse("23:59:59.999999");
        long minTimeMicros = ChronoUnit.MICROS.between(LocalTime.MIDNIGHT, minTime);
        long maxTimeMicros = ChronoUnit.MICROS.between(LocalTime.MIDNIGHT, maxTime);
        if (value < minTimeMicros || value > maxTimeMicros) {
            throw new IllegalArgumentException(
                    String.format("Time passed should be between %s and %s.", minTime, maxTime));
        }
    }

    @VisibleForTesting
    static String convertTime(Object value, boolean micros) {
        if (value instanceof String) {
            return (String) value;
        }
        Long microSecondsSinceMidnight = null;
        /* Check if time-millis is in int and time-micros are provided in long*/
        if (micros) {
            if (value instanceof Long) {
                microSecondsSinceMidnight = (long) value;
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Expecting a value as LONG type for "
                                        + "%s. Instead %s was obtained",
                                "Time(micros)", value.getClass()));
            }
        } else {
            if (value instanceof Integer) {
                microSecondsSinceMidnight = TimeUnit.MILLISECONDS.toMicros((int) value);
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Expecting a value as INTEGER type for "
                                        + "%s. Instead %s was obtained",
                                "Time(millis)", value.getClass()));
            }
        }
        // Either microSecondsSinceMidnight would be set or error would be thrown.
        validateTime(microSecondsSinceMidnight);
        LocalTime time =
                LocalTime.MIDNIGHT.plusNanos(
                        TimeUnit.MICROSECONDS.toNanos(microSecondsSinceMidnight));
        return time.toString();
    }

    // 1. There is no way to check the precision and scale of NUMERIC/BIGNUMERIC fields,
    // so we can just check by the maximum value.
    // 2. decimal() logical type is mapped to BIGNUMERIC bigquery field.
    // So in case we attempt to add decimal with precision >9 to a NUMERIC BQ field.
    // The .append() method would be responsible for the error.
    // .serialise() would successfully serialize it without any error indications.
    @VisibleForTesting
    static ByteString convertBigDecimal(Object value, String fieldName) {
        // Assuming decimal (value) comes in big-endian encoding.
        ByteBuffer byteBuffer = (ByteBuffer) value;
        // Reverse before sending to big endian convertor.
        // decodeBigNumericByteString() assumes string to be provided in little-endian.
        byte[] byteArray = byteBuffer.array();
        ArrayUtils.reverse(byteArray); // Converted to little-endian.
        BigDecimal bigDecimal =
                BigDecimalByteStringEncoder.decodeBigNumericByteString(
                        ByteString.copyFrom(byteArray));
        return BigDecimalByteStringEncoder.encodeToBigNumericByteString(bigDecimal);
    }

    @VisibleForTesting
    static String convertGeography(Object value) {
        if (value instanceof Utf8) {
            return ((Utf8) value).toString();
        }
        Preconditions.checkArgument(
                value instanceof String,
                String.format(
                        "Expecting a value as String/Utf8 type (geography_wkt or geojson format)."
                                + "\nInstead got %s: ",
                        value.getClass()));
        return (String) value;
    }

    @VisibleForTesting
    static String convertJson(Object value) {
        String jsonString;
        if (value instanceof Utf8) {
            jsonString = ((Utf8) value).toString();
        } else if (value instanceof String) {
            jsonString = (String) value;
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Expecting a value as String/Utf8 type (json format)."
                                    + "\nInstead got %s: ",
                            value.getClass()));
        }
        try {
            new JSONObject(jsonString);
        } catch (JSONException e) {
            throw new IllegalArgumentException(
                    String.format("The input string %s is not in valid JSON Format.", jsonString));
        }
        return jsonString;
    }
}
