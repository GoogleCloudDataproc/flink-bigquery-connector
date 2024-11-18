/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.flink.bigquery.source.reader.deserializer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import com.google.api.client.util.Preconditions;
import com.google.cloud.Timestamp;
import com.google.cloud.flink.bigquery.sink.serializer.AvroSchemaConvertor;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.joda.time.Days;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tool class used to convert from Avro {@link GenericRecord} to {@link RowData}. <br>
 * <a
 * href="https://github.com/apache/flink/blob/master/flink-formats/flink-avro/src/main/java/org/apache/flink/formats/avro/AvroToRowDataConverters.java">AvroToRowDataConvertors</a>
 * is a pre-existing implementation, However, it does not take into account microsecond precision
 * for TIMESTAMP and DATETIME (TIMESTAMP_WITH_LOCAL_TIMEZONE in Flink's LogicalType) which is
 * required for reading TIMESTAMP and DATETIME BigQuery fields.
 */
@Internal
public class AvroToRowDataConverters {

    private static final Logger LOG = LoggerFactory.getLogger(AvroToRowDataConverters.class);
    private static final int MILLIS_PRECISION = 3;
    private static final int MICROS_PRECISION = 6;

    /**
     * Runtime converter that converts Avro data structures into objects of Flink Table & SQL
     * internal data structures.
     */
    @FunctionalInterface
    public interface AvroToRowDataConverter extends Serializable {
        Object convert(Object object);
    }

    // -------------------------------------------------------------------------------------
    // Runtime Converters
    // -------------------------------------------------------------------------------------

    public static AvroToRowDataConverter createRowConverter(RowType rowType) {
        final AvroToRowDataConverter[] fieldConverters =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(AvroToRowDataConverters::createNullableConverter)
                        .toArray(AvroToRowDataConverter[]::new);
        final int arity = rowType.getFieldCount();

        return avroObject -> {
            IndexedRecord record = (IndexedRecord) avroObject;
            GenericRowData row = new GenericRowData(arity);
            for (int i = 0; i < arity; ++i) {
                // avro always deserialize successfully even though the type isn't matched
                // so no need to throw exception about which field can't be deserialized
                row.setField(i, fieldConverters[i].convert(record.get(i)));
            }
            return row;
        };
    }

    /** Creates a runtime converter which is null safe. */
    private static AvroToRowDataConverter createNullableConverter(LogicalType type) {
        final AvroToRowDataConverter converter = createConverter(type);
        return avroObject -> {
            if (avroObject == null) {
                return null;
            }
            return converter.convert(avroObject);
        };
    }

    /** Creates a runtime converter which assuming input object is not null. */
    private static AvroToRowDataConverter createConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return avroObject -> null;
            case TINYINT:
                return avroObject -> ((Integer) avroObject).byteValue();
            case SMALLINT:
                return avroObject -> ((Integer) avroObject).shortValue();
            case BOOLEAN: // boolean
            case INTEGER: // int
            case INTERVAL_YEAR_MONTH: // long
            case BIGINT: // long
            case INTERVAL_DAY_TIME: // long
            case FLOAT: // float
            case DOUBLE: // double
                return avroObject -> avroObject;
            case DATE:
                return AvroToRowDataConverters::convertToDate;
            case TIME_WITHOUT_TIME_ZONE:
                int timePrecision = ((TimeType) type).getPrecision();
                if (timePrecision <= MILLIS_PRECISION) {
                    return avroObject -> convertToMillisTime(avroObject, MILLIS_PRECISION);
                } else if (timePrecision <= MICROS_PRECISION) {
                    return avroObject -> convertToMicrosTime(avroObject, MICROS_PRECISION);
                } else {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Avro to RowData Conversion Error:%n"
                                            + "The TIME type within the Avro schema uses a precision of '%d',"
                                            + " which is higher than the maximum supported TIME precision %d.",
                                    timePrecision, MICROS_PRECISION));
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                int localTsPrecision = ((LocalZonedTimestampType) type).getPrecision();
                return createTimeDatetimeConvertor(localTsPrecision);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                int tsPrecision = ((TimestampType) type).getPrecision();
                return createTimeDatetimeConvertor(tsPrecision);
            case CHAR:
            case VARCHAR:
                return avroObject -> StringData.fromString(avroObject.toString());
            case BINARY:
            case VARBINARY:
                return AvroToRowDataConverters::convertToBytes;
            case DECIMAL:
                return createDecimalConverter((DecimalType) type);
            case ARRAY:
                return createArrayConverter((ArrayType) type);
            case ROW:
                return createRowConverter((RowType) type);
            case MAP:
            case MULTISET:
                return createMapConverter(type);
            case RAW:
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Avro to RowData Conversion Error:%n"
                                        + "Input Logical Type %s is not supported.",
                                type.asSummaryString()));
        }
    }

    private static AvroToRowDataConverter createTimeDatetimeConvertor(int precision) {
        if (precision <= MILLIS_PRECISION) {
            return avroObject ->
                    AvroToRowDataConverters.convertToTimestamp(avroObject, MILLIS_PRECISION);
        } else if (precision <= MICROS_PRECISION) {
            return avroObject ->
                    AvroToRowDataConverters.convertToTimestamp(avroObject, MICROS_PRECISION);
        } else {
            String invalidPrecisionError =
                    String.format(
                            "Avro to RowData Conversion Error:%n"
                                    + "The TIMESTAMP/DATETIME type within the Avro schema uses a precision of '%d',"
                                    + " which is higher than the maximum supported TIMESTAMP/DATETIME precision %d.",
                            precision, MICROS_PRECISION);
            throw new UnsupportedOperationException(invalidPrecisionError);
        }
    }

    private static AvroToRowDataConverter createDecimalConverter(DecimalType decimalType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        return avroObject -> {
            final byte[] bytes;
            if (avroObject instanceof GenericFixed) {
                bytes = ((GenericFixed) avroObject).bytes();
            } else if (avroObject instanceof ByteBuffer) {
                ByteBuffer byteBuffer = (ByteBuffer) avroObject;
                bytes = new byte[byteBuffer.remaining()];
                byteBuffer.get(bytes);
            } else if (avroObject instanceof byte[]) {
                bytes = (byte[]) avroObject;
            } else {
                String invalidFormatError =
                        getErrorMessage(
                                avroObject,
                                "GenericFixed, byte[] and java.nio.ByteBuffer",
                                "DECIMAL");
                throw new IllegalArgumentException(invalidFormatError);
            }
            return DecimalData.fromUnscaledBytes(bytes, precision, scale);
        };
    }

    private static String getErrorMessage(
            Object avroObject, String expectedType, String convertingType) {
        return String.format(
                "Avro to RowData Conversion Error:%n"
                        + "Unexpected Avro object %s of type '%s' input for "
                        + "conversion to Flink's '%s' logical type. Supported type(s): '%s'",
                avroObject, avroObject.getClass(), convertingType, expectedType);
    }

    private static AvroToRowDataConverter createArrayConverter(ArrayType arrayType) {
        final AvroToRowDataConverter elementConverter =
                createNullableConverter(arrayType.getElementType());
        final Class<?> elementClass =
                LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());

        return avroObject -> {
            Preconditions.checkArgument(
                    avroObject instanceof List<?>, getErrorMessage(avroObject, "LIST", "ARRAY"));
            final List<?> list = (List<?>) avroObject;
            final int length = list.size();
            final Object[] array = (Object[]) Array.newInstance(elementClass, length);
            for (int i = 0; i < length; ++i) {
                array[i] = elementConverter.convert(list.get(i));
            }
            return new GenericArrayData(array);
        };
    }

    private static AvroToRowDataConverter createMapConverter(LogicalType type) {
        final AvroToRowDataConverter keyConverter =
                createConverter(DataTypes.STRING().getLogicalType());
        final AvroToRowDataConverter valueConverter =
                createNullableConverter(AvroSchemaConvertor.extractValueTypeToAvroMap(type));

        return avroObject -> {
            Preconditions.checkArgument(
                    avroObject instanceof Map<?, ?>, getErrorMessage(avroObject, "MAP", "MAP"));
            final Map<?, ?> map = (Map<?, ?>) avroObject;
            Map<Object, Object> result = new HashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object key = keyConverter.convert(entry.getKey());
                Object value = valueConverter.convert(entry.getValue());
                result.put(key, value);
            }
            return new GenericMapData(result);
        };
    }

    static TimestampData convertToTimestamp(Object object, int precision) {
        final long micros;
        long tempMicros;
        if (object instanceof Long) {
            tempMicros = (Long) object;
            if (precision == MILLIS_PRECISION) {
                // If millisecond precision.
                return TimestampData.fromEpochMillis(tempMicros);
            }
        } else if (object instanceof Instant) {
            // Precision is automatically transferred.
            return TimestampData.fromInstant(((Instant) object));
        } else if (object instanceof String || object instanceof Utf8) {
            // ---- DATETIME TYPE Conversion ----
            // LocalDateTime.parse is also responsible for validating the string passed.
            // If the text cannot be parsed DateTimeParseException is thrown.
            // Formatting,
            // according to
            // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#datetime_type.
            String tsValue;
            if (object instanceof String) {
                tsValue = (String) object;
            } else {
                tsValue = ((Utf8) object).toString();
            }
            try {
                // LocalDateTime will handle the precision.
                return TimestampData.fromLocalDateTime(
                        LocalDateTime.parse(
                                tsValue,
                                DateTimeFormatter.ofPattern(
                                        "yyyy-M[M]-d[d][[' ']['T']['t']H[H]':'m[m]':'s[s]['.'SSSSSS]['.'SSSSS]['.'SSSS]['.'SSS]['.'SS]['.'S]]")));
            } catch (DateTimeParseException e) {
                String invalidFormatError =
                        String.format(
                                "Avro to RowData Conversion Error:%n"
                                        + "The Avro datetime string input for conversion to Flink's 'TIMESTAMP_LTZ' Logical Type "
                                        + "%s, is of invalid format.",
                                object);
                // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#datetime_type for more details.
                String supportedFormat =
                        "civil_date_part[time_part]\n"
                                + "civil_date_part: YYYY-[M]M-[D]D\n"
                                + "time_part: { |T|t}[H]H:[M]M:[S]S[.F]\n"
                                + "YYYY: Four-digit year.\n"
                                + "[M]M: One or two digit month.\n"
                                + "[D]D: One or two digit day.\n"
                                + "{ |T|t}: A space or a T or t separator. The T and t separators are flags for time.\n"
                                + "[H]H: One or two digit hour (valid values from 00 to 23).\n"
                                + "[M]M: One or two digit minutes (valid values from 00 to 59).\n"
                                + "[S]S: One or two digit seconds (valid values from 00 to 60).\n"
                                + "[.F]: Up to six fractional digits (microsecond precision).";
                LOG.error(
                        String.format(
                                "%s%nSupported Format:%n%s", invalidFormatError, supportedFormat));
                throw new IllegalArgumentException(invalidFormatError);
            }
        } else if (object instanceof Timestamp) {
            //  com.google.cloud.Timestamp Instance.
            final Timestamp localDateTime = (Timestamp) object;
            tempMicros =
                    TimeUnit.SECONDS.toMicros(localDateTime.getSeconds())
                            + TimeUnit.NANOSECONDS.toMicros(localDateTime.getNanos());
        } else {
            String invalidFormatError =
                    getErrorMessage(
                            object,
                            "LONG, STRING/UTF-8,"
                                    + " com.google.cloud.Timestamp and java.time.Instant",
                            "TIMESTAMP/DATETIME");
            throw new IllegalArgumentException(invalidFormatError);
        }

        // All values are in Micros, millis have been returned.
        micros = tempMicros;
        long millis = TimeUnit.MICROSECONDS.toMillis(micros);
        long nanos = micros % 1000;
        nanos = TimeUnit.MICROSECONDS.toNanos(nanos);
        return TimestampData.fromEpochMillis(millis, (int) nanos);
    }

    private static int convertToDate(Object object) {
        if (object instanceof Integer) {
            return (Integer) object;
        } else if (object instanceof LocalDate) {
            return (int) ((LocalDate) object).toEpochDay();
        } else if (object instanceof org.joda.time.LocalDate) {
            final org.joda.time.LocalDate value = (org.joda.time.LocalDate) object;
            return (int) value.toDate().getTime();
        } else {
            String invalidFormatError =
                    getErrorMessage(
                            object, "INT, org.joda.time.LocalDate and java.time.LocalDate", "DATE");
            throw new IllegalArgumentException(invalidFormatError);
        }
    }

    private static int convertToMillisTime(Object object, int precision) {
        // if precision is 3. Otherwise, Error.
        Preconditions.checkArgument(
                precision == MILLIS_PRECISION,
                String.format(
                        "Avro to RowData Conversion Error:%n"
                                + "The millisecond-precision TIME type within the Avro schema uses a precision of '%d',"
                                + "Supported '%d'",
                        precision, MILLIS_PRECISION));
        if (object instanceof Integer) {
            return (int) object;
        } else if (object instanceof LocalTime) {
            return ((LocalTime) object).get(ChronoField.MILLI_OF_DAY);
        } else {
            String invalidFormatError =
                    getErrorMessage(object, "INT and java.time.LocalTime", "MILLIS-TIME");
            throw new IllegalArgumentException(invalidFormatError);
        }
    }

    private static long convertToMicrosTime(Object object, int precision) {
        // if precision is 6. Otherwise, Error.
        Preconditions.checkArgument(
                precision == MICROS_PRECISION,
                String.format(
                        "Avro to RowData Conversion Error:%n"
                                + "The microsecond-precision TIME type within the Avro schema uses a precision of '%d',"
                                + "Supported '%d'",
                        precision, MICROS_PRECISION));
        if (object instanceof Long) {
            return ((Long) object);
        } else if (object instanceof LocalTime) {
            return TimeUnit.NANOSECONDS.toMicros(((LocalTime) object).toNanoOfDay());
        } else {
            String invalidFormatError =
                    getErrorMessage(object, "LONG and java.time.LocalTime", "MICROS-TIME");
            throw new IllegalArgumentException(invalidFormatError);
        }
    }

    private static byte[] convertToBytes(Object object) {
        if (object instanceof GenericFixed) {
            return ((GenericFixed) object).bytes();
        } else if (object instanceof ByteBuffer) {
            ByteBuffer byteBuffer = (ByteBuffer) object;
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            return bytes;
        } else if (object instanceof byte[]) {
            return (byte[]) object;
        } else {
            String invalidFormatError =
                    getErrorMessage(object, "GenericFixed, byte[] and java.nio.ByteBuffer", "BYTE");
            throw new IllegalArgumentException(invalidFormatError);
        }
    }
}
