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
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
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

import static org.apache.flink.formats.avro.typeutils.AvroSchemaConverter.extractValueTypeToAvroMap;

/** Tool class used to convert from Avro {@link GenericRecord} to {@link RowData}. * */
@Internal
public class AvroToRowDataConverters {

    private static final Logger LOG = LoggerFactory.getLogger(AvroToRowDataConverters.class);

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
                if (timePrecision <= 3) {
                    return avroObject -> convertToMillisTime(avroObject, 3);
                } else if (timePrecision <= 6) {
                    return avroObject -> convertToMicrosTime(avroObject, 6);
                } else {
                    throw new UnsupportedOperationException(
                            "TIME type with Precision > 6 is not supported!");
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
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static AvroToRowDataConverter createTimeDatetimeConvertor(int precision) {
        if (precision <= 3) {
            return avroObject -> AvroToRowDataConverters.convertToTimestamp(avroObject, 3);
        } else if (precision <= 6) {
            return avroObject -> AvroToRowDataConverters.convertToTimestamp(avroObject, 6);
        } else {
            throw new UnsupportedOperationException(
                    "TIMESTAMP/DATETIME TYPE of Precision > 6 is not supported!");
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
            } else {
                bytes = (byte[]) avroObject;
            }
            return DecimalData.fromUnscaledBytes(bytes, precision, scale);
        };
    }

    private static AvroToRowDataConverter createArrayConverter(ArrayType arrayType) {
        final AvroToRowDataConverter elementConverter =
                createNullableConverter(arrayType.getElementType());
        final Class<?> elementClass =
                LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());

        return avroObject -> {
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
                createNullableConverter(extractValueTypeToAvroMap(type));

        return avroObject -> {
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

    private static TimestampData convertToTimestamp(Object object, int precision) {
        final long micros;
        long tempMicros;
        if (object instanceof Long) {
            tempMicros = (Long) object;
            if (precision == 3) {
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
                throw new IllegalArgumentException(
                        String.format(
                                "The datetime string obtained %s, is of invalid format.", object));
            }
        } else {
            //  com.google.cloud.Timestamp Instant.
            JodaConverter jodaConverter = JodaConverter.getConverter();
            if (jodaConverter != null) {
                tempMicros = jodaConverter.convertTimestamp(object);
            } else {
                throw new IllegalArgumentException(
                        "Unexpected object type for TIMESTAMP logical type. Received: " + object);
            }
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
        } else {
            JodaConverter jodaConverter = JodaConverter.getConverter();
            if (jodaConverter != null) {
                return (int) jodaConverter.convertDate(object);
            } else {
                throw new IllegalArgumentException(
                        "Unexpected object type for DATE logical type. Received: " + object);
            }
        }
    }

    private static int convertToMillisTime(Object object, int precision) {
        // if precision is 3. Otherwise, Error.
        Preconditions.checkArgument(
                precision == 3,
                String.format(
                        "Invalid precision '%d' obtained for Millisecond Conversion", precision));
        if (object instanceof Integer) {
            return (int) object;
        } else if (object instanceof LocalTime) {
            return ((LocalTime) object).get(ChronoField.MILLI_OF_DAY);
        } else {
            String invalidFormatError =
                    String.format(
                            "Unexpected object '%s' of type '%s' obtained for TIME logical type.",
                            object, object.getClass());
            LOG.error(
                    String.format(
                            "%s%nSupported Types are 'INT' and 'java.time.LocalTime'",
                            invalidFormatError));
            throw new IllegalArgumentException(invalidFormatError);
        }
    }

//        private static int convertToMicrosTime(Object object, int precision) {
//            // if precision is 6. Otherwise, Error.
//            Preconditions.checkArgument(
//                    precision == 6,
//                    String.format(
//                            "Invalid precision '%d' obtained for Millisecond Conversion",
//     precision));
//            if (object instanceof Long) {
//                return (int) TimeUnit.MICROSECONDS.toMillis((Long) object);
//            } else if (object instanceof LocalTime) {
//                return (int) TimeUnit.NANOSECONDS.toMillis(((LocalTime) object).toNanoOfDay());
//            } else {
//                String invalidFormatError =
//                        String.format(
//                                "Unexpected object '%s' of type '%s' obtained for TIME logical
//     type.",
//                                object, object.getClass());
//                LOG.error(
//                        String.format(
//                                "%s%nSupported Types are 'LONG' and 'java.time.LocalTime'",
//                                invalidFormatError));
//                throw new IllegalArgumentException(invalidFormatError);
//            }
//        }

    private static long convertToMicrosTime(Object object, int precision) {
        // if precision is 6. Otherwise, Error.
        Preconditions.checkArgument(
                precision == 6,
                String.format(
                        "Invalid precision '%d' obtained for Millisecond Conversion", precision));
        if (object instanceof Long) {
            return ((Long) object);
        } else if (object instanceof LocalTime) {
            return TimeUnit.NANOSECONDS.toMicros(((LocalTime) object).toNanoOfDay());
        } else {
            String invalidFormatError =
                    String.format(
                            "Unexpected object '%s' of type '%s' obtained for TIME logical type.",
                            object, object.getClass());
            LOG.error(
                    String.format(
                            "%s%nSupported Types are 'LONG' and java.time.LocalTime'",
                            invalidFormatError));
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
        } else {
            return (byte[]) object;
        }
    }
}
