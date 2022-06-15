/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.flink.bigquery.util.avro;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

/** Tool class used to convert from Avro {@link GenericRecord} to {@link RowData}. * */
@Internal
public class AvroToRowDataConverters {

  /**
   * Runtime converter that converts Avro data structures into objects of Flink Table & SQL internal
   * data structures.
   */
  @FunctionalInterface
  public interface AvroToRowDataConverter extends Serializable {
    Object convert(Object object);
  }

  // -------------------------------------------------------------------------------------
  // Runtime Converters
  // -------------------------------------------------------------------------------------

  public static AvroToRowDataConverter createRowConverter(
      RowType rowType, List<String> readAvroFields, List<String> selectedFieldList) {
    final AvroToRowDataConverter[] fieldConverters =
        rowType.getFields().stream()
            .map(RowType.RowField::getType)
            .map(AvroToRowDataConverters::createNullableConverter)
            .toArray(AvroToRowDataConverter[]::new);
    final int arity = rowType.getFieldCount();
    List<String> rowFieldNameList = rowType.getFieldNames();

    return avroObject -> {
      return getAvroObject(
          fieldConverters, arity, rowFieldNameList, readAvroFields, avroObject, selectedFieldList);
    };
  }

  private static Object getAvroObject(
      final AvroToRowDataConverter[] fieldConverters,
      final int arity,
      final List<String> rowFieldNameList,
      final List<String> readAvroFields,
      Object avroObject,
      List<String> selectedFieldList) {
    IndexedRecord record = (IndexedRecord) avroObject;
    GenericRowData row = new GenericRowData(arity);
    if (avroObject instanceof IndexedRecord) {
      for (int i = 0; i < arity; ++i) {
        String selectedField = selectedFieldList.get(i);
        int readAvroFieldIndx = readAvroFields.indexOf(selectedField);
        row.setField(i, fieldConverters[i].convert(record.get(readAvroFieldIndx)));
      }
    }
    return row;
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
    AvroToRowDataConverter value;
    switch (type.getTypeRoot()) {
      case NULL:
        return avroObject -> null;
      case TINYINT:
        return avroObject -> avroObject;
      case SMALLINT:
        return avroObject -> ((Integer) avroObject).shortValue();
      case DATE:
        return AvroToRowDataConverters::convertToDate;
      case TIME_WITHOUT_TIME_ZONE:
        return AvroToRowDataConverters::convertToTime;
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return AvroToRowDataConverters::convertToTimestamp;
      case BOOLEAN: // boolean
      case INTEGER: // int
      case INTERVAL_YEAR_MONTH: // long
      case BIGINT: // long
      case INTERVAL_DAY_TIME: // long
      case FLOAT: // float
      case DOUBLE: // double
        return avroObject -> avroObject;
      case CHAR:
      case VARCHAR:
        return avroObject -> StringData.fromString(avroObject.toString());
      case BINARY:
      case VARBINARY:
        return AvroToRowDataConverters::convertToBytes;
      case DECIMAL:
        value = createDecimalConverter((DecimalType) type);
        return value;
      case ARRAY:
        return createArrayConverter((ArrayType) type);
      case ROW:
        return createRowConverter(
            (RowType) type, ((RowType) type).getFieldNames(), ((RowType) type).getFieldNames());
      case MAP:
      case RAW:
      default:
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }
  }

  private static AvroToRowDataConverter createDecimalConverter(DecimalType decimalType) {
    final int precision = decimalType.getPrecision();
    final int scale = decimalType.getScale();
    return avroObject -> {
      final byte[] bytes;
      if (avroObject instanceof ByteBuffer) {
        ByteBuffer byteBuffer = (ByteBuffer) avroObject;
        bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
      } else if (avroObject instanceof BigDecimal) {
        DecimalData bdValue = DecimalData.fromBigDecimal((BigDecimal) avroObject, precision, scale);
        return bdValue;
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

  private static TimestampData convertToTimestamp(Object object) {
    final long millis;
    if (object instanceof Long) {
      millis = ((Long) object) / 1000;
    } else if (object instanceof Instant) {
      millis = ((Instant) object).toEpochMilli();
    } else {
      JavaUtilConverter javaUtilConverter = JavaUtilConverter.getConverter();
      if (javaUtilConverter != null) {
        millis = javaUtilConverter.convertTimestamp(object);
      } else {
        throw new IllegalArgumentException(
            "Unexpected object type for TIMESTAMP logical type. Received: " + object);
      }
    }
    return TimestampData.fromEpochMillis(millis);
  }

  private static int convertToDate(Object object) {
    if (object instanceof Integer) {
      return (Integer) object;
    } else if (object instanceof LocalDate) {
      return (int) ((LocalDate) object).toEpochDay();
    } else {
      JavaUtilConverter javaUtilConverter = JavaUtilConverter.getConverter();
      if (javaUtilConverter != null) {
        return (int) javaUtilConverter.convertDate(object);
      } else {
        throw new IllegalArgumentException(
            "Unexpected object type for DATE logical type. Received: " + object);
      }
    }
  }

  private static int convertToTime(Object object) {
    ZoneId zoneId = ZoneId.of("UTC");
    final int millis;
    if (object instanceof Integer) {
      int value = ((Integer) object);
      millis = (Integer) (Math.abs(value) / 1000);
    } else if (object instanceof Long) {
      Long value = ((Long) object);
      value = (Math.abs(value) / 1000);
      LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochMilli(value), zoneId);
      millis = (int) time.atZone(zoneId).toInstant().toEpochMilli();

    } else if (object instanceof LocalTime) {
      millis = ((LocalTime) object).get(ChronoField.MILLI_OF_DAY);
    } else {
      JavaUtilConverter javaUtilConverter = JavaUtilConverter.getConverter();
      if (javaUtilConverter != null) {
        millis = javaUtilConverter.convertTime(object);
      } else {
        throw new IllegalArgumentException(
            "Unexpected object type for TIME logical type. Received: " + object);
      }
    }
    return millis;
  }

  private static byte[] convertToBytes(Object object) {
    if (object instanceof ByteBuffer) {
      ByteBuffer byteBuffer = (ByteBuffer) object;
      byte[] bytes = new byte[byteBuffer.remaining()];
      byteBuffer.get(bytes);
      return bytes;
    } else {
      return (byte[]) object;
    }
  }

  private static class JavaUtilConverter {

    private static JavaUtilConverter instance;
    private static boolean instantiated = false;

    public static JavaUtilConverter getConverter() {
      if (instantiated) {
        return instance;
      }
      try {
        Class.forName(
            "java.time.LocalDateTime", false, Thread.currentThread().getContextClassLoader());
        instance = new JavaUtilConverter();
      } catch (Exception e) {
        instance = null;
      } finally {
        instantiated = true;
      }
      return instance;
    }

    public long convertDate(Object object) {
      final java.time.LocalDate localDate = (LocalDate) object;
      return localDate.toEpochDay();
    }

    public int convertTime(Object object) {
      final java.time.LocalDateTime localDateTime = (LocalDateTime) object;
      return (int) localDateTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
    }

    public long convertTimestamp(Object object) {
      final java.time.LocalDateTime localDateTime = (LocalDateTime) object;
      return localDateTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
    }

    private JavaUtilConverter() {}
  }
}
