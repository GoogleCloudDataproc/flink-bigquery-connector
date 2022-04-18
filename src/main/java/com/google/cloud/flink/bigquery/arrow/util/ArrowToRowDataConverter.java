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
package com.google.cloud.flink.bigquery.arrow.util;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;

@FunctionalInterface
public interface ArrowToRowDataConverter extends Serializable {

  Object convert(Object object);

  // -------------------------------------------------------------------------------------
  // Runtime Converters
  // -------------------------------------------------------------------------------------

  public static ArrowToRowDataConverter createRowConverter(RowType rowType) {
    final ArrowToRowDataConverter[] fieldConverters =
        rowType.getFields().stream()
            .map(RowType.RowField::getType)
            .map(ArrowToRowDataConverter::createNullableConverter)
            .toArray(ArrowToRowDataConverter[]::new);
    final int arity = rowType.getFieldCount();

    return arrowObject -> {
      VectorSchemaRoot record = (VectorSchemaRoot) arrowObject;
      int numOfRows = record.getRowCount();
      List<GenericRowData> rowdatalist = new ArrayList<GenericRowData>();
      for (int row = 0; row < numOfRows; ++row) {
        GenericRowData genericRowData = new GenericRowData(arity);
        for (int col = 0; col < arity; col++) {
          genericRowData.setField(
              col, fieldConverters[col].convert(record.getFieldVectors().get(col).getObject(row)));
        }
        rowdatalist.add(genericRowData);
      }
      return rowdatalist;
    };
  }

  /** Creates a runtime converter which is null safe. */
  static ArrowToRowDataConverter createNullableConverter(LogicalType type) {
    final ArrowToRowDataConverter converter = createConverter(type);
    return arrowObject -> {
      if (arrowObject == null) {
        return null;
      }
      return converter.convert(arrowObject);
    };
  }

  /** Creates a runtime converter which assuming input object is not null. */
  static ArrowToRowDataConverter createConverter(LogicalType type) {
    ArrowToRowDataConverter value;
    switch (type.getTypeRoot()) {
      case NULL:
        return arrowObject -> null;
      case TINYINT:
        return arrowObject -> ((Integer) arrowObject).byteValue();
      case SMALLINT:
        return arrowObject -> ((Integer) arrowObject).shortValue();
      case DATE: // ########
        return ArrowToRowDataConverter::convertToDate;
      case TIME_WITHOUT_TIME_ZONE: // #####
        return ArrowToRowDataConverter::convertToTime;
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        //			return ArrowToRowDataConverter::convertToTimestamp;//##########
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE: // ###########
        return ArrowToRowDataConverter::convertToTimestamp;
      case BOOLEAN: // boolean
      case INTEGER: // int
      case INTERVAL_YEAR_MONTH: // long
      case BIGINT: // long
      case INTERVAL_DAY_TIME: // long
      case FLOAT: // float
      case DOUBLE: // double
        return arrowObject -> arrowObject;
      case CHAR:
      case VARCHAR:
        return arrowObject -> StringData.fromString(arrowObject.toString());
      case BINARY:
      case VARBINARY:
        return ArrowToRowDataConverter::convertToBytes;
      case DECIMAL:
        value = createDecimalConverter((DecimalType) type);
        return value;
      case ARRAY:
        return createArrayConverter((ArrayType) type);
      case ROW:
        return createRowConverter((RowType) type);
      case MAP:
      case RAW:
      default:
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }
  }

  public static TimestampData convertToTimestamp(Object object) {
    final long millis;
    if (object instanceof Long) {
      millis = (Long) object;
    } else if (object instanceof Instant) {
      millis = ((Instant) object).toEpochMilli();
    } else {
      JodaConverter jodaConverter = JodaConverter.getConverter();
      if (jodaConverter != null) {
        millis = jodaConverter.convertTimestamp(object);
      } else {
        throw new IllegalArgumentException(
            "Unexpected object type for TIMESTAMP logical type. Received: " + object);
      }
    }
    return TimestampData.fromEpochMillis(millis);
  }

  public static int convertToDate(Object object) {
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

  static int convertToTime(Object object) {
    final int millis;
    boolean neg = false;
    if (object instanceof Integer) {
      int objectMillis = Integer.parseInt(object.toString());
      java.sql.Time sqlTime = new java.sql.Time(objectMillis);
      millis = 0;
    } else if (object instanceof LocalTime) {
      millis = ((LocalTime) object).get(ChronoField.MILLI_OF_DAY);
    } else {
      JodaConverter jodaConverter = JodaConverter.getConverter();
      if (jodaConverter != null) {
        millis = jodaConverter.convertTime(object);
      } else {
        throw new IllegalArgumentException(
            "Unexpected object type for TIME logical type. Received: " + object);
      }
    }
    return millis;
  }

  static ArrowToRowDataConverter createDecimalConverter(DecimalType decimalType) {
    final int precision = decimalType.getPrecision();
    final int scale = decimalType.getScale();

    return arrowObject -> {
      final byte[] bytes;
      if (arrowObject instanceof ByteBuffer) {
        ByteBuffer byteBuffer = (ByteBuffer) arrowObject;
        bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
      } else if (arrowObject instanceof BigDecimal) {
        DecimalData bdValue = DecimalData.fromBigDecimal((BigDecimal) arrowObject, 38, 0);
        return bdValue;
      } else {
        bytes = (byte[]) arrowObject;
      }
      return DecimalData.fromUnscaledBytes(bytes, 38, 0);
    };
  }

  static ArrowToRowDataConverter createArrayConverter(ArrayType arrayType) {
    final ArrowToRowDataConverter elementConverter =
        createNullableConverter(arrayType.getElementType());
    final Class<?> elementClass =
        LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());

    return arrowObject -> {
      final List<?> list = (List<?>) arrowObject;
      final int length = list.size();
      final Object[] array = (Object[]) Array.newInstance(elementClass, length);
      for (int i = 0; i < length; ++i) {
        array[i] = elementConverter.convert(list.get(i));
      }
      return new GenericArrayData(array);
    };
  }

  public static byte[] convertToBytes(Object object) {
    if (object instanceof ByteBuffer) {
      ByteBuffer byteBuffer = (ByteBuffer) object;
      byte[] bytes = new byte[byteBuffer.remaining()];
      byteBuffer.get(bytes);
      return bytes;
    } else {
      return (byte[]) object;
    }
  }

  static class JodaConverter {

    private static JodaConverter instance;
    private static boolean instantiated = false;

    public static JodaConverter getConverter() {
      if (instantiated) {
        return instance;
      }
      try {
        Class.forName(
            "org.joda.time.DateTime", false, Thread.currentThread().getContextClassLoader());
        instance = new JodaConverter();
      } catch (Exception e) {
        instance = null;
      } finally {
        instantiated = true;
      }
      return instance;
    }

    public long convertDate(Object object) {
      final org.joda.time.LocalDate value = (org.joda.time.LocalDate) object;
      return value.toDate().getTime();
    }

    public int convertTime(Object object) {
      final org.joda.time.LocalTime value = (org.joda.time.LocalTime) object;
      return value.get(DateTimeFieldType.millisOfDay());
    }

    public long convertTimestamp(Object object) {
      final DateTime value = DateTime.parse(object.toString());
      return value.toDate().getTime();
    }

    private JodaConverter() {}
  }
}
