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
import java.sql.Time;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.JsonStringHashMap;
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
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.format.ISODateTimeFormat;

/** Tool class used to convert from Arrow {@link GenericRecord} to {@link RowData}. * */
@Internal
public class ArrowToRowDataConverters {

  /**
   * Runtime converter that converts Arrow data structures into objects of Flink Table & SQL
   * internal data structures.
   */
  @FunctionalInterface
  public interface ArrowToRowDataConverter extends Serializable {
    Object convert(Object object);
  }

  // -------------------------------------------------------------------------------------
  // Runtime Converters
  // -------------------------------------------------------------------------------------

  public static ArrowToRowDataConverter createRowConverter(
      RowType rowType, List<String> readSessionFieldNames, List<String> selectedFieldList) {
    final ArrowToRowDataConverter[] fieldConverters =
        rowType.getFields().stream()
            .map(RowType.RowField::getType)
            .map(ArrowToRowDataConverters::createNullableConverter)
            .toArray(ArrowToRowDataConverter[]::new);
    final int arity = rowType.getFieldCount();
    final List<String> selectedFields;
    if (selectedFieldList == null) {
      selectedFields = new ArrayList<String>();
    } else {
      selectedFields = selectedFieldList;
    }

    final List<String> arrowFields = new ArrayList<String>();

    return arrowObject -> {
      List<GenericRowData> rowdatalist = new ArrayList<GenericRowData>();
      if (arrowObject instanceof VectorSchemaRoot) {
        VectorSchemaRoot record = (VectorSchemaRoot) arrowObject;
        record.getSchema().getFields().stream()
            .forEach(
                field -> {
                  arrowFields.add(field.getName());
                });
        if (selectedFields.isEmpty()) {
          record.getSchema().getFields().stream()
              .forEach(
                  field -> {
                    selectedFields.add(field.getName());
                  });
        }
        int numOfRows = record.getRowCount();
        for (int row = 0; row < numOfRows; ++row) {
          GenericRowData genericRowData = new GenericRowData(arity);
          for (int col = 0; col < arity; col++) {
            String rowTypeField = selectedFields.get(col);
            int arrowFieldIdx = arrowFields.indexOf(rowTypeField);
            genericRowData.setField(
                col,
                fieldConverters[col].convert(
                    record.getFieldVectors().get(arrowFieldIdx).getObject(row)));
          }
          rowdatalist.add(genericRowData);
        }
      } else if (arrowObject instanceof JsonStringHashMap) {
        JsonStringHashMap record = (JsonStringHashMap) arrowObject;
        Set columnSet = record.keySet();
        Object[] columnSetArray = columnSet.toArray();
        GenericRowData genericRowData = new GenericRowData(arity);
        for (int col = 0; col < arity; col++) {
          genericRowData.setField(
              col, fieldConverters[col].convert(record.get(columnSetArray[col])));
        }
        return genericRowData;
      }
      return rowdatalist;
    };
  }

  /** Creates a runtime converter which is null safe. */
  private static ArrowToRowDataConverter createNullableConverter(LogicalType type) {
    final ArrowToRowDataConverter converter = createConverter(type);
    return arrowObject -> {
      if (arrowObject == null) {
        return null;
      }
      return converter.convert(arrowObject);
    };
  }

  /** Creates a runtime converter which assuming input object is not null. */
  private static ArrowToRowDataConverter createConverter(LogicalType type) {
    ArrowToRowDataConverter value;
    switch (type.getTypeRoot()) {
      case NULL:
        return arrowObject -> null;
      case TINYINT:
        return arrowObject -> arrowObject;
      case SMALLINT:
        return arrowObject -> ((Integer) arrowObject).shortValue();
      case DATE:
        return ArrowToRowDataConverters::convertToDate;
      case TIME_WITHOUT_TIME_ZONE:
        return ArrowToRowDataConverters::convertToTime;
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return ArrowToRowDataConverters::convertToTimestamp;
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
        return ArrowToRowDataConverters::convertToBytes;
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

  private static TimestampData convertToTimestamp(Object object) {
    final long millis;
    if (object instanceof Long) {
      millis = ((Long) object) / 1000;
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

  private static int convertToTime(Object object) {
    final int millis;
    if (object instanceof Integer) {
      int value = ((Integer) object);
      millis = (Integer) (Math.abs(value) / 1000);
    } else if (object instanceof Long) {
      int value = ((Long) object).intValue();
      Integer noZoneMillis = (Integer) (Math.abs(value) / 1000);
      int timeZoneOffset = new Time(noZoneMillis).getTimezoneOffset();
      millis = noZoneMillis + Math.abs(timeZoneOffset);
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

  private static ArrowToRowDataConverter createDecimalConverter(DecimalType decimalType) {
    final int precision = decimalType.getPrecision();
    final int scale = decimalType.getScale();
    return arrowObject -> {
      final byte[] bytes;
      if (arrowObject instanceof ByteBuffer) {
        ByteBuffer byteBuffer = (ByteBuffer) arrowObject;
        bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
      } else if (arrowObject instanceof BigDecimal) {
        DecimalData bdValue =
            DecimalData.fromBigDecimal((BigDecimal) arrowObject, precision, scale);
        return bdValue;
      } else {
        bytes = (byte[]) arrowObject;
      }
      return DecimalData.fromUnscaledBytes(bytes, precision, scale);
    };
  }

  private static ArrowToRowDataConverter createArrayConverter(ArrayType arrayType) {
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

  private static class JodaConverter {

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
      long dateTime = 0;
      if (!(object.toString().length() == 26)) {
        final DateTime value =
            DateTime.parse(object.toString(), ISODateTimeFormat.dateTimeParser().withZoneUTC());

        dateTime = value.toDate().getTime();
      } else if (object.toString().length() == 26) {
        DateTime value =
            DateTime.parse(object.toString(), ISODateTimeFormat.dateTimeParser().withZoneUTC());
        dateTime = value.toDate().getTime();
      }

      return dateTime;
    }

    private JodaConverter() {}
  }
}
