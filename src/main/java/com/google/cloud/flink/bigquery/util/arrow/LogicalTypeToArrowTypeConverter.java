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
package com.google.cloud.flink.bigquery.util.arrow;

import java.math.BigDecimal;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;

/** Converter for logical types to Arrow types */
public class LogicalTypeToArrowTypeConverter extends LogicalTypeDefaultVisitor<ArrowType> {

  @Override
  protected ArrowType defaultMethod(LogicalType logicalType) {
    if (logicalType instanceof LegacyTypeInformationType) {
      Class<?> typeClass =
          ((LegacyTypeInformationType<?>) logicalType).getTypeInformation().getTypeClass();
      if (typeClass == BigDecimal.class) {
        return new ArrowType.Decimal(38, 18, 128);
      }
    }
    throw new UnsupportedOperationException(
        String.format(
            "Python vectorized UDF doesn't support logical type %s currently.",
            logicalType.asSummaryString()));
  }

  public static final LogicalTypeToArrowTypeConverter INSTANCE =
      new LogicalTypeToArrowTypeConverter();

  @Override
  public ArrowType visit(TinyIntType tinyIntType) {
    return new ArrowType.Int(8, true);
  }

  @Override
  public ArrowType visit(SmallIntType smallIntType) {
    return new ArrowType.Int(2 * 8, true);
  }

  @Override
  public ArrowType visit(IntType intType) {
    return new ArrowType.Int(4 * 8, true);
  }

  @Override
  public ArrowType visit(BigIntType bigIntType) {
    return new ArrowType.Int(8 * 8, true);
  }

  @Override
  public ArrowType visit(BooleanType booleanType) {
    return ArrowType.Bool.INSTANCE;
  }

  @Override
  public ArrowType visit(FloatType floatType) {
    return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
  }

  @Override
  public ArrowType visit(DoubleType doubleType) {
    return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
  }

  @Override
  public ArrowType visit(VarCharType varCharType) {
    return ArrowType.Utf8.INSTANCE;
  }

  @Override
  public ArrowType visit(VarBinaryType varCharType) {
    return ArrowType.Binary.INSTANCE;
  }

  @Override
  public ArrowType visit(DecimalType decimalType) {
    return new ArrowType.Decimal(decimalType.getPrecision(), decimalType.getScale(), 128);
  }

  @Override
  public ArrowType visit(DateType dateType) {
    return new ArrowType.Date(DateUnit.DAY);
  }

  @Override
  public ArrowType visit(TimeType timeType) {
    if (timeType.getPrecision() == 0) {
      return new ArrowType.Time(TimeUnit.SECOND, 32);
    } else if (timeType.getPrecision() >= 1 && timeType.getPrecision() <= 3) {
      return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
    } else if (timeType.getPrecision() >= 4 && timeType.getPrecision() <= 6) {
      return new ArrowType.Time(TimeUnit.MICROSECOND, 64);
    } else {
      return new ArrowType.Time(TimeUnit.NANOSECOND, 64);
    }
  }

  @Override
  public ArrowType visit(LocalZonedTimestampType localZonedTimestampType) {
    if (localZonedTimestampType.getPrecision() == 0) {
      return new ArrowType.Timestamp(TimeUnit.SECOND, null);
    } else if (localZonedTimestampType.getPrecision() >= 1
        && localZonedTimestampType.getPrecision() <= 3) {
      return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
    } else if (localZonedTimestampType.getPrecision() >= 4
        && localZonedTimestampType.getPrecision() <= 6) {
      return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
    } else {
      return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
    }
  }

  @Override
  public ArrowType visit(TimestampType timestampType) {
    if (timestampType.getPrecision() == 0) {
      return new ArrowType.Timestamp(TimeUnit.SECOND, null);
    } else if (timestampType.getPrecision() >= 1 && timestampType.getPrecision() <= 3) {
      return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
    } else if (timestampType.getPrecision() >= 4 && timestampType.getPrecision() <= 6) {
      return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
    } else {
      return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
    }
  }

  @Override
  public ArrowType visit(ArrayType arrayType) {
    return ArrowType.List.INSTANCE;
  }

  @Override
  public ArrowType visit(RowType rowType) {
    return ArrowType.Struct.INSTANCE;
  }
}
