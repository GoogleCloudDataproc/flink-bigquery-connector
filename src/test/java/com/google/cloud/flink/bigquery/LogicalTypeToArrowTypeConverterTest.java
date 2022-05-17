/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.flink.bigquery;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.flink.bigquery.arrow.util.LogicalTypeToArrowTypeConverter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.junit.Test;

public class LogicalTypeToArrowTypeConverterTest {

  @Test
  public void logicalTypeToArrowTypeConverterVisitTest() {
    ArrowType arrowType;
    LogicalType logicalType;

    logicalType = DataTypes.STRING().getLogicalType();
    arrowType = logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE);
    assertThat(arrowType).isEqualTo(ArrowType.Utf8.INSTANCE);

    logicalType = DataTypes.TINYINT().getLogicalType();
    arrowType = logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE);
    assertThat(arrowType).isEqualTo(new ArrowType.Int(8, true));

    logicalType = DataTypes.SMALLINT().getLogicalType();
    arrowType = logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE);
    assertThat(arrowType).isEqualTo(new ArrowType.Int(2 * 8, true));

    logicalType = DataTypes.INT().getLogicalType();
    arrowType = logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE);
    assertThat(arrowType).isEqualTo(new ArrowType.Int(4 * 8, true));

    logicalType = DataTypes.BIGINT().getLogicalType();
    arrowType = logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE);
    assertThat(arrowType).isEqualTo(new ArrowType.Int(8 * 8, true));

    logicalType = DataTypes.BOOLEAN().getLogicalType();
    arrowType = logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE);
    assertThat(arrowType).isEqualTo(ArrowType.Bool.INSTANCE);

    logicalType = DataTypes.FLOAT().getLogicalType();
    arrowType = logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE);
    assertThat(arrowType).isEqualTo(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));

    logicalType = DataTypes.DOUBLE().getLogicalType();
    arrowType = logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE);
    assertThat(arrowType).isEqualTo(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));

    logicalType = DataTypes.VARCHAR(10).getLogicalType();
    arrowType = logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE);
    assertThat(arrowType).isEqualTo(ArrowType.Utf8.INSTANCE);

    logicalType = DataTypes.VARBINARY(10).getLogicalType();
    arrowType = logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE);
    assertThat(arrowType).isEqualTo(ArrowType.Binary.INSTANCE);

    logicalType = DataTypes.DECIMAL(38, 10).getLogicalType();
    arrowType = logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE);
    assertThat(arrowType)
        .isEqualTo(
            new ArrowType.Decimal(
                ((DecimalType) logicalType).getPrecision(),
                ((DecimalType) logicalType).getScale()));

    logicalType = DataTypes.DATE().getLogicalType();
    arrowType = logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE);
    assertThat(arrowType).isEqualTo(new ArrowType.Date(DateUnit.DAY));

    logicalType = DataTypes.TIME(0).getLogicalType();
    arrowType = logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE);
    assertThat(arrowType).isEqualTo(new ArrowType.Time(TimeUnit.SECOND, 32));

    logicalType = DataTypes.TIME(1).getLogicalType();
    arrowType = logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE);
    assertThat(arrowType).isEqualTo(new ArrowType.Time(TimeUnit.MILLISECOND, 32));

    logicalType = DataTypes.TIME(5).getLogicalType();
    arrowType = logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE);
    assertThat(arrowType).isEqualTo(new ArrowType.Time(TimeUnit.MICROSECOND, 64));

    logicalType = DataTypes.TIME(7).getLogicalType();
    arrowType = logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE);
    assertThat(arrowType).isEqualTo(new ArrowType.Time(TimeUnit.NANOSECOND, 64));

    logicalType = DataTypes.TIMESTAMP(0).getLogicalType();
    arrowType = logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE);
    assertThat(arrowType).isEqualTo(new ArrowType.Timestamp(TimeUnit.SECOND, null));

    logicalType = DataTypes.TIMESTAMP(1).getLogicalType();
    arrowType = logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE);
    assertThat(arrowType).isEqualTo(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null));

    logicalType = DataTypes.TIMESTAMP(5).getLogicalType();
    arrowType = logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE);
    assertThat(arrowType).isEqualTo(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null));

    logicalType = DataTypes.TIMESTAMP(7).getLogicalType();
    arrowType = logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE);
    assertThat(arrowType).isEqualTo(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null));

    logicalType = DataTypes.ARRAY(DataTypes.INT().notNull()).getLogicalType();
    arrowType = logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE);
    assertThat(arrowType).isEqualTo(ArrowType.List.INSTANCE);

    logicalType = DataTypes.ROW().getLogicalType();
    arrowType = logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE);
    assertThat(arrowType).isEqualTo(ArrowType.Struct.INSTANCE);

    logicalType = DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(0).getLogicalType();
    arrowType = logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE);
    assertThat(arrowType).isEqualTo(new ArrowType.Timestamp(TimeUnit.SECOND, null));

    logicalType = DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(1).getLogicalType();
    arrowType = logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE);
    assertThat(arrowType).isEqualTo(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null));

    logicalType = DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(5).getLogicalType();
    arrowType = logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE);
    assertThat(arrowType).isEqualTo(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null));

    logicalType = DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(7).getLogicalType();
    arrowType = logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE);
    assertThat(arrowType).isEqualTo(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null));
  }
}
