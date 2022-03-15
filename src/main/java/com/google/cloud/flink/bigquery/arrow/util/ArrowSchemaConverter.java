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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import org.apache.flink.table.data.vector.ColumnVector;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Converts an Avro schema into Flink's type information. It uses
 * {@link RowTypeInfo} for representing objects and converts Avro types into
 * types that are compatible with Flink's Table & SQL API.
 *
 * <p>
 * Note: Changes in this class need to be kept in sync with the corresponding
 * runtime classes {@link AvroRowDeserializationSchema} and
 * {@link AvroRowSerializationSchema}.
 */
public class ArrowSchemaConverter implements Serializable {

	private static final long serialVersionUID = 1L;
	private static Schema schema;

	public ArrowSchemaConverter() {

	}

	public static Schema convertToSchema(RowType rowType) {
		Collection<Field> fields = rowType.getFields().stream().map(f -> convertToSchema(f.getName(), f.getType()))
				.collect(Collectors.toCollection(ArrayList::new));
		return new Schema(fields);
	}

	public static Field convertToSchema(String fieldName, LogicalType logicalType) {
		FieldType fieldType = new FieldType(logicalType.isNullable(),
				logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE), null);
		List<Field> children = null;
		if (logicalType instanceof ArrayType) {
			children = Collections
					.singletonList(convertToSchema("element", ((ArrayType) logicalType).getElementType()));
		} else if (logicalType instanceof RowType) {
			RowType rowType = (RowType) logicalType;
			children = new ArrayList<>(rowType.getFieldCount());
			for (RowType.RowField field : rowType.getFields()) {
				children.add(convertToSchema(field.getName(), field.getType()));
			}
		}
		return new Field(fieldName, fieldType, children);
	}

	public static ColumnVector createColumnVector(ValueVector vector, LogicalType fieldType) {
		if (vector instanceof TinyIntVector) {
			return new ArrowTinyIntColumnVector((TinyIntVector) vector);
		} else if (vector instanceof SmallIntVector) {
			return new ArrowSmallIntColumnVector((SmallIntVector) vector);
		} else if (vector instanceof IntVector) {
			return new ArrowIntColumnVector((IntVector) vector);
		} else if (vector instanceof BigIntVector) {
			return new ArrowBigIntColumnVector((BigIntVector) vector);
		} else if (vector instanceof BitVector) {
			return new ArrowBooleanColumnVector((BitVector) vector);
		} else if (vector instanceof Float4Vector) {
			return new ArrowFloatColumnVector((Float4Vector) vector);
		} else if (vector instanceof Float8Vector) {
			return new ArrowDoubleColumnVector((Float8Vector) vector);
		} else if (vector instanceof VarCharVector) {
			return new ArrowVarCharColumnVector((VarCharVector) vector);
		} else if (vector instanceof VarBinaryVector) {
			return new ArrowVarBinaryColumnVector((VarBinaryVector) vector);
		} else if (vector instanceof DecimalVector) {
			return new ArrowDecimalColumnVector((DecimalVector) vector);
		} else if (vector instanceof DateDayVector) {
			return new ArrowDateColumnVector((DateDayVector) vector);
		} else if (vector instanceof TimeSecVector || vector instanceof TimeMilliVector
				|| vector instanceof TimeMicroVector || vector instanceof TimeNanoVector) {
			return new ArrowTimeColumnVector(vector);
		} else if (vector instanceof TimeStampVector
				&& ((ArrowType.Timestamp) vector.getField().getType()).getTimezone() == null) {
			return new ArrowTimestampColumnVector(vector);
		} else if (vector instanceof ListVector) {
			ListVector listVector = (ListVector) vector;
			return new ArrowArrayColumnVector(listVector,
					createColumnVector(listVector.getDataVector(), ((ArrayType) fieldType).getElementType()));
		} else if (vector instanceof StructVector) {
			StructVector structVector = (StructVector) vector;
			ColumnVector[] fieldColumns = new ColumnVector[structVector.size()];
			for (int i = 0; i < fieldColumns.length; ++i) {
				fieldColumns[i] = createColumnVector(structVector.getVectorById(i), ((RowType) fieldType).getTypeAt(i));
			}
			return new ArrowRowColumnVector(structVector, fieldColumns);
		} else {
			throw new UnsupportedOperationException(String.format("Unsupported type %s.", fieldType));
		}
	}
}
