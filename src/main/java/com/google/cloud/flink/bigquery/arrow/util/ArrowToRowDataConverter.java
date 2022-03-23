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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

@FunctionalInterface
public interface ArrowToRowDataConverter extends Serializable {

	Object convert(Object object);

	public static ArrowToRowDataConverter createRowConverter(RowType rowType) {
		final ArrowToRowDataConverter[] fieldConverters = rowType.getFields().stream().map(RowType.RowField::getType)
				.map(ArrowToRowDataConverter::createNullableConverter).toArray(ArrowToRowDataConverter[]::new);
		final int arity = rowType.getFieldCount();

		return arrowObject -> {
			VectorSchemaRoot record = (VectorSchemaRoot) arrowObject;
			int numOfRows = record.getRowCount();
			List<GenericRowData> rowdatalist = new ArrayList<GenericRowData>();
			for (int row = 0; row < numOfRows; ++row) {
				GenericRowData genericRowData = new GenericRowData(arity);
				for (int col = 0; col < arity; col++) {
					genericRowData.setField(col,
							fieldConverters[col].convert(record.getFieldVectors().get(col).getObject(row)));
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
		switch (type.getTypeRoot()) {
		case NULL:
			return arrowObject -> null;
		case TINYINT:
			return arrowObject -> ((Integer) arrowObject).byteValue();
		case SMALLINT:
			return arrowObject -> ((Integer) arrowObject).shortValue();
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
			return createDecimalConverter((DecimalType) type);
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

	static ArrowToRowDataConverter createDecimalConverter(DecimalType decimalType) {
		final int precision = decimalType.getPrecision();
		final int scale = decimalType.getScale();
		return arrowObject -> {
			final byte[] bytes;
			if (arrowObject instanceof ByteBuffer) {
				ByteBuffer byteBuffer = (ByteBuffer) arrowObject;
				bytes = new byte[byteBuffer.remaining()];
				byteBuffer.get(bytes);
			} else {
				bytes = (byte[]) arrowObject;
			}
			return DecimalData.fromUnscaledBytes(bytes, precision, scale);
		};
	}

	static ArrowToRowDataConverter createArrayConverter(ArrayType arrayType) {
		final ArrowToRowDataConverter elementConverter = createNullableConverter(arrayType.getElementType());
		final Class<?> elementClass = LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());

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

	static byte[] convertToBytes(Object object) {
		if (object instanceof ByteBuffer) {
			ByteBuffer byteBuffer = (ByteBuffer) object;
			byte[] bytes = new byte[byteBuffer.remaining()];
			byteBuffer.get(bytes);
			return bytes;
		} else {
			return (byte[]) object;
		}
	}
}
