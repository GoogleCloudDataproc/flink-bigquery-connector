package com.arrow.utils;

import org.apache.arrow.vector.complex.ListVector;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.ColumnarArrayData;
import org.apache.flink.table.data.vector.ArrayColumnVector;
import org.apache.flink.table.data.vector.ColumnVector;
import org.apache.flink.util.Preconditions;

/** Arrow column vector for Array. */

public final class ArrowArrayColumnVector implements ArrayColumnVector {

    /** Container which is used to store the sequence of array values of a column to read. */
    private final ListVector listVector;

    private final ColumnVector elementVector;

    public ArrowArrayColumnVector(ListVector listVector, ColumnVector elementVector) {
        this.listVector = Preconditions.checkNotNull(listVector);
        this.elementVector = Preconditions.checkNotNull(elementVector);
    }

    @Override
    public ArrayData getArray(int i) {
        int index = i * ListVector.OFFSET_WIDTH;
        int start = listVector.getOffsetBuffer().getInt(index);
        int end = listVector.getOffsetBuffer().getInt(index + ListVector.OFFSET_WIDTH);
        return new ColumnarArrayData(elementVector, start, end - start);
    }

    @Override
    public boolean isNullAt(int i) {
        return listVector.isNull(i);
    }
}
