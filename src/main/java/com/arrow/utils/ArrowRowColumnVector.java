package com.arrow.utils;

import org.apache.arrow.vector.complex.StructVector;
import org.apache.flink.table.data.ColumnarRowData;
import org.apache.flink.table.data.vector.ColumnVector;
import org.apache.flink.table.data.vector.RowColumnVector;
import org.apache.flink.table.data.vector.VectorizedColumnBatch;
import org.apache.flink.util.Preconditions;

/** Arrow column vector for Row. */

public final class ArrowRowColumnVector implements RowColumnVector {

    /** Container which is used to store the sequence of row values of a column to read. */
    private final StructVector structVector;

    private final ColumnVector[] fieldColumns;

    public ArrowRowColumnVector(StructVector structVector, ColumnVector[] fieldColumns) {
        this.structVector = Preconditions.checkNotNull(structVector);
        this.fieldColumns = Preconditions.checkNotNull(fieldColumns);
    }

    @Override
    public ColumnarRowData getRow(int i) {
        VectorizedColumnBatch vectorizedColumnBatch = new VectorizedColumnBatch(fieldColumns);
        return new ColumnarRowData(vectorizedColumnBatch, i);
    }

    @Override
    public boolean isNullAt(int i) {
        return structVector.isNull(i);
    }
}
