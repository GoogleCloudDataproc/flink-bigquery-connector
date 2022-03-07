package com.arrow.utils;

import org.apache.arrow.vector.Float8Vector;
import org.apache.flink.table.data.vector.DoubleColumnVector;
import org.apache.flink.util.Preconditions;

/** Arrow column vector for Double. */

public final class ArrowDoubleColumnVector implements DoubleColumnVector {

    /** Container which is used to store the sequence of double values of a column to read. */
    private final Float8Vector doubleVector;

    public ArrowDoubleColumnVector(Float8Vector doubleVector) {
        this.doubleVector = Preconditions.checkNotNull(doubleVector);
    }

    @Override
    public double getDouble(int i) {
        return doubleVector.get(i);
    }

    @Override
    public boolean isNullAt(int i) {
        return doubleVector.isNull(i);
    }
}
