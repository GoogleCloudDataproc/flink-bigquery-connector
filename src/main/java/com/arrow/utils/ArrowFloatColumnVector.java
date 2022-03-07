package com.arrow.utils;

import org.apache.arrow.vector.Float4Vector;
import org.apache.flink.table.data.vector.FloatColumnVector;
import org.apache.flink.util.Preconditions;

/** Arrow column vector for Float. */

public final class ArrowFloatColumnVector implements FloatColumnVector {

    /** Container which is used to store the sequence of float values of a column to read. */
    private final Float4Vector floatVector;

    public ArrowFloatColumnVector(Float4Vector floatVector) {
        this.floatVector = Preconditions.checkNotNull(floatVector);
    }

    @Override
    public float getFloat(int i) {
        return floatVector.get(i);
    }

    @Override
    public boolean isNullAt(int i) {
        return floatVector.isNull(i);
    }
}
