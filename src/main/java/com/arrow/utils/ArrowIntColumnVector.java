package com.arrow.utils;

import org.apache.arrow.vector.IntVector;
import org.apache.flink.table.data.vector.IntColumnVector;
import org.apache.flink.util.Preconditions;

/** Arrow column vector for Int. */

public final class ArrowIntColumnVector implements IntColumnVector {

    private final IntVector intVector;

    public ArrowIntColumnVector(IntVector intVector) {
        this.intVector = Preconditions.checkNotNull(intVector);
    }

    @Override
    public int getInt(int i) {
        return intVector.get(i);
    }

    @Override
    public boolean isNullAt(int i) {
        return intVector.isNull(i);
    }
}
