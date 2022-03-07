package com.arrow.utils;

import org.apache.arrow.vector.BitVector;
import org.apache.flink.table.data.vector.BooleanColumnVector;
import org.apache.flink.util.Preconditions;

/** Arrow column vector for Boolean. */

public final class ArrowBooleanColumnVector implements BooleanColumnVector {

    /** Container which is used to store the sequence of boolean values of a column to read. */
    private final BitVector bitVector;

    public ArrowBooleanColumnVector(BitVector bitVector) {
        this.bitVector = Preconditions.checkNotNull(bitVector);
    }

    @Override
    public boolean getBoolean(int i) {
        return bitVector.get(i) != 0;
    }

    @Override
    public boolean isNullAt(int i) {
        return bitVector.isNull(i);
    }
}
