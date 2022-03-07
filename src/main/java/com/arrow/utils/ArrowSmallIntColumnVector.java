package com.arrow.utils;

import org.apache.arrow.vector.SmallIntVector;
import org.apache.flink.table.data.vector.ShortColumnVector;
import org.apache.flink.util.Preconditions;

/** Arrow column vector for Int. */

public final class ArrowSmallIntColumnVector implements ShortColumnVector {

    private final SmallIntVector smallIntVector;

    public ArrowSmallIntColumnVector(SmallIntVector smallIntVector) {
        this.smallIntVector = Preconditions.checkNotNull(smallIntVector);
    }

    @Override
    public short getShort(int i) {
        return smallIntVector.get(i);
    }

    @Override
    public boolean isNullAt(int i) {
        return smallIntVector.isNull(i);
    }
}
