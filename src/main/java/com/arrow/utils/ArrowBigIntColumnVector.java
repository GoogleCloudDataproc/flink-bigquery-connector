package com.arrow.utils;

import org.apache.arrow.vector.BigIntVector;
import org.apache.flink.table.data.vector.LongColumnVector;
import org.apache.flink.util.Preconditions;

/** Arrow column vector for BigInt. */

public final class ArrowBigIntColumnVector implements LongColumnVector {

    /** Container which is used to store the sequence of bigint values of a column to read. */
    private final BigIntVector bigIntVector;

    public ArrowBigIntColumnVector(BigIntVector bigIntVector) {
        this.bigIntVector = Preconditions.checkNotNull(bigIntVector);
    }

    @Override
    public long getLong(int i) {
        return bigIntVector.get(i);
    }

    @Override
    public boolean isNullAt(int i) {
        return bigIntVector.isNull(i);
    }
}
