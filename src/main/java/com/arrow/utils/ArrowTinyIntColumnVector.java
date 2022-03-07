package com.arrow.utils;

import org.apache.arrow.vector.TinyIntVector;
import org.apache.flink.table.data.vector.ByteColumnVector;
import org.apache.flink.util.Preconditions;

/** Arrow column vector for TinyInt. */

public final class ArrowTinyIntColumnVector implements ByteColumnVector {

    private final TinyIntVector tinyIntVector;

    public ArrowTinyIntColumnVector(TinyIntVector tinyIntVector) {
        this.tinyIntVector = Preconditions.checkNotNull(tinyIntVector);
    }

    @Override
    public byte getByte(int i) {
        return tinyIntVector.get(i);
    }

    @Override
    public boolean isNullAt(int i) {
        return tinyIntVector.isNull(i);
    }
}
