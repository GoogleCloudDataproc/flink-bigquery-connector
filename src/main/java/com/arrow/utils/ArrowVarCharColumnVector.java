package com.arrow.utils;

import org.apache.arrow.vector.VarCharVector;
import org.apache.flink.table.data.vector.BytesColumnVector;
import org.apache.flink.util.Preconditions;

/** Arrow column vector for VarChar. */

public final class ArrowVarCharColumnVector implements BytesColumnVector {

    /** Container which is used to store the sequence of varchar values of a column to read. */
    private final VarCharVector varCharVector;

    public ArrowVarCharColumnVector(VarCharVector varCharVector) {
        this.varCharVector = Preconditions.checkNotNull(varCharVector);
    }

    @Override
    public Bytes getBytes(int i) {
        byte[] bytes = varCharVector.get(i);
        return new Bytes(bytes, 0, bytes.length);
    }

    @Override
    public boolean isNullAt(int i) {
        return varCharVector.isNull(i);
    }
}
