package com.arrow.utils;

import org.apache.arrow.vector.VarBinaryVector;
import org.apache.flink.table.data.vector.BytesColumnVector;
import org.apache.flink.util.Preconditions;

/** Arrow column vector for VarBinary. */

public final class ArrowVarBinaryColumnVector implements BytesColumnVector {

    /** Container which is used to store the sequence of varbinary values of a column to read. */
    private final VarBinaryVector varBinaryVector;

    public ArrowVarBinaryColumnVector(VarBinaryVector varBinaryVector) {
        this.varBinaryVector = Preconditions.checkNotNull(varBinaryVector);
    }

    @Override
    public Bytes getBytes(int i) {
        byte[] bytes = varBinaryVector.get(i);
        return new Bytes(bytes, 0, bytes.length);
    }

    @Override
    public boolean isNullAt(int i) {
        return varBinaryVector.isNull(i);
    }
}
