package com.arrow.utils;

import org.apache.arrow.vector.DecimalVector;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.vector.DecimalColumnVector;
import org.apache.flink.util.Preconditions;

/** Arrow column vector for DecimalData. */

public final class ArrowDecimalColumnVector implements DecimalColumnVector {

    /** Container which is used to store the sequence of DecimalData values of a column to read. */
    private final DecimalVector decimalVector;

    public ArrowDecimalColumnVector(DecimalVector decimalVector) {
        this.decimalVector = Preconditions.checkNotNull(decimalVector);
    }

    @Override
    public DecimalData getDecimal(int i, int precision, int scale) {
        return DecimalData.fromBigDecimal(decimalVector.getObject(i), precision, scale);
    }

    @Override
    public boolean isNullAt(int i) {
        return decimalVector.isNull(i);
    }
}
