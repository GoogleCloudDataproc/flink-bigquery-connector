package com.arrow.utils;

import org.apache.arrow.vector.DateDayVector;
import org.apache.flink.table.data.vector.IntColumnVector;
import org.apache.flink.util.Preconditions;

/** Arrow column vector for Date. */

public final class ArrowDateColumnVector implements IntColumnVector {

    /** Container which is used to store the sequence of date values of a column to read. */
    private final DateDayVector dateDayVector;

    public ArrowDateColumnVector(DateDayVector dateDayVector) {
        this.dateDayVector = Preconditions.checkNotNull(dateDayVector);
    }

    @Override
    public int getInt(int i) {
        return dateDayVector.get(i);
    }

    @Override
    public boolean isNullAt(int i) {
        return dateDayVector.isNull(i);
    }
}
