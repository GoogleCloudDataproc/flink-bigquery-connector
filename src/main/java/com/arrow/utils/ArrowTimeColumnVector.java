package com.arrow.utils;

import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.flink.table.data.vector.IntColumnVector;
import org.apache.flink.util.Preconditions;

/** Arrow column vector for Time. */

public final class ArrowTimeColumnVector implements IntColumnVector {

    /** Container which is used to store the sequence of time values of a column to read. */
    private final ValueVector valueVector;

    public ArrowTimeColumnVector(ValueVector valueVector) {
        this.valueVector = Preconditions.checkNotNull(valueVector);
        Preconditions.checkState(
                valueVector instanceof TimeSecVector
                        || valueVector instanceof TimeMilliVector
                        || valueVector instanceof TimeMicroVector
                        || valueVector instanceof TimeNanoVector);
    }

    @Override
    public int getInt(int i) {
        if (valueVector instanceof TimeSecVector) {
            return ((TimeSecVector) valueVector).get(i) * 1000;
        } else if (valueVector instanceof TimeMilliVector) {
            return ((TimeMilliVector) valueVector).get(i);
        } else if (valueVector instanceof TimeMicroVector) {
            return (int) (((TimeMicroVector) valueVector).get(i) / 1000);
        } else {
            return (int) (((TimeNanoVector) valueVector).get(i) / 1000000);
        }
    }

    @Override
    public boolean isNullAt(int i) {
        return valueVector.isNull(i);
    }
}
