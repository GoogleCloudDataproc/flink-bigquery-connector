package com.google.cloud.flink.bigquery.source.reader.deserializer;

import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;

/**
 * Encapsulates joda optional dependency. Instantiates this class only if joda is available on the
 * classpath.
 *
 * <p>This implementation is copied from
 */
class JodaConverter {

    private static JodaConverter instance;
    private static boolean instantiated = false;

    public static JodaConverter getConverter() {
        if (instantiated) {
            return instance;
        }

        try {
            Class.forName(
                    "org.joda.time.DateTime",
                    false,
                    Thread.currentThread().getContextClassLoader());
            instance = new JodaConverter();
        } catch (ClassNotFoundException e) {
            instance = null;
        } finally {
            instantiated = true;
        }
        return instance;
    }

    public long convertDate(Object object) {
        final org.joda.time.LocalDate value = (org.joda.time.LocalDate) object;

        return value.toDate().getTime();
    }

    public int convertTime(Object object) {
        final org.joda.time.LocalTime value = (org.joda.time.LocalTime) object;
        return value.get(DateTimeFieldType.millisOfDay());
    }

    public long convertTimestamp(Object object) {
        final DateTime value = (DateTime) object;
        return value.toDate().getTime();
    }

    private JodaConverter() {}
}
