package com.google.cloud.flink.bigquery.fakes;

import org.junit.Test;

import java.io.IOException;

/** Test to check if the added substitution for FlinkVersion works as expected. */
public class FlinkVersionTest {

    @Test
    public void testFlinkVersion() throws IOException {
        // This gives 1.17
        String flinkVersionImport = org.apache.flink.FlinkVersion.current().toString();

        // These give 1.17.1
        String flinkVersionCustom =
                com.google.cloud.flink.bigquery.common.utils.flink.core.FlinkVersion.getVersion();

        String flinkVersionEnv = org.apache.flink.runtime.util.EnvironmentInformation.getVersion();

        assert flinkVersionCustom.equals(flinkVersionEnv);
    }
}
