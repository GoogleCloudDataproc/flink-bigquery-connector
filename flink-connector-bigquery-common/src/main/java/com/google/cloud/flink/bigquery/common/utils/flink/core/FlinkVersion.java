package com.google.cloud.flink.bigquery.common.utils.flink.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Replacement of EnvironmentInformation to obtain the current flink version.
 * EnvironmentInformation.getVersion() -> getVersionsInstance() -> VersionsHolder.INSTANCE -> new
 * Versions() String PROP_FILE = ".flink-runtime.version.properties" -> projectVersion =
 * getProperty(properties, "project.version", UNKNOWN); -> properties.getProperty(key)
 */
public class FlinkVersion {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkVersion.class);
    private static final String PROP_FILE = ".flink-runtime.version.properties";
    public static final String UNKNOWN = "<unknown>";

    private FlinkVersion() {}

    public static String getVersion() {
        ClassLoader classLoader = FlinkVersion.class.getClassLoader();
        try (InputStream propFile = classLoader.getResourceAsStream(PROP_FILE)) {
            Properties properties = new Properties();
            properties.load(propFile);
            String projectVersion = properties.getProperty("project.version");
            if (projectVersion == null || projectVersion.charAt(0) == '$') {
                return FlinkVersion.UNKNOWN;
            }
            return projectVersion;
        } catch (IOException e) {
            LOG.error(String.format("Could not obtain Flink Version.%nError: %s", e.getMessage()));
        }
        return null;
    }
}
