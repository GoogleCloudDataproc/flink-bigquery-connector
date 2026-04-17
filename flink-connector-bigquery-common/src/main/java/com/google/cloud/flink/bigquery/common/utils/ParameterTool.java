package com.google.cloud.flink.bigquery.common.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * A simple utility to parse command line arguments into properties, replacing Flink's ParameterTool
 * which was removed in Flink 2.0.
 */
public class ParameterTool {
    private final Map<String, String> data;

    private ParameterTool(Map<String, String> data) {
        this.data = data;
    }

    public static ParameterTool fromArgs(String[] args) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                String key = args[i].substring(2);
                String value = "";
                if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                    value = args[i + 1];
                    i++; // skip value
                }
                map.put(key, value);
            } else if (args[i].startsWith("-")) {
                String key = args[i].substring(1);
                String value = "";
                if (i + 1 < args.length && !args[i + 1].startsWith("-")) {
                    value = args[i + 1];
                    i++; // skip value
                }
                map.put(key, value);
            }
        }
        return new ParameterTool(map);
    }

    public int getNumberOfParameters() {
        return data.size();
    }

    public String getRequired(String key) {
        if (!data.containsKey(key)) {
            throw new IllegalArgumentException("No data for required key '" + key + "'");
        }
        return data.get(key);
    }

    public String get(String key) {
        return data.get(key);
    }

    public String get(String key, String defaultValue) {
        return data.getOrDefault(key, defaultValue);
    }

    public Integer getInt(String key) {
        String val = getRequired(key);
        return Integer.parseInt(val);
    }

    public Integer getInt(String key, int defaultValue) {
        return data.containsKey(key) ? Integer.parseInt(data.get(key)) : defaultValue;
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        if (!data.containsKey(key)) {
            return defaultValue;
        }
        String val = data.get(key);
        if (val.isEmpty()) {
            return true;
        }
        return Boolean.parseBoolean(val);
    }

    public Long getLong(String key) {
        String val = getRequired(key);
        return Long.parseLong(val);
    }

    public Long getLong(String key, long defaultValue) {
        return data.containsKey(key) ? Long.parseLong(data.get(key)) : defaultValue;
    }
}
