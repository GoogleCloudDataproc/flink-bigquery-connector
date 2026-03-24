/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.flink.bigquery.integration;

import java.util.HashMap;
import java.util.Map;

/** A simple parameter tool for command-line argument parsing. */
public class SimpleParameterTool {
    private final Map<String, String> params = new HashMap<>();

    public static SimpleParameterTool fromArgs(String[] args) {
        SimpleParameterTool tool = new SimpleParameterTool();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                String key = args[i].substring(2);
                if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                    tool.params.put(key, args[i + 1]);
                    i++;
                } else {
                    tool.params.put(key, "true");
                }
            } else if (args[i].startsWith("-")) {
                String key = args[i].substring(1);
                if (i + 1 < args.length && !args[i + 1].startsWith("-")) {
                    tool.params.put(key, args[i + 1]);
                    i++;
                } else {
                    tool.params.put(key, "true");
                }
            }
        }
        return tool;
    }

    public String getRequired(String key) {
        if (!params.containsKey(key)) {
            throw new IllegalArgumentException("Missing required parameter: " + key);
        }
        return params.get(key);
    }

    public String get(String key) {
        return params.get(key);
    }

    public String get(String key, String def) {
        return params.getOrDefault(key, def);
    }

    public int getInt(String key) {
        if (!params.containsKey(key)) {
            throw new IllegalArgumentException("No data for required key " + key);
        }
        return Integer.parseInt(params.get(key));
    }

    public int getInt(String key, int def) {
        return params.containsKey(key) ? Integer.parseInt(params.get(key)) : def;
    }

    public long getLong(String key, long def) {
        return params.containsKey(key) ? Long.parseLong(params.get(key)) : def;
    }

    public boolean getBoolean(String key, boolean def) {
        return params.containsKey(key) ? Boolean.parseBoolean(params.get(key)) : def;
    }

    public int getNumberOfParameters() {
        return params.size();
    }
}
