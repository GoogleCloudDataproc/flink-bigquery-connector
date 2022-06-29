/*
 * Copyright 2020 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.flink.bigquery.common;

import com.google.cloud.flink.bigquery.exception.FlinkBigQueryException;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

/** Flink utilities */
public class FlinkBigQueryUtil {

  private static final Properties BUILD_PROPERTIES = loadBuildProperties();
  static final String CONNECTOR_VERSION = BUILD_PROPERTIES.getProperty("connector.version");

  private static Properties loadBuildProperties() {
    try {
      Properties buildProperties = new Properties();
      buildProperties.load(
          FlinkBigQueryUtil.class.getResourceAsStream("/flink-bigquery-connector.properties"));
      return buildProperties;
    } catch (IOException e) {
      throw new FlinkBigQueryException("Error while loading properties file");
    }
  }

  public static String getJobId(Configuration flinkConf) {
    ConfigOption<String> flink_yarn_tag =
        ConfigOptions.key("flink.yarn.tags").stringType().noDefaultValue();
    ConfigOption<String> flink_app_id =
        ConfigOptions.key("flink.app.id").stringType().noDefaultValue();
    return getJobIdInternal(
        flinkConf.getString(flink_yarn_tag, "missing"),
        flinkConf.getString(flink_app_id, "generated-" + UUID.randomUUID()));
  }

  static String getJobIdInternal(String yarnTags, String applicationId) {
    return Stream.of(yarnTags.split(","))
        .filter(tag -> tag.startsWith("dataproc_job_"))
        .findFirst()
        .orElseGet(() -> applicationId);
  }
}
