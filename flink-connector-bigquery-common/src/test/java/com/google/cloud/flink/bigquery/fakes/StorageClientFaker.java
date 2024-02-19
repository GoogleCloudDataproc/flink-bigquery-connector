/*
 * Copyright (C) 2023 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.flink.bigquery.fakes;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

import java.util.Arrays;

/** Utility class to generate mocked objects for the BQ storage client. */
public class StorageClientFaker {
    public static final TableSchema SIMPLE_BQ_TABLE_SCHEMA =
            new TableSchema()
                    .setFields(
                            Arrays.asList(
                                    new TableFieldSchema()
                                            .setName("name")
                                            .setType("STRING")
                                            .setMode("REQUIRED"),
                                    new TableFieldSchema()
                                            .setName("number")
                                            .setType("INTEGER")
                                            .setMode("REQUIRED"),
                                    new TableFieldSchema()
                                            .setName("ts")
                                            .setType("TIMESTAMP")
                                            .setMode("REQUIRED")));
}
