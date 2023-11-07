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

package com.google.cloud.flink.bigquery.common.utils;

import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.flink.bigquery.common.exceptions.BigQueryConnectorException;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

/** */
public class BigQueryTableNameUtilsTest {

    @Test
    public void testParseTableReferenceURN() {
        String tableName = "projects/project/datasets/dataset/tables/table";
        TableReference expected =
                new TableReference()
                        .setProjectId("project")
                        .setDatasetId("dataset")
                        .setTableId("table");

        TableReference actual = BigQueryTableNameUtils.parseTableReference(tableName);

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testParseTableReference() {
        String tableName = "project.dataset.table";
        TableReference expected =
                new TableReference()
                        .setProjectId("project")
                        .setDatasetId("dataset")
                        .setTableId("table");

        TableReference actual = BigQueryTableNameUtils.parseTableReference(tableName);

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testParseTableReferenceColon() {
        String tableName = "project:dataset.table";
        TableReference expected =
                new TableReference()
                        .setProjectId("project")
                        .setDatasetId("dataset")
                        .setTableId("table");

        TableReference actual = BigQueryTableNameUtils.parseTableReference(tableName);

        assertThat(actual).isEqualTo(expected);
    }

    @Test(expected = BigQueryConnectorException.class)
    public void testParseTableReferenceError() {
        String tableName = "something/not/conformant";

        BigQueryTableNameUtils.parseTableReference(tableName);
    }
}
