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

package com.google.cloud.flink.bigquery.table;

import org.apache.flink.table.api.DataTypes;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

/** Tests for partition selection in {@link BigQueryDynamicTableSource}. */
public class BigQueryPartitionSelectionTest {

    @Test
    public void testEmptySelectionAlwaysReadsNoRows() {
        for (String restriction : new String[] {"", "a = 1 OR b = 2"}) {
            BigQueryDynamicTableSource source =
                    Mockito.spy(
                            new BigQueryDynamicTableSource(
                                    BigQueryReadOptions.builder()
                                            .setBigQueryConnectOptions(
                                                    Mockito.mock(BigQueryConnectOptions.class))
                                            .setRowRestriction(restriction)
                                            .build(),
                                    DataTypes.ROW(
                                            DataTypes.FIELD("a", DataTypes.INT()),
                                            DataTypes.FIELD("b", DataTypes.INT()))));
            Mockito.doReturn(Optional.empty()).when(source).retrievePartitionInfo();
            source.applyPartitions(Collections.emptyList());
            assertEquals("FALSE", source.getReadOptions().getRowRestriction());
            assertEquals(
                    "FALSE",
                    ((BigQueryDynamicTableSource) source.copy())
                            .getReadOptions()
                            .getRowRestriction());
        }
    }
}
