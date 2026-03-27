/*
 * Copyright (C) 2024 Google Inc.
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

package com.google.cloud.flink.bigquery.sink.serializer;

import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

/**
 * {@link CdcChangeTypeProvider} for Flink {@link RowData} that maps {@link RowKind} to CDC change
 * type.
 *
 * <p>RowKind mapping: INSERT, UPDATE_AFTER -> UPSERT; UPDATE_BEFORE, DELETE -> DELETE.
 */
public final class RowDataCdcChangeTypeProvider implements CdcChangeTypeProvider<RowData> {

    private static final RowDataCdcChangeTypeProvider INSTANCE = new RowDataCdcChangeTypeProvider();

    private RowDataCdcChangeTypeProvider() {}

    @Override
    public String getChangeType(RowData record) {
        if (record == null) {
            return "UPSERT";
        }
        RowKind kind = record.getRowKind();
        return (kind == RowKind.UPDATE_BEFORE || kind == RowKind.DELETE) ? "DELETE" : "UPSERT";
    }

    public static RowDataCdcChangeTypeProvider getInstance() {
        return INSTANCE;
    }
}
