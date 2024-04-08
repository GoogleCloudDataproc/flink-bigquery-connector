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

package com.google.cloud.flink.bigquery.source.reader.deserializer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.flink.bigquery.common.utils.flink.annotations.Internal;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;

/** Simple implementation for the Deserialization schema (from Avro GenericRecord to RowData). */
@Internal
public class AvroToRowDataDeserializationSchema
        implements BigQueryDeserializationSchema<GenericRecord, RowData> {
    private final AvroToRowDataConverters.AvroToRowDataConverter converter;
    private final TypeInformation<RowData> typeInfo;

    public AvroToRowDataDeserializationSchema(RowType rowType, TypeInformation<RowData> typeInfo) {
        this.converter = AvroToRowDataConverters.createRowConverter(rowType);
        this.typeInfo = typeInfo;
    }

    @Override
    public RowData deserialize(GenericRecord record) throws IOException {
        return (GenericRowData) converter.convert(record);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return typeInfo;
    }
}
