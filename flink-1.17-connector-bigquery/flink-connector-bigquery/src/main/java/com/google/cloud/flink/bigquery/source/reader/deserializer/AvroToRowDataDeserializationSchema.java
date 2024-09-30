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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.flink.bigquery.common.exceptions.BigQueryConnectorException;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Simple implementation for the Deserialization schema (from Avro GenericRecord to RowData). */
@Internal
public class AvroToRowDataDeserializationSchema
        implements BigQueryDeserializationSchema<GenericRecord, RowData> {
    private final AvroToRowDataConverters.AvroToRowDataConverter converter;
    private final TypeInformation<RowData> typeInfo;
    private static final Logger LOG =
            LoggerFactory.getLogger(AvroToRowDataDeserializationSchema.class);

    public AvroToRowDataDeserializationSchema(RowType rowType, TypeInformation<RowData> typeInfo) {
        this.converter = AvroToRowDataConverters.createRowConverter(rowType);
        this.typeInfo = typeInfo;
    }

    @Override
    public RowData deserialize(GenericRecord record) throws BigQueryConnectorException {
        try {
            return (GenericRowData) converter.convert(record);
        } catch (RuntimeException e) {
            LOG.error(
                    String.format(
                            "Error deserializing Avro Generic Record %s to Row Data.%nError: %s.%nCause:%s ",
                            record.toString(), e.getMessage(), e.getCause()));
            throw new BigQueryConnectorException("Error in deserializing to Row Data", e);
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return typeInfo;
    }
}
