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
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;

import com.google.cloud.flink.bigquery.common.exceptions.BigQueryConnectorException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple Identity de-serialization for pipelines that just want {@link GenericRecord} as response
 * from BigQuery.
 */
@Internal
public class AvroDeserializationSchema
        implements BigQueryDeserializationSchema<GenericRecord, GenericRecord> {

    private final String avroSchemaString;
    private static final Logger LOG = LoggerFactory.getLogger(AvroDeserializationSchema.class);

    public AvroDeserializationSchema(String avroSchemaString) {
        this.avroSchemaString = avroSchemaString;
    }

    @Override
    public GenericRecord deserialize(GenericRecord record) throws BigQueryConnectorException {
        try {
            return record;
        } catch (RuntimeException e) {
            LOG.error(
                    String.format(
                            "Error deserializing Avro Generic Record %s to Avro Generic Record.%nError: %s.%nCause:%s ",
                            record.toString(), e.getMessage(), e.getCause()));
            throw new BigQueryConnectorException(
                    "Error in deserializing to Avro Generic Record", e);
        }
    }

    @Override
    public TypeInformation<GenericRecord> getProducedType() {
        return new GenericRecordAvroTypeInfo(new Schema.Parser().parse(avroSchemaString));
    }
}
