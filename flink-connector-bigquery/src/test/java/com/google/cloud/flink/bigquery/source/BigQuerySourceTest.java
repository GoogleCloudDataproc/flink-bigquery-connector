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

package com.google.cloud.flink.bigquery.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;

import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.source.config.BigQueryReadOptions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.io.IOException;

import static com.google.common.truth.Truth.assertThat;

/** */
public class BigQuerySourceTest {

    @Test
    public void testReadAvros() throws IOException {
        BigQueryReadOptions readOptions =
                StorageClientFaker.createReadOptions(
                        10, 2, StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING);
        BigQuerySource<GenericRecord> source = BigQuerySource.readAvros(readOptions);
        TypeInformation<GenericRecord> expected =
                new GenericRecordAvroTypeInfo(StorageClientFaker.SIMPLE_AVRO_SCHEMA);
        assertThat(source.getDeserializationSchema().getProducedType()).isEqualTo(expected);
    }

    @Test
    public void testReadAvrosFromQuery() throws IOException {
        // by default the faker includes a dummy query in the read options
        BigQueryReadOptions readOptions =
                StorageClientFaker.createReadOptions(
                        10, 2, StorageClientFaker.SIMPLE_AVRO_SCHEMA_STRING);
        BigQuerySource<GenericRecord> source = BigQuerySource.readAvrosFromQuery(readOptions);

        TypeInformation<GenericRecord> expected =
                new GenericRecordAvroTypeInfo(
                        new Schema.Parser()
                                .parse(StorageClientFaker.SIMPLE_AVRO_SCHEMA_FORQUERY_STRING));
        assertThat(source.getDeserializationSchema().getProducedType()).isEqualTo(expected);
    }
}
