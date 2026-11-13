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

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests for {@link CdcChangeTypeProvider}. */
public class CdcChangeTypeProviderTest {

    @Test
    public void testUpsertOnly_returnsUpsertForAnyRecord() {
        CdcChangeTypeProvider<GenericRecord> provider = CdcChangeTypeProvider.upsertOnly();
        assertEquals("UPSERT", provider.getChangeType(null));
    }

    @Test
    public void testCustomProvider_returnsCorrectChangeType() {
        CdcChangeTypeProvider<String> provider =
                record -> {
                    if (record != null && record.startsWith("DELETE:")) {
                        return "DELETE";
                    }
                    return "UPSERT";
                };
        assertEquals("UPSERT", provider.getChangeType("INSERT:data"));
        assertEquals("DELETE", provider.getChangeType("DELETE:data"));
    }

    @Test
    public void testUpsertOnly_withRecord() {
        BigQuerySchemaProvider schemaProvider = TestBigQuerySchemas.getSimpleRecordSchema();
        GenericRecord record =
                new org.apache.avro.generic.GenericRecordBuilder(schemaProvider.getAvroSchema())
                        .set("long_field", 123L)
                        .set("string_field", "test")
                        .build();

        CdcChangeTypeProvider<GenericRecord> provider = CdcChangeTypeProvider.upsertOnly();
        assertEquals("UPSERT", provider.getChangeType(record));
    }
}
