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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests for {@link CdcChangeTypeProvider}. */
public class CdcChangeTypeProviderTest {

    @Test
    public void testUpsertOnlyProvider() {
        CdcChangeTypeProvider<Object> provider = CdcChangeTypeProvider.upsertOnly();

        assertEquals("UPSERT", provider.getChangeType("any record"));
        assertEquals("UPSERT", provider.getChangeType(null));
        assertEquals("UPSERT", provider.getChangeType(123));
    }

    @Test
    public void testCustomProvider() {
        CdcChangeTypeProvider<String> provider =
                record -> {
                    if (record != null && record.contains("delete")) {
                        return "DELETE";
                    }
                    return "UPSERT";
                };

        assertEquals("UPSERT", provider.getChangeType("insert record"));
        assertEquals("DELETE", provider.getChangeType("delete record"));
        assertEquals("UPSERT", provider.getChangeType(null));
    }

    @Test
    public void testProviderWithComplexObject() {
        // Simulating a record with an operation field
        class TestRecord {
            final String operation;

            TestRecord(String operation) {
                this.operation = operation;
            }
        }

        CdcChangeTypeProvider<TestRecord> provider =
                record -> {
                    if (record != null && "D".equals(record.operation)) {
                        return "DELETE";
                    }
                    return "UPSERT";
                };

        assertEquals("UPSERT", provider.getChangeType(new TestRecord("I")));
        assertEquals("UPSERT", provider.getChangeType(new TestRecord("U")));
        assertEquals("DELETE", provider.getChangeType(new TestRecord("D")));
    }
}
