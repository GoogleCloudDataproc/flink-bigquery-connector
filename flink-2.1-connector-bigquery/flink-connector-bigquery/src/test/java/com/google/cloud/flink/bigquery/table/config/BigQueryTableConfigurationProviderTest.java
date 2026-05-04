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

package com.google.cloud.flink.bigquery.table.config;

import org.apache.flink.configuration.Configuration;

import com.google.cloud.flink.bigquery.sink.WriteMode;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/** Tests for {@link BigQueryTableConfigurationProvider}'s new write-mode methods. */
public class BigQueryTableConfigurationProviderTest {

    @Test
    public void getWriteModeDefaultsToDirect() {
        Configuration cfg = new Configuration();
        BigQueryTableConfigurationProvider p = new BigQueryTableConfigurationProvider(cfg);
        assertEquals(WriteMode.STORAGE_WRITE_API, p.getWriteMode());
    }

    @Test
    public void getWriteModeParsesIndirect() {
        Configuration cfg = new Configuration();
        cfg.setString(BigQueryConnectorOptions.WRITE_MODE.key(), "INDIRECT");
        BigQueryTableConfigurationProvider p = new BigQueryTableConfigurationProvider(cfg);
        assertEquals(WriteMode.INDIRECT, p.getWriteMode());
    }

    @Test
    public void getWriteModeParsesLowercaseIndirect() {
        // getWriteMode() applies toUpperCase() so lowercase input is accepted.
        Configuration cfg = new Configuration();
        cfg.setString(BigQueryConnectorOptions.WRITE_MODE.key(), "indirect");
        BigQueryTableConfigurationProvider p = new BigQueryTableConfigurationProvider(cfg);
        assertEquals(WriteMode.INDIRECT, p.getWriteMode());
    }

    @Test
    public void getWriteModeParsesMixedCaseIndirect() {
        Configuration cfg = new Configuration();
        cfg.setString(BigQueryConnectorOptions.WRITE_MODE.key(), "Indirect");
        BigQueryTableConfigurationProvider p = new BigQueryTableConfigurationProvider(cfg);
        assertEquals(WriteMode.INDIRECT, p.getWriteMode());
    }

    @Test
    public void getWriteModeUnknownValueThrows() {
        Configuration cfg = new Configuration();
        cfg.setString(BigQueryConnectorOptions.WRITE_MODE.key(), "SIDEWAYS");
        BigQueryTableConfigurationProvider p = new BigQueryTableConfigurationProvider(cfg);
        assertThrows(IllegalArgumentException.class, p::getWriteMode);
    }

    @Test
    public void getGcsTempPathAbsentReturnsEmpty() {
        Configuration cfg = new Configuration();
        BigQueryTableConfigurationProvider p = new BigQueryTableConfigurationProvider(cfg);
        assertEquals(Optional.empty(), p.getGcsTempPath());
    }

    @Test
    public void getGcsTempPathPresentReturnsValue() {
        Configuration cfg = new Configuration();
        cfg.setString(BigQueryConnectorOptions.GCS_TEMP_PATH.key(), "gs://bucket/tmp");
        BigQueryTableConfigurationProvider p = new BigQueryTableConfigurationProvider(cfg);
        assertEquals(Optional.of("gs://bucket/tmp"), p.getGcsTempPath());
    }
}
