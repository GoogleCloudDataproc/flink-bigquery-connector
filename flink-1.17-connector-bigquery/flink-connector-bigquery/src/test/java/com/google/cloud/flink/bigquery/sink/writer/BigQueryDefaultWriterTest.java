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

package com.google.cloud.flink.bigquery.sink.writer;

import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

/** Tests for {@link BigQueryDefaultWriter}. */
public class BigQueryDefaultWriterTest {

    @Test
    public void testWriteIsUnsupported() {
        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> new BigQueryDefaultWriter().write(null, null));
        assertThat(exception).hasMessageThat().contains("write method is not supported");
    }

    @Test
    public void testFlushIsUnsupported() {
        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> new BigQueryDefaultWriter().flush(false));
        assertThat(exception).hasMessageThat().contains("flush method is not supported");
    }

    @Test
    public void testCloseIsUnsupported() {
        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> new BigQueryDefaultWriter().close());
        assertThat(exception).hasMessageThat().contains("close method is not supported");
    }
}
