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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.flink.bigquery.source.config;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

/** Tests for {@link BigQueryReadOptions}. */
public class BigQueryReadOptionsTest {

    @Test(expected = IllegalStateException.class)
    public void rejectsNegativeProjectedAvroBatchSize() {
        optionsBuilder().setAvroProjectedBatchSize(-1).build();
    }

    @Test
    public void acceptsProjectedAvroBatchSizeAboveWarningThreshold() {
        BigQueryReadOptions options =
                optionsBuilder()
                        .setAvroProjectedBatchSize(
                                BigQueryReadOptions.AVRO_PROJECTED_BATCH_SIZE_WARNING_THRESHOLD + 1)
                        .build();

        assertThat(options.getAvroProjectedBatchSize())
                .isEqualTo(BigQueryReadOptions.AVRO_PROJECTED_BATCH_SIZE_WARNING_THRESHOLD + 1);
    }

    @Test
    public void projectedAvroPathsFallBackToTheColumnNames() {
        BigQueryReadOptions options =
                optionsBuilder().setColumnNames(Arrays.asList("id", "items")).build();
        assertThat(options.getProjectedAvroPaths()).containsExactly("id", "items").inOrder();
        options = options.toBuilder().setAvroDecodePaths(Arrays.asList("id", "items.keep")).build();
        assertThat(options.getProjectedAvroPaths()).containsExactly("id", "items.keep").inOrder();
    }

    @Test
    public void keepsACopyOfTheAvroDecodePaths() {
        List<String> paths = new ArrayList<>(Arrays.asList("id", "items.keep"));
        BigQueryReadOptions options = optionsBuilder().setAvroDecodePaths(paths).build();
        paths.add("items.drop");
        assertThat(options.getAvroDecodePaths()).containsExactly("id", "items.keep").inOrder();
        assertThrows(
                UnsupportedOperationException.class,
                () -> options.getAvroDecodePaths().add("items.drop"));
    }

    @Test
    public void rejectsNullAvroDecodePaths() {
        assertThrows(
                IllegalArgumentException.class,
                () -> optionsBuilder().setAvroDecodePaths(Arrays.asList("id", null)).build());
        assertThrows(NullPointerException.class, () -> optionsBuilder().setAvroDecodePaths(null));
    }

    private static BigQueryReadOptions.Builder optionsBuilder() {
        return BigQueryReadOptions.builder()
                .setBigQueryConnectOptions(
                        BigQueryConnectOptions.builder()
                                .setProjectId("project")
                                .setDataset("dataset")
                                .setTable("table")
                                .build());
    }
}
