/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.flink.bigquery;

import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.ReadSessionCreator;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfig;
import com.google.cloud.bigquery.connector.common.ReadSessionResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.common.collect.ImmutableList;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

public class BigQueryReadSession {

  public static ReadSession getReadsession(
      Credentials credentials,
      FlinkBigQueryConfig bqconfig,
      String table,
      BigQueryClientFactory bigQueryReadClientFactory,
      Map<String, String> configOption)
      throws FileNotFoundException, IOException {

    final BigQuery bigquery =
        BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
    BigQueryClient bigQueryClient = new BigQueryClient(bigquery, null, null);
    ReadSessionCreatorConfig readSessionCreatorConfig = bqconfig.toReadSessionCreatorConfig();
    ReadSessionCreator readSessionCreator =
        new ReadSessionCreator(readSessionCreatorConfig, bigQueryClient, bigQueryReadClientFactory);
    TableId tableId =
        TableId.of(table.split("\\.")[0], table.split("\\.")[1], table.split("\\.")[2]);

    ImmutableList<String> selectedFields =
        ImmutableList.copyOf(Arrays.asList((configOption.get("selectedFields")).split(",")));

    Optional<String> filter =
        configOption.get("filter") != null
            ? Optional.of(configOption.get("filter"))
            : Optional.empty();
    ReadSessionResponse response = readSessionCreator.create(tableId, selectedFields, filter);
    return response.getReadSession();
  }
}
