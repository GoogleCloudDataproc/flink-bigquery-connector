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
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.ReadSessionCreator;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfig;
import com.google.cloud.bigquery.connector.common.ReadSessionResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.flink.bigquery.util.FlinkBigQueryConfig;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import net.sf.jsqlparser.JSQLParserException;

public class BigQueryReadSession {

  public static ReadSession getReadsession(
      Credentials credentials,
      FlinkBigQueryConfig bqconfig,
      BigQueryClientFactory bigQueryReadClientFactory)
      throws FileNotFoundException, IOException, JSQLParserException {

    final BigQuery bigquery =
        BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
    Optional<String> materializationProject =
        bqconfig.getQuery().isPresent()
            ? Optional.of(bqconfig.getParentProjectId())
            : Optional.empty();
    Optional<String> materializationDataset =
        bqconfig.getQuery().isPresent() ? bqconfig.getMaterializationDataset() : Optional.empty();
    Cache<String, TableInfo> destinationTableCache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(bqconfig.getCacheExpirationTimeInMinutes(), TimeUnit.MINUTES)
            .maximumSize(1000)
            .build();
    BigQueryClient bigQueryClient =
        new BigQueryClient(
            bigquery,
            materializationProject,
            materializationDataset,
            destinationTableCache,
            bqconfig.getBigQueryJobLabels());
    ReadSessionCreatorConfig readSessionCreatorConfig = bqconfig.toReadSessionCreatorConfig();
    ReadSessionCreator readSessionCreator =
        new ReadSessionCreator(readSessionCreatorConfig, bigQueryClient, bigQueryReadClientFactory);

    TableId tabId = null;
    if (bqconfig.getQuery().isPresent()) {

      int expirationTimeInMinutes = bqconfig.getMaterializationExpirationTimeInMinutes();
      TableInfo tableInfo =
          bigQueryClient.materializeQueryToTable(
              bqconfig.getQuery().get(), expirationTimeInMinutes);
      tabId = tableInfo.getTableId();
    }

    TableId tableId = bqconfig.getQuery().isPresent() ? tabId : bqconfig.getTableId();
    ImmutableList<String> selectedFields =
        ImmutableList.copyOf(Arrays.asList(bqconfig.getSelectedFields().split(",")));
    Optional<String> filter =
        bqconfig.getFilter().isPresent() ? bqconfig.getFilter() : Optional.empty();
    ReadSessionResponse response = readSessionCreator.create(tableId, selectedFields, filter);
    return response.getReadSession();
  }
}
