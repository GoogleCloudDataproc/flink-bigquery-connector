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

/** Instantiating big query read session */
public class BigQueryReadSession {

  public static ReadSession getReadsession(
      Credentials credentials,
      FlinkBigQueryConfig bqConfig,
      BigQueryClientFactory bigQueryReadClientFactory)
      throws FileNotFoundException, IOException, JSQLParserException {

    final BigQuery bigQuery =
        BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
    Optional<String> materializationProject =
        bqConfig.getQuery().isPresent()
            ? Optional.of(bqConfig.getParentProjectId())
            : Optional.empty();
    Optional<String> materializationDataset =
        bqConfig.getQuery().isPresent() ? bqConfig.getMaterializationDataset() : Optional.empty();
    Cache<String, TableInfo> destinationTableCache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(bqConfig.getCacheExpirationTimeInMinutes(), TimeUnit.MINUTES)
            .maximumSize(1000)
            .build();
    BigQueryClient bigQueryClient =
        new BigQueryClient(
            bigQuery,
            materializationProject,
            materializationDataset,
            destinationTableCache,
            bqConfig.getBigQueryJobLabels());
    ReadSessionCreatorConfig readSessionCreatorConfig = bqConfig.toReadSessionCreatorConfig();
    ReadSessionCreator readSessionCreator =
        new ReadSessionCreator(readSessionCreatorConfig, bigQueryClient, bigQueryReadClientFactory);

    TableId tabId = null;
    if (bqConfig.getQuery().isPresent()) {

      int expirationTimeInMinutes = bqConfig.getMaterializationExpirationTimeInMinutes();
      TableInfo tableInfo =
          bigQueryClient.materializeQueryToTable(
              bqConfig.getQuery().get(), expirationTimeInMinutes);
      tabId = tableInfo.getTableId();
    }

    TableId tableId = bqConfig.getQuery().isPresent() ? tabId : bqConfig.getTableId();
    ImmutableList<String> selectedFields =
        ImmutableList.copyOf(Arrays.asList(bqConfig.getSelectedFields().split(",")));
    Optional<String> filter =
        bqConfig.getFilter().isPresent() ? bqConfig.getFilter() : Optional.empty();
    ReadSessionResponse response = readSessionCreator.create(tableId, selectedFields, filter);
    return response.getReadSession();
  }
}
