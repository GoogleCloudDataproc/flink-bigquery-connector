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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryCredentialsSupplier;
import com.google.cloud.bigquery.connector.common.ReadSessionCreator;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfig;
import com.google.cloud.bigquery.connector.common.ReadSessionResponse;
import com.google.cloud.flink.bigquery.common.UserAgentHeaderProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class BigQueryReadSession {

	public static com.google.cloud.bigquery.storage.v1.ReadSession getReadsession(String projectId, String table,
			String dataset, Map<String, String> configOption) throws FileNotFoundException, IOException {

		GoogleCredentials credentials = GoogleCredentials
				.fromStream(new FileInputStream(configOption.get("credentialKeyFile")));
		Optional<String> credentialkey_file = Optional.of(configOption.get("credentialKeyFile"));

		BigQueryCredentialsSupplier bigQueryCredentialsSupplier = new BigQueryCredentialsSupplier(Optional.empty(),
				Optional.empty(), credentialkey_file, Optional.empty(), Optional.empty(), Optional.empty());

		int DEFAULT_PARALLELISM = Integer.parseInt(configOption.get("DEFAULT_PARALLELISM"));
		String FLINK_VERSION = (String) configOption.get("FLINK_VERSION");

		Configuration hadoopConfiguration = new Configuration();

		ImmutableMap<String, String> defaultOptions = ImmutableMap.of("table", projectId + "." + dataset + "." + table);

		FlinkBigQueryConfig bqconfig = FlinkBigQueryConfig.from(defaultOptions, defaultOptions, // ImmutableMap.of(),
				hadoopConfiguration, DEFAULT_PARALLELISM, new org.apache.flink.configuration.Configuration(),
				FLINK_VERSION, Optional.empty());
		final UserAgentHeaderProvider userAgentHeaderProvider = new UserAgentHeaderProvider("test-agent");
		BigQueryClientFactory bigQueryReadClientFactory = new BigQueryClientFactory(bigQueryCredentialsSupplier,
				userAgentHeaderProvider, bqconfig);
		final BigQuery bigquery = BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
		BigQueryClient bigQueryClient = new BigQueryClient(bigquery, null, null);
		ReadSessionCreatorConfig readSessionCreatorConfig = bqconfig.toReadSessionCreatorConfig();
		ReadSessionCreator readSessionCreator = new ReadSessionCreator(readSessionCreatorConfig, bigQueryClient,
				bigQueryReadClientFactory);
		TableId tableId = TableId.of(dataset, table);
		
		ImmutableList<String> selectedFields =
        ImmutableList.copyOf(Arrays.asList(configOption.get("selectedfields").split(",")));
        Optional<String> filter = Optional.of(configOption.get("filter"));
		
		ReadSessionResponse response = readSessionCreator.create(tableId, selectedFields, filter);
		return response.getReadSession();
	}
}
