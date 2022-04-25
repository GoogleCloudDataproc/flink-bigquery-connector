package com.google.cloud.flink.bigquery.unittest;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.junit.Assert;
import org.junit.Test;

import com.google.auth.Credentials;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryCredentialsSupplier;
import com.google.cloud.bigquery.storage.v1.ArrowSchema;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.flink.bigquery.BigQueryDynamicTableFactory;
import com.google.cloud.flink.bigquery.BigQueryReadSession;
import com.google.cloud.flink.bigquery.FlinkBigQueryConfig;
import com.google.cloud.flink.bigquery.common.FlinkBigQueryConnectorUserAgentProvider;
import com.google.cloud.flink.bigquery.common.UserAgentHeaderProvider;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;

import net.sf.jsqlparser.JSQLParserException;

public class BigQueryReadSessionTest {

	@Test
	public void getReadsessionTest() throws FileNotFoundException, IOException, JSQLParserException {
		ConfigOption<String> table = ConfigOptions.key("table").stringType().noDefaultValue();
		ConfigOption<String> selectedFields = ConfigOptions.key("selectedFields").stringType().noDefaultValue();
		ImmutableMap<String, String> defaultOptions = ImmutableMap.of("credentialsFile",
				System.getenv("GOOGLE_APPLICATION_CREDENTIALS"));
		BigQueryDynamicTableFactory bigQueryDynamicTableFactory = new BigQueryDynamicTableFactory();
		Assert.assertNotNull(bigQueryDynamicTableFactory);
		
		Configuration options = new Configuration();
		options.set(table, "bigquery-public-data.samples.shakespeare");
		options.set(selectedFields, "word,word_count");
		FlinkBigQueryConfig bqconfig = FlinkBigQueryConfig.from(bigQueryDynamicTableFactory.requiredOptions(),
				bigQueryDynamicTableFactory.optionalOptions(), options, defaultOptions,
				new org.apache.hadoop.conf.Configuration(), 10, new org.apache.flink.configuration.Configuration(),
				"1.11.0", Optional.empty());
		Assert.assertNotNull(bqconfig);
		
		Credentials credentials = bqconfig.createCredentials();
		Assert.assertNotNull(credentials);
		
		BigQueryCredentialsSupplier bigQueryCredentialsSupplier = new BigQueryCredentialsSupplier(
				bqconfig.getAccessToken(), bqconfig.getCredentialsKey(), bqconfig.getCredentialsFile(),
				Optional.empty(), Optional.empty(), Optional.empty());
		Assert.assertNotNull(bigQueryCredentialsSupplier);
		
		FlinkBigQueryConnectorUserAgentProvider agentProvider = new FlinkBigQueryConnectorUserAgentProvider("1.11.0");
		Assert.assertNotNull(agentProvider);
		
		UserAgentHeaderProvider userAgentHeaderProvider = new UserAgentHeaderProvider(agentProvider.getUserAgent());
		Assert.assertNotNull(userAgentHeaderProvider);
		
		BigQueryClientFactory bigQueryReadClientFactory = new BigQueryClientFactory(bigQueryCredentialsSupplier,
				userAgentHeaderProvider, bqconfig);
		Assert.assertNotNull(bigQueryReadClientFactory);

		ReadSession readSession = BigQueryReadSession.getReadsession(credentials, bqconfig, bigQueryReadClientFactory);
		Assert.assertNotNull(readSession);
		Assert.assertEquals(readSession.getTable().toString(), "projects/bigquery-public-data/datasets/samples/tables/shakespeare");
		Assert.assertTrue(readSession.hasArrowSchema());
		Assert.assertNotNull(readSession.getArrowSchema());
		Assert.assertTrue(readSession.getArrowSchema() instanceof ArrowSchema);
		Assert.assertTrue(readSession.getArrowSchema().getSerializedSchema() instanceof ByteString);
		Assert.assertTrue(readSession.getStreamsCount() > 0);
		Assert.assertNotNull(readSession.getStreams(0).getName());
	}
}
