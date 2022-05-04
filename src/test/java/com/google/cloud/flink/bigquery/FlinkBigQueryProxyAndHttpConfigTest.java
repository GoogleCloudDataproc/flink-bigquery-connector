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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.client.http.apache.v2.ApacheHttpTransport;
import com.google.api.core.ApiFunction;
import com.google.auth.http.HttpTransportFactory;
import com.google.cloud.bigquery.connector.common.BigQueryProxyTransporterBuilder;
import com.google.common.collect.ImmutableMap;
import io.grpc.ManagedChannelBuilder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class FlinkBigQueryProxyAndHttpConfigTest {

  private final ImmutableMap<String, String> defaultOptions =
      ImmutableMap.<String, String>builder()
          .put("proxyAddress", "http://bq-connector-host:1234")
          .put("proxyUsername", "bq-connector-user")
          .put("proxyPassword", "bq-connector-password")
          .put("httpMaxRetry", "10")
          .put("httpConnectTimeout", "10000")
          .put("httpReadTimeout", "20000")
          .build();

  private final ImmutableMap<String, String> defaultGlobalOptions =
      ImmutableMap.<String, String>builder()
          .put("flink.datasource.bigquery.proxyAddress", "http://bq-connector-host-global:1234")
          .put("flink.datasource.bigquery.proxyUsername", "bq-connector-user-global")
          .put("flink.datasource.bigquery.proxyPassword", "bq-connector-password-global")
          .put("flink.datasource.bigquery.httpMaxRetry", "20")
          .put("flink.datasource.bigquery.httpConnectTimeout", "20000")
          .put("flink.datasource.bigquery.httpReadTimeout", "30000")
          .build();

  private final Configuration defaultHadoopConfiguration = getHadoopConfiguration();

  private Configuration getHadoopConfiguration() {
    Configuration hadoopConfiguration = new Configuration();
    hadoopConfiguration.set("fs.gs.proxy.address", "http://bq-connector-host-hadoop:1234");
    hadoopConfiguration.set("fs.gs.proxy.username", "bq-connector-user-hadoop");
    hadoopConfiguration.set("fs.gs.proxy.password", "bq-connector-password-hadoop");
    hadoopConfiguration.set("fs.gs.http.max.retry", "30");
    hadoopConfiguration.set("fs.gs.http.connect-timeout", "30000");
    hadoopConfiguration.set("fs.gs.http.read-timeout", "40000");
    return hadoopConfiguration;
  }

  private static final Optional<URI> optionalProxyURI =
      Optional.of(URI.create("http://bq-connector-transporter-builder-host:1234"));
  private static final Optional<String> optionalProxyUserName =
      Optional.of("transporter-builder-user");
  private static final Optional<String> optionalProxyPassword =
      Optional.of("transporter-builder-password");

  @Test
  public void testSerializability()
      throws IOException { // need to confirm the parameter of from method
    org.apache.flink.configuration.Configuration options =
        new org.apache.flink.configuration.Configuration();
    ConfigOption<String> table = ConfigOptions.key("table").stringType().noDefaultValue();
    ConfigOption<String> selectedFields =
        ConfigOptions.key("selectedFields").stringType().noDefaultValue();
    options.set(table, "bigquery-public-data.samples.shakespeare");
    options.set(selectedFields, "word,word_count");
    new ObjectOutputStream(new ByteArrayOutputStream())
        .writeObject(
            FlinkBigQueryProxyAndHttpConfig.from(
                defaultOptions, defaultGlobalOptions, defaultHadoopConfiguration));
  }

  @Test
  public void testConfigFromOptions() throws URISyntaxException {
    Configuration emptyHadoopConfiguration = new Configuration();

    FlinkBigQueryProxyAndHttpConfig config =
        FlinkBigQueryProxyAndHttpConfig.from(
            defaultOptions,
            ImmutableMap.of(), // empty
            // globalOptions
            emptyHadoopConfiguration);

    assertThat(config.getProxyUri())
        .isEqualTo(Optional.of(getURI("http", "bq-connector-host", 1234)));
    assertThat(config.getProxyUsername()).isEqualTo(Optional.of("bq-connector-user"));
    assertThat(config.getProxyPassword()).isEqualTo(Optional.of("bq-connector-password"));
    assertThat(config.getHttpMaxRetry()).isEqualTo(Optional.of(10));
    assertThat(config.getHttpConnectTimeout()).isEqualTo(Optional.of(10000));
    assertThat(config.getHttpReadTimeout()).isEqualTo(Optional.of(20000));
  }

  @Test
  public void fromTest() throws URISyntaxException {
    FlinkBigQueryProxyAndHttpConfig config =
        FlinkBigQueryProxyAndHttpConfig.from(
            ImmutableMap.of(), // empty
            // options
            ImmutableMap.of(), // empty global options
            defaultHadoopConfiguration);
    assertThat(config.getProxyUri())
        .isEqualTo(Optional.of(getURI("http", "bq-connector-host-hadoop", 1234)));
    assertThat(config.getProxyUsername()).isEqualTo(Optional.of("bq-connector-user-hadoop"));
    assertThat(config.getProxyPassword()).isEqualTo(Optional.of("bq-connector-password-hadoop"));
    assertThat(config.getHttpMaxRetry()).isEqualTo(Optional.of(30));
    assertThat(config.getHttpConnectTimeout()).isEqualTo(Optional.of(30000));
    assertThat(config.getHttpReadTimeout()).isEqualTo(Optional.of(40000));
  }

  @Test
  public void testConfigFromGlobalOptions() throws URISyntaxException {
    Configuration emptyHadoopConfiguration = new Configuration();
    ImmutableMap<String, String> globalOptions =
        FlinkBigQueryConfig.normalizeConf(defaultGlobalOptions);
    FlinkBigQueryProxyAndHttpConfig config =
        FlinkBigQueryProxyAndHttpConfig.from(
            ImmutableMap.of(), // empty option
            globalOptions,
            emptyHadoopConfiguration);

    assertThat(config.getProxyUri())
        .isNotEqualTo(Optional.of(getURI("http", "bq-connector-host-hadoop", 1234)));
    assertThat(config.getProxyUsername()).isNotEqualTo(Optional.of("bq-connector-user-hadoop"));
    assertThat(config.getProxyPassword()).isNotEqualTo(Optional.of("bq-connector-password-hadoop"));
    assertThat(config.getHttpMaxRetry()).isNotEqualTo(Optional.of(30));
    assertThat(config.getHttpConnectTimeout()).isNotEqualTo(Optional.of(30000));
    assertThat(config.getHttpReadTimeout()).isNotEqualTo(Optional.of(40000));
  }

  @Test
  public void testConfigFromHadoopConfigurationOptions() throws URISyntaxException {
    FlinkBigQueryProxyAndHttpConfig config =
        FlinkBigQueryProxyAndHttpConfig.from(
            ImmutableMap.of(), // empty
            // options
            ImmutableMap.of(), // empty global options
            defaultHadoopConfiguration);

    assertThat(config.getProxyUri())
        .isEqualTo(Optional.of(getURI("http", "bq-connector-host-hadoop", 1234)));
    assertThat(config.getProxyUsername()).isEqualTo(Optional.of("bq-connector-user-hadoop"));
    assertThat(config.getProxyPassword()).isEqualTo(Optional.of("bq-connector-password-hadoop"));
    assertThat(config.getHttpMaxRetry()).isEqualTo(Optional.of(30));
    assertThat(config.getHttpConnectTimeout()).isEqualTo(Optional.of(30000));
    assertThat(config.getHttpReadTimeout()).isEqualTo(Optional.of(40000));
  }

  @Test
  public void testConfigWithGlobalParametersAndHadoopConfig() throws URISyntaxException {
    ImmutableMap<String, String> globalOptions =
        FlinkBigQueryConfig.normalizeConf(defaultGlobalOptions);
    FlinkBigQueryProxyAndHttpConfig config =
        FlinkBigQueryProxyAndHttpConfig.from(
            ImmutableMap.of(), // empty
            // options
            globalOptions,
            defaultHadoopConfiguration);

    assertThat(config.getProxyUri())
        .isEqualTo(Optional.of(getURI("http", "bq-connector-host-global", 1234)));
    assertThat(config.getProxyUsername()).isEqualTo(Optional.of("bq-connector-user-global"));
    assertThat(config.getProxyPassword()).isEqualTo(Optional.of("bq-connector-password-global"));
    assertThat(config.getHttpMaxRetry()).isEqualTo(Optional.of(20));
    assertThat(config.getHttpConnectTimeout()).isEqualTo(Optional.of(20000));
    assertThat(config.getHttpReadTimeout()).isEqualTo(Optional.of(30000));
  }

  @Test
  public void testParseProxyAddress() throws Exception {
    // map of input string v/s expected return
    HashMap<String, URI> inputOutputMap = new HashMap<>();
    inputOutputMap.put("bq-connector-host:1234", getURI(null, "bq-connector-host", 1234));
    inputOutputMap.put("http://bq-connector-host:1234", getURI("http", "bq-connector-host", 1234));
    inputOutputMap.put(
        "https://bq-connector-host:1234", getURI("https", "bq-connector-host", 1234));

    for (Map.Entry<String, URI> entry : inputOutputMap.entrySet()) {
      String address = entry.getKey();
      URI expectedUri = entry.getValue();
      URI uri = FlinkBigQueryProxyAndHttpConfig.parseProxyAddress(address);
      assertThat(uri).isEqualTo(expectedUri);
    }
  }

  @Test
  public void testParseProxyAddressIllegalPath() {
    ArrayList<String> addresses = new ArrayList<>();
    addresses.add("bq-connector-host-with-illegal-char^:1234");
    addresses.add("bq-connector-host:1234/some/path");

    for (String address : addresses) {
      IllegalArgumentException exception =
          assertThrows(
              IllegalArgumentException.class,
              () -> FlinkBigQueryProxyAndHttpConfig.parseProxyAddress(address));
      assertThat(exception)
          .hasMessageThat()
          .isEqualTo(String.format("Invalid proxy address '%s'.", address));
    }
  }

  @Test
  public void testParseProxyAddressNoPort() {
    ArrayList<String> addresses = new ArrayList<>();
    addresses.add("bq-connector-host");
    addresses.add("http://bq-connector-host");
    addresses.add("https://bq-connector-host");

    for (String address : addresses) {
      IllegalArgumentException exception =
          assertThrows(
              IllegalArgumentException.class,
              () -> FlinkBigQueryProxyAndHttpConfig.parseProxyAddress(address));
      assertThat(exception)
          .hasMessageThat()
          .isEqualTo(String.format("Proxy address '%s' has no port.", address));
    }
  }

  @Test
  public void testParseProxyAddressWrongScheme() {
    ArrayList<String> addresses = new ArrayList<>();
    addresses.add("socks5://bq-connector-host:1234");
    addresses.add("htt://bq-connector-host:1234"); // a missing p in http

    for (String address : addresses) {
      IllegalArgumentException exception =
          assertThrows(
              IllegalArgumentException.class,
              () -> FlinkBigQueryProxyAndHttpConfig.parseProxyAddress(address));
      assertThat(exception)
          .hasMessageThat()
          .contains(String.format("Proxy address '%s' has invalid scheme", address));
    }
  }

  @Test
  public void testParseProxyAddressNoHost() {
    String address = ":1234";

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> FlinkBigQueryProxyAndHttpConfig.parseProxyAddress(address));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(String.format("Proxy address '%s' has no host.", address));
  }

  private URI getURI(String scheme, String host, int port) throws URISyntaxException {
    return new URI(scheme, null, host, port, null, null, null);
  }

  @Test
  public void testBigQueryProxyTransporterBuilder() {
    ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder> apiFunction =
        BigQueryProxyTransporterBuilder.createGrpcChannelConfigurator(
            optionalProxyURI, optionalProxyUserName, optionalProxyPassword);

    assertThat(apiFunction.apply(ManagedChannelBuilder.forTarget("test-target")))
        .isInstanceOf(ManagedChannelBuilder.class);

    HttpTransportFactory httpTransportFactory =
        BigQueryProxyTransporterBuilder.createHttpTransportFactory(
            optionalProxyURI, optionalProxyUserName, optionalProxyPassword);

    assertThat(httpTransportFactory.create()).isInstanceOf(ApacheHttpTransport.class);
  }

  @Test
  public void testBigQueryProxyTransporterBuilderWithErrors() {
    IllegalArgumentException exceptionWithPasswordHttp =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                BigQueryProxyTransporterBuilder.createHttpTransportFactory(
                    optionalProxyURI, Optional.empty(), optionalProxyPassword));

    IllegalArgumentException exceptionWithUserNameHttp =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                BigQueryProxyTransporterBuilder.createHttpTransportFactory(
                    optionalProxyURI, optionalProxyUserName, Optional.empty()));

    IllegalArgumentException exceptionWithPasswordGrpc =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                BigQueryProxyTransporterBuilder.createGrpcChannelConfigurator(
                    optionalProxyURI, Optional.empty(), optionalProxyPassword));

    IllegalArgumentException exceptionWithUserNameGrpc =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                BigQueryProxyTransporterBuilder.createGrpcChannelConfigurator(
                    optionalProxyURI, optionalProxyUserName, Optional.empty()));

    Arrays.asList(
            exceptionWithPasswordHttp,
            exceptionWithUserNameHttp,
            exceptionWithPasswordGrpc,
            exceptionWithUserNameGrpc)
        .stream()
        .forEach(
            exception ->
                assertThat(exception)
                    .hasMessageThat()
                    .contains(
                        "Both proxyUsername and proxyPassword should be defined or not defined together"));
  }
}
