package com.google.cloud.spark.bigquery;

import com.google.api.client.http.apache.v2.ApacheHttpTransport;
import com.google.api.core.ApiFunction;
import com.google.auth.http.HttpTransportFactory;
import com.google.cloud.bigquery.connector.common.BigQueryProxyTransporterBuilder;
import com.google.common.collect.ImmutableMap;
import io.grpc.ManagedChannelBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

public class SparkBigQueryProxyAndHttpConfigTest {

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
          .put("spark.datasource.bigquery.proxyAddress", "http://bq-connector-host-global:1234")
          .put("spark.datasource.bigquery.proxyUsername", "bq-connector-user-global")
          .put("spark.datasource.bigquery.proxyPassword", "bq-connector-password-global")
          .put("spark.datasource.bigquery.httpMaxRetry", "20")
          .put("spark.datasource.bigquery.httpConnectTimeout", "20000")
          .put("spark.datasource.bigquery.httpReadTimeout", "30000")
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
  public void testSerializability() throws IOException {
    DataSourceOptions options = new DataSourceOptions(defaultOptions);
    // test to make sure all members can be serialized.
    new ObjectOutputStream(new ByteArrayOutputStream())
        .writeObject(
            SparkBigQueryProxyAndHttpConfig.from(
                options.asMap(), defaultGlobalOptions, defaultHadoopConfiguration));
  }

  @Test
  public void testConfigFromOptions() throws URISyntaxException {
    Configuration emptyHadoopConfiguration = new Configuration();
    DataSourceOptions options = new DataSourceOptions(defaultOptions);
    SparkBigQueryProxyAndHttpConfig config =
        SparkBigQueryProxyAndHttpConfig.from(
            options.asMap(),
            ImmutableMap.of(), // empty globalOptions
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
  public void testConfigFromGlobalOptions() throws URISyntaxException {
    Configuration emptyHadoopConfiguration = new Configuration();
    ImmutableMap<String, String> globalOptions =
        SparkBigQueryConfig.normalizeConf(defaultGlobalOptions);
    SparkBigQueryProxyAndHttpConfig config =
        SparkBigQueryProxyAndHttpConfig.from(
            ImmutableMap.of(), // empty options
            globalOptions,
            emptyHadoopConfiguration);

    assertThat(config.getProxyUri())
        .isEqualTo(Optional.of(getURI("http", "bq-connector-host-global", 1234)));
    assertThat(config.getProxyUsername()).isEqualTo(Optional.of("bq-connector-user-global"));
    assertThat(config.getProxyPassword()).isEqualTo(Optional.of("bq-connector-password-global"));
    assertThat(config.getHttpMaxRetry()).isEqualTo(Optional.of(20));
    assertThat(config.getHttpConnectTimeout()).isEqualTo(Optional.of(20000));
    assertThat(config.getHttpReadTimeout()).isEqualTo(Optional.of(30000));
  }

  @Test
  public void testConfigFromHadoopConfigurationOptions() throws URISyntaxException {
    SparkBigQueryProxyAndHttpConfig config =
        SparkBigQueryProxyAndHttpConfig.from(
            ImmutableMap.of(), // empty options
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
  public void testConfigWithAllThreeParameters() throws URISyntaxException {
    DataSourceOptions options = new DataSourceOptions(defaultOptions);
    ImmutableMap<String, String> globalOptions =
        SparkBigQueryConfig.normalizeConf(defaultGlobalOptions);
    SparkBigQueryProxyAndHttpConfig config =
        SparkBigQueryProxyAndHttpConfig.from(
            options.asMap(), globalOptions, defaultHadoopConfiguration);

    assertThat(config.getProxyUri())
        .isEqualTo(Optional.of(getURI("http", "bq-connector-host", 1234)));
    assertThat(config.getProxyUsername()).isEqualTo(Optional.of("bq-connector-user"));
    assertThat(config.getProxyPassword()).isEqualTo(Optional.of("bq-connector-password"));
    assertThat(config.getHttpMaxRetry()).isEqualTo(Optional.of(10));
    assertThat(config.getHttpConnectTimeout()).isEqualTo(Optional.of(10000));
    assertThat(config.getHttpReadTimeout()).isEqualTo(Optional.of(20000));
  }

  @Test
  public void testConfigWithGlobalParametersAndHadoopConfig() throws URISyntaxException {
    ImmutableMap<String, String> globalOptions =
        SparkBigQueryConfig.normalizeConf(defaultGlobalOptions);
    SparkBigQueryProxyAndHttpConfig config =
        SparkBigQueryProxyAndHttpConfig.from(
            ImmutableMap.of(), // empty options
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
  public void testConfigViaSparkBigQueryConfigWithAllThreeParameters() throws URISyntaxException {
    HashMap<String, String> sparkConfigOptions = new HashMap<>(defaultOptions);
    sparkConfigOptions.put("table", "dataset.table");
    ImmutableMap<String, String> globalOptions =
        SparkBigQueryConfig.normalizeConf(defaultGlobalOptions);
    DataSourceOptions options = new DataSourceOptions(sparkConfigOptions);
    SparkBigQueryConfig sparkConfig =
        SparkBigQueryConfig.from(
            options.asMap(),
            globalOptions,
            defaultHadoopConfiguration,
            10,
            new SQLConf(),
            "2.4.0",
            Optional.empty());

    SparkBigQueryProxyAndHttpConfig config =
        (SparkBigQueryProxyAndHttpConfig) sparkConfig.getBigQueryProxyConfig();

    assertThat(config.getProxyUri())
        .isEqualTo(Optional.of(getURI("http", "bq-connector-host", 1234)));
    assertThat(config.getProxyUsername()).isEqualTo(Optional.of("bq-connector-user"));
    assertThat(config.getProxyPassword()).isEqualTo(Optional.of("bq-connector-password"));
    assertThat(config.getHttpMaxRetry()).isEqualTo(Optional.of(10));
    assertThat(config.getHttpConnectTimeout()).isEqualTo(Optional.of(10000));
    assertThat(config.getHttpReadTimeout()).isEqualTo(Optional.of(20000));
  }

  @Test
  public void testConfigViaSparkBigQueryConfigWithGlobalOptionsAndHadoopConfiguration()
      throws URISyntaxException {
    HashMap<String, String> sparkConfigOptions = new HashMap<>();
    sparkConfigOptions.put("table", "dataset.table");
    ImmutableMap<String, String> globalOptions =
        SparkBigQueryConfig.normalizeConf(defaultGlobalOptions);
    DataSourceOptions options = new DataSourceOptions(sparkConfigOptions);
    SparkBigQueryConfig sparkConfig =
        SparkBigQueryConfig.from(
            options.asMap(), // contains only one key "table"
            globalOptions,
            defaultHadoopConfiguration,
            10,
            new SQLConf(),
            "2.4.0",
            Optional.empty());

    SparkBigQueryProxyAndHttpConfig config =
        (SparkBigQueryProxyAndHttpConfig) sparkConfig.getBigQueryProxyConfig();

    assertThat(config.getProxyUri())
        .isEqualTo(Optional.of(getURI("http", "bq-connector-host-global", 1234)));
    assertThat(config.getProxyUsername()).isEqualTo(Optional.of("bq-connector-user-global"));
    assertThat(config.getProxyPassword()).isEqualTo(Optional.of("bq-connector-password-global"));
    assertThat(config.getHttpMaxRetry()).isEqualTo(Optional.of(20));
    assertThat(config.getHttpConnectTimeout()).isEqualTo(Optional.of(20000));
    assertThat(config.getHttpReadTimeout()).isEqualTo(Optional.of(30000));
  }

  @Test
  public void testConfigViaSparkBigQueryConfigWithHadoopConfiguration() throws URISyntaxException {
    HashMap<String, String> sparkConfigOptions = new HashMap<>();
    sparkConfigOptions.put("table", "dataset.table");
    DataSourceOptions options = new DataSourceOptions(sparkConfigOptions);
    SparkBigQueryConfig sparkConfig =
        SparkBigQueryConfig.from(
            options.asMap(), // contains only one key "table"
            ImmutableMap.of(), // empty global options,
            defaultHadoopConfiguration,
            10,
            new SQLConf(),
            "2.4.0",
            Optional.empty());

    SparkBigQueryProxyAndHttpConfig config =
        (SparkBigQueryProxyAndHttpConfig) sparkConfig.getBigQueryProxyConfig();

    assertThat(config.getProxyUri())
        .isEqualTo(Optional.of(getURI("http", "bq-connector-host-hadoop", 1234)));
    assertThat(config.getProxyUsername()).isEqualTo(Optional.of("bq-connector-user-hadoop"));
    assertThat(config.getProxyPassword()).isEqualTo(Optional.of("bq-connector-password-hadoop"));
    assertThat(config.getHttpMaxRetry()).isEqualTo(Optional.of(30));
    assertThat(config.getHttpConnectTimeout()).isEqualTo(Optional.of(30000));
    assertThat(config.getHttpReadTimeout()).isEqualTo(Optional.of(40000));
  }

  @Test
  public void testWhenProxyIsNotSetAndUserNamePasswordAreNotNull() {
    ImmutableMap<String, String> optionsMap =
        ImmutableMap.<String, String>builder()
            .put("proxyUsername", "bq-connector-user")
            .put("proxyPassword", "bq-connector-password")
            .build();

    Configuration emptyHadoopConfiguration = new Configuration();
    DataSourceOptions options = new DataSourceOptions(optionsMap);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                SparkBigQueryProxyAndHttpConfig.from(
                    options.asMap(), ImmutableMap.of(), emptyHadoopConfiguration));

    assertThat(exception)
        .hasMessageThat()
        .contains(
            "Please set proxyAddress in order to use a proxy. "
                + "Setting proxyUsername or proxyPassword is not enough");
  }

  @Test
  public void testWhenProxyIsSetAndUserNameIsNull() {
    ImmutableMap<String, String> optionsMap =
        ImmutableMap.<String, String>builder()
            .put("proxyAddress", "http://bq-connector-host:1234")
            .put("proxyPassword", "bq-connector-password")
            .build();

    Configuration emptyHadoopConfiguration = new Configuration();
    DataSourceOptions options = new DataSourceOptions(optionsMap);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                SparkBigQueryProxyAndHttpConfig.from(
                    options.asMap(), ImmutableMap.of(), emptyHadoopConfiguration));

    assertThat(exception)
        .hasMessageThat()
        .contains("Both proxyUsername and proxyPassword should be defined or not defined together");
  }

  @Test
  public void testWhenProxyIsSetAndPasswordIsNull() {
    ImmutableMap<String, String> optionsMap =
        ImmutableMap.<String, String>builder()
            .put("proxyAddress", "http://bq-connector-host:1234")
            .put("proxyUsername", "bq-connector-user")
            .build();

    Configuration emptyHadoopConfiguration = new Configuration();
    DataSourceOptions options = new DataSourceOptions(optionsMap);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                SparkBigQueryProxyAndHttpConfig.from(
                    options.asMap(), ImmutableMap.of(), emptyHadoopConfiguration));

    assertThat(exception)
        .hasMessageThat()
        .contains("Both proxyUsername and proxyPassword should be defined or not defined together");
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
      URI uri = SparkBigQueryProxyAndHttpConfig.parseProxyAddress(address);
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
              () -> SparkBigQueryProxyAndHttpConfig.parseProxyAddress(address));
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
              () -> SparkBigQueryProxyAndHttpConfig.parseProxyAddress(address));
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
              () -> SparkBigQueryProxyAndHttpConfig.parseProxyAddress(address));
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
            () -> SparkBigQueryProxyAndHttpConfig.parseProxyAddress(address));

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
