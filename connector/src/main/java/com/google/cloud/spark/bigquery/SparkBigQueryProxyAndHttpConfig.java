package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.connector.common.BigQueryProxyConfig;
import com.google.cloud.bigquery.connector.common.BigQueryProxyTransporterBuilder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;

import static com.google.cloud.bigquery.connector.common.BigQueryUtil.firstPresent;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Optional.fromJavaUtil;

public class SparkBigQueryProxyAndHttpConfig implements BigQueryProxyConfig, Serializable {

  // HTTP proxy with address in host:port format
  public static final String PROXY_ADDRESS_OPTION = "proxyAddress";
  public static final String PROXY_USERNAME_OPTION = "proxyUsername";
  public static final String PROXY_PASSWORD_OPTION = "proxyPassword";
  public static final String HTTP_MAX_RETRY_OPTION = "httpMaxRetry";
  public static final String HTTP_CONNECT_TIMEOUT_OPTION = "httpConnectTimeout";
  public static final String HTTP_READ_TIMEOUT_OPTION = "httpReadTimeout";

  // HTTP proxy with address in host:port format
  public static final String GCS_CONFIG_PROXY_ADDRESS_PROPERTY = "fs.gs.proxy.address";
  public static final String GCS_CONFIG_PROXY_USERNAME_PROPERTY = "fs.gs.proxy.username";
  public static final String GCS_CONFIG_PROXY_PASSWORD_PROPERTY = "fs.gs.proxy.password";
  public static final String GCS_CONFIG_HTTP_MAX_RETRY_PROPERTY = "fs.gs.http.max.retry";
  public static final String GCS_CONFIG_HTTP_CONNECT_TIMEOUT_PROPERTY =
      "fs.gs.http.connect-timeout";
  public static final String GCS_CONFIG_HTTP_READ_TIMEOUT_PROPERTY = "fs.gs.http.read-timeout";

  private com.google.common.base.Optional<URI> proxyUri;
  private com.google.common.base.Optional<String> proxyUsername;
  private com.google.common.base.Optional<String> proxyPassword;
  private com.google.common.base.Optional<Integer> httpMaxRetry;
  private com.google.common.base.Optional<Integer> httpConnectTimeout;
  private com.google.common.base.Optional<Integer> httpReadTimeout;

  @VisibleForTesting
  SparkBigQueryProxyAndHttpConfig() {
    // empty
  }

  @VisibleForTesting
  public static SparkBigQueryProxyAndHttpConfig from(
      Map<String, String> options,
      ImmutableMap<String, String> globalOptions,
      Configuration hadoopConfiguration)
      throws IllegalArgumentException {
    SparkBigQueryProxyAndHttpConfig config = new SparkBigQueryProxyAndHttpConfig();

    com.google.common.base.Optional<String> proxyAddress =
        getProperty(
            options,
            globalOptions,
            hadoopConfiguration,
            PROXY_ADDRESS_OPTION,
            GCS_CONFIG_PROXY_ADDRESS_PROPERTY);
    config.proxyUri = fromNullable(parseProxyAddress(proxyAddress.or("")));
    config.proxyUsername =
        getProperty(
            options,
            globalOptions,
            hadoopConfiguration,
            PROXY_USERNAME_OPTION,
            GCS_CONFIG_PROXY_USERNAME_PROPERTY);
    config.proxyPassword =
        getProperty(
            options,
            globalOptions,
            hadoopConfiguration,
            PROXY_PASSWORD_OPTION,
            GCS_CONFIG_PROXY_PASSWORD_PROPERTY);
    checkProxyParamsValidity(config);

    config.httpMaxRetry =
        getProperty(
                options,
                globalOptions,
                hadoopConfiguration,
                HTTP_MAX_RETRY_OPTION,
                GCS_CONFIG_HTTP_MAX_RETRY_PROPERTY)
            .transform(Integer::valueOf);
    config.httpConnectTimeout =
        getProperty(
                options,
                globalOptions,
                hadoopConfiguration,
                HTTP_CONNECT_TIMEOUT_OPTION,
                GCS_CONFIG_HTTP_CONNECT_TIMEOUT_PROPERTY)
            .transform(Integer::valueOf);
    config.httpReadTimeout =
        getProperty(
                options,
                globalOptions,
                hadoopConfiguration,
                HTTP_READ_TIMEOUT_OPTION,
                GCS_CONFIG_HTTP_READ_TIMEOUT_PROPERTY)
            .transform(Integer::valueOf);
    checkHttpParamsValidity(config);

    return config;
  }

  private static void checkProxyParamsValidity(SparkBigQueryProxyAndHttpConfig config)
      throws IllegalArgumentException {
    if (!config.proxyUri.isPresent()
        && (config.proxyUsername.isPresent() || config.proxyPassword.isPresent())) {
      throw new IllegalArgumentException(
          "Please set proxyAddress in order to use a proxy. "
              + "Setting proxyUsername or proxyPassword is not enough");
    }

    BigQueryProxyTransporterBuilder.checkProxyParamsValidity(
        config.getProxyUsername(), config.getProxyPassword());
  }

  private static void checkHttpParamsValidity(SparkBigQueryProxyAndHttpConfig config)
      throws IllegalArgumentException {
    // Not checking for httpConnectTimeout and httpReadTimeout as negative values are allowed for
    // these parameters.
    // A negative number is for the default value (20000). And 0 for an infinite timeout.
    if (config.getHttpMaxRetry().isPresent() && config.getHttpMaxRetry().get() < 0) {
      throw new IllegalArgumentException("Http Max Retry value cannot be negative");
    }
  }

  private static com.google.common.base.Optional<String> getProperty(
      Map<String, String> options,
      ImmutableMap<String, String> globalOptions,
      Configuration hadoopConfiguration,
      String bqOption,
      String gcsProperty) {
    return fromJavaUtil(
        firstPresent(
            getFirstOrSecondOption(options, globalOptions, bqOption).toJavaUtil(),
            fromNullable(hadoopConfiguration.get(gcsProperty)).toJavaUtil()));
  }

  private static com.google.common.base.Optional<String> getFirstOrSecondOption(
      Map<String, String> options, ImmutableMap<String, String> globalOptions, String name) {
    return com.google.common.base.Optional.fromNullable(options.get(name.toLowerCase()))
        .or(com.google.common.base.Optional.fromNullable(globalOptions.get(name)));
  }

  @VisibleForTesting
  static URI parseProxyAddress(String proxyAddress) {
    if (Strings.isNullOrEmpty(proxyAddress)) {
      return null;
    }
    String uriString = (proxyAddress.contains("//") ? "" : "//") + proxyAddress;
    try {
      URI uri = new URI(uriString);
      String scheme = uri.getScheme();
      String host = uri.getHost();
      int port = uri.getPort();
      checkArgument(
          Strings.isNullOrEmpty(scheme) || scheme.matches("https?"),
          "Proxy address '%s' has invalid scheme '%s'.",
          proxyAddress,
          scheme);
      checkArgument(!Strings.isNullOrEmpty(host), "Proxy address '%s' has no host.", proxyAddress);
      checkArgument(port != -1, "Proxy address '%s' has no port.", proxyAddress);
      checkArgument(
          uri.equals(new URI(scheme, null, host, port, null, null, null)),
          "Invalid proxy address '%s'.",
          proxyAddress);
      return uri;
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          String.format("Invalid proxy address '%s'.", proxyAddress), e);
    }
  }

  public Optional<URI> getProxyUri() {
    return proxyUri.toJavaUtil();
  }

  public Optional<String> getProxyUsername() {
    return proxyUsername.toJavaUtil();
  }

  public Optional<String> getProxyPassword() {
    return proxyPassword.toJavaUtil();
  }

  Optional<Integer> getHttpMaxRetry() {
    return httpMaxRetry.toJavaUtil();
  }

  Optional<Integer> getHttpConnectTimeout() {
    return httpConnectTimeout.toJavaUtil();
  }

  Optional<Integer> getHttpReadTimeout() {
    return httpReadTimeout.toJavaUtil();
  }
}
