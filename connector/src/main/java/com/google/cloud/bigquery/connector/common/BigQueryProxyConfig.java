package com.google.cloud.bigquery.connector.common;

import java.net.URI;
import java.util.Optional;

/**
 * Config interface to provide proxy parameters. The concrete class implementing this interface
 * should implement methods to provide ProxyUri, ProxyUserName and ProxyPassword.
 */
public interface BigQueryProxyConfig {

  /**
   * The Uniform Resource Identifier ({@link URI URI}) of the proxy sever. This is an Optional field
   * so can be empty.
   *
   * @return java.util.Optional<URI>
   */
  Optional<URI> getProxyUri();

  /**
   * The userName used to connect to the proxy. This is an Optional field so can be empty.
   *
   * @return java.util.Optional<String>
   */
  Optional<String> getProxyUsername();

  /**
   * The password used to connect to the proxy. This is an Optional field so can be empty.
   *
   * @return java.util.Optional<String>
   */
  Optional<String> getProxyPassword();
}
