package com.google.cloud.bigquery.connector.common;

import com.google.api.client.json.GenericJson;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;

public class BigQueryCredentialsSupplierTest {

  // Taken from
  // https://github.com/googleapis/google-auth-library-java/blob/a329c4171735c3d4ee574978e6c3742b96c01f74/oauth2_http/javatests/com/google/auth/oauth2/ServiceAccountCredentialsTest.java
  // As this code is private it cannot be directly used.

  static final String PRIVATE_KEY_PKCS8 =
      "-----BEGIN PRIVATE KEY-----\n"
          + "MIICdgIBADANBgkqhkiG9w0BAQEFAASCAmAwggJcAgEAAoGBALX0PQoe1igW12i"
          + "kv1bN/r9lN749y2ijmbc/mFHPyS3hNTyOCjDvBbXYbDhQJzWVUikh4mvGBA07qTj79Xc3yBDfKP2IeyYQIFe0t0"
          + "zkd7R9Zdn98Y2rIQC47aAbDfubtkU1U72t4zL11kHvoa0/RuFZjncvlr42X7be7lYh4p3NAgMBAAECgYASk5wDw"
          + "4Az2ZkmeuN6Fk/y9H+Lcb2pskJIXjrL533vrDWGOC48LrsThMQPv8cxBky8HFSEklPpkfTF95tpD43iVwJRB/Gr"
          + "CtGTw65IfJ4/tI09h6zGc4yqvIo1cHX/LQ+SxKLGyir/dQM925rGt/VojxY5ryJR7GLbCzxPnJm/oQJBANwOCO6"
          + "D2hy1LQYJhXh7O+RLtA/tSnT1xyMQsGT+uUCMiKS2bSKx2wxo9k7h3OegNJIu1q6nZ6AbxDK8H3+d0dUCQQDTrP"
          + "SXagBxzp8PecbaCHjzNRSQE2in81qYnrAFNB4o3DpHyMMY6s5ALLeHKscEWnqP8Ur6X4PvzZecCWU9BKAZAkAut"
          + "LPknAuxSCsUOvUfS1i87ex77Ot+w6POp34pEX+UWb+u5iFn2cQacDTHLV1LtE80L8jVLSbrbrlH43H0DjU5AkEA"
          + "gidhycxS86dxpEljnOMCw8CKoUBd5I880IUahEiUltk7OLJYS/Ts1wbn3kPOVX3wyJs8WBDtBkFrDHW2ezth2QJ"
          + "ADj3e1YhMVdjJW5jqwlD/VNddGjgzyunmiZg0uOXsHXbytYmsA545S8KRQFaJKFXYYFo2kOjqOiC1T2cAzMDjCQ"
          + "==\n-----END PRIVATE KEY-----\n";
  private static final String TYPE = "service_account";
  private static final String CLIENT_EMAIL =
      "36680232662-vrd7ji19qe3nelgchd0ah2csanun6bnr@developer.gserviceaccount.com";
  private static final String CLIENT_ID =
      "36680232662-vrd7ji19qe3nelgchd0ah2csanun6bnr.apps.googleusercontent.com";
  private static final String PRIVATE_KEY_ID = "d84a4fefcf50791d4a90f2d7af17469d6282df9d";
  private static final String ACCESS_TOKEN = "1/MkSJoj1xsli0AccessToken_NKPY2";
  private static final Collection<String> SCOPES = Collections.singletonList("dummy.scope");
  private static final String QUOTA_PROJECT = "sample-quota-project-id";

  @Test
  public void testCredentialsFromAccessToken() throws Exception {
    Credentials credentials =
            new BigQueryCredentialsSupplier(
                    Optional.of(ACCESS_TOKEN), Optional.empty(), Optional.empty())
                    .getCredentials();
    assertThat(credentials).isInstanceOf(GoogleCredentials.class);
    GoogleCredentials googleCredentials = (GoogleCredentials) credentials;
    assertThat(googleCredentials.getAccessToken().getTokenValue()).isEqualTo(ACCESS_TOKEN);
  }

  @Test
  public void testCredentialsFromKey() throws Exception {
    String json = createServiceAccountJson("key");
    String credentialsKey =
            Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));

    Credentials credentials =
            new BigQueryCredentialsSupplier(
                    Optional.empty(), Optional.of(credentialsKey), Optional.empty())
                    .getCredentials();
    assertThat(credentials).isInstanceOf(ServiceAccountCredentials.class);
    ServiceAccountCredentials serviceAccountCredentials = (ServiceAccountCredentials) credentials;
    assertThat(serviceAccountCredentials.getProjectId()).isEqualTo("key");
    assertThat(serviceAccountCredentials.getClientEmail()).isEqualTo(CLIENT_EMAIL);
    assertThat(serviceAccountCredentials.getClientId()).isEqualTo(CLIENT_ID);
    assertThat(serviceAccountCredentials.getQuotaProjectId()).isEqualTo(QUOTA_PROJECT);
  }

  @Test
  public void testCredentialsFromFile() throws Exception {
    String json = createServiceAccountJson("file");
    File credentialsFile = File.createTempFile("dummy-credentials", ".json");
    credentialsFile.deleteOnExit();
    Files.write(credentialsFile.toPath(), json.getBytes(StandardCharsets.UTF_8));

    Credentials credentials =
        new BigQueryCredentialsSupplier(
                Optional.empty(), Optional.empty(), Optional.of(credentialsFile.getAbsolutePath()))
            .getCredentials();
    assertThat(credentials).isInstanceOf(ServiceAccountCredentials.class);
    ServiceAccountCredentials serviceAccountCredentials = (ServiceAccountCredentials) credentials;
    assertThat(serviceAccountCredentials.getProjectId()).isEqualTo("file");
    assertThat(serviceAccountCredentials.getClientEmail()).isEqualTo(CLIENT_EMAIL);
    assertThat(serviceAccountCredentials.getClientId()).isEqualTo(CLIENT_ID);
    assertThat(serviceAccountCredentials.getQuotaProjectId()).isEqualTo(QUOTA_PROJECT);
  }

  private String createServiceAccountJson(String projectId) throws Exception {
    GenericJson json = new GenericJson();
    json.setFactory(JacksonFactory.getDefaultInstance());
    json.put("type", TYPE);
    json.put("client_id", CLIENT_ID);
    json.put("client_email", CLIENT_EMAIL);
    json.put("private_key", PRIVATE_KEY_PKCS8);
    json.put("private_key_id", PRIVATE_KEY_ID);
    json.put("project_id", projectId);
    json.put("quota_project_id", QUOTA_PROJECT);
    return json.toPrettyString();
  }
}
