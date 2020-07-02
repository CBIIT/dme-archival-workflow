package gov.nih.nci.hpc.dmesync;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.http.HttpHost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;

@Component
public class RestTemplateFactory {

  @Value("${dmesync.proxy.url:}")
  private String proxyUrl;

  @Value("${dmesync.proxy.port:}")
  private String proxyPort;

  public RestTemplate getRestTemplate() {
    try {
      HttpComponentsClientHttpRequestFactory requestFactory =
          new HttpComponentsClientHttpRequestFactory(
              constructHttpClientPrototype(proxyUrl, proxyPort));
      requestFactory.setBufferRequestBody(false);
      return new RestTemplate(requestFactory);
    } catch (NoSuchAlgorithmException | KeyManagementException e) {
      throw new RuntimeException("Unable to produce RestTemplate!", e);
    }
  }

  public RestTemplate getRestTemplate(ResponseErrorHandler pRespErrHandler) {
    try {

      HttpComponentsClientHttpRequestFactory requestFactory =
          new HttpComponentsClientHttpRequestFactory(
              constructHttpClientPrototype(proxyUrl, proxyPort));

      requestFactory.setReadTimeout(240 * 1000);
      requestFactory.setConnectTimeout(120 * 1000);

      RestTemplate template = new RestTemplate(requestFactory);
      template.setErrorHandler(pRespErrHandler);
      return template;
    } catch (NoSuchAlgorithmException | KeyManagementException e) {
      throw new RuntimeException("Unable to produce RestTemplate!", e);
    }
  }

  /** Trust manager that does not perform any checks. */
  private static class NullX509TrustManager implements X509TrustManager {

    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
      // intentional: do nothing
    }

    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
      // intentional: do nothing
    }

    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
    }
  }

  private static CloseableHttpClient constructHttpClientPrototype(
      String httpProxyUrl, String httpProxyPort)
      throws NoSuchAlgorithmException, KeyManagementException {
    HttpHost proxy = null;
    if (httpProxyUrl != null && !httpProxyUrl.isEmpty()) {
      proxy = new HttpHost(httpProxyUrl.trim(), Integer.parseInt(httpProxyPort.trim()));
      return HttpClients.custom()
          .setSSLContext(constructSslContext())
          .setSSLHostnameVerifier(new NoopHostnameVerifier())
          .setProxy(proxy)
          .build();
    }
    return HttpClients.custom()
        .setSSLContext(constructSslContext())
        .setSSLHostnameVerifier(new NoopHostnameVerifier())
        .build();
  }

  private static SSLContext constructSslContext()
      throws NoSuchAlgorithmException, KeyManagementException {
    TrustManager[] trustManagerArray = {new NullX509TrustManager()};
    SSLContext sslCtx = SSLContext.getInstance("TLS");
    sslCtx.init(null, trustManagerArray, null);
    return sslCtx;
  }
}
