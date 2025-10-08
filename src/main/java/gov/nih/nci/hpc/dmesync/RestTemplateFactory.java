package gov.nih.nci.hpc.dmesync;

import java.io.FileInputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;


import org.apache.http.HttpHost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
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
  
  @Value("${hpc.ssl.keystore.path:}")
  private  String hpcCertPath;

  @Value("${hpc.ssl.keystore.password:}")
  private  String hpcCertPassword;
  
  

  public RestTemplate getRestTemplate() {
    try {
      HttpComponentsClientHttpRequestFactory requestFactory =
          new HttpComponentsClientHttpRequestFactory(
              constructHttpClientPrototype(proxyUrl, proxyPort ,hpcCertPath, hpcCertPassword));
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
              constructHttpClientPrototype(proxyUrl, proxyPort ,hpcCertPath, hpcCertPassword));

      requestFactory.setReadTimeout(240 * 1000);
      requestFactory.setConnectTimeout(120 * 1000);

      RestTemplate template = new RestTemplate(requestFactory);
      template.setErrorHandler(pRespErrHandler);
      return template;
    } catch (NoSuchAlgorithmException | KeyManagementException e) {
      throw new RuntimeException("Unable to produce RestTemplate!", e);
    }
  }

  private static CloseableHttpClient constructHttpClientPrototype(
      String httpProxyUrl, String httpProxyPort , String hpcCertPath,String hpcCertPassword)
      throws NoSuchAlgorithmException, KeyManagementException {
    HttpHost proxy = null;
    if (httpProxyUrl != null && !httpProxyUrl.isEmpty()) {
      proxy = new HttpHost(httpProxyUrl.trim(), Integer.parseInt(httpProxyPort.trim()));
      return HttpClients.custom()
          .setSSLContext(constructSslContext(hpcCertPath, hpcCertPassword))
          .setSSLHostnameVerifier(new NoopHostnameVerifier())
          .setProxy(proxy)
          .build();
    }
   /* final SSLConnectionSocketFactory sslSF = new SSLConnectionSocketFactory(constructSslContext(hpcCertPath, hpcCertPassword),
            NoopHostnameVerifier.INSTANCE);
        
        final Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory> create()
            .register("https", sslSF)
            .register("http", new PlainConnectionSocketFactory())
            .build();

        final BasicHttpClientConnectionManager connectionManager = 
            new BasicHttpClientConnectionManager(socketFactoryRegistry);
        final CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(connectionManager)
                .build();
        return httpClient; */
        
     return HttpClients.custom()
        .setSSLContext(constructSslContext( hpcCertPath, hpcCertPassword))
        .setSSLHostnameVerifier(new NoopHostnameVerifier())
        .build();
  }

  private static SSLContext constructSslContext(String hpcCertPath,String hpcCertPassword )
      throws NoSuchAlgorithmException, KeyManagementException {
	  try {
			if (hpcCertPath != null && hpcCertPassword != null) {
				FileInputStream fis = new java.io.FileInputStream(hpcCertPath);
				KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
				keyStore.load(fis, hpcCertPassword.toCharArray());
				KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
				kmf.init(keyStore, hpcCertPassword.toCharArray());
				KeyManager[] keyManagers = kmf.getKeyManagers();

				TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
				tmf.init(keyStore);
				TrustManager[] trustManagers = tmf.getTrustManagers();

				SSLContext sslContext = SSLContext.getInstance("TLS");
				sslContext.init(keyManagers, trustManagers, null);
				return sslContext;
			} else {
				final TrustStrategy acceptingTrustStrategy = (cert, authType) -> true;
				final SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy)
						.build();
				return sslContext;
			}
		} catch (Exception e) {
			throw new RuntimeException("Failed to initialize SSLContext", e);
		}
    //TrustManager[] trustManagerArray = {new NullX509TrustManager()};
    //SSLContext sslCtx = SSLContext.getInstance("TLS");
    //sslCtx.init(null, trustManagerArray, null);
    //return sslCtx;
  }
}
