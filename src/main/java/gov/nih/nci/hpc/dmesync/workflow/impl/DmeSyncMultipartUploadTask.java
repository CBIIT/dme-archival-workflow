package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Callable;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import gov.nih.nci.hpc.domain.datatransfer.HpcUploadPartETag;
import gov.nih.nci.hpc.domain.datatransfer.HpcUploadPartURL;

/**
 * DME Sync Multipart Upload Task
 * 
 * @author dinhys
 */
public class DmeSyncMultipartUploadTask implements Callable<HpcUploadPartETag> {

	private final HpcUploadPartURL uploadPartUrl;
	private final File file;
	private final long partSize;
	private final String proxyUrl;
	private final String proxyPort;
	private RetryTemplate retryTemplate;
	final Logger logger = LoggerFactory.getLogger(getClass().getName());

	public DmeSyncMultipartUploadTask(HpcUploadPartURL uploadPartUrl, File file, long partSize, String proxyUrl,
			String proxyPort) {
		this.uploadPartUrl = uploadPartUrl;
		this.file = file;
		this.partSize = partSize;
		this.proxyUrl = proxyUrl;
		this.proxyPort = proxyPort;
		
		SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(3);
  
        FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
        backOffPolicy.setBackOffPeriod(1L);
  
        retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(retryPolicy);
        retryTemplate.setBackOffPolicy(backOffPolicy);
	}

	@Override
	public HpcUploadPartETag call() throws Exception {
		//Retry a number of times.
		return retryTemplate.execute(new RetryCallback<HpcUploadPartETag, IOException>() {

		    public HpcUploadPartETag doWithRetry(RetryContext context) throws IOException {
		        return uploadPart();
		    }

		});
	}

	private HpcUploadPartETag uploadPart() throws IOException {

		HpcUploadPartETag uploadPartETag = new HpcUploadPartETag();
        
		try (FileInputStream inputStream = new FileInputStream(file)) {

			long partialFileSize = (uploadPartUrl.getPartNumber() * partSize < file.length() ? partSize
					: file.length() - ((uploadPartUrl.getPartNumber() - 1) * partSize));

		    logger.info("part number {}", uploadPartUrl.getPartNumber());
		    logger.info("part size {}", partialFileSize);
		        
			// Create destination URLs.
			URL destURL = new URL(uploadPartUrl.getPartUploadRequestURL());
			HttpURLConnection httpConnection;

			// Open destination URL connections.
			if (proxyUrl != null && !proxyUrl.isEmpty()) {
				Proxy proxy = new Proxy(Proxy.Type.HTTP,
						new InetSocketAddress(proxyUrl.trim(), Integer.parseInt(proxyPort.trim())));
				httpConnection = (HttpURLConnection) destURL.openConnection(proxy);
			} else {
				httpConnection = (HttpURLConnection) destURL.openConnection();
			}

			try {
				httpConnection.setRequestMethod("PUT");
				httpConnection.setRequestProperty("Content-Type", "multipart/form-data");
	
				httpConnection.setFixedLengthStreamingMode(partialFileSize);
				httpConnection.setDoInput(true);
				httpConnection.setDoOutput(true);
				httpConnection.setConnectTimeout(99999999);
				httpConnection.setReadTimeout(99999999);
	
				OutputStream out = httpConnection.getOutputStream();
				MessageDigest md = null;
	            try {
	              md = MessageDigest.getInstance("MD5");
	            } catch (NoSuchAlgorithmException e) {
	              throw new IOException();
	            }
	            
				// Copy data from source to destination.
				byte[] buf = new byte[1024];
				int count;
				long total = 0;
	
				long offset = (uploadPartUrl.getPartNumber() - 1) * partSize;
				inputStream.getChannel().position(offset);
				while ((count = inputStream.read(buf)) != -1) {
					if (Thread.interrupted()) {
						throw new IOException();
					}
					out.write(buf, 0, count);
					md.update(buf, 0, count);
					total += count;
					if (total >= partialFileSize) {
					    logger.debug("total {}", total);
						break;
					}
				}
	
				int responseCode = httpConnection.getResponseCode();
				byte[] md5 = md.digest();
	            String hash = new String(Hex.encodeHex(md5));
	            String eTag = httpConnection.getHeaderField("ETag").replace("\"", "");
				// Set part number and eTag returned.
				if (responseCode == 200 && StringUtils.equals(hash, eTag)) {
					uploadPartETag.setPartNumber(uploadPartUrl.getPartNumber());
					uploadPartETag.setETag(httpConnection.getHeaderField("ETag"));
				}
				
				return uploadPartETag;
			} finally {
				// Close the URL connections.
				httpConnection.disconnect();
			}
		}

	}

}
