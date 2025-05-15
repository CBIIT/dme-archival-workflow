package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.PostConstruct;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.BaseEncoding;

import gov.nih.nci.hpc.dmesync.CustomLowerCamelCase;
import gov.nih.nci.hpc.dmesync.RestTemplateFactory;
import gov.nih.nci.hpc.dmesync.RestTemplateResponseErrorHandler;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncVerificationException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncTask;
import gov.nih.nci.hpc.domain.datatransfer.HpcMultipartUpload;
import gov.nih.nci.hpc.domain.datatransfer.HpcUploadPartETag;
import gov.nih.nci.hpc.domain.datatransfer.HpcUploadPartURL;
import gov.nih.nci.hpc.domain.metadata.HpcMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.HpcCompleteMultipartUploadRequestDTO;
import gov.nih.nci.hpc.dto.datamanagement.HpcCompleteMultipartUploadResponseDTO;
import gov.nih.nci.hpc.dto.datamanagement.HpcDataObjectRegistrationResponseDTO;
import gov.nih.nci.hpc.dto.error.HpcExceptionDTO;

/**
 * DME Sync Pre-signed URL Upload Task Implementation
 * 
 * @author dinhys
 */
@Component
public class DmeSyncPresignUploadTaskImpl extends AbstractDmeSyncTask implements DmeSyncTask {

  @Autowired private RestTemplateFactory restTemplateFactory;
  @Autowired private ObjectMapper objectMapper;
  @Autowired private DmeSyncDeleteDataObject dmeSyncDeleteDataObject;

  @Value("${hpc.server.url}")
  private String serverUrl;

  @Value("${auth.token}")
  private String authToken;

  @Value("${dmesync.proxy.url:}")
  private String proxyUrl;

  @Value("${dmesync.proxy.port:}")
  private String proxyPort;
  
  @Value("${dmesync.multipart.threshold:}")
  private String multipartThreshold;
  
  @Value("${dmesync.multipart.chunksize:}")
  private String multipartChunksize;
  
  @Value("${dmesync.multipart.threadpoolsize}")
  private String threadPoolSize;
  
  @Value("${dmesync.checksum:true}")
  private boolean checksum;
  
  @Value("${dmesync.replace.modified.files:false}")
  private boolean replaceModifiedFiles;
  
  @Value("${dmesync.destination.s3.archive.configuration.id:}")
  private String s3ArchiveConfigurationId;
  
  @PostConstruct
  public boolean init() {
    super.setTaskName("UploadTask");
    return true;
  }
  
  @Override
  public StatusInfo process(StatusInfo object)
      throws DmeSyncWorkflowException, DmeSyncVerificationException {

    HpcDataObjectRegistrationResponseDTO serviceResponse = null;
    HpcExceptionDTO errorResponse;
    ResponseEntity<Object> response;

    File file = new File(object.getSourceFilePath());

    //Check whether it requires multi-part upload (default > 50MB)
    boolean multipartUpload = false;
    long partSize = Long.parseLong(multipartChunksize);
    long multipartThresholdSize = Long.parseLong(multipartThreshold);
	multipartThresholdSize = multipartThresholdSize < (5L * 1024 * 1025) ? (5L * 1024 * 1025) : multipartThresholdSize;
	multipartThresholdSize = multipartThresholdSize > (5L * 1024 * 1024 * 1025) ? (5L * 1024 * 1024 * 1025) : multipartThresholdSize;
    int parts = 0;
    long contentLength = file.length();
    if(file.length() > multipartThresholdSize) {
    	multipartUpload = true;
    	partSize = partSize < (5L * 1024 * 1024) ? (5L * 1024 * 1024) : partSize;
    	partSize = partSize > (5L * 1024 * 1024 * 1024) ? (5L * 1024 * 1024 * 1024) : partSize;
    	//Adjust part size to be a multiple of 1024
    	partSize = (new Double(round((double) partSize, 1024))).longValue();
	    parts = (int) Math.ceil((double) contentLength / partSize);
	    //Parts can not exceed 10000
	    if(parts > 10000) {
	    	//Adjust the part chunk size so that the number of parts is less than 10000
	    	partSize = (new Double(round((double) contentLength / 10000, 1024))).longValue();
	    	parts = (int) Math.ceil((double) contentLength / partSize);
	    }
    }
    
    try {
      //Call dataObjectRegistration API
      final URI dataObjectUrl =
          UriComponentsBuilder.fromHttpUrl(serverUrl)
              .path("/v2/dataObject".concat(object.getFullDestinationPath()))
              .build().encode()
              .toUri();

      HttpHeaders header = new HttpHeaders();
      header.setContentType(MediaType.MULTIPART_FORM_DATA);
      header.set(HttpHeaders.AUTHORIZATION, "Bearer ".concat(authToken));

      MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();

      //Include checksum in DataObjectRegistrationRequestDTO
      if(checksum) {
    	  HpcMetadataEntry objectEntry = new HpcMetadataEntry();
    	  objectEntry.setAttribute("source_checksum");
    	  objectEntry.setValue(object.getChecksum());
    	  object.getDataObjectRegistrationRequestDTO().getMetadataEntries().add(objectEntry);
    	  if(!multipartUpload) {
    		  String md5Checksum = BaseEncoding.base64().encode(Hex.decodeHex(object.getChecksum()));
    		  object.getDataObjectRegistrationRequestDTO().setChecksum(md5Checksum);
    	  }
      }
      if(StringUtils.isNotBlank(s3ArchiveConfigurationId)) {
    	  object.getDataObjectRegistrationRequestDTO().setS3ArchiveConfigurationId(s3ArchiveConfigurationId);
      }
      object.getDataObjectRegistrationRequestDTO().setGenerateUploadRequestURL(true);
      object.getDataObjectRegistrationRequestDTO().setUploadCompletion(true);
      if(multipartUpload) {
    	  object.getDataObjectRegistrationRequestDTO().setUploadParts(parts);
      }

      HttpHeaders jsonHeader = new HttpHeaders();
      jsonHeader.setContentType(MediaType.APPLICATION_JSON);
      objectMapper.setSerializationInclusion(Include.NON_NULL);
      String jsonRequestDto = objectMapper.writeValueAsString(object.getDataObjectRegistrationRequestDTO());
      HttpEntity<String> jsonHttpEntity =
          new HttpEntity<>(jsonRequestDto, jsonHeader);

      body.add("dataObjectRegistration", jsonHttpEntity);

      response =
          restTemplateFactory
              .getRestTemplate(new RestTemplateResponseErrorHandler())
              .exchange(
                  dataObjectUrl,
                  HttpMethod.PUT,
                  new HttpEntity<Object>(body, header),
                  Object.class);

      if (HttpStatus.OK.equals(response.getStatusCode())
          || HttpStatus.CREATED.equals(response.getStatusCode())) {
        String json = objectMapper.writeValueAsString(response.getBody());
        serviceResponse = objectMapper.readValue(json, HpcDataObjectRegistrationResponseDTO.class);
        logger.info(
            "[{}] Registration with presign url successful [{}]", super.getTaskName(),
            multipartUpload ? "Multipart upload URLs" : serviceResponse.getUploadRequestURL());
      } else {
    	String json = objectMapper.writeValueAsString(response.getBody());
	    errorResponse = objectMapper.readValue(json, HpcExceptionDTO.class);
    	if(replaceModifiedFiles && errorResponse.getMessage().contains("already archived")) {
    		//Perform soft delete and call registration again.
    		dmeSyncDeleteDataObject.deleteDataObject(object.getFullDestinationPath());
    		throw new DmeSyncWorkflowException(errorResponse.getMessage());
    	}
	    logger.error("[{}] {}", super.getTaskName(), errorResponse.getStackTrace());
	    throw new DmeSyncVerificationException(errorResponse.getMessage());
    	
      }
    } catch (DmeSyncWorkflowException | DmeSyncVerificationException  e) {
      throw e;
    } catch (Exception e) {
      logger.error("[{}] Registration with presign url failed", super.getTaskName(), e);
      throw new DmeSyncWorkflowException("Registration with presign url failed - " + e.getMessage(), e);
    }

    try {
      object.setUploadStartTimestamp(new Date());
      int responseCode = 0;
      // Upload to presigned URL
      if(multipartUpload) {
    	  responseCode = uploadToUrls(object,
                  serviceResponse.getMultipartUpload(),
                  file,
                  partSize);
      } else {
    	  responseCode =
          uploadToUrl(object,
              serviceResponse.getUploadRequestURL(),
              file,
              object.getDataObjectRegistrationRequestDTO().getChecksum());
      }

      if (responseCode == 200) {
        logger.debug("[{}] File upload successful", super.getTaskName());
        object.setUploadEndTimestamp(new Date());
        object = dmeSyncWorkflowService.getService(access).saveStatusInfo(object);
      } else {
        logger.error(
            "[{}] Upload with presign url failed with responseCode {}, requires action!!!", super.getTaskName(),
            responseCode);
        throw new DmeSyncWorkflowException(
            "Upload with presign url failed with responseCode " + responseCode);
      }

    } catch (Exception e) {
      logger.error("[{}] error occured during upload task, {}", super.getTaskName(), e.getMessage(), e);
      throw new DmeSyncWorkflowException("Error occurred while uploading with presign url", e);
    }

    return object;
  }
  
  private int uploadToUrl(StatusInfo object, String urlStr, File file, String checksum) throws IOException, DmeSyncWorkflowException {
    
    logger.info("[{}] uploadToUrl {}", super.getTaskName(), urlStr);
    logger.info("[{}] checksum {}", super.getTaskName(), checksum);
    
    try (InputStream inputStream = new FileInputStream(file)){
      // Create destination URLs.
      URL destURL = new URL(urlStr);
      HttpURLConnection httpConnection;

      // Open destination URL connections.
      if (proxyUrl != null && !proxyUrl.isEmpty()) {
        Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyUrl.trim(), Integer.parseInt(proxyPort.trim())));
        httpConnection = (HttpURLConnection) destURL.openConnection(proxy);
      } else {
        httpConnection =
          (HttpURLConnection) destURL.openConnection();
      }
      
      httpConnection.setRequestMethod("PUT");
      httpConnection.setRequestProperty("Content-Type", "multipart/form-data");
      
      httpConnection.setFixedLengthStreamingMode(file.length());
      httpConnection.setDoInput(true);
      httpConnection.setDoOutput(true);
      httpConnection.setConnectTimeout(99999999);
      httpConnection.setReadTimeout(99999999);
      
      if (!StringUtils.isEmpty(checksum)) httpConnection.addRequestProperty("content-md5", checksum);
      
      // Copy data from source to destination.
      IOUtils.copyLarge(
              inputStream,
              httpConnection.getOutputStream());

      int responseCode = httpConnection.getResponseCode();
      logger.debug("[{}] responseCode {}", super.getTaskName(), responseCode);

      // Close the URL connections.
      httpConnection.disconnect();

      return completeMultipartUpload(object, null);
    }
    
  }
  
  private int uploadToUrls(StatusInfo object, HpcMultipartUpload multipartUpload, File file, long partSize) throws DmeSyncWorkflowException {
	
		HpcCompleteMultipartUploadRequestDTO dto = new HpcCompleteMultipartUploadRequestDTO();
		dto.setMultipartUploadId(multipartUpload.getId());
		logger.info("[{}] fileId {}", super.getTaskName(), multipartUpload.getId());
		
		int poolSize = Integer.parseInt(threadPoolSize);
		ExecutorService executorService = Executors.newFixedThreadPool(poolSize);
		
		List<Callable<HpcUploadPartETag>> callableTasks = new ArrayList<>();
		for (HpcUploadPartURL uploadPartUrl : multipartUpload.getParts()) {
			logger.info("[{}] uploadToUrl {}", super.getTaskName(), uploadPartUrl.getPartUploadRequestURL());
			logger.info("[{}] partNumber {}", super.getTaskName(), uploadPartUrl.getPartNumber());
			callableTasks.add(new DmeSyncMultipartUploadTask(uploadPartUrl, file, partSize, proxyUrl, proxyPort));
		}
		List<Future<HpcUploadPartETag>> futures = null;
		try {
			futures = executorService.invokeAll(callableTasks);
			for (Future<HpcUploadPartETag> future : futures) {
				HpcUploadPartETag result = future.get();
				dto.getUploadPartETags().add(result);
				logger.info("[{}] part {}: eTag: {}", super.getTaskName(), result.getPartNumber(), result.getETag());
			}
		} catch (InterruptedException | ExecutionException e) {
			throw new DmeSyncWorkflowException(e);
		}
		
		executorService.shutdown();
		
		return completeMultipartUpload(object, dto);
	}
  
	private int completeMultipartUpload(StatusInfo object, HpcCompleteMultipartUploadRequestDTO dto)
			throws DmeSyncWorkflowException {

		HpcExceptionDTO errorResponse;
		ResponseEntity<Object> response;

		try {
			// Call completeMultipartUpload API
			final URI completeUrl = UriComponentsBuilder.fromHttpUrl(serverUrl)
					.path("/dataObject".concat(object.getFullDestinationPath()).concat("/completeMultipartUpload"))
					.build().encode().toUri();

			HttpHeaders header = new HttpHeaders();
			header.setContentType(MediaType.APPLICATION_JSON);
			header.set(HttpHeaders.AUTHORIZATION, "Bearer ".concat(authToken));
			
			objectMapper.configure(MapperFeature.USE_STD_BEAN_NAMING, true);
			objectMapper.setPropertyNamingStrategy(new CustomLowerCamelCase());
			String jsonRequestDto = null;
			if (dto == null) 
				jsonRequestDto = "{}";
			else
				jsonRequestDto = objectMapper.writeValueAsString(dto);
					
			final HttpEntity<String> entity = new HttpEntity<>(jsonRequestDto, header);
			
			response = restTemplateFactory.getRestTemplate(new RestTemplateResponseErrorHandler()).exchange(completeUrl,
					HttpMethod.POST, entity, Object.class);

			if (HttpStatus.OK.equals(response.getStatusCode())) {
			    HpcCompleteMultipartUploadResponseDTO responseDto = null;
			    String json = objectMapper.writeValueAsString(response.getBody());
			    responseDto = objectMapper.readValue(json, HpcCompleteMultipartUploadResponseDTO.class);
			    if (responseDto != null) {
			        //We will not compute the final eTag to compare against the returned eTag since checksum of each part is verified.
                    logger.info("[{}] Complete multipart upload successful: eTag {}", super.getTaskName(), responseDto.getChecksum());            
                }
			} else {
				String json = objectMapper.writeValueAsString(response.getBody());
				errorResponse = objectMapper.readValue(json, HpcExceptionDTO.class);
				logger.error("[{}] {}", super.getTaskName(), errorResponse.getStackTrace());
				throw new DmeSyncWorkflowException(errorResponse.getMessage());
			}
		} catch (Exception e) {
			logger.error("[{}] Complete multipart upload failed", super.getTaskName(), e);
			throw new DmeSyncWorkflowException("Complete multipart upload failed - " + e.getMessage(), e);
		}

		return response.getStatusCodeValue();
	}
	
	
	private double round(double num, int multipleOf) {
		return Math.ceil((num + multipleOf / 2.0) / multipleOf) * multipleOf;
	}

}
