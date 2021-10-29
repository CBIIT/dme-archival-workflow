package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.net.URI;
import java.util.Date;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
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
import com.fasterxml.jackson.databind.ObjectMapper;

import gov.nih.nci.hpc.dmesync.RestTemplateFactory;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncTask;

/**
 * DME Sync Upload Task Implementation
 * 
 * @author dinhys
 */
@Component
public class DmeSyncUploadTaskImpl extends AbstractDmeSyncTask implements DmeSyncTask {

  @Autowired private RestTemplateFactory restTemplateFactory;
  @Autowired private ObjectMapper objectMapper;

  @Value("${hpc.server.url}")
  private String serverUrl;

  @Value("${auth.token}")
  private String authToken;

  @Value("${dmesync.checksum:true}")
  private boolean checksum;
  
  @Value("${dmesync.metadata.update.only:false}")
  private boolean metadataUpdateOnly;
  
  @PostConstruct
  public boolean init() {
    super.setTaskName("UploadTask");
    return true;
  }
  
  @Override
  public StatusInfo process(StatusInfo object)
      throws DmeSyncMappingException, DmeSyncWorkflowException {

    object.setUploadStartTimestamp(new Date());

    try {
      //Call dataObjectRegistration API
      final URI dataObjectUrl =
          UriComponentsBuilder.fromHttpUrl(serverUrl)
              .path("/dataObject".concat(object.getFullDestinationPath()))
              .build().encode()
              .toUri();

      HttpHeaders header = new HttpHeaders();
      header.setContentType(MediaType.MULTIPART_FORM_DATA);
      header.set(HttpHeaders.AUTHORIZATION, "Bearer ".concat(authToken));

      MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();

      // creating an HttpEntity for dataObjectRegistration
      //Include checksum in DataObjectRegistrationRequestDTO
      if(checksum)
    	  object.getDataObjectRegistrationRequestDTO().setChecksum(object.getChecksum());
      
      HttpHeaders jsonHeader = new HttpHeaders();
      jsonHeader.setContentType(MediaType.APPLICATION_JSON);
      objectMapper.setSerializationInclusion(Include.NON_NULL);
      String jsonRequestDto = objectMapper.writeValueAsString(object.getDataObjectRegistrationRequestDTO());
      HttpEntity<String> jsonHttpEntity =
          new HttpEntity<>(jsonRequestDto, jsonHeader);
      body.add("dataObjectRegistration", jsonHttpEntity);
      
      // creating an HttpEntity for dataObject
      if(metadataUpdateOnly) {
    	  body.add("dataObject", null);
      } else {
	      HttpHeaders fileHeader = new HttpHeaders();
	      fileHeader.setContentType(MediaType.APPLICATION_OCTET_STREAM);
	      FileSystemResource file = new FileSystemResource(object.getSourceFilePath());
	      HttpEntity fileEntity = new HttpEntity<>(file, fileHeader);
	      body.add("dataObject", fileEntity);
      }

      ResponseEntity<String> serviceResponse =
          restTemplateFactory
              .getRestTemplate()
              .exchange(
                  dataObjectUrl,
                  HttpMethod.PUT,
                  new HttpEntity<Object>(body, header),
                  String.class);

      if (HttpStatus.OK.equals(serviceResponse.getStatusCode())
          || HttpStatus.CREATED.equals(serviceResponse.getStatusCode())) {
        logger.info("[{}] File upload successful", super.getTaskName());
        //Update DB to completed or shall we wait till the end of workflow?
        object.setStatus("COMPLETED");
        object.setUploadEndTimestamp(new Date());
        object = dmeSyncWorkflowService.saveStatusInfo(object);
      } else {
        logger.error(
            "[{}] Upload failed with responseCode {}", super.getTaskName(), serviceResponse.getStatusCode());
        throw new DmeSyncWorkflowException("Upload failed with responseCode " + serviceResponse.getStatusCode());
      }
    } catch (Exception e) {
      if (!metadataUpdateOnly) {
    	  logger.error("[{}] error occured during upload task", super.getTaskName(), e);
	      throw new DmeSyncWorkflowException("Error occured during upload", e);
      }
    }

    return object;
  }
}
