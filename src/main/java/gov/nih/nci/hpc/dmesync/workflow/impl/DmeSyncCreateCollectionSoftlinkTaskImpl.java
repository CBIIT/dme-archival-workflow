package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.net.URI;
import java.util.Date;
import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;

import gov.nih.nci.hpc.dmesync.RestTemplateFactory;
import gov.nih.nci.hpc.dmesync.RestTemplateResponseErrorHandler;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncTask;
import gov.nih.nci.hpc.dto.datamanagement.HpcCollectionRegistrationDTO;
import gov.nih.nci.hpc.dto.error.HpcExceptionDTO;

/**
 * DME Sync Create Collection Softlink Task Implementation
 * 
 * @author dinhys
 */
@Component
public class DmeSyncCreateCollectionSoftlinkTaskImpl extends AbstractDmeSyncTask implements DmeSyncTask {

  @Autowired private RestTemplateFactory restTemplateFactory;
  @Autowired private ObjectMapper objectMapper;
  
  @Value("${hpc.server.url}")
  private String serverUrl;

  @Value("${auth.token}")
  private String authToken;

  @PostConstruct
  public boolean init() {
    super.setTaskName("CreateCollectionSoftlinkTask");
    return true;
  }
  
  @Override
  public StatusInfo process(StatusInfo object)
      throws DmeSyncMappingException, DmeSyncWorkflowException {

	HpcExceptionDTO errorResponse;
	ResponseEntity<Object> response;
	
    try {
      object.setUploadStartTimestamp(new Date());
    	
      //Call dataObjectRegistration API
      final URI collectionUrl =
          UriComponentsBuilder.fromHttpUrl(serverUrl)
              .path("/collection".concat(object.getFullDestinationPath()))
              .build().encode()
              .toUri();

      HttpHeaders header = new HttpHeaders();
      header.setContentType(MediaType.APPLICATION_JSON);
      header.set(HttpHeaders.AUTHORIZATION, "Bearer ".concat(authToken));

      //Set the link source path for softlink creation.
      HpcCollectionRegistrationDTO collectionRegistrationDTO = new HpcCollectionRegistrationDTO();
      collectionRegistrationDTO.setCreateParentCollections(true);
      collectionRegistrationDTO.setLinkSourcePath(object.getOriginalFilePath());
      collectionRegistrationDTO.getMetadataEntries().addAll(object.getDataObjectRegistrationRequestDTO().getMetadataEntries());
      collectionRegistrationDTO.setParentCollectionsBulkMetadataEntries(object.getDataObjectRegistrationRequestDTO().getParentCollectionsBulkMetadataEntries());
      
      HttpHeaders jsonHeader = new HttpHeaders();
      jsonHeader.setContentType(MediaType.APPLICATION_JSON);
      objectMapper.setSerializationInclusion(Include.NON_NULL);
      String jsonRequestDto = objectMapper.writeValueAsString(collectionRegistrationDTO);
      
      response = restTemplateFactory.getRestTemplate(new RestTemplateResponseErrorHandler())
				.exchange(collectionUrl, HttpMethod.PUT, new HttpEntity<Object>(jsonRequestDto, header), Object.class);

      if (HttpStatus.OK.equals(response.getStatusCode())
          || HttpStatus.CREATED.equals(response.getStatusCode())) {
        logger.info("[{}] Collection softlink creation successful", super.getTaskName());
        object.setStatus("COMPLETED");
        object.setUploadEndTimestamp(new Date());
        dmeSyncWorkflowService.getService(access).saveStatusInfo(object);
      } else {
    	  String json = objectMapper.writeValueAsString(response.getBody());
          errorResponse = objectMapper.readValue(json, HpcExceptionDTO.class);
          logger.error("[{}] {}", super.getTaskName(), errorResponse.getStackTrace());
          throw new DmeSyncWorkflowException(errorResponse.getMessage());
      }
    } catch (Exception e) {
      logger.error("[{}] error occurred during create collection softlink", super.getTaskName(), e);
      throw new DmeSyncWorkflowException("Error occurred during collection softlink creation", e);
    }

    return object;
  }
}
