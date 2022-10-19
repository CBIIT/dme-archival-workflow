package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;

import gov.nih.nci.hpc.dmesync.RestTemplateFactory;
import gov.nih.nci.hpc.dmesync.RestTemplateResponseErrorHandler;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncTask;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.HpcCollectionRegistrationDTO;
import gov.nih.nci.hpc.dto.error.HpcExceptionDTO;

/**
 * DME Sync Move Data Object Task Implementation
 * 
 * @author dinhys
 */
@Component
public class DmeSyncMoveDataObjectTaskImpl extends AbstractDmeSyncTask implements DmeSyncTask {

  @Autowired private RestTemplateFactory restTemplateFactory;
  @Autowired private ObjectMapper objectMapper;
  
  @Value("${hpc.server.url}")
  private String serverUrl;

  @Value("${auth.token}")
  private String authToken;

  @PostConstruct
  public boolean init() {
    super.setTaskName("MoveDataObjectTask");
    return true;
  }
  
  @Override
  public StatusInfo process(StatusInfo object)
      throws DmeSyncMappingException, DmeSyncWorkflowException {

	HpcExceptionDTO errorResponse;
	ResponseEntity<Object> response;
	
    try {
      //Create or update all parent collections.
      for (HpcBulkMetadataEntry entry : object.getDataObjectRegistrationRequestDTO().getParentCollectionsBulkMetadataEntries().getPathsMetadataEntries()) {
    	  HpcCollectionRegistrationDTO collectionDTO = new HpcCollectionRegistrationDTO();
    	  collectionDTO.getMetadataEntries().addAll(entry.getPathMetadataEntries());
    	  final URI collectionUrl =
    	          UriComponentsBuilder.fromHttpUrl(serverUrl)
    	              .path("/collection".concat(entry.getPath()))
    	              .build().encode()
    	              .toUri();

    	      HttpHeaders header = new HttpHeaders();
    	      header.set(HttpHeaders.AUTHORIZATION, "Bearer ".concat(authToken));

    	      response = restTemplateFactory
    	    	            .getRestTemplate(new RestTemplateResponseErrorHandler())
    	    	            .exchange(
    	    	            	collectionUrl,
    	    	                HttpMethod.PUT,
    	    	                new HttpEntity<Object>(collectionDTO, header),
    	    	                Object.class);

    	      if (HttpStatus.OK.equals(response.getStatusCode()) || HttpStatus.CREATED.equals(response.getStatusCode())) {
    	        logger.info("[{}] Parent collection {} successfully created", super.getTaskName(), entry.getPath());
    	      } else {
    	    	  String json = objectMapper.writeValueAsString(response.getBody());
    	          errorResponse = objectMapper.readValue(json, HpcExceptionDTO.class);
    	          logger.error("[{}] {}", super.getTaskName(), errorResponse.getStackTrace());
    	          throw new DmeSyncWorkflowException(errorResponse.getMessage());
    	      }
      }
      object.setUploadStartTimestamp(new Date());
    	
      //Call moveDataObject API
      
		String dataObjectUrl = serverUrl + 
				"/dataObject/"
						.concat(URLEncoder.encode(object.getMoveDataObjectOrignalPath().substring(1), StandardCharsets.UTF_8.name()))
						.concat("/move/")
						.concat(URLEncoder.encode(object.getFullDestinationPath().substring(1), StandardCharsets.UTF_8.name()));

      HttpHeaders header = new HttpHeaders();
      header.set(HttpHeaders.AUTHORIZATION, "Bearer ".concat(authToken));

      response = restTemplateFactory
    	            .getRestTemplate(new RestTemplateResponseErrorHandler())
    	            .exchange(
    	                new URI(dataObjectUrl),
    	                HttpMethod.POST,
    	                new HttpEntity<Object>(header),
    	                Object.class);

      if (HttpStatus.OK.equals(response.getStatusCode())) {
        logger.info("[{}] Move data object successful", super.getTaskName());
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
      logger.error("[{}] error occured during move data object", super.getTaskName(), e);
      throw new DmeSyncWorkflowException("Error occured during move data object", e);
    }

    return object;
  }
}
