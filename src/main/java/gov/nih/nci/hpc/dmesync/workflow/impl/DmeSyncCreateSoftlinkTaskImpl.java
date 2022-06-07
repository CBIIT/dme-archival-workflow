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
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;

import gov.nih.nci.hpc.dmesync.RestTemplateFactory;
import gov.nih.nci.hpc.dmesync.RestTemplateResponseErrorHandler;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncTask;
import gov.nih.nci.hpc.domain.datatransfer.HpcFileLocation;
import gov.nih.nci.hpc.domain.datatransfer.HpcUploadSource;
import gov.nih.nci.hpc.dto.error.HpcExceptionDTO;

/**
 * DME Sync Create Softlink Task Implementation
 * 
 * @author dinhys
 */
@Component
public class DmeSyncCreateSoftlinkTaskImpl extends AbstractDmeSyncTask implements DmeSyncTask {

  @Autowired private RestTemplateFactory restTemplateFactory;
  @Autowired private ObjectMapper objectMapper;
  
  @Value("${hpc.server.url}")
  private String serverUrl;

  @Value("${auth.token}")
  private String authToken;

  @PostConstruct
  public boolean init() {
    super.setTaskName("CreateSoftlinkTask");
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
      final URI dataObjectUrl =
          UriComponentsBuilder.fromHttpUrl(serverUrl)
              .path("/v2/dataObject".concat(object.getFullDestinationPath()))
              .build().encode()
              .toUri();

      HttpHeaders header = new HttpHeaders();
      header.setContentType(MediaType.MULTIPART_FORM_DATA);
      header.set(HttpHeaders.AUTHORIZATION, "Bearer ".concat(authToken));

      MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();

      //Set the link source path for softlink creation.
      object.getDataObjectRegistrationRequestDTO().setLinkSourcePath(object.getSourceFilePath());
      object.getDataObjectRegistrationRequestDTO().setGenerateUploadRequestURL(null);
      
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
        logger.info("[{}] Softlink creation successful", super.getTaskName());
        object.setStatus("COMPLETED");
        object.setUploadEndTimestamp(new Date());
        dmeSyncWorkflowService.saveStatusInfo(object);
      } else {
    	  String json = objectMapper.writeValueAsString(response.getBody());
          errorResponse = objectMapper.readValue(json, HpcExceptionDTO.class);
          logger.error("[{}] {}", super.getTaskName(), errorResponse.getStackTrace());
          throw new DmeSyncWorkflowException(errorResponse.getMessage());
      }
    } catch (Exception e) {
      logger.error("[{}] error occured during create softlink", super.getTaskName(), e);
      throw new DmeSyncWorkflowException("Error occured during softlink creation", e);
    }

    return object;
  }
}
