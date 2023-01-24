package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.net.URI;
import java.util.Map;
import java.util.stream.Collectors;
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
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.nih.nci.hpc.dmesync.RestTemplateFactory;
import gov.nih.nci.hpc.dmesync.RestTemplateResponseErrorHandler;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncVerificationException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncTask;
import gov.nih.nci.hpc.domain.metadata.HpcMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.HpcDataObjectListDTO;

/**
 * DME Sync Verify Task Implementation
 * 
 * @author dinhys
 */
@Component
public class DmeSyncVerifyTaskImpl extends AbstractDmeSyncTask implements DmeSyncTask {

  @Value("${hpc.server.url}")
  private String serverUrl;

  @Value("${auth.token}")
  private String authToken;

  @Value("${dmesync.checksum:true}")
  private boolean checksum;
  
  @Value("${dmesync.filesystem.upload:false}")
  private boolean fileSystemUpload;
  
  @Autowired private RestTemplateFactory restTemplateFactory;
  
  @Autowired private ObjectMapper objectMapper;

  @PostConstruct
  public boolean init() {
    super.setTaskName("VerifyTask");
    return true;
  }
  
  @Override
  public StatusInfo process(StatusInfo object) throws DmeSyncWorkflowException, DmeSyncVerificationException {

    //Verify, call GET dataObject and verify file size and checksum against local db.
    try {
      //Call dataObject API
      final URI dataObjectUrl =
          UriComponentsBuilder.fromHttpUrl(serverUrl)
              .path("/dataObject".concat(object.getFullDestinationPath()))
              .queryParam("excludeNonMetadataAttributes", Boolean.TRUE.toString())
              .build().encode()
              .toUri();

      HttpHeaders header = new HttpHeaders();
      header.setContentType(MediaType.MULTIPART_FORM_DATA);
      header.set(HttpHeaders.AUTHORIZATION, "Bearer ".concat(authToken));

      ResponseEntity<Object> response =
          restTemplateFactory
              .getRestTemplate(new RestTemplateResponseErrorHandler())
              .exchange(
                  dataObjectUrl,
                  HttpMethod.GET,
                  new HttpEntity<Object>(header),
                  Object.class);

      if (HttpStatus.OK.equals(response.getStatusCode())) {
        logger.debug("[{}] Received 200 response", super.getTaskName());
        String json = objectMapper.writeValueAsString(response.getBody());
        HpcDataObjectListDTO dataObjectListDTO = objectMapper.readValue(json, HpcDataObjectListDTO.class);
        
        //Set metaDataEntries into a map
        Map<String, String> map =
            dataObjectListDTO
                .getDataObjects()
                .get(0)
                .getMetadataEntries()
                .getSelfMetadataEntries()
                .stream()
                .collect(
                    Collectors.toMap(HpcMetadataEntry::getAttribute, HpcMetadataEntry::getValue));

        if (map.get("source_file_size") != null
            && !map.get("source_file_size").equals(object.getFilesize().toString())) {
          String msg =
              "File size does not match local "
                  + object.getFilesize()
                  + ", DME "
                  + map.get("source_file_size");
          logger.error("[{}] {}", super.getTaskName(), msg);
          object.setError(msg);
        }
        if (checksum && map.get("checksum") != null && !map.get("checksum").contains("-") && !map.get("checksum").equals(object.getChecksum())) {
          String msg =
              "Checksum does not match local "
                  + object.getChecksum()
                  + ", DME "
                  + map.get("checksum");
          logger.error("[{}] {}", super.getTaskName(), msg);
          object.setError(msg);
        }
        if (map.get("data_transfer_status") != null
            && !map.get("data_transfer_status").equals("ARCHIVED")) {
          String msg =
              "Data_transfer_status is not in ARCHIVED, it is " + map.get("data_transfer_status");
          logger.error("[{}] {}", super.getTaskName(), msg);
          object.setError(msg);
          if (fileSystemUpload) {
        	  throw new DmeSyncVerificationException(msg);
          }
        }
      } else {
        logger.error(
            "[{}] Received bad response from verify dataObject, responseCode {}", super.getTaskName(),
                response.getStatusCode());
        if (fileSystemUpload) {
          //For file system async upload, this means that the registration itself was not successful. Cleanup the tasks to start over.
          dmeSyncWorkflowService.getService(access).deleteTaskInfoByObjectId(object.getId());
      	  throw new DmeSyncVerificationException("Data object registration not successful");
        }
        throw new DmeSyncWorkflowException("Received bad response from verify dataObject");
      }
    } catch (DmeSyncVerificationException e) {
        throw e;
    } catch (Exception e) {
      logger.error("[{}] Error occured during verify task", super.getTaskName(), e);
      throw new DmeSyncWorkflowException("Error occured during verify task - " + e.getMessage(), e);
    }

    return object;
  }
}
