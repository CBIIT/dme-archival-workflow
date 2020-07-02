package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
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
import gov.nih.nci.hpc.dmesync.domain.PermissionBookmarkInfo;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncTask;
import gov.nih.nci.hpc.domain.datamanagement.HpcPermission;
import gov.nih.nci.hpc.domain.datamanagement.HpcUserPermission;
import gov.nih.nci.hpc.dto.databrowse.HpcBookmarkRequestDTO;
import gov.nih.nci.hpc.dto.datamanagement.HpcEntityPermissionsDTO;
import gov.nih.nci.hpc.dto.datamanagement.HpcUserPermissionDTO;
import gov.nih.nci.hpc.dto.error.HpcExceptionDTO;

/**
 * DME Sync Permission and Bookmark Task Implementation
 * 
 * @author dinhys
 */
@Component
public class DmeSyncPermissionBookmarkTaskImpl extends AbstractDmeSyncTask implements DmeSyncTask {

  @Value("${hpc.server.url}")
  private String serverUrl;

  @Value("${auth.token}")
  private String authToken;

  @Autowired private RestTemplateFactory restTemplateFactory;
  @Autowired private ObjectMapper objectMapper;

  @PostConstruct
  public boolean init() {
    super.setTaskName("PermissionBookmarkTask");
    return true;
  }
  
  @Override
  public StatusInfo process(StatusInfo object) {

    try {
      //Get all permission-bookmark entries that are not yet created
      List<PermissionBookmarkInfo> entries = dmeSyncWorkflowService.findAllPermissionBookmarkInfoByCreated("N");

      for (PermissionBookmarkInfo entry : entries) {
        //For each entry, check if the entry is a part of our DME path
        if (object.getFullDestinationPath().contains(entry.getPath())) {
          //Check user permission on path
          HpcUserPermissionDTO permission =
              getPermissionForUser(entry.getPath(), entry.getUserId());
          if (permission == null
              || !permission.getPermission().equals(HpcPermission.valueOf(entry.getPermission()))) {
            //Add the user permission specified if permission is not there
            HpcUserPermission userPermission = new HpcUserPermission();
            userPermission.setUserId(entry.getUserId());
            userPermission.setPermission(HpcPermission.valueOf(entry.getPermission()));
            HpcEntityPermissionsDTO dto = new HpcEntityPermissionsDTO();
            dto.getUserPermissions().add(userPermission);

            //Call DME API to add permission
            if (!updatePermission(entry.getPath(), dto)) {
              entry.setCreated("N");
              entry.setError("Error setting permission");
            } else {
              entry.setCreated("Y");
            }
            //If updating the permission failed, we shall record the error
          }

          if (entry.getCreateBookmark().equalsIgnoreCase("Y")) {
            // Can't check if bookmark exist since the api only checks for invoker's bookmarks
            Path bookmarkPath = Paths.get(entry.getPath());
            String bookmarkName = bookmarkPath.getFileName().toString();

            //Add bookmark and ignore error if already exists.
            createBookmark(entry.getPath(), bookmarkName, entry.getUserId());
          }

          //Save entry
          dmeSyncWorkflowService.savePermissionBookmarkInfo(entry);
        }
      }

    } catch (Exception e) {
      logger.error("[{}] Error occured during permission bookmark task", super.getTaskName(), e);
    }

    return object;
  }

  private boolean createBookmark(String path, String bookmarkName, String userId) {
    //Call bookmark API
    HpcBookmarkRequestDTO dto = new HpcBookmarkRequestDTO();
    dto.setPath(path);
    dto.setUserId(userId);

    final URI bookmarkUrl =
        UriComponentsBuilder.fromHttpUrl(serverUrl)
            .path("/bookmark/{bookmark-name}")
            .buildAndExpand(bookmarkName)
            .encode()
            .toUri();

    HttpHeaders header = new HttpHeaders();
    header.set(HttpHeaders.AUTHORIZATION, "Bearer ".concat(authToken));

    ResponseEntity<Object> response =
        restTemplateFactory
            .getRestTemplate(new RestTemplateResponseErrorHandler())
            .exchange(
                bookmarkUrl, HttpMethod.PUT, new HttpEntity<Object>(dto, header), Object.class);

    if (HttpStatus.CREATED.equals(response.getStatusCode())) {
      logger.debug("Received 201 response");

    } else {
      String json = null;
      HpcExceptionDTO exception = null;
      try {
        json = objectMapper.writeValueAsString(response.getBody());
        exception = objectMapper.readValue(json, HpcExceptionDTO.class);
      } catch (IOException e) {
        logger.error(
            "[PermissionBookmarkTask] Attempt to add bookmark responseCode {}, can't parse message",
            response.getStatusCode());
      }
      logger.info(
          "[PermissionBookmarkTask] Attempt to add bookmark responseCode {}, message {}",
          response.getStatusCode(),
          exception == null? "" : exception.getMessage());
    }
    return true;
  }

  private HpcUserPermissionDTO getPermissionForUser(String path, String userId) {

    //Call acl API
    final URI permissionUrl =
        UriComponentsBuilder.fromHttpUrl(serverUrl)
            .path("/collection/{dme-archive-path}/acl/user/{user-id}")
            .buildAndExpand(path, userId)
            .encode()
            .toUri();

    HttpHeaders header = new HttpHeaders();
    header.set(HttpHeaders.AUTHORIZATION, "Bearer ".concat(authToken));

    ResponseEntity<HpcUserPermissionDTO> response =
        restTemplateFactory
            .getRestTemplate(new RestTemplateResponseErrorHandler())
            .exchange(
                permissionUrl,
                HttpMethod.GET,
                new HttpEntity<Object>(header),
                HpcUserPermissionDTO.class);

    if (HttpStatus.OK.equals(response.getStatusCode())) {
      logger.debug("Received 200 response");

    } else {
      logger.info(
          "[PermissionBookmarkTask] Attempt to get user permission for user {} on path {} responseCode {}",
          userId,
          path,
          response.getStatusCode());
      //Assume that the user has no permissions
      return null;
    }
    return response.getBody();
  }

  private boolean updatePermission(String path, HpcEntityPermissionsDTO dto) {

    //Call acl API
    final URI permissionUrl =
        UriComponentsBuilder.fromHttpUrl(serverUrl)
            .path("/collection/{dme-archive-path}/acl")
            .buildAndExpand(path)
            .encode()
            .toUri();

    HttpHeaders header = new HttpHeaders();
    header.set(HttpHeaders.AUTHORIZATION, "Bearer ".concat(authToken));
    List<MediaType> mediaTypeList = new ArrayList<>();
    mediaTypeList.add(
        new MediaType(
            MediaType.APPLICATION_JSON.getType(),
            MediaType.APPLICATION_JSON.getSubtype(),
            StandardCharsets.UTF_8));
    header.setAccept(mediaTypeList);

    ResponseEntity<Object> response =
        restTemplateFactory
            .getRestTemplate(new RestTemplateResponseErrorHandler())
            .exchange(
                permissionUrl, HttpMethod.POST, new HttpEntity<Object>(dto, header), Object.class);

    if (HttpStatus.OK.equals(response.getStatusCode())) {
      logger.debug("Received 200 response");

    } else {
      String json = null;
      HpcExceptionDTO exception = null;
      try {
        json = objectMapper.writeValueAsString(response.getBody());
        exception = objectMapper.readValue(json, HpcExceptionDTO.class);
      } catch (IOException e) {
        logger.error(
            "[PermissionBookmarkTask] Failed to set permission, responseCode {}, can't parse message",
            response.getStatusCode());
      }
      logger.error(
          "[PermissionBookmarkTask] Failed to set permission, responseCode {}, message {}",
          response.getStatusCode(),
          exception == null? "" : exception.getMessage());
      return false;
    }
    return true;
  }
}
