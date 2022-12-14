/*******************************************************************************
 * Copyright SVG, Inc.
 * Copyright Leidos Biomedical Research, Inc.
 *  
 * Distributed under the OSI-approved BSD 3-Clause License.
 * See https://github.com/CBIIT/HPC_DME_APIs/LICENSE.txt for details.
 ******************************************************************************/
package gov.nih.nci.hpc.dmesync.workflow.impl;


import gov.nih.nci.hpc.dmesync.RestTemplateFactory;
import gov.nih.nci.hpc.dmesync.RestTemplateResponseErrorHandler;
import gov.nih.nci.hpc.dto.error.HpcExceptionDTO;
import gov.nih.nci.hpc.exception.HpcException;

import java.io.IOException;
import java.net.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

@Component
public class DmeSyncDeleteDataObject {

	@Value("${hpc.server.url}")
	private String serverUrl;

	@Value("${auth.token}")
	private String authToken;
	
	@Autowired private RestTemplateFactory restTemplateFactory;
	  
	@Autowired private ObjectMapper objectMapper;

	// ---------------------------------------------------------------------//
	// Constants
	// ---------------------------------------------------------------------//

	// Logger
	private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

  /**
   * Delete Data Object
   * 
   * @param dataObjectPath The data object path to delete
   * @return True if success.
   * @throws HpcException The exception
   * @throws IOException 
   */
  public boolean deleteDataObject(String dataObjectPath) throws HpcException, IOException {

	  boolean results = false;
	  
    //Call queryAllDataObjectsInPath API
    final URI dataObjectUrl =
        UriComponentsBuilder.fromHttpUrl(serverUrl)
            .path("/dataObject/".concat(dataObjectPath))
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
                HttpMethod.DELETE,
                new HttpEntity<Object>(header),
                Object.class);

    if (HttpStatus.OK.equals(response.getStatusCode())) {
	  logger.info("Soft delete success for {}", dataObjectPath);
	  results = true;
	} else {
	  String json = objectMapper.writeValueAsString(response.getBody());
	  HpcExceptionDTO errorResponse = objectMapper.readValue(json, HpcExceptionDTO.class);
	  logger.info("Failed to soft delete data object: {}, response: {}", dataObjectPath, errorResponse.getMessage());
	}
    
    return results;
  
  }
}
