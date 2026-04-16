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
import gov.nih.nci.hpc.dmesync.util.HpcPathAttributes;
import gov.nih.nci.hpc.domain.error.HpcErrorType;
import gov.nih.nci.hpc.dto.datamanagement.HpcDataObjectListDTO;
import gov.nih.nci.hpc.dto.datamanagement.HpcDataObjectDTO;
import gov.nih.nci.hpc.exception.HpcException;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
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
public class DmeSyncDataObjectListQuery {

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
	 * Get attributes of a file/directory.
	 * 
	 * @param collectionPath The collection/path to check for data objects.
	 * @return The list of HpcPathAttributes
	 * @throws HpcException The exception
	 */
	public List<HpcPathAttributes> getPathAttributes(String collectionPath) throws HpcException {
		List<HpcPathAttributes> pathAttributes = new ArrayList<>();

		try {
		    logger.debug("getPathAttributes: fileLocation: {}", collectionPath);
		    List<HpcDataObjectDTO> dataObjects = listDataObjects(collectionPath);
			getPathAttributes(pathAttributes, dataObjects);
		} catch (Exception e) {
		  logger.error(e.getMessage(), e);
			throw new HpcException("Failed to get path attributes: " + collectionPath,
					HpcErrorType.DATA_TRANSFER_ERROR, e);
		}

		return pathAttributes;
	}

	/**
	 * Get attributes of a data object.
	 * 
	 * @param attributes The path attributes list.
	 * @param dataObjects The data object to check.
	 * @throws HpcException The exception
	 */
	private void getPathAttributes(List<HpcPathAttributes> attributes, List<HpcDataObjectDTO> dataObjects) {

		if (dataObjects != null) {
			for (HpcDataObjectDTO dataObject : dataObjects) {
				HpcPathAttributes pathAttributes = new HpcPathAttributes();
				pathAttributes.setName(Paths.get(dataObject.getDataObject().getAbsolutePath()).getFileName().toString());
				pathAttributes.setPath(dataObject.getDataObject().getAbsolutePath());
				pathAttributes.setUpdatedDate(dataObject.getDataObject().getCreatedAt().getTime());
				pathAttributes.setAbsolutePath(dataObject.getDataObject().getAbsolutePath());
				pathAttributes.setIsDirectory(false);
				attributes.add(pathAttributes);
			}
		}

	}

  /**
   * List Data Objects
   * 
   * @param collectionPath The collection path to list data objects under.
   * @return List of HpcDataObjectDTO.
   * @throws HpcException The exception
   * @throws IOException 
   */
  public List<HpcDataObjectDTO> listDataObjects(String collectionPath) throws HpcException, IOException {

	  List<HpcDataObjectDTO> results = new ArrayList<>();
	  
    //Call queryAllDataObjectsInPath API
    final URI dataObjectUrl =
        UriComponentsBuilder.fromHttpUrl(serverUrl)
            .path("/dataObject/query/all/".concat(collectionPath))
            .queryParam("page", 1)
            .queryParam("pageSize", 10000)
            .queryParam("totalCount", Boolean.TRUE)
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
	  logger.debug("[{}] Received 200 response");
	      String json = objectMapper.writeValueAsString(response.getBody());
	      HpcDataObjectListDTO dataObjectListDTO = objectMapper.readValue(json, HpcDataObjectListDTO.class);
	
	      results = dataObjectListDTO.getDataObjects();
	}
    
    return results;
  
  }
}
