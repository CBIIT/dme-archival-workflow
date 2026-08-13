package gov.nih.nci.hpc.dmesync.util;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import gov.nih.nci.hpc.dmesync.RestTemplateFactory;
import gov.nih.nci.hpc.dmesync.RestTemplateResponseErrorHandler;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.domain.metadata.HpcMetadataValidationRule;
import gov.nih.nci.hpc.dto.datamanagement.HpcDataManagementModelDTO;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Metadata Model Builder. Gets Metadata Map from cache if present, else
 * retrieves from the spreadsheet. reloads whenever scheduler runs
 *
 * @author konerum3
 */
@Service
public class DmeMetadataBuilder {

	protected Logger logger = LoggerFactory.getLogger(this.getClass());
	
	@Value("${hpc.server.url}")
	private String serverUrl;

	@Value("${auth.token}")
	private String authToken;

	@Autowired
	private RestTemplateFactory restTemplateFactory;

	@Autowired
	private ObjectMapper objectMapper;

	@Cacheable(value = "metadata", key = "'dmeMetadata'", sync = true)
	public Map<String, Map<String, String>> getMetadataMap(String metadataFile, String key)
			throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {

		logger.info("Parsing the Metadata Spreadsheet and creating Metadata Map");
		return ExcelUtil.parseBulkMetadataEntries(metadataFile, key);
	}

	@CachePut(value = "metadata", key = "'dmeMetadata'")
	public Map<String, Map<String, String>> updateMetadataMap(String metadataFile, String key)
			throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {

		logger.info("Updating the Metadata Spreadsheet and creating Metadata Map");
		return ExcelUtil.parseBulkMetadataEntries(metadataFile, key);
	}

	@Cacheable(value = "metadata", key = "'piMetadata'", sync = true)
	public Map<String, Map<String, String>> getPIMetadataMap(String metadataFile, String key)
			throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {
		logger.info("Parsing the PI Metadata Spreadsheet and creating Map");
		return ExcelUtil.parseBulkMetadataEntries(metadataFile, key);
	}

	@CachePut(value = "metadata", key = "'piMetadata'")
	public Map<String, Map<String, String>> updatePIMetadataMap(String metadataFile, String key)
			throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {
		logger.info("Updating the PI Metadata Spreadsheet and creating Map");
		return ExcelUtil.parseBulkMetadataEntries(metadataFile, key);
	}
	
	@Cacheable(value = "metadata", key = "'dmeMetadataTwoKeys'", sync = true)
	public Map<String, Map<String, String>> getMetadataMapWithTwoKeys(String metadataFile, String key1, String key2)
			throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {

		logger.info("Parsing the Metadata Spreadsheet and creating Metadata Map");
		return ExcelUtil.parseBulkMetadataEntries(metadataFile, key1, key2);
	}

	@CachePut(value = "metadata", key = "'dmeMetadataTwoKeys'")
	public Map<String, Map<String, String>> updateMetadataMapWithTwoKeys(String metadataFile, String key1, String key2)
			throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {

		logger.info("Updating the Metadata Spreadsheet and creating Metadata Map");
		return ExcelUtil.parseBulkMetadataEntries(metadataFile, key1, key2);
	}

	/**
	 * Retrieving metadata model from DME 
	 * @param destinationBaseDir
	 * @return metadataDMEModel
	 * @throws DmeSyncWorkflowException
	 */
	@Cacheable(value = "metadata", key = "'dmeMetadataModel_' + #destinationBaseDir", sync = true)
	public List<HpcMetadataValidationRule> getDMEMetadataModel(String destinationBaseDir)
			throws DmeSyncWorkflowException {

		logger.info("Retrieving metadata model from DME for path {}", destinationBaseDir);
		return fetchMetadataDMEModel(destinationBaseDir);
	}

	/**
	 * Updating metadata model from DME 
	 * @param destinationBaseDir
	 * @return metadataDMEModel
	 * @throws DmeSyncWorkflowException
	 */
	@CachePut(value = "metadata", key = "'dmeMetadataModel_' + #destinationBaseDir")
	public List<HpcMetadataValidationRule> updateDMEMetadataModel(String destinationBaseDir)
			throws DmeSyncWorkflowException {

		logger.info("Updating metadata model from DME for path {}", destinationBaseDir);
		return fetchMetadataDMEModel(destinationBaseDir);
	}
	
	
	@CacheEvict(value = "metadata", allEntries = true)
	public void evictMetadataMap() {
		logger.info("Clearing the cached Metadata Map");

	}
	
	/**
	 * Retrieving metadata model from DME for construction of metadata entries
	 * @param destinationBaseDir
	 * @return
	 * @throws DmeSyncWorkflowException
	 */
	private List<HpcMetadataValidationRule> fetchMetadataDMEModel(String destinationBaseDir)
			throws DmeSyncWorkflowException {
		try {
			final URI dataObjectUrl = UriComponentsBuilder.fromHttpUrl(serverUrl)
					.path("/dm/model/".concat(destinationBaseDir))
					.build()
					.encode()
					.toUri();

			HttpHeaders header = new HttpHeaders();
			header.setContentType(MediaType.MULTIPART_FORM_DATA);
			header.set(HttpHeaders.AUTHORIZATION, "Bearer ".concat(authToken));

			ResponseEntity<Object> response = restTemplateFactory
					.getRestTemplate(new RestTemplateResponseErrorHandler())
					.exchange(dataObjectUrl, HttpMethod.GET, new HttpEntity<Object>(header), Object.class);

			if (!HttpStatus.OK.equals(response.getStatusCode())) {
				logger.error("Received bad response from metadata dataObject, responseCode {}", response.getStatusCode());
				throw new DmeSyncWorkflowException("Received bad response from metadata dataObject");
			}

			String json = objectMapper.writeValueAsString(response.getBody());
			HpcDataManagementModelDTO dataObjectListDTO =
					objectMapper.readValue(json, HpcDataManagementModelDTO.class);

			if (dataObjectListDTO.getDocRules() == null
 					|| dataObjectListDTO.getDocRules().isEmpty()
 					|| dataObjectListDTO.getDocRules().get(0).getRules() == null
 					|| dataObjectListDTO.getDocRules().get(0).getRules().isEmpty()
 					|| dataObjectListDTO.getDocRules().get(0).getRules().get(0).getCollectionMetadataValidationRules() == null) {
 				throw new DmeSyncWorkflowException(
 						"Metadata model response missing collection metadata validation rules for path " + destinationBaseDir);
 			}
			
			return dataObjectListDTO.getDocRules().get(0).getRules().get(0)
					.getCollectionMetadataValidationRules()
					.stream()
					.collect(Collectors.toList());

		} catch (DmeSyncWorkflowException e) {
			throw e;
		} catch (Exception e) {
			logger.error("Error retrieving metadata model from DME", e);
			throw new DmeSyncWorkflowException("Error retrieving metadata model from DME", e);
		}
	}

	

}
