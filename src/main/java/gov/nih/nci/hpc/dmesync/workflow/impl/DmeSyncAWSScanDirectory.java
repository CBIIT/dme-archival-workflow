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
import gov.nih.nci.hpc.domain.datatransfer.HpcFileLocation;
import gov.nih.nci.hpc.domain.datatransfer.HpcS3Account;
import gov.nih.nci.hpc.domain.datatransfer.HpcS3ScanDirectory;
import gov.nih.nci.hpc.domain.error.HpcErrorType;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcBulkDataObjectRegistrationRequestDTO;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcBulkDataObjectRegistrationResponseDTO;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationItemDTO;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDirectoryScanRegistrationItemDTO;
import gov.nih.nci.hpc.dto.error.HpcExceptionDTO;
import gov.nih.nci.hpc.exception.HpcException;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
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

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class DmeSyncAWSScanDirectory {

	@Value("${hpc.server.url}")
	private String serverUrl;

	@Value("${auth.token}")
	private String authToken;

	@Value("${dmesync.source.aws.bucket}")
	private String awsBucket;

	@Value("${dmesync.source.aws.access.key}")
	private String awsAccessKey;

	@Value("${dmesync.source.aws.secret.key}")
	private String awsSecretKey;

	@Value("${dmesync.source.aws.region}")
	private String awsRegion;

	@Value("${dmesync.exclude.pattern:}")
	private String excludePattern;

	@Value("${dmesync.include.pattern:}")
	private String includePattern;

	@Value("${dmesync.destination.base.dir}")
	protected String destinationBaseDir;

	@Autowired
	private RestTemplateFactory restTemplateFactory;

	@Autowired
	private ObjectMapper objectMapper;

	// ---------------------------------------------------------------------//
	// Constants
	// ---------------------------------------------------------------------//

	// Logger
	private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

	/**
	 * Get attributes of a file/directory from AWS S3 scan
	 * 
	 * @param path The path to check for data objects.
	 * @return The list of HpcPathAttributes
	 * @throws HpcException The exception
	 */
	public List<HpcPathAttributes> getPathAttributes(String path) throws HpcException {
		List<HpcPathAttributes> pathAttributes = new ArrayList<>();

		try {
			logger.debug("getPathAttributes: fileId: {}", path);
			HpcBulkDataObjectRegistrationResponseDTO dto = registerBulkDatafiles(path);

			getPathAttributes(pathAttributes, dto.getDataObjectRegistrationItems());
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new HpcException("Failed to get path attributes: " + path, HpcErrorType.DATA_TRANSFER_ERROR, e);
		}

		return pathAttributes;
	}

	/**
	 * Get attributes of a data object.
	 * 
	 * @param attributes The path attributes list.
	 * @param list       The data object to check.
	 * @throws HpcException The exception
	 */
	private void getPathAttributes(List<HpcPathAttributes> attributes, List<HpcDataObjectRegistrationItemDTO> list) {

		if (list != null) {
			for (HpcDataObjectRegistrationItemDTO dataObject : list) {
				HpcPathAttributes pathAttributes = new HpcPathAttributes();
				pathAttributes.setName(Paths.get(dataObject.getPath()).getFileName().toString());
				pathAttributes.setPath(StringUtils.substring(dataObject.getPath(), destinationBaseDir.length() + 1));
				pathAttributes
						.setAbsolutePath(StringUtils.substring(dataObject.getPath(), destinationBaseDir.length() + 1));
				pathAttributes.setIsDirectory(false);
				attributes.add(pathAttributes);
			}
		}

	}

	/**
	 * registerBulkDatafiles (Scan AWS S3 bucket)
	 * 
	 * @param path The path to scan.
	 * @return HpcBulkDataObjectRegistrationResponseDTO response.
	 * @throws JsonMappingException
	 * @throws JsonParseException
	 * @throws HpcException         The exception
	 * @throws IOException
	 */
	private HpcBulkDataObjectRegistrationResponseDTO registerBulkDatafiles(String path)
			throws JsonParseException, JsonMappingException, IOException {

		HpcBulkDataObjectRegistrationRequestDTO dto = new HpcBulkDataObjectRegistrationRequestDTO();
		dto.setDryRun(true);

		HpcDirectoryScanRegistrationItemDTO item = new HpcDirectoryScanRegistrationItemDTO();
		HpcS3ScanDirectory directory = new HpcS3ScanDirectory();
		HpcFileLocation source = new HpcFileLocation();
		source.setFileContainerId(awsBucket);
		source.setFileId(path);
		directory.setDirectoryLocation(source);

		HpcS3Account s3Account = new HpcS3Account();
		s3Account.setAccessKey(awsAccessKey);
		s3Account.setSecretKey(awsSecretKey);
		s3Account.setRegion(awsRegion);
		directory.setAccount(s3Account);

		item.setS3ScanDirectory(directory);
		item.setBasePath(destinationBaseDir);

		dto.getDirectoryScanRegistrationItems().add(item);
		objectMapper.setSerializationInclusion(Include.NON_NULL);
		String jsonRequestDto = objectMapper.writeValueAsString(dto);

		// Call queryAllDataObjectsInPath API
		HpcBulkDataObjectRegistrationResponseDTO results = null;
		final URI bulkRegistrationURL = UriComponentsBuilder.fromHttpUrl(serverUrl).path("/v2/registration").build()
				.encode().toUri();

		HttpHeaders header = new HttpHeaders();
		header.setContentType(MediaType.APPLICATION_JSON);
		header.set(HttpHeaders.AUTHORIZATION, "Bearer ".concat(authToken));

		ResponseEntity<Object> response = restTemplateFactory.getRestTemplate(new RestTemplateResponseErrorHandler())
				.exchange(bulkRegistrationURL, HttpMethod.PUT, new HttpEntity<Object>(jsonRequestDto, header),
						Object.class);

		if (HttpStatus.OK.equals(response.getStatusCode()) || HttpStatus.CREATED.equals(response.getStatusCode())) {
			logger.debug("[{}] Received 200 response");
			String json = objectMapper.writeValueAsString(response.getBody());
			results = objectMapper.readValue(json, HpcBulkDataObjectRegistrationResponseDTO.class);
		} else {
			String json = null;
			HpcExceptionDTO exception = null;
			try {
				json = objectMapper.writeValueAsString(response.getBody());
				exception = objectMapper.readValue(json, HpcExceptionDTO.class);
			} catch (IOException e) {
				logger.error("Failed to scan AWS S3 bucket, responseCode {}, can't parse message",
						response.getStatusCode());
				throw e;
			}
			logger.error("Failed to scan AWS S3 bucket, responseCode {}, message {}", response.getStatusCode(),
					exception == null ? "" : exception.getMessage());
		}

		return results;

	}

}
