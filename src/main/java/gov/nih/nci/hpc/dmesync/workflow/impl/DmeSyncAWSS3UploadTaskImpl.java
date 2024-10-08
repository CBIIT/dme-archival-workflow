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
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncTask;
import gov.nih.nci.hpc.domain.datatransfer.HpcFileLocation;
import gov.nih.nci.hpc.domain.datatransfer.HpcS3Account;
import gov.nih.nci.hpc.domain.datatransfer.HpcStreamingUploadSource;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;

/**
 * DME Sync AWS S3 Upload Task Implementation
 * 
 * @author dinhys
 */
@Component
public class DmeSyncAWSS3UploadTaskImpl extends AbstractDmeSyncTask implements DmeSyncTask {

	@Autowired
	private RestTemplateFactory restTemplateFactory;
	@Autowired
	private ObjectMapper objectMapper;

	@Value("${hpc.server.url}")
	private String serverUrl;

	@Value("${auth.token}")
	private String authToken;

	@Value("${dmesync.source.aws.bucket:}")
	private String awsBucket;

	@Value("${dmesync.source.aws.access.key:}")
	private String awsAccessKey;

	@Value("${dmesync.source.aws.secret.key:}")
	private String awsSecretKey;

	@Value("${dmesync.source.aws.region:}")
	private String awsRegion;

	@PostConstruct
	public boolean init() {
		super.setTaskName("UploadTask");
		return true;
	}

	@Override
	public StatusInfo process(StatusInfo object) throws DmeSyncMappingException, DmeSyncWorkflowException {

		object.setUploadStartTimestamp(new Date());

		try {
			// Call dataObjectRegistration API
			final URI dataObjectUrl = UriComponentsBuilder.fromHttpUrl(serverUrl)
					.path("/v2/dataObject".concat(object.getFullDestinationPath())).build().encode().toUri();

			HttpHeaders header = new HttpHeaders();
			header.setContentType(MediaType.MULTIPART_FORM_DATA);
			header.set(HttpHeaders.AUTHORIZATION, "Bearer ".concat(authToken));

			MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();

			// creating an HttpEntity for dataObjectRegistration
			HttpHeaders jsonHeader = new HttpHeaders();
			jsonHeader.setContentType(MediaType.APPLICATION_JSON);
			objectMapper.setSerializationInclusion(Include.NON_NULL);
			HpcDataObjectRegistrationRequestDTO requestDto = object.getDataObjectRegistrationRequestDTO();

			HpcFileLocation source = new HpcFileLocation();
			source.setFileContainerId(awsBucket);
			source.setFileId(object.getSourceFilePath());
			HpcStreamingUploadSource s3UploadSource = new HpcStreamingUploadSource();
			HpcS3Account s3Account = new HpcS3Account();
			s3Account.setAccessKey(awsAccessKey);
			s3Account.setSecretKey(awsSecretKey);
			s3Account.setRegion(awsRegion);
			s3UploadSource.setAccount(s3Account);
			s3UploadSource.setSourceLocation(source);
			requestDto.setS3UploadSource(s3UploadSource);
			requestDto.setCreateParentCollections(true);
			requestDto.setGenerateUploadRequestURL(false);
			String jsonRequestDto = objectMapper.writeValueAsString(requestDto);
			HttpEntity<String> jsonHttpEntity = new HttpEntity<>(jsonRequestDto, jsonHeader);
			body.add("dataObjectRegistration", jsonHttpEntity);

			ResponseEntity<String> serviceResponse = restTemplateFactory.getRestTemplate().exchange(dataObjectUrl,
					HttpMethod.PUT, new HttpEntity<Object>(body, header), String.class);

			if (HttpStatus.OK.equals(serviceResponse.getStatusCode())
					|| HttpStatus.CREATED.equals(serviceResponse.getStatusCode())) {
				logger.info("[{}] File upload successful", super.getTaskName());
				object = dmeSyncWorkflowService.getService(access).saveStatusInfo(object);
			} else {
				logger.error("[{}] Upload failed with responseCode {}", super.getTaskName(),
						serviceResponse.getStatusCode());
				throw new DmeSyncWorkflowException(
						"Upload failed with responseCode " + serviceResponse.getStatusCode());
			}
		} catch (Exception e) {
			logger.error("[{}] error occured during upload task", super.getTaskName(), e);
			throw new DmeSyncWorkflowException("Error occured during upload", e);
		}

		return object;
	}
}
