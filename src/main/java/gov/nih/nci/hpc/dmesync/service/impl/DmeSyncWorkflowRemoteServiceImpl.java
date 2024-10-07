package gov.nih.nci.hpc.dmesync.service.impl;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.util.UriComponentsBuilder;

import gov.nih.nci.hpc.dmesync.RestTemplateFactory;
import gov.nih.nci.hpc.dmesync.RestTemplateResponseErrorHandler;
import gov.nih.nci.hpc.dmesync.domain.CollectionNameMapping;
import gov.nih.nci.hpc.dmesync.domain.MetadataInfo;
import gov.nih.nci.hpc.dmesync.domain.MetadataMapping;
import gov.nih.nci.hpc.dmesync.domain.PermissionBookmarkInfo;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.domain.TaskInfo;
import gov.nih.nci.hpc.dmesync.service.DmeSyncWorkflowService;

@Service("remote")
public class DmeSyncWorkflowRemoteServiceImpl implements DmeSyncWorkflowService {

	@Value("${dmesync.db.access.remoteUrl}")
	String serverUrl;
	@Autowired
	private RestTemplateFactory restTemplateFactory;

	@Override
	public void completeWorkflow(StatusInfo statusInfo) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl).path("/api/completeWorkflow").build().encode()
				.toUri();
		final HttpEntity<StatusInfo> entity = new HttpEntity<>(statusInfo);
		restTemplateFactory.getRestTemplate(new RestTemplateResponseErrorHandler()).exchange(finalUrl, HttpMethod.POST,
				entity, Object.class);
	}

	@Override
	public void retryWorkflow(StatusInfo statusInfo, Exception e) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl).path("/api/retryWorkflow").build().encode()
				.toUri();
		final HttpEntity<StatusInfo> entity = new HttpEntity<>(statusInfo);
		restTemplateFactory.getRestTemplate(new RestTemplateResponseErrorHandler()).exchange(finalUrl, HttpMethod.POST,
				entity, Object.class);
	}

	@Override
	public void recordError(StatusInfo statusInfo) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl).path("/api/recordError").build().encode()
				.toUri();
		final HttpEntity<StatusInfo> entity = new HttpEntity<>(statusInfo);
		restTemplateFactory.getRestTemplate(new RestTemplateResponseErrorHandler()).exchange(finalUrl, HttpMethod.POST,
				entity, Object.class);
	}

	@Override
	public StatusInfo findFirstStatusInfoByOriginalFilePathAndStatus(String originalFilePath, String status) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl)
				.path("/api/findFirstStatusInfoByOriginalFilePathAndStatus")
				.queryParam("originalFilePath", originalFilePath).queryParam("status", status).build().encode().toUri();
		return restTemplateFactory.getRestTemplate(new RestTemplateResponseErrorHandler()).getForObject(finalUrl,
				StatusInfo.class);
	}

	@Override
	public List<StatusInfo> findAllStatusInfoByOriginalFilePathAndStatus(String originalFilePath, String status) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl)
				.path("/api/findAllStatusInfoByOriginalFilePathAndStatus")
				.queryParam("originalFilePath", originalFilePath).queryParam("status", status).build().encode().toUri();
		ResponseEntity<StatusInfo[]> response = restTemplateFactory
				.getRestTemplate(new RestTemplateResponseErrorHandler()).getForEntity(finalUrl, StatusInfo[].class);
		StatusInfo[] statusInfoArray = response.getBody();
		return new ArrayList<>(Arrays.asList(statusInfoArray));
	}

	@Override
    public List<StatusInfo> findAllStatusInfoLikeOriginalFilePath(String originalFilePath) {
        final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl)
                .path("/api/findAllStatusInfoLikeOriginalFilePath")
                .queryParam("originalFilePath", originalFilePath).build().encode().toUri();
        ResponseEntity<StatusInfo[]> response = restTemplateFactory
                .getRestTemplate(new RestTemplateResponseErrorHandler()).getForEntity(finalUrl, StatusInfo[].class);
        StatusInfo[] statusInfoArray = response.getBody();
        return new ArrayList<>(Arrays.asList(statusInfoArray));
    }
	
	
	@Override
    public List<StatusInfo> findAllByDocAndRunIdAndLikeOriginalFilePath(String doc, String runId, String originalFilePath) {
        final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl)
                .path("/api/findAllByDocAndRunIdAndLikeOriginalFilePath")
                .queryParam("doc", doc).queryParam("runId", runId).queryParam("originalFilePath", originalFilePath).build().encode().toUri();
        ResponseEntity<StatusInfo[]> response = restTemplateFactory
                .getRestTemplate(new RestTemplateResponseErrorHandler()).getForEntity(finalUrl, StatusInfo[].class);
        StatusInfo[] statusInfoArray = response.getBody();
        return new ArrayList<>(Arrays.asList(statusInfoArray));
    }
	
	 @Override
	  public List<StatusInfo> findAllByDocAndLikeOriginalFilePath(String doc,String originalFilePath) {
		 final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl)
	                .path("/api/findAllByDocAndLikeOriginalFilePath")
	                .queryParam("doc", doc).queryParam("originalFilePath", originalFilePath).build().encode().toUri();
	        ResponseEntity<StatusInfo[]> response = restTemplateFactory
	                .getRestTemplate(new RestTemplateResponseErrorHandler()).getForEntity(finalUrl, StatusInfo[].class);
	        StatusInfo[] statusInfoArray = response.getBody();
	        return new ArrayList<>(Arrays.asList(statusInfoArray));
	 }
	
	@Override
	public List<StatusInfo> findStatusInfoByRunIdAndDoc(String runId, String doc) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl).path("/api/findStatusInfoByRunIdAndDoc")
				.queryParam("runId", runId).queryParam("doc", doc).build().encode().toUri();
		ResponseEntity<StatusInfo[]> response = restTemplateFactory
				.getRestTemplate(new RestTemplateResponseErrorHandler()).getForEntity(finalUrl, StatusInfo[].class);
		StatusInfo[] statusInfoArray = response.getBody();
		return new ArrayList<>(Arrays.asList(statusInfoArray));
	}

	@Override
	public StatusInfo findFirstStatusInfoByOriginalFilePathAndSourceFileNameAndStatus(String originalFilePath,
			String sourceFileName, String status) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl)
				.path("/api/findFirstStatusInfoByOriginalFilePathAndSourceFileNameAndStatus")
				.queryParam("originalFilePath", originalFilePath).queryParam("sourceFileName", sourceFileName)
				.queryParam("status", status).build().encode().toUri();
		return restTemplateFactory.getRestTemplate(new RestTemplateResponseErrorHandler()).getForObject(finalUrl,
				StatusInfo.class);
	}

	@Override
	public StatusInfo findTopStatusInfoByDocAndOriginalFilePathStartsWithOrderByStartTimestampDesc(String doc,
			String baseDir) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl)
				.path("/api/findTopStatusInfoByDocAndOriginalFilePathStartsWithOrderByStartTimestampDesc")
				.queryParam("doc", doc).queryParam("baseDir", baseDir).build().encode().toUri();
		return restTemplateFactory.getRestTemplate(new RestTemplateResponseErrorHandler()).getForObject(finalUrl,
				StatusInfo.class);
	}

	@Override
	public List<StatusInfo> findAllStatusInfoByOriginalFilePathAndStatusAndRunId(String originalFilePath, String status,
			String runId) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl)
				.path("/api/findAllStatusInfoByOriginalFilePathAndStatusAndRunId")
				.queryParam("originalFilePath", originalFilePath).queryParam("status", status)
				.queryParam("runId", runId).build().encode().toUri();
		ResponseEntity<StatusInfo[]> response = restTemplateFactory
				.getRestTemplate(new RestTemplateResponseErrorHandler()).getForEntity(finalUrl, StatusInfo[].class);
		StatusInfo[] statusInfoArray = response.getBody();
		return new ArrayList<>(Arrays.asList(statusInfoArray));
	}

	@Override
	public CollectionNameMapping findCollectionNameMappingByMapKeyAndCollectionTypeAndDoc(String key,
			String collectionType, String doc) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl)
				.path("/api/findCollectionNameMappingByMapKeyAndCollectionTypeAndDoc").queryParam("key", key)
				.queryParam("collectionType", collectionType).queryParam("doc", doc).build().encode().toUri();
		return restTemplateFactory.getRestTemplate(new RestTemplateResponseErrorHandler()).getForObject(finalUrl,
				CollectionNameMapping.class);
	}

	@Override
	public List<MetadataMapping> findAllMetadataMappingByCollectionTypeAndCollectionNameAndDoc(String collectionType,
			String collectionName, String doc) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl)
				.path("/api/findAllMetadataMappingByCollectionTypeAndCollectionNameAndDoc")
				.queryParam("collectionType", collectionType).queryParam("collectionName", collectionName)
				.queryParam("doc", doc).build().encode().toUri();
		ResponseEntity<MetadataMapping[]> response = restTemplateFactory
				.getRestTemplate(new RestTemplateResponseErrorHandler())
				.getForEntity(finalUrl, MetadataMapping[].class);
		MetadataMapping[] metadataMappingArray = response.getBody();
		return new ArrayList<>(Arrays.asList(metadataMappingArray));
	}

	@Override
	public MetadataMapping findByMetadataMappingByCollectionTypeAndCollectionNameAndMetaDataKeyAndDoc(
			String collectionType, String collectionName, String metaDataKey, String doc) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl)
				.path("/api/findByMetadataMappingByCollectionTypeAndCollectionNameAndMetaDataKeyAndDoc")
				.queryParam("collectionType", collectionType).queryParam("collectionName", collectionName)
				.queryParam("metaDataKey", metaDataKey).queryParam("doc", doc).build().encode().toUri();
		return restTemplateFactory.getRestTemplate(new RestTemplateResponseErrorHandler()).getForObject(finalUrl,
				MetadataMapping.class);
	}

	@Override
	public List<PermissionBookmarkInfo> findAllPermissionBookmarkInfoByCreated(String flag) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl)
				.path("/api/findAllPermissionBookmarkInfoByCreated").queryParam("flag", flag).build().encode().toUri();
		ResponseEntity<PermissionBookmarkInfo[]> response = restTemplateFactory
				.getRestTemplate(new RestTemplateResponseErrorHandler())
				.getForEntity(finalUrl, PermissionBookmarkInfo[].class);
		PermissionBookmarkInfo[] permissionBookmarkInfoArray = response.getBody();
		return new ArrayList<>(Arrays.asList(permissionBookmarkInfoArray));
	}

	@Override
	public void deleteMetadataInfoByObjectId(Long objectId) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl).path("/api/deleteMetadataInfoByObjectId")
				.build().encode().toUri();
		final HttpEntity<Long> entity = new HttpEntity<>(objectId);
		restTemplateFactory.getRestTemplate(new RestTemplateResponseErrorHandler()).exchange(finalUrl, HttpMethod.POST,
				entity, Object.class);
	}

	@Override
	public List<MetadataInfo> findAllMetadataInfoByRunIdAndDoc(String runId, String doc) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl).path("/api/findAllMetadataInfoByRunIdAndDoc")
				.queryParam("runId", runId).queryParam("doc", doc).build().encode().toUri();
		ResponseEntity<MetadataInfo[]> response = restTemplateFactory
				.getRestTemplate(new RestTemplateResponseErrorHandler()).getForEntity(finalUrl, MetadataInfo[].class);
		MetadataInfo[] metadataInfoArray = response.getBody();
		return new ArrayList<>(Arrays.asList(metadataInfoArray));
	}

	@Override
	public List<MetadataInfo> findAllMetadataInfoByRunIdAndMetaDataKeyAndDoc(String runId, String key, String doc) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl)
				.path("/api/findAllMetadataInfoByRunIdAndMetaDataKeyAndDoc").queryParam("runId", runId)
				.queryParam("key", key).queryParam("doc", doc).build().encode().toUri();
		ResponseEntity<MetadataInfo[]> response = restTemplateFactory
				.getRestTemplate(new RestTemplateResponseErrorHandler()).getForEntity(finalUrl, MetadataInfo[].class);
		MetadataInfo[] metadataInfoArray = response.getBody();
		return new ArrayList<>(Arrays.asList(metadataInfoArray));
	}

	@Override
	public Optional<StatusInfo> findStatusInfoById(Long objectId) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl).path("/api/findStatusInfoById")
				.queryParam("objectId", objectId).build().encode().toUri();
		StatusInfo statusInfo = restTemplateFactory.getRestTemplate(new RestTemplateResponseErrorHandler()).getForObject(finalUrl,
				StatusInfo.class);
		return Optional.ofNullable(statusInfo);
	}

	@Override
	public StatusInfo saveStatusInfo(StatusInfo statusInfo) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl).path("/api/saveStatusInfo").build().encode()
				.toUri();
		final HttpEntity<StatusInfo> entity = new HttpEntity<>(statusInfo);
		return restTemplateFactory.getRestTemplate(new RestTemplateResponseErrorHandler()).postForObject(finalUrl,
				entity, StatusInfo.class);
	}

	@Override
	public TaskInfo findFirstTaskInfoByObjectIdAndTaskName(Long id, String taskName) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl)
				.path("/api/findFirstTaskInfoByObjectIdAndTaskName").queryParam("id", id)
				.queryParam("taskName", taskName).build().encode().toUri();
		return restTemplateFactory.getRestTemplate(new RestTemplateResponseErrorHandler()).getForObject(finalUrl,
				TaskInfo.class);
	}

	@Override
	public void deleteTaskInfoByObjectId(Long objectId) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl).path("/api/deleteTaskInfoByObjectId").build()
				.encode().toUri();
		final HttpEntity<Long> entity = new HttpEntity<>(objectId);
		restTemplateFactory.getRestTemplate(new RestTemplateResponseErrorHandler()).exchange(finalUrl, HttpMethod.POST,
				entity, Object.class);
	}

	@Override
	public void saveTaskInfo(TaskInfo task) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl).path("/api/saveTaskInfo").build().encode()
				.toUri();
		final HttpEntity<TaskInfo> entity = new HttpEntity<>(task);
		restTemplateFactory.getRestTemplate(new RestTemplateResponseErrorHandler()).exchange(finalUrl, HttpMethod.POST,
				entity, Object.class);
	}

	@Override
	public void saveMetadataInfo(MetadataInfo metadataInfo) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl).path("/api/saveMetadataInfo").build().encode()
				.toUri();
		final HttpEntity<MetadataInfo> entity = new HttpEntity<>(metadataInfo);
		restTemplateFactory.getRestTemplate(new RestTemplateResponseErrorHandler()).exchange(finalUrl, HttpMethod.POST,
				entity, Object.class);
	}

	@Override
	public void savePermissionBookmarkInfo(PermissionBookmarkInfo entry) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl).path("/api/savePermissionBookmarkInfo").build()
				.encode().toUri();
		final HttpEntity<PermissionBookmarkInfo> entity = new HttpEntity<>(entry);
		restTemplateFactory.getRestTemplate(new RestTemplateResponseErrorHandler()).exchange(finalUrl, HttpMethod.POST,
				entity, Object.class);
	}

	@Override
	public StatusInfo findFirstStatusInfoByOriginalFilePathOrderByStartTimestampDesc(String originalFilePath) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl)
				.path("/api/findFirstStatusInfoByOriginalFilePathOrderByStartTimestampDesc")
				.queryParam("originalFilePath", originalFilePath).build().encode().toUri();
		return restTemplateFactory.getRestTemplate(new RestTemplateResponseErrorHandler()).getForObject(finalUrl,
				StatusInfo.class);
	}

	@Override
	public StatusInfo findTopStatusInfoByDocOrderByStartTimestampDesc(String doc) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl)
				.path("/api/findTopStatusInfoByDocOrderByStartTimestampDesc")
				.queryParam("doc", doc).build().encode().toUri();
		return restTemplateFactory.getRestTemplate(new RestTemplateResponseErrorHandler()).getForObject(finalUrl,
				StatusInfo.class);
	}

	@Override
	  public StatusInfo findTopStatusInfoByDocAndSourceFilePath(String doc, String sourceFilePath) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl)
				.path("/api/findTopStatusInfoByDocAndSourceFilePath")
				.queryParam("doc", doc).queryParam("baseDir", sourceFilePath).build().encode().toUri();
		return restTemplateFactory.getRestTemplate(new RestTemplateResponseErrorHandler()).getForObject(finalUrl,
				StatusInfo.class);	  }
	
	
	@Override
	  public StatusInfo findTopBySourceFileNameAndRunId(String sourceFileName, String runId) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl)
				.path("/api/findTopBySourceFilePathAndRunId")
				.queryParam("sourceFileName", sourceFileName).queryParam("runId", runId).build().encode().toUri();
		return restTemplateFactory.getRestTemplate(new RestTemplateResponseErrorHandler()).getForObject(finalUrl,
				StatusInfo.class);	  }
	@Override
	  public StatusInfo findTopByDocAndSourceFilePathAndRunId(String doc,String sourceFilePath, String runId) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl)
				.path("/api/findTopStatusInfoByDocAndSourceFilePathAndRunId")
				.queryParam("sourceFilePath", sourceFilePath).queryParam("doc", doc).queryParam("runId", runId).build().encode().toUri();
		return restTemplateFactory.getRestTemplate(new RestTemplateResponseErrorHandler()).getForObject(finalUrl,
				StatusInfo.class);	  }
	
	@Override
	public List<StatusInfo> findStatusInfoByDocAndStatus(String doc, String status) {
		final URI finalUrl = UriComponentsBuilder.fromHttpUrl(serverUrl)
				.path("/api/findStatusInfoByDocAndStatus")
				.queryParam("doc", doc).queryParam("status", status).build().encode().toUri();
		ResponseEntity<StatusInfo[]> response = restTemplateFactory
				.getRestTemplate(new RestTemplateResponseErrorHandler()).getForEntity(finalUrl, StatusInfo[].class);
		StatusInfo[] statusInfoArray = response.getBody();
		return new ArrayList<>(Arrays.asList(statusInfoArray));
	}
}
