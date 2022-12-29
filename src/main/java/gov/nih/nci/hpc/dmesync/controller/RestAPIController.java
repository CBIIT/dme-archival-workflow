package gov.nih.nci.hpc.dmesync.controller;

import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import gov.nih.nci.hpc.dmesync.DmeSyncWorkflowServiceFactory;
import gov.nih.nci.hpc.dmesync.domain.CollectionNameMapping;
import gov.nih.nci.hpc.dmesync.domain.MetadataInfo;
import gov.nih.nci.hpc.dmesync.domain.MetadataMapping;
import gov.nih.nci.hpc.dmesync.domain.PermissionBookmarkInfo;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.domain.TaskInfo;

@RestController
@RequestMapping(value = "/api")
public class RestAPIController {

	@Autowired
	private DmeSyncWorkflowServiceFactory dmeSyncWorkflowService;

	@PostMapping(value = "/completeWorkflow")
	public ResponseEntity<?> completeWorkflow(@RequestBody StatusInfo statusInfo) {
		dmeSyncWorkflowService.getService("local").completeWorkflow(statusInfo);
		return new ResponseEntity<>(HttpStatus.OK);
	}

	@PostMapping(value = "/retryWorkflow")
	public ResponseEntity<?> retryWorkflow(@RequestBody StatusInfo statusInfo, @RequestBody Exception e) {
		dmeSyncWorkflowService.getService("local").retryWorkflow(statusInfo, e);
		return new ResponseEntity<>(HttpStatus.OK);
	}

	@PostMapping(value = "/recordError")
	public ResponseEntity<?> recordError(@RequestBody StatusInfo statusInfo) {
		dmeSyncWorkflowService.getService("local").recordError(statusInfo);
		return new ResponseEntity<>(HttpStatus.OK);
	}

	@GetMapping(value = "/findFirstStatusInfoByOriginalFilePathAndStatus")
	public StatusInfo findFirstStatusInfoByOriginalFilePathAndStatus(
			@RequestParam(required = true) String originalFilePath, @RequestParam(required = true) String status) {
		return dmeSyncWorkflowService.getService("local")
				.findFirstStatusInfoByOriginalFilePathAndStatus(originalFilePath, status);
	}

	@GetMapping(value = "/findAllStatusInfoByOriginalFilePathAndStatus")
	public List<StatusInfo> findAllStatusInfoByOriginalFilePathAndStatus(
			@RequestParam(required = true) String originalFilePath, @RequestParam(required = true) String status) {
		return dmeSyncWorkflowService.getService("local").findAllStatusInfoByOriginalFilePathAndStatus(originalFilePath,
				status);
	}

	@GetMapping(value = "/findStatusInfoByRunIdAndDoc")
	public List<StatusInfo> findStatusInfoByRunIdAndDoc(@RequestParam(required = true) String runId,
			@RequestParam(required = true) String doc) {
		return dmeSyncWorkflowService.getService("local").findStatusInfoByRunIdAndDoc(runId, doc);
	}

	@GetMapping(value = "/findFirstStatusInfoByOriginalFilePathAndSourceFileNameAndStatus")
	public StatusInfo findFirstStatusInfoByOriginalFilePathAndSourceFileNameAndStatus(
			@RequestParam(required = true) String originalFilePath,
			@RequestParam(required = true) String sourceFileName, @RequestParam(required = true) String status) {
		return dmeSyncWorkflowService.getService("local")
				.findFirstStatusInfoByOriginalFilePathAndSourceFileNameAndStatus(originalFilePath, sourceFileName,
						status);
	}

	@GetMapping(value = "/findTopStatusInfoByDocAndOriginalFilePathStartsWithOrderByStartTimestampDesc")
	public StatusInfo findTopStatusInfoByDocAndOriginalFilePathStartsWithOrderByStartTimestampDesc(
			@RequestParam(required = true) String doc, @RequestParam(required = true) String baseDir) {
		return dmeSyncWorkflowService.getService("local")
				.findTopStatusInfoByDocAndOriginalFilePathStartsWithOrderByStartTimestampDesc(doc, baseDir);
	}

	@GetMapping(value = "/findAllStatusInfoByOriginalFilePathAndStatusAndRunId")
	public List<StatusInfo> findAllStatusInfoByOriginalFilePathAndStatusAndRunId(
			@RequestParam(required = true) String originalFilePath, @RequestParam(required = true) String status,
			@RequestParam(required = true) String runId) {
		return dmeSyncWorkflowService.getService("local")
				.findAllStatusInfoByOriginalFilePathAndStatusAndRunId(originalFilePath, status, runId);
	}

	@GetMapping(value = "/findCollectionNameMappingByMapKeyAndCollectionTypeAndDoc")
	public CollectionNameMapping findCollectionNameMappingByMapKeyAndCollectionTypeAndDoc(
			@RequestParam(required = true) String key, @RequestParam(required = true) String collectionType,
			@RequestParam(required = true) String doc) {
		return dmeSyncWorkflowService.getService("local").findCollectionNameMappingByMapKeyAndCollectionTypeAndDoc(key,
				collectionType, doc);
	}

	@GetMapping(value = "/findAllMetadataMappingByCollectionTypeAndCollectionNameAndDoc")
	public List<MetadataMapping> findAllMetadataMappingByCollectionTypeAndCollectionNameAndDoc(
			@RequestParam(required = true) String collectionType, @RequestParam(required = true) String collectionName,
			@RequestParam(required = true) String doc) {
		return dmeSyncWorkflowService.getService("local")
				.findAllMetadataMappingByCollectionTypeAndCollectionNameAndDoc(collectionType, collectionName, doc);
	}

	@GetMapping(value = "/findByMetadataMappingByCollectionTypeAndCollectionNameAndMetaDataKeyAndDoc")
	public MetadataMapping findByMetadataMappingByCollectionTypeAndCollectionNameAndMetaDataKeyAndDoc(
			@RequestParam(required = true) String collectionType, @RequestParam(required = true) String collectionName,
			@RequestParam(required = true) String metaDataKey, @RequestParam(required = true) String doc) {
		return dmeSyncWorkflowService.getService("local")
				.findByMetadataMappingByCollectionTypeAndCollectionNameAndMetaDataKeyAndDoc(collectionType,
						collectionName, metaDataKey, doc);
	}

	@GetMapping(value = "/findAllPermissionBookmarkInfoByCreated")
	public List<PermissionBookmarkInfo> findAllPermissionBookmarkInfoByCreated(
			@RequestParam(required = true) String flag) {
		return dmeSyncWorkflowService.getService("local").findAllPermissionBookmarkInfoByCreated(flag);
	}

	@PostMapping(value = "/deleteMetadataInfoByObjectId")
	public ResponseEntity<?> deleteMetadataInfoByObjectId(@RequestParam(required = true) Long objectId) {
		dmeSyncWorkflowService.getService("local").deleteMetadataInfoByObjectId(objectId);
		return new ResponseEntity<>(HttpStatus.OK);
	}

	@GetMapping(value = "/findAllMetadataInfoByRunIdAndDoc")
	public List<MetadataInfo> findAllMetadataInfoByRunIdAndDoc(@RequestParam(required = true) String runId,
			@RequestParam(required = true) String doc) {
		return dmeSyncWorkflowService.getService("local").findAllMetadataInfoByRunIdAndDoc(runId, doc);
	}

	@GetMapping(value = "/findAllMetadataInfoByRunIdAndMetaDataKeyAndDoc")
	public List<MetadataInfo> findAllMetadataInfoByRunIdAndMetaDataKeyAndDoc(
			@RequestParam(required = true) String runId, @RequestParam(required = true) String key,
			@RequestParam(required = true) String doc) {
		return dmeSyncWorkflowService.getService("local").findAllMetadataInfoByRunIdAndMetaDataKeyAndDoc(runId, key,
				doc);
	}

	@GetMapping(value = "/findStatusInfoById")
	public StatusInfo findStatusInfoById(@RequestParam(required = true) Long objectId) {
		return dmeSyncWorkflowService.getService("local").findStatusInfoById(objectId).get();
	}

	@PostMapping(value = "/saveStatusInfo")
	public StatusInfo saveStatusInfo(@RequestBody StatusInfo statusInfo) {
		return dmeSyncWorkflowService.getService("local").saveStatusInfo(statusInfo);
	}

	@GetMapping(value = "/findFirstTaskInfoByObjectIdAndTaskName")
	public TaskInfo findFirstTaskInfoByObjectIdAndTaskName(@RequestParam(required = true) Long id,
			@RequestParam(required = true) String taskName) {
		return dmeSyncWorkflowService.getService("local").findFirstTaskInfoByObjectIdAndTaskName(id, taskName);
	}

	@PostMapping(value = "/deleteTaskInfoByObjectId")
	public ResponseEntity<?> deleteTaskInfoByObjectId(@RequestParam(required = true) Long objectId) {
		dmeSyncWorkflowService.getService("local").deleteTaskInfoByObjectId(objectId);
		return new ResponseEntity<>(HttpStatus.OK);
	}

	@PostMapping(value = "/saveTaskInfo")
	public ResponseEntity<?> saveTaskInfo(@RequestBody TaskInfo task) {
		dmeSyncWorkflowService.getService("local").saveTaskInfo(task);
		return new ResponseEntity<>(HttpStatus.OK);
	}

	@PostMapping(value = "/saveMetadataInfo")
	public ResponseEntity<?> saveMetadataInfo(@RequestBody MetadataInfo metadataInfo) {
		dmeSyncWorkflowService.getService("local").saveMetadataInfo(metadataInfo);
		return new ResponseEntity<>(HttpStatus.OK);
	}

	@PostMapping(value = "/savePermissionBookmarkInfo")
	public ResponseEntity<?> savePermissionBookmarkInfo(@RequestBody PermissionBookmarkInfo entry) {
		dmeSyncWorkflowService.getService("local").savePermissionBookmarkInfo(entry);
		return new ResponseEntity<>(HttpStatus.OK);
	}

	@GetMapping(value = "/findFirstStatusInfoByOriginalFilePathOrderByStartTimestampDesc")
	public StatusInfo findFirstStatusInfoByOriginalFilePathOrderByStartTimestampDesc(
			@RequestParam(required = true) String originalFilePath) {
		return dmeSyncWorkflowService.getService("local")
				.findFirstStatusInfoByOriginalFilePathOrderByStartTimestampDesc(originalFilePath);
	}
	
	@GetMapping(value = "/findTopStatusInfoByDocOrderByStartTimestampDesc")
	public StatusInfo findTopStatusInfoByDocOrderByStartTimestampDesc(
			@RequestParam(required = true) String doc) {
		return dmeSyncWorkflowService.getService("local")
				.findTopStatusInfoByDocOrderByStartTimestampDesc(doc);
	}
	
	@GetMapping(value = "/findStatusInfoByDocAndStatus")
	public List<StatusInfo> findStatusInfoByDocAndStatus(
			@RequestParam(required = true) String doc, @RequestParam(required = true) String status) {
		return dmeSyncWorkflowService.getService("local").findStatusInfoByDocAndStatus(doc,
				status);
	}
}
