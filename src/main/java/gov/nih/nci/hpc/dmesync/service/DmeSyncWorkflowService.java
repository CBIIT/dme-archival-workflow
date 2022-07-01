package gov.nih.nci.hpc.dmesync.service;

import java.util.List;
import java.util.Optional;
import gov.nih.nci.hpc.dmesync.domain.CollectionNameMapping;
import gov.nih.nci.hpc.dmesync.domain.MetadataInfo;
import gov.nih.nci.hpc.dmesync.domain.MetadataMapping;
import gov.nih.nci.hpc.dmesync.domain.PermissionBookmarkInfo;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.domain.TaskInfo;

/**
 * DME Sync Workflow Service Interface
 *
 * @author dinhys
 */
public interface DmeSyncWorkflowService {

  /**
   * findFirstStatusInfoByOriginalFilePathAndStatus
   *
   * @param originalFilePath the original file path
   * @param status the status
   * @return the StatusInfo object
   */
  StatusInfo findFirstStatusInfoByOriginalFilePathAndStatus(String originalFilePath, String status);

  /**
   * findAllStatusInfoByOriginalFilePathAndStatus
   *
   * @param originalFilePath the original file path
   * @param status the status
   * @return the list of StatusInfo objects
   */
  List<StatusInfo> findAllStatusInfoByOriginalFilePathAndStatus(
      String originalFilePath, String status);

  /**
   * findStatusInfoByRunId
   *
   * @param runId the runId
   * @return the list of StatusInfo objects
   */
  List<StatusInfo> findStatusInfoByRunId(String runId);

  /**
   * findFirstStatusInfoByOriginalFilePathAndSourceFileNameAndStatus
   *
   * @param originalFilePath the original file path
   * @param sourceFileName the source file name
   * @param status the status
   * @return the StatusInfo object
   */
  StatusInfo findFirstStatusInfoByOriginalFilePathAndSourceFileNameAndStatus(
      String originalFilePath, String sourceFileName, String status);

  /**
   * findTopStatusInfoByOrderByStartTimestampDesc
   *
   * @return the StatusInfo object
   */
  StatusInfo findTopStatusInfoByOrderByStartTimestampDesc();

  /**
   * findAllStatusInfoByOriginalFilePathAndStatusAndRunId
   *
   * @param originalFilePath the original file path
   * @param status the status
   * @param runId the runId
   * @return the list of StatusInfo objects
   */
  List<StatusInfo> findAllStatusInfoByOriginalFilePathAndStatusAndRunId(
      String originalFilePath, String status, String runId);

  /**
   * findCollectionNameMappingByMapKeyAndCollectionType
   *
   * @param key the key
   * @param collectionType the collection type
   * @return CollectionNameMapping
   */
  CollectionNameMapping findCollectionNameMappingByMapKeyAndCollectionType(
      String key, String collectionType);

  /**
   * findAllMetadataMappingByCollectionTypeAndCollectionName
   *
   * @param collectionType the collection type
   * @param collectionName the collection name
   * @return List of MetadataMapping
   */
  List<MetadataMapping> findAllMetadataMappingByCollectionTypeAndCollectionName(
      String collectionType, String collectionName);

  /**
   * findByMetadataMappingByCollectionTypeAndCollectionNameAndMetaDataKey
   *
   * @param collectionType the collection type
   * @param collectionName the collection name
   * @param metaDataKey the key
   * @return MetadataMapping
   */
  MetadataMapping findByMetadataMappingByCollectionTypeAndCollectionNameAndMetaDataKey(
      String collectionType, String collectionName, String metaDataKey);
  /**
   * findAllPermissionBookmarkInfoByCreated
   *
   * @param flag the flag Y or N
   * @return list of PermissionBookmarkInfo
   */
  List<PermissionBookmarkInfo> findAllPermissionBookmarkInfoByCreated(String flag);

  /**
   * deleteMetadataInfoByObjectId
   *
   * @param objectId the id
   */
  void deleteMetadataInfoByObjectId(Long objectId);

  /**
   * findAllMetadataInfoByRunId
   *
   * @param runId the runId
   * @return list of MetadataInfo
   */
  List<MetadataInfo> findAllMetadataInfoByRunId(String runId);
  
  /**
   * findAllMetadataInfoByRunIdAndMetaDataKey
   *
   * @param runId the runId
   * @param key the metaDataKey
   * @return list of MetadataInfo
   */
  List<MetadataInfo> findAllMetadataInfoByRunIdAndMetaDataKey(String runId, String key);

  /**
   * Save the end timestamp and remove temp task states
   *
   * @param statusInfo the StatusInfo object
   */
  public void completeWorkflow(StatusInfo statusInfo);

  /**
   * Save the error and perform cleanup on metadata entries for retry
   *
   * @param statusInfo the StatusInfo object
   * @param e the exception to be recorded
   */
  public void retryWorkflow(StatusInfo statusInfo, Exception e);

  /**
   * Saves the error
   *
   * @param statusInfo the StatusInfo object
   * @param e the exception to be recorded
   */
  public void recordError(StatusInfo statusInfo, Exception e);

  /**
   * findStatusInfoById
   * @param objectId the StatusInfo object
   * @return Optional StatusInfo
   */
  Optional<StatusInfo> findStatusInfoById(Long objectId);

  /**
   * Saves the StatusInfo object
   * @param statusInfo the StatusInfo object
   * @return StatusInfo
   */
  StatusInfo saveStatusInfo(StatusInfo statusInfo);
  
  /**
   * findFirstTaskInfoByObjectIdAndTaskName
   * @param id the object id
   * @param taskName the task name
   * @return TaskInfo
   */
  TaskInfo findFirstTaskInfoByObjectIdAndTaskName(Long id, String taskName);

  /**
   * deleteTaskInfoByObjectId
   * @param objectId the object id
   */
  void deleteTaskInfoByObjectId(Long objectId);

  /**
   * Saves TaskInfo
   * @param task the task to save
   */
  void saveTaskInfo(TaskInfo task);

  /**
   * Saves MetadataInfo
   * @param metadataInfo the metadata info
   */
  void saveMetadataInfo(MetadataInfo metadataInfo);

  /**
   * savePermissionBookmarkInfo
   * @param entry permission bookmark entry
   */
  void savePermissionBookmarkInfo(PermissionBookmarkInfo entry);

  /**
   * findFirstStatusInfoByOriginalFilePathOrderByStartTimestampDesc
   * @param absolutePath
   * @return the StatusInfo object
   */
  StatusInfo findFirstStatusInfoByOriginalFilePathOrderByStartTimestampDesc(String absolutePath);
  
  /**
   * findStatusInfoByStatus
   * 
   * @param status the status
   * @return the list of StatusInfo objects
   */
  List<StatusInfo> findStatusInfoByStatus(String status);
}
