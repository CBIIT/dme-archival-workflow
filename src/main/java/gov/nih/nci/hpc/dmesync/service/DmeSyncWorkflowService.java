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
   * findStatusInfoByRunIdAndDoc
   *
   * @param runId the runId
   * @param doc the doc
   * @return the list of StatusInfo objects
   */
  List<StatusInfo> findStatusInfoByRunIdAndDoc(String runId, String doc);

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
   * findTopStatusInfoByDocAndOriginalFilePathStartsWithOrderByStartTimestampDesc
   * @param doc the doc
   * @param baseDir the base directory
   * @return the StatusInfo object
   */
  StatusInfo findTopStatusInfoByDocAndOriginalFilePathStartsWithOrderByStartTimestampDesc(String doc, String baseDir);

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
   * findCollectionNameMappingByMapKeyAndCollectionTypeAndDoc
   *
   * @param key the key
   * @param collectionType the collection type
   * @param doc the doc
   * @return CollectionNameMapping
   */
  CollectionNameMapping findCollectionNameMappingByMapKeyAndCollectionTypeAndDoc(
      String key, String collectionType, String doc);

  /**
   * findAllMetadataMappingByCollectionTypeAndCollectionNameAndDoc
   *
   * @param collectionType the collection type
   * @param collectionName the collection name
   * @param doc the doc
   * @return List of MetadataMapping
   */
  List<MetadataMapping> findAllMetadataMappingByCollectionTypeAndCollectionNameAndDoc(
      String collectionType, String collectionName, String doc);

  /**
   * findByMetadataMappingByCollectionTypeAndCollectionNameAndMetaDataKeyAndDoc
   *
   * @param collectionType the collection type
   * @param collectionName the collection name
   * @param metaDataKey the key
   * @param doc the doc
   * @return MetadataMapping
   */
  MetadataMapping findByMetadataMappingByCollectionTypeAndCollectionNameAndMetaDataKeyAndDoc(
      String collectionType, String collectionName, String metaDataKey, String doc);
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
   * findAllMetadataInfoByRunIdAndDoc
   *
   * @param runId the runId
   * @param doc the doc
   * @return list of MetadataInfo
   */
  List<MetadataInfo> findAllMetadataInfoByRunIdAndDoc(String runId, String doc);
  
  /**
   * findAllMetadataInfoByRunIdAndMetaDataKeyAndDoc
   *
   * @param runId the runId
   * @param key the metaDataKey
   * @param doc the doc
   * @return list of MetadataInfo
   */
  List<MetadataInfo> findAllMetadataInfoByRunIdAndMetaDataKeyAndDoc(String runId, String key, String doc);

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
}
