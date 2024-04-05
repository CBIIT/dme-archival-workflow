package gov.nih.nci.hpc.dmesync.service.impl;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import gov.nih.nci.hpc.dmesync.dao.CollectionNameMappingDao;
import gov.nih.nci.hpc.dmesync.dao.MetadataInfoDao;
import gov.nih.nci.hpc.dmesync.dao.MetadataMappingDao;
import gov.nih.nci.hpc.dmesync.dao.PermissionBookmarkInfoDao;
import gov.nih.nci.hpc.dmesync.dao.StatusInfoDao;
import gov.nih.nci.hpc.dmesync.dao.TaskInfoDao;
import gov.nih.nci.hpc.dmesync.domain.CollectionNameMapping;
import gov.nih.nci.hpc.dmesync.domain.MetadataInfo;
import gov.nih.nci.hpc.dmesync.domain.MetadataMapping;
import gov.nih.nci.hpc.dmesync.domain.PermissionBookmarkInfo;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.domain.TaskInfo;
import gov.nih.nci.hpc.dmesync.service.DmeSyncWorkflowService;

@Service("local")
@Transactional
public class DmeSyncWorkflowServiceImpl implements DmeSyncWorkflowService {

  @Autowired protected StatusInfoDao<StatusInfo> statusInfoDao;
  @Autowired protected MetadataInfoDao<MetadataInfo> metadataInfoDao;
  @Autowired protected TaskInfoDao<TaskInfo> taskInfoDao;
  @Autowired protected MetadataMappingDao<MetadataMapping> metadataMappingDao;
  @Autowired protected CollectionNameMappingDao<CollectionNameMapping> collectionNameMappingDao;
  @Autowired protected PermissionBookmarkInfoDao<PermissionBookmarkInfo> permissionBookmarkInfoDao;

  @Override
  public void completeWorkflow(StatusInfo statusInfo) {
    statusInfo.setEndTimestamp(new Date());
    statusInfoDao.saveAndFlush(statusInfo);
    taskInfoDao.deleteByObjectId(statusInfo.getId());
  }

  @Override
  public void retryWorkflow(StatusInfo statusInfo, Exception e) {
	statusInfo.setError(e.getMessage());
    recordError(statusInfo);
    // Delete the metadata info created for this object ID
    metadataInfoDao.deleteByObjectId(statusInfo.getId());
  }

  @Override
  public void recordError(StatusInfo info) {
    statusInfoDao.saveAndFlush(info);
  }

  @Override
  public StatusInfo findFirstStatusInfoByOriginalFilePathAndStatus(
      String originalFilePath, String status) {
    return statusInfoDao.findFirstByOriginalFilePathAndStatusOrderByStartTimestampDesc(originalFilePath, status);
  }

  @Override
  public List<StatusInfo> findAllStatusInfoByOriginalFilePathAndStatus(
      String originalFilePath, String status) {
    return statusInfoDao.findAllByOriginalFilePathAndStatus(originalFilePath, status);
  }
  
  @Override
  public List<StatusInfo> findAllStatusInfoLikeOriginalFilePath(String originalFilePath) {
    return statusInfoDao.findAllLikeOriginalFilePath(originalFilePath);
  }

  @Override
  public List<StatusInfo> findStatusInfoByRunIdAndDoc(String runId, String doc) {
    return statusInfoDao.findByRunIdAndDoc(runId, doc);
  }

  @Override
  public StatusInfo findFirstStatusInfoByOriginalFilePathAndSourceFileNameAndStatus(
      String originalFilePath, String sourceFileName, String status) {
    return statusInfoDao.findFirstByOriginalFilePathAndSourceFileNameAndStatus(
        originalFilePath, sourceFileName, status);
  }

  @Override
  public StatusInfo findTopStatusInfoByDocAndOriginalFilePathStartsWithOrderByStartTimestampDesc(String doc, String baseDir) {
    return statusInfoDao.findTopStatusInfoByDocAndOriginalFilePathStartsWithOrderByStartTimestampDesc(doc, baseDir);
  }
  
  @Override
  public StatusInfo findTopStatusInfoByDocAndOriginalFilePathStartsWithOrderByTarEndTimestampDesc(String doc, String baseDir) {
    return statusInfoDao.findTopStatusInfoByDocAndTarEndTimestampNotNullAndOriginalFilePathStartsWithAndStatusOrderByTarEndTimestampDesc(doc, baseDir, "COMPLETED");
  }
  
  @Override
  public List<StatusInfo> findAllStatusInfoByOriginalFilePathAndStatusAndRunId(
      String originalFilePath, String status, String runId) {
    return statusInfoDao.findAllByOriginalFilePathAndStatusAndRunId(
        originalFilePath, status, runId);
  }
  
  @Override
  public StatusInfo findTopBySourceFilePathAndRunId(
      String originalFilePath, String runId) {
    return statusInfoDao.findTopBySourceFilePathAndRunId(
        originalFilePath, runId);
  }

  @Override
  public CollectionNameMapping findCollectionNameMappingByMapKeyAndCollectionTypeAndDoc(
      String key, String collectionType, String doc) {
    return collectionNameMappingDao.findByMapKeyAndCollectionTypeAndDoc(key, collectionType, doc);
  }

  @Override
  public List<MetadataMapping> findAllMetadataMappingByCollectionTypeAndCollectionNameAndDoc(
      String collectionType, String collectionName, String doc) {
    return metadataMappingDao.findAllByCollectionTypeAndCollectionNameAndDoc(
        collectionType, collectionName, doc);
  }
  
  @Override
  public MetadataMapping findByMetadataMappingByCollectionTypeAndCollectionNameAndMetaDataKeyAndDoc(
      String collectionType, String collectionName, String metaDataKey, String doc) {
    return metadataMappingDao.findByCollectionTypeAndCollectionNameAndMetaDataKeyAndDoc(
        collectionType, collectionName, metaDataKey, doc);
  }

  @Override
  public List<PermissionBookmarkInfo> findAllPermissionBookmarkInfoByCreated(String flag) {
    return permissionBookmarkInfoDao.findAllByCreated(flag);
  }

  @Override
  public void deleteMetadataInfoByObjectId(Long objectId) {
    metadataInfoDao.deleteByObjectId(objectId);
  }

  @Override
  public List<MetadataInfo> findAllMetadataInfoByRunIdAndDoc(String runId, String doc) {
    return metadataInfoDao.findAllByRunIdAndDoc(runId, doc);
  }
  
  @Override
  public List<MetadataInfo> findAllMetadataInfoByRunIdAndMetaDataKeyAndDoc(String runId, String key, String doc) {
    return metadataInfoDao.findAllByRunIdAndMetaDataKeyAndDoc(runId, key, doc);
  }

  @Override
  public Optional<StatusInfo> findStatusInfoById(Long objectId) {
    return statusInfoDao.findById(objectId);
  }

  @Override
  public StatusInfo saveStatusInfo(StatusInfo statusInfo) {
    return statusInfoDao.save(statusInfo);
  }

  @Override
  public TaskInfo findFirstTaskInfoByObjectIdAndTaskName(Long id, String taskName) {
    return taskInfoDao.findFirstByObjectIdAndTaskName(id, taskName);
  }

  @Override
  public void deleteTaskInfoByObjectId(Long objectId) {
    taskInfoDao.deleteByObjectId(objectId);
  }

  @Override
  public void saveTaskInfo(TaskInfo task) {
    taskInfoDao.save(task);
  }

  @Override
  public void saveMetadataInfo(MetadataInfo metadataInfo) {
    metadataInfoDao.save(metadataInfo);
  }

  @Override
  public void savePermissionBookmarkInfo(PermissionBookmarkInfo entry) {
    permissionBookmarkInfoDao.save(entry);
  }
  
  @Override
  public StatusInfo findFirstStatusInfoByOriginalFilePathOrderByStartTimestampDesc(
      String originalFilePath) {
    return statusInfoDao.findFirstStatusInfoByOriginalFilePathOrderByStartTimestampDesc(originalFilePath);
  }

  @Override
  public StatusInfo findTopStatusInfoByDocOrderByStartTimestampDesc(String doc) {
	return statusInfoDao.findTopStatusInfoByDocOrderByStartTimestampDesc(doc);
  }

  @Override
  public List<StatusInfo> findStatusInfoByDocAndStatus(String doc, String status) {
	return statusInfoDao.findStatusInfoByDocAndStatus(doc, status);
  }
}
