package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import gov.nih.nci.hpc.dmesync.DmeSyncWorkflowServiceFactory;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncVerificationException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncTask;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncWorkflow;

/**
 * DME Sync Workflow Implementation
 * 
 * @author dinhys
 */
@Component
public class DmeSyncWorkflowImpl implements DmeSyncWorkflow {

  final Logger logger = LoggerFactory.getLogger(getClass().getName());

  private List<DmeSyncTask> tasks;

  @Autowired private DmeSyncUploadTaskImpl syncUploadTask;
  @Autowired private DmeSyncPresignUploadTaskImpl presignUploadTask;
  @Autowired private DmeSyncFileSystemUploadTaskImpl fileSystemUploadTask;
  @Autowired private DmeSyncAWSS3UploadTaskImpl awsS3UploadTask;
  @Autowired private DmeSyncMetadataTaskImpl metadataTask;
  @Autowired private DmeSyncTarTaskImpl tarTask;
  @Autowired private DmeSyncCompressTaskImpl compressTask;
  @Autowired private DmeSyncUntarTaskImpl untarTask;
  @Autowired private DmeSyncVerifyTaskImpl verifyTask;
  @Autowired private DmeSyncPermissionBookmarkTaskImpl permissionBookmarkTask;
  @Autowired private DmeSyncCleanupTaskImpl cleanupTask;
  @Autowired private DmeSyncCreateChecksumTaskImpl createChecksumTask;
  @Autowired private DmeSyncWorkflowServiceFactory dmeSyncWorkflowService;
  @Autowired private DmeSyncPermissionArchiveTaskImpl permissionArchiveTask;
  @Autowired private DmeSyncCreateSoftlinkTaskImpl createSoftlinkTask;
  @Autowired private DmeSyncMoveDataObjectTaskImpl moveDataObjectTask;
  
  @Value("${dmesync.db.access:local}")
  private String access;

  @Value("${dmesync.tar:false}")
  private boolean tar;
  
  @Value("${dmesync.file.tar:false}")
  private boolean tarIndividualFiles;

  @Value("${dmesync.untar:false}")
  private boolean untar;

  @Value("${dmesync.dryrun:false}")
  private boolean dryRun;

  @Value("${dmesync.compress:false}")
  private boolean compress;
  
  @Value("${dmesync.checksum:true}")
  private boolean checksum;
  
  @Value("${dmesync.filesystem.upload:false}")
  private boolean fileSystemUpload;
  
  @Value("${dmesync.create.softlink:false}")
  private boolean createSoftlink;
  
  @Value("${dmesync.metadata.update.only:false}")
  private boolean metadataUpdateOnly;
  
  @Value("${dmesync.move.processed.files:false}")
  private boolean moveProcessedFiles;
  
  @Value("${dmesync.source.aws:false}")
  private boolean awsFlag;
  
  @PostConstruct
  public boolean init() {
    // Workflow init, add all applicable tasks, also need to create taskImpl class
    tasks = new ArrayList<>();
    if (!awsFlag) {
	    if (tar || tarIndividualFiles) tasks.add(tarTask);
	    else if (compress) tasks.add(compressTask);
	    if (untar) tasks.add(untarTask);
    }

    tasks.add(metadataTask);

    if (!dryRun) {
      if(checksum && !createSoftlink && !moveProcessedFiles && !awsFlag)
        tasks.add(createChecksumTask);
      if(fileSystemUpload)
    	  tasks.add(fileSystemUploadTask);
      else if (metadataUpdateOnly)
    	  tasks.add(syncUploadTask);
      else if (createSoftlink)
    	  tasks.add(createSoftlinkTask);
      else if (moveProcessedFiles)
    	  tasks.add(moveDataObjectTask);
      else if (awsFlag)
    	  tasks.add(awsS3UploadTask);
      else
    	  tasks.add(presignUploadTask);
      if(!metadataUpdateOnly && !createSoftlink && !moveProcessedFiles && !awsFlag) {
	      tasks.add(verifyTask);
	      tasks.add(permissionBookmarkTask);
	      if(fileSystemUpload)
	    	  tasks.add(permissionArchiveTask);
	      if (tar || tarIndividualFiles || untar || compress) tasks.add(cleanupTask);
      }
    }
    
    return true;
  }

  @Override
  public void start(StatusInfo statusInfo) throws DmeSyncWorkflowException {
    // Execute tasks. If any task fails with a need for retry, throw exception for rollback
    logger.info("[Workflow] Starting");

    try {
      //Clear any previous error in case of a retry
      statusInfo.setError("");
      statusInfo.setStartTimestamp(new Date());
      
      for (DmeSyncTask task : tasks) {
        statusInfo = task.processTask(statusInfo);
      }
      
      dmeSyncWorkflowService.getService(access).completeWorkflow(statusInfo);
      logger.info("[Workflow] Completed");
      
    } catch (DmeSyncMappingException | DmeSyncVerificationException  e) {
      
      // In case of mapping or verification exception on async, retry will not help.
      statusInfo.setError(e.getMessage());
      dmeSyncWorkflowService.getService(access).recordError(statusInfo);
      
    } catch (DmeSyncWorkflowException e) {
      
      statusInfo.setRetryCount(statusInfo.getRetryCount() + 1);
      dmeSyncWorkflowService.getService(access).retryWorkflow(statusInfo, e);
      
      throw e;
      
    } catch (Exception e) {
      
      dmeSyncWorkflowService.getService(access).retryWorkflow(statusInfo, e);
    }
  }

}
