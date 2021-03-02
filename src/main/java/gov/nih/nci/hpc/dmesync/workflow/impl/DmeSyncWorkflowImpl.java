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
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.service.DmeSyncWorkflowService;
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

  @Autowired private DmeSyncPresignUploadTaskImpl presignUploadTask;
  @Autowired private DmeSyncFileSystemUploadTaskImpl fileSystemUploadTask;
  @Autowired private DmeSyncMetadataTaskImpl metadataTask;
  @Autowired private DmeSyncTarTaskImpl tarTask;
  @Autowired private DmeSyncCompressTaskImpl compressTask;
  @Autowired private DmeSyncUntarTaskImpl untarTask;
  @Autowired private DmeSyncVerifyTaskImpl verifyTask;
  @Autowired private DmeSyncPermissionBookmarkTaskImpl permissionBookmarkTask;
  @Autowired private DmeSyncCleanupTaskImpl cleanupTask;
  @Autowired private DmeSyncCreateChecksumTaskImpl createChecksumTask;
  @Autowired private DmeSyncWorkflowService dmeSyncWorkflowService;

  @Value("${dmesync.tar:false}")
  private boolean tar;

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
  
  @PostConstruct
  public boolean init() {
    // Workflow init, add all applicable tasks, also need to create taskImpl class
    tasks = new ArrayList<>();
    if (tar) tasks.add(tarTask);
    else if (compress) tasks.add(compressTask);
    if (untar) tasks.add(untarTask);

    tasks.add(metadataTask);

    if (!dryRun) {
      if(checksum)
        tasks.add(createChecksumTask);
      if(fileSystemUpload)
    	  tasks.add(fileSystemUploadTask);
      else
    	  tasks.add(presignUploadTask);
      tasks.add(verifyTask);
      tasks.add(permissionBookmarkTask);
      if (tar || untar || compress) tasks.add(cleanupTask);
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
      
      dmeSyncWorkflowService.completeWorkflow(statusInfo);
      logger.info("[Workflow] Completed");
      
    } catch (DmeSyncMappingException e) {
      
      // In case of mapping exception, retry will not help.
      dmeSyncWorkflowService.recordError(statusInfo, e);
      
    } catch (DmeSyncWorkflowException e) {
      
      statusInfo.setRetryCount(statusInfo.getRetryCount() + 1);
      dmeSyncWorkflowService.retryWorkflow(statusInfo, e);
      
      throw e;
      
    } catch (Exception e) {
      
      dmeSyncWorkflowService.retryWorkflow(statusInfo, e);
    }
  }

}
