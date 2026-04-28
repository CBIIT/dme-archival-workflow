package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import gov.nih.nci.hpc.dmesync.DmeSyncWorkflowServiceFactory;
import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncVerificationException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncStorageException;

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
  @Autowired private DmeSyncProcessMultipleTarsTaskImpl processMultipleTarsTask;
  @Autowired private DmeSyncTarTaskImpl tarTask;
  @Autowired private DmeSyncTarContentsFileTaskImpl tarContentsfileTask;
  @Autowired private DmeSyncCompressTaskImpl compressTask;
  @Autowired private DmeSyncUntarTaskImpl untarTask;
  @Autowired private DmeSyncVerifyTaskImpl verifyTask;
  @Autowired private DmeSyncPermissionBookmarkTaskImpl permissionBookmarkTask;
  @Autowired private DmeSyncCleanupTaskImpl cleanupTask;
  @Autowired private DmeSyncCreateChecksumTaskImpl createChecksumTask;
  @Autowired private DmeSyncWorkflowServiceFactory dmeSyncWorkflowService;
  @Autowired private DmeSyncPermissionArchiveTaskImpl permissionArchiveTask;
  @Autowired private DmeSyncCreateSoftlinkTaskImpl createSoftlinkTask;
  @Autowired private DmeSyncCreateCollectionSoftlinkTaskImpl createCollectionSoftlinkTask;
  @Autowired private DmeSyncMoveDataObjectTaskImpl moveDataObjectTask;
  
  @Value("${dmesync.db.access:local}")
  private String access;
 
  public void start(StatusInfo statusInfo, DocConfig config) throws DmeSyncWorkflowException {

    logger.info("[Workflow] Starting for DOC {} (version {})", config.getDocName(), config.getVersion());
    tasks = new ArrayList<>();

    DocConfig.SourceRule sourceRule = config.getSourceRule();
    DocConfig.PreprocessingConfig pre = config.getPreprocessingConfig();
    DocConfig.PreprocessingRule preRule = config.getPreprocessingRule();
    DocConfig.UploadConfig upload = config.getUploadConfig();

    // Workflow select tasks based on config, also need to create taskImpl class
    tasks = new ArrayList<>();
    if (!sourceRule.aws) {
    	if (preRule.processMultipleTars)  tasks.add(processMultipleTarsTask);
	    if (pre.tar || pre.fileTar || sourceRule.selectiveScan) {
	    	tasks.add(tarTask);
	    	if(preRule.tarContentsFile) {
	    		tasks.add(tarContentsfileTask);
	    	}
	    }
	    else if (pre.compressTar) tasks.add(compressTask);
	    if (pre.untar) tasks.add(untarTask);
    }

    tasks.add(metadataTask);

    if (!upload.dryRun) {
	      if(upload.checksum && !upload.softlink && !upload.collectionSoftlink && !upload.moveProcessedFiles && !sourceRule.aws)
	        tasks.add(createChecksumTask);
	      if(upload.fileSystemUpload)
	    	  tasks.add(fileSystemUploadTask);
	      else if (upload.metadataUpdateOnly)
	    	  tasks.add(syncUploadTask);
	      else if (upload.softlink)
	    	  tasks.add(createSoftlinkTask);
	      else if (upload.collectionSoftlink)
	    	  tasks.add(createCollectionSoftlinkTask);
	      else if (upload.moveProcessedFiles)
	    	  tasks.add(moveDataObjectTask);
	      else if (sourceRule.aws)
	    	  tasks.add(awsS3UploadTask);
	      else
	    	  tasks.add(presignUploadTask);
	      if(!upload.metadataUpdateOnly && !upload.softlink && !upload.collectionSoftlink && !upload.moveProcessedFiles && !sourceRule.aws) {
		      tasks.add(verifyTask);
		      tasks.add(permissionBookmarkTask);
		      if(upload.fileSystemUpload)
		    	  tasks.add(permissionArchiveTask);
		      if (pre.tar || pre.fileTar || pre.untar || pre.compressTar || sourceRule.selectiveScan) tasks.add(cleanupTask);
	      }
	      
	    // Execute tasks. If any task fails with a need for retry, throw exception for rollback
	 
	    try {
	      //Clear any previous error in case of a retry
	      statusInfo.setError("");
	      statusInfo.setStartTimestamp(new Date());
	      
	      String sourceDirLeafNode = statusInfo.getSourceFilePath() != null
					? ((Paths.get(statusInfo.getSourceFilePath())).getFileName()).toString()
					: null;
	      
	      for (DmeSyncTask task : tasks) {
	    	   statusInfo = task.processTask(statusInfo, config);
	    	   // This condition is used when we want to perform specific task and complete the workflow
	    	   if((upload.checkEndWorkflow && checkEndWorkflowFlag(statusInfo.getId())) ){
	    		      logger.info("[Workflow] End Workflow Flag is set to true , so no further task processing is required");
	    		   break;
	    	   }
	      }
	      
	      dmeSyncWorkflowService.getService(access).completeWorkflow(statusInfo);
	      logger.info("[Workflow] Completed");
	      
	    } catch (DmeSyncMappingException | DmeSyncVerificationException  e) {
	      
	      // In case of mapping or verification exception on async, retry will not help.
	      statusInfo.setError(e.getMessage());
	      dmeSyncWorkflowService.getService(access).recordError(statusInfo);
	      
	    } catch (DmeSyncStorageException e) {
	        
	        // In case of space issue while tarring, retry will not help.
	        statusInfo.setError(e.getMessage());
	        dmeSyncWorkflowService.getService(access).recordError(statusInfo);
	        
	    }catch (DmeSyncWorkflowException e) {
	      
	      statusInfo.setRetryCount(statusInfo.getRetryCount() + 1);
	      dmeSyncWorkflowService.getService(access).retryWorkflow(statusInfo, e);
	      
	      throw e;
	      
	    } catch (Exception e) {
	      
	      dmeSyncWorkflowService.getService(access).retryWorkflow(statusInfo, e);
	    }
	  } // not dry run
  }

  private boolean checkEndWorkflowFlag(Long objectId) {

	  Optional<StatusInfo> statusInfo = dmeSyncWorkflowService.getService(access).findStatusInfoById(objectId);

	  return (statusInfo.isPresent() && statusInfo.get().isEndWorkflow()!=null &&
			  Boolean.TRUE.equals(statusInfo.get().isEndWorkflow()));
	  }
}
