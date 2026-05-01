package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import gov.nih.nci.hpc.dmesync.DmeSyncMailServiceFactory;
import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.dto.DmeSyncMessageDto;
import gov.nih.nci.hpc.dmesync.jms.DmeSyncProducer;
import gov.nih.nci.hpc.dmesync.util.TarUtil;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncTask;

/**
 * DME Sync Cleanup Task Implementation
 * 
 * @author dinhys
 */
@Component
public class DmeSyncCleanupTaskImpl extends AbstractDmeSyncTask implements DmeSyncTask {
  
  @Autowired
  private DmeSyncProducer sender;
  
  @Autowired private DmeSyncMailServiceFactory dmeSyncMailServiceFactory;

  
  @PostConstruct
  public boolean init() {
    super.setTaskName("CleanupTask");
    super.setCheckTaskForCompletion(false);
    
    return true;
  }
  
  @Override
  public StatusInfo process(StatusInfo object, DocConfig config) {

    DocConfig.SourceConfig sourceConfig = config.getSourceConfig();
    DocConfig.SourceRule sourceRule = config.getSourceRule();
	DocConfig.PreprocessingConfig pre = config.getPreprocessingConfig();
	DocConfig.PreprocessingRule preRule = config.getPreprocessingRule();
	DocConfig.UploadConfig upload = config.getUploadConfig();
		  
    //Cleanup any files from the work directory.
    if (pre.tar || pre.untar || pre.compressTar || pre.fileTar || sourceRule.selectiveScan) {
      // Remove the tar file from the work directory. If no other files exists, we can remove the parent directories.
      try {
			if (upload.cleanupWorkdir) {				
				if (sourceRule.selectiveScan && TarUtil.isSelectiveScanFileUpload(Paths.get(object.getOriginalFilePath()))) {
					// Skipping this task for the selective scan files
					return object;
				} else {
					String sourceDirLeafNode = object.getSourceFilePath() != null
							? ((Paths.get(object.getOriginalFilePath())).getFileName()).toString()
							: null;
					if (preRule.processMultipleTars && TarUtil.matchesAnyMultipleTarFolder( preRule.multipleTarsDirFolders , sourceDirLeafNode )
							&& object.getTarEndTimestamp() != null) {

						cleanUpTaskForMultipleTars(object, config);

					} else {
						TarUtil.deleteTarAndParentsIfEmpty(object.getSourceFilePath(), sourceConfig.workBaseDir, config.getDocName());
					}
				}
			} else
				logger.info("[{}] Test so it will not remove but clean up called for {} WORK_DIR: {}",
						super.getTaskName(), object.getSourceFilePath(), sourceConfig.workBaseDir);
		} catch (Exception e) {

    	  String errorMessage="Upload successful but failed to remove file ";
        // For cleanup, we need not to rollback.
        logger.error("[{}] Upload successful but failed to remove file", super.getTaskName(), e);
        // Record it in DB as well
        object.setError(errorMessage);
        dmeSyncWorkflowService.getService(access).saveStatusInfo(object);
        updateTarCounterForMultipleTars(object, config);
        dmeSyncMailServiceFactory.getService(config.getDocName()).sendErrorMail("HPCDME Auto Archival Cleanup Error " ,
        		errorMessage + object.getSourceFilePath()+ ": " + e.getMessage() + "\n\n" + e.getCause().getMessage(), config);
      }
    }

    return object;
  }
  
  private void cleanUpTaskForMultipleTars(StatusInfo object, DocConfig config) throws IOException {
	  
	// Seperate cleanup logic is implemented for the multiple tars design CSB movies folder.
		/*
		 * if this a multiple tars folder, cleanup should perform below steps 1.
		 * decrement the counter then read the counter value and check if it is zero: 
		 * If yes(all the tars for folder are uplaoded)
		 *  1.1 Enqueue the tarContentsFile upload
		 *  1.2 Delete the parent folder
		 * If false then just delete the tar file.
		 * 
		 */
	  	DocConfig.SourceConfig sourceConfig = config.getSourceConfig();
		
		// Retrieve the record for contents file.
		String tarFileParentName   = Paths.get(object.getOriginalFilePath()).getParent().getFileName().toString();
		String tarContentsFileName = tarFileParentName + "_" + object.getOrginalFileName() + "_TarContentsFile.txt";
		StatusInfo recordForContentsfile = dmeSyncWorkflowService.getService(access)
				.findTopBySourceFileNameAndRunIdAndOriginalFilePath(tarContentsFileName, object.getRunId(),
						object.getOriginalFilePath());

		synchronized (this) {
			
			// Retrieve the movies folder row from DB for the counter. the movies row will have the sourcefilename as movies other rows have tarnames.
			StatusInfo tarFolderRow = dmeSyncWorkflowService.getService(access)
					.findTopStatusInfoByDocAndSourceFilePathAndLikeRunId(object.getDoc(),object.getOriginalFilePath(), object.getRunId()+"%");
			
			if (tarFolderRow != null) {
				logger.info(
						"[{}] Decrementing the tar counter old value{} , new value {} ",
						super.getTaskName(), tarFolderRow.getTarContentsCount(),
						tarFolderRow.getTarContentsCount()-1);
				
				// decrement and read the counter
				tarFolderRow.setTarContentsCount(tarFolderRow.getTarContentsCount() - 1);
				tarFolderRow = dmeSyncWorkflowService.getService(access).saveStatusInfo(tarFolderRow);

				// if counter value is 0 then enqueue the contents file and delete the movies folder.
				if (tarFolderRow != null && tarFolderRow.getTarContentsCount() == 0) {

					logger.info(
							"[{}] Deleting tarFile and dataset folder since all the tars are uploaded to DME  {} with counter ",
							super.getTaskName(), object.getOriginalFilePath(),
							tarFolderRow.getTarContentsCount());
					
					// Delete both Parent folder and Tar file
					TarUtil.deleteTarAndParentsIfEmpty(object.getSourceFilePath(), sourceConfig.workBaseDir, config.getDocName());

					if (recordForContentsfile != null && tarFolderRow.getError() == null) {
						logger.info("[{}] Enqueing the tarContents File upload to JMS {} with id {} ",
								super.getTaskName(), recordForContentsfile.getSourceFileName(),
								recordForContentsfile.getId());
						// Send the Tar contents file record to the message queue for processing
						DmeSyncMessageDto message = new DmeSyncMessageDto();
						message.setObjectId(recordForContentsfile.getId());
						message.setDocConfigId(config.getId());
						sender.send(message, "inbound.queue");
						logger.info("get queue count" + sender.getQueueCount("inbound.queue"));
					}
				} else {
					logger.info(
							"[{}] Deleting only tarFile since all the tars aren't uploaded to DME  {} with counter ",
							super.getTaskName(), object.getSourceFilePath(),
							tarFolderRow.getTarContentsCount());
					// Delete only TarFile.
					TarUtil.deleteTarFile(object.getSourceFilePath(), sourceConfig.workBaseDir, config.getDocName());
				}

			}

		}
  }
  void updateTarCounterForMultipleTars(StatusInfo object, DocConfig config){
	
	DocConfig.PreprocessingRule preRule = config.getPreprocessingRule();
	if(preRule.processMultipleTars) {
	  // This block only executes for mulitpleTars feature
	  synchronized (this) {
		// retrieve the movies folder row from DB for the counter. the movies row will have the sourcefilename as movies  other rows have tarnames.
			StatusInfo tarFolderRow = dmeSyncWorkflowService.getService(access)
					.findTopStatusInfoByDocAndSourceFilePathAndLikeRunId(object.getDoc(),object.getOriginalFilePath(), object.getRunId()+"%");
			if (tarFolderRow != null && tarFolderRow.getTarContentsCount()!=null) {
				logger.info(
						"[{}] Decrementing the tar counter old value when cleanup has error in execption block{} , new value {} ",
						super.getTaskName(), tarFolderRow.getTarContentsCount(),
						tarFolderRow.getTarContentsCount()-1);
				// decrement and read the counter
				tarFolderRow.setTarContentsCount(tarFolderRow.getTarContentsCount() - 1);
				tarFolderRow = dmeSyncWorkflowService.getService(access).saveStatusInfo(tarFolderRow);
	  
	  }}}
	  }
}
