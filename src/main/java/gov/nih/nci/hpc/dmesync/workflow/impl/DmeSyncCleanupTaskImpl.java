package gov.nih.nci.hpc.dmesync.workflow.impl;

import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.util.TarUtil;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncTask;

/**
 * DME Sync Cleanup Task Implementation
 * 
 * @author dinhys
 */
@Component
public class DmeSyncCleanupTaskImpl extends AbstractDmeSyncTask implements DmeSyncTask {

  @Value("${dmesync.tar:false}")
  private boolean tar;

  @Value("${dmesync.untar:false}")
  private boolean untar;

  @Value("${dmesync.work.base.dir}")
  private String syncWorkDir;
  
  @Value("${dmesync.cleanup:false}")
  private boolean cleanup;

  @Value("${dmesync.compress:false}")
  private boolean compress;
  
  @Value("${dmesync.file.tar:false}")
  private boolean tarIndividualFiles;
  
  @PostConstruct
  public boolean init() {
    super.setTaskName("CleanupTask");
    super.setCheckTaskForCompletion(false);
    
    return true;
  }
  
  @Override
  public StatusInfo process(StatusInfo object) {

    //Cleanup any files from the work directory.
    if (tar || untar || compress || tarIndividualFiles) {
      // Remove the tar file from the work directory. If no other files exists, we can remove the parent directories.
      try {
        if(cleanup)
          TarUtil.deleteTar(object.getSourceFilePath(), syncWorkDir);
        else
          logger.info("[{}] Test so it will not remove but clean up called for {} WORK_DIR: {}", super.getTaskName(), object.getSourceFilePath(), syncWorkDir);
      } catch (Exception e) {
        // For cleanup, we need not to rollback.
        logger.error("[{}] Upload successful but failed to remove file", super.getTaskName(), e);
        // Record it in DB as well
        object.setError("Upload successful but failed to remove file");
        dmeSyncWorkflowService.saveStatusInfo(object);
      }
    }

    return object;
  }
}
