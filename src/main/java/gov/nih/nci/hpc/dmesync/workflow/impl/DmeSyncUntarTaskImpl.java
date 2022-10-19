package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.util.TarUtil;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncTask;

/**
 * DME Sync Untar Task Implementation
 * 
 * @author dinhys
 */
@Component
public class DmeSyncUntarTaskImpl extends AbstractDmeSyncTask implements DmeSyncTask {

  @Value("${dmesync.source.base.dir}")
  private String syncBaseDir;

  @Value("${dmesync.work.base.dir}")
  private String syncWorkDir;

  @Value("${dmesync.cleanup:false}")
  private boolean cleanup;
  
  @PostConstruct
  public boolean init() {
    super.setTaskName("UntarTask");
    return true;
  }
  
  @Override
  public StatusInfo process(StatusInfo object)
      throws DmeSyncMappingException, DmeSyncWorkflowException {

    //Untar files in work directory for processing if the specified file does not exist.
    try {
      //Construct work dir path
      Path baseDirPath = Paths.get(syncBaseDir).toRealPath();
      Path workDirPath = Paths.get(syncWorkDir).toRealPath();
      Path sourceDirPath = Paths.get(object.getOriginalFilePath());
      Path relativePath = baseDirPath.relativize(sourceDirPath);
      String tarWorkDir = workDirPath.toString() + File.separatorChar + relativePath.getParent().toString();
      Path tarWorkDirPath = Paths.get(tarWorkDir);
      String sourceFile = tarWorkDirPath.toString() + File.separatorChar + object.getSourceFileName();
      Path sourceFilePath = Paths.get(sourceFile);

      //Check if the file exist.
      if (sourceFilePath.toFile().exists()) {
        Files.createDirectories(tarWorkDirPath);

        //Extract tar file if file doesn't exist in the work dir.
        TarUtil.untar(object.getOriginalFilePath(), tarWorkDirPath.toFile());

        // Remove any COMPLETED files from the work directory from this tar file for this run
        List<StatusInfo> completedFiles =
            dmeSyncWorkflowService.getService(access).findAllStatusInfoByOriginalFilePathAndStatusAndRunId(
                object.getOriginalFilePath(), "COMPLETED", object.getRunId());
        for (StatusInfo completedFile : completedFiles) {
          if(cleanup)
            TarUtil.deleteTar(completedFile.getSourceFilePath(), syncWorkDir);
          else
            logger.info("[{}] Test so it will not remove any files but remove called for {}  WORK_DIR: {}", super.getTaskName(), completedFile.getSourceFilePath(), syncWorkDir);
        }
      }

      // Update the record for upload
      object.setSourceFileName(object.getSourceFileName());
      object.setSourceFilePath(sourceFilePath.toString());
      object = dmeSyncWorkflowService.getService(access).saveStatusInfo(object);

    } catch (Exception e) {
      logger.error("[{}] error {}", super.getTaskName(), e.getMessage(), e);
      throw new DmeSyncWorkflowException("Error occurred during untar", e);
    }

    return object;
  }
}
