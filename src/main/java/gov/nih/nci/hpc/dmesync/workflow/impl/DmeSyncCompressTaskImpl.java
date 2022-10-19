package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.util.TarUtil;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncTask;

/**
 * DME Sync Tar Task Implementation
 * 
 * @author dinhys
 */
@Component
public class DmeSyncCompressTaskImpl extends AbstractDmeSyncTask implements DmeSyncTask {

  @Value("${dmesync.compress:false}")
  private boolean compress;

  @Value("${dmesync.source.base.dir}")
  private String syncBaseDir;

  @Value("${dmesync.work.base.dir}")
  private String syncWorkDir;

  @Value("${dmesync.dryrun:false}")
  private boolean dryRun;
  
  @PostConstruct
  public boolean init() {
    super.setTaskName("CompressTask");
    return true;
  }
  
  @Override
  public StatusInfo process(StatusInfo object)
      throws DmeSyncMappingException, DmeSyncWorkflowException {
    
    //Task: Compress file in work directory for processing
    try {
      //Construct work dir path
      Path baseDirPath = Paths.get(syncBaseDir).toRealPath();
      Path workDirPath = Paths.get(syncWorkDir).toRealPath();
      Path sourceDirPath = Paths.get(object.getOriginalFilePath());
      Path relativePath = baseDirPath.relativize(sourceDirPath);
      String compressWorkDir = workDirPath.toString() + File.separatorChar + relativePath.toString();
      Path compressWorkDirPath = Paths.get(compressWorkDir);
      Files.createDirectories(compressWorkDirPath);
      
      String compressFileName = object.getOrginalFileName() + ".gz";
      String compressFile = compressWorkDir + File.separatorChar + compressFileName;
      compressFile = Paths.get(compressFile).normalize().toString();
      File originalFile = new File(object.getOriginalFilePath());

      logger.info("[{}] Creating compress file in {}", super.getTaskName(), compressFile);
      
      if (!dryRun) {
          TarUtil.compress(compressFile, originalFile);
      }

      // Update the record for upload
      File createdCompressedFile = new File(compressFile);
      object.setFilesize(createdCompressedFile.length());
      object.setSourceFileName(compressFileName);
      object.setSourceFilePath(compressFile);
      object = dmeSyncWorkflowService.getService(access).saveStatusInfo(object);

    } catch (Exception e) {
      logger.error("[{}] error {}", super.getTaskName(), e.getMessage(), e);
      throw new DmeSyncWorkflowException("Error occurred during compress", e);
    }

    return object;
  }
}
