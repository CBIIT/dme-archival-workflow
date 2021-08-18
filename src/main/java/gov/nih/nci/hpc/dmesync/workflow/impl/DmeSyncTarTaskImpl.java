package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
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
 * DME Sync Tar Task Implementation
 * 
 * @author dinhys
 */
@Component
public class DmeSyncTarTaskImpl extends AbstractDmeSyncTask implements DmeSyncTask {

  @Value("${dmesync.compress:false}")
  private boolean compress;

  @Value("${dmesync.source.base.dir}")
  private String syncBaseDir;

  @Value("${dmesync.work.base.dir}")
  private String syncWorkDir;

  @Value("${dmesync.dryrun:false}")
  private boolean dryRun;
  
  @Value("${dmesync.tar.exclude.folder:}")
  private String excludeFolder;
  
  @PostConstruct
  public boolean init() {
    super.setTaskName("TarTask");
    return true;
  }
  
  @Override
  public StatusInfo process(StatusInfo object)
      throws DmeSyncMappingException, DmeSyncWorkflowException {
    
    //Task: Create tar file in work directory for processing
    try {
      object.setTarStartTimestamp(new Date());
      //Construct work dir path
      Path baseDirPath = Paths.get(syncBaseDir).toRealPath();
      Path workDirPath = Paths.get(syncWorkDir).toRealPath();
      Path sourceDirPath = Paths.get(object.getOriginalFilePath());
      Path relativePath = baseDirPath.relativize(sourceDirPath);
      String tarWorkDir = workDirPath.toString() + File.separatorChar + relativePath.toString();
      Path tarWorkDirPath = Paths.get(tarWorkDir);
      Files.createDirectories(tarWorkDirPath);
      
      String tarFileName = object.getOrginalFileName() + ".tar";
      String tarFile = tarWorkDir + File.separatorChar + tarFileName;
      tarFile = Paths.get(tarFile).normalize().toString();
      File directory = new File(object.getOriginalFilePath());

      logger.info("[{}] Creating tar file in {}", super.getTaskName(), tarFile);
      
      List<String> excludeFolders =
          excludeFolder == null || excludeFolder.isEmpty()
              ? null
              : new ArrayList<>(Arrays.asList(excludeFolder.split(",")));
      
      //Check directory permission
      if (!Files.isReadable(Paths.get(object.getOriginalFilePath()))) {
    	  throw new Exception("No Read permission to " + object.getOriginalFilePath());
      }
      if (compress) {
        tarFile = tarFile + ".gz";
        tarFileName = tarFileName + ".gz";
        if (!dryRun) {
          TarUtil.targz(tarFile, excludeFolders, directory);
        }
      } else {
        if (!dryRun) {
          TarUtil.tar(tarFile, excludeFolders, directory);
        }
      }

      // Update the record for upload
      File createdTarFile = new File(tarFile);
      object.setFilesize(createdTarFile.length());
      object.setSourceFileName(tarFileName);
      object.setSourceFilePath(tarFile);
      object.setTarEndTimestamp(new Date());
      object = dmeSyncWorkflowService.saveStatusInfo(object);

    } catch (Exception e) {
      logger.error("[{}] error {}", super.getTaskName(), e.getMessage(), e);
      throw new DmeSyncWorkflowException("Error occurred during tar. " + e.getMessage(), e);
    }

    return object;
  }
}
