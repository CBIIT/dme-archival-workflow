package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.io.File;
import java.io.IOException;
import javax.annotation.PostConstruct;
import org.springframework.stereotype.Component;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncTask;

/**
 * DME Sync Checksum Task Implementation
 * 
 * @author dinhys
 */
@Component
public class DmeSyncCreateChecksumTaskImpl extends AbstractDmeSyncTask implements DmeSyncTask {

  @PostConstruct
  public boolean init() {
    super.setTaskName("ChecksumTask");
    return true;
  }
  
  @Override
  public StatusInfo process(StatusInfo object)
      throws DmeSyncMappingException, DmeSyncWorkflowException {
 
    //Create checksum and record in db for verification task.
    try {

      File file = new File(object.getSourceFilePath());
      String checkSum = Files.hash(file, Hashing.md5()).toString();

      object.setChecksum(checkSum);
      StatusInfo copy = object;
      dmeSyncWorkflowService.saveStatusInfo(object);
      object = copy;

    } catch (IOException e) {
      logger.error("[{}] Error while creating checksum ", super.getTaskName(), e);
      throw new DmeSyncWorkflowException("Error while creating checksum", e);
    }
    
    return object;
  }
}
