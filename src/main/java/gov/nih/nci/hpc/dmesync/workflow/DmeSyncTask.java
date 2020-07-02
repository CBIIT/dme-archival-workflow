package gov.nih.nci.hpc.dmesync.workflow;

import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;

/**
 * DME Sync Task Interface
 * 
 * @author dinhys
 *
 */
public interface DmeSyncTask {

  /**
   * Process the task
   * 
   * @param statusInfo StatusInfo object
   * @return StatusInfo
   * @throws DmeSyncMappingException on mapping error
   * @throws DmeSyncWorkflowException on workflow error
   */
  StatusInfo processTask(StatusInfo statusInfo)
      throws DmeSyncMappingException, DmeSyncWorkflowException;
}
