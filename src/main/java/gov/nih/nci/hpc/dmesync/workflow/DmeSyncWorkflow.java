package gov.nih.nci.hpc.dmesync.workflow;

import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;

/**
 * DME Sync Workflow Interface
 * 
 * @author dinhys
 */
public interface DmeSyncWorkflow {

  /**
   * Start the workflow
   * 
   * @param statusInfo StatusInfo object
   * @throws DmeSyncWorkflowException on workflow error
   */
  void start(StatusInfo statusInfo) throws DmeSyncWorkflowException;
}
