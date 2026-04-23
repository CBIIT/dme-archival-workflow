package gov.nih.nci.hpc.dmesync.workflow;

import gov.nih.nci.hpc.dmesync.domain.DocConfig;
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
   * @param docConfig DOC configuration
   * @throws DmeSyncWorkflowException on workflow error
   */
  void start(StatusInfo statusInfo, DocConfig docConfig) throws DmeSyncWorkflowException;
}