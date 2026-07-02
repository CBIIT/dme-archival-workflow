package gov.nih.nci.hpc.dmesync.service;

import gov.nih.nci.hpc.dmesync.domain.WorkflowRunInfo;

/**
 * DME Sync Workflow Log Service Interface
 * 
 * @author konerum3
 */

public interface DmeSyncWorkflowRunLogService {

	/**
	 * findFirstByRunIdAndUserId : retrive the workflowRunId for the workflow run
	 * 
	 * @param runId run_id
	 * @param doc   doc name
	 * @return WorkflowRunInfo information
	 */

	public WorkflowRunInfo findFirstByRunIdAndUserId(String runId, String doc);

	/**
	 * update Only the Heartbeat : *
	 * 
	 * @param Id WorkflowRunInfo_id
	 * @return null
	 */

	public void logWorkflowRunStartHeartbeat(Long id);

	/**
	 * Update workflow run end information, including run end time, duration,
	 * status, error message, and uploaded size.
	 * 
	 * @param runId        workflow run identifier
	 * @param doc          document name or user identifier associated with the run
	 * @param finalStatus  final status of the workflow run
	 * @param errorMessage error message associated with the workflow run, if any
	 */

	public void updateWorkflowRunEnd(String runId, String doc, String finalStatus, String errorMessage);

	/**
	 * Save workflow Run Info information*
	 * 
	 * @param WorkflowRunInfo WorkflowRunInfo
	 * @return updated WorkflowRunInfo information
	 */
	public WorkflowRunInfo saveWorkflowRunInfo(WorkflowRunInfo WorkflowRunInfo);
}
