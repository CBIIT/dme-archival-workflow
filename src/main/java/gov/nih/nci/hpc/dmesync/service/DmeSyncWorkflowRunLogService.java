package gov.nih.nci.hpc.dmesync.service;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import gov.nih.nci.hpc.dmesync.domain.WorkflowRunInfo;



@Service
public interface DmeSyncWorkflowRunLogService {


	
	public WorkflowRunInfo findFirstByRunIdAndUserId(String runId,String doc);

	/** Update only the heartbeat timestamp */
	
	public void logWorkflowRunStartHeartbeat(Long id) ;

	/** Mark end state; durationMinutes may be null (we'll compute if null). */
	
	public void updateWorkflowRunEnd(String runId, String doc, String finalStatus, String errorMessage) ;

	
	public  WorkflowRunInfo saveWorkflowRunInfo(WorkflowRunInfo workflowRunInfo);
}
