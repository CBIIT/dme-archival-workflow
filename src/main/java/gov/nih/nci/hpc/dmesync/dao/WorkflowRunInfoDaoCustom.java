package gov.nih.nci.hpc.dmesync.dao;

import java.time.Instant;

public interface WorkflowRunInfoDaoCustom {

	public Instant findLastScheduledTime(Long docId);
	
	public void resetWorkflowRunInfo();
}
