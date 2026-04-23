package gov.nih.nci.hpc.dmesync.dao;

import java.time.Instant;

public interface WorkflowRunInfoDaoCustom {

	Instant findLastScheduledTime(Long docId);
}
