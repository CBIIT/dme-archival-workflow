package gov.nih.nci.hpc.dmesync.dao;

import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import gov.nih.nci.hpc.dmesync.domain.WorkflowRunInfo;

public interface WorkflowRunInfoDao<T extends WorkflowRunInfo> extends JpaRepository<T, Long> {

	WorkflowRunInfo findFirstByRunIdAndUserId(String runId, String doc);

}
