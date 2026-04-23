package gov.nih.nci.hpc.dmesync.dao;

import org.springframework.data.jpa.repository.*;
import gov.nih.nci.hpc.dmesync.domain.WorkflowRunInfo;

public interface WorkflowRunInfoDao<T extends WorkflowRunInfo> extends JpaRepository<T, Long>, WorkflowRunInfoDaoCustom {

	WorkflowRunInfo findFirstByRunIdAndDoc(String runId, String doc);

}
