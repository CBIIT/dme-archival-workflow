package gov.nih.nci.hpc.dmesync.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import gov.nih.nci.hpc.dmesync.dao.StatusInfoDao;
import gov.nih.nci.hpc.dmesync.dao.WorkflowRunInfoDao;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.domain.WorkflowRunInfo;
import gov.nih.nci.hpc.dmesync.service.DmeSyncWorkflowRunLogService;
import gov.nih.nci.hpc.dmesync.util.ExcelUtil;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

@Service
@Transactional
public class DmeSyncWorkflowRunLogServiceImpl implements DmeSyncWorkflowRunLogService {

	@Autowired
	protected WorkflowRunInfoDao<WorkflowRunInfo> workflowRunInfoDao;

	@Autowired
	protected StatusInfoDao<StatusInfo> statusInfoDao;

	@Override
	public WorkflowRunInfo saveWorkflowRunInfo(WorkflowRunInfo workflowRunInfo) {
		return workflowRunInfoDao.save(workflowRunInfo);
	}

	@Override
	public WorkflowRunInfo findFirstByRunIdAndUserId(String runId, String doc) {
		return workflowRunInfoDao.findFirstByRunIdAndUserId(runId, doc);
	}

	@Override
	public void logWorkflowRunStartHeartbeat(Long id) {
	}

	@Override
	public void updateWorkflowRunEnd(String runId, String doc, String finalStatus, String errorMessage) {

		WorkflowRunInfo workflowRunInfo = workflowRunInfoDao.findFirstByRunIdAndUserId(runId, doc);

		// If duration not provided, compute in app from start/end timestamps
		if (workflowRunInfo != null) {
			// Compute Duration Mins
			Long durationMinutes;
			WorkflowRunInfo e = workflowRunInfoDao.findById(workflowRunInfo.getId())
					.orElseThrow(() -> new IllegalArgumentException("Run not found: " + workflowRunInfo.getId()));
			Instant start = e.getRunStartTimestamp().toInstant();
			long mins = Math.max(0, Duration.between(start, Instant.now()).toMinutes());

			// Compute Uploaded Size

			List<StatusInfo> runIdRows = statusInfoDao.findByRunIdAndDoc(runId, doc);

			long totalSize = runIdRows.stream().filter(f -> "COMPLETED".equalsIgnoreCase(f.getStatus()))
					.map(StatusInfo::getFilesize).filter(Objects::nonNull).mapToLong(Long::longValue).sum();

			durationMinutes = mins;
			workflowRunInfo.setRunLastHeartbeatTimestamp(Timestamp.from(Instant.now()));
			workflowRunInfo.setRunEndTimestamp(Timestamp.from(Instant.now()));
			workflowRunInfo.setDuration(durationMinutes);
			workflowRunInfo.setStatus(finalStatus);
			workflowRunInfo.setErrorMessage(errorMessage);
			workflowRunInfo.setUploadedSize(ExcelUtil.humanReadableByteCount(Long.valueOf(totalSize), true));
			workflowRunInfoDao.save(workflowRunInfo);
		}
	}

}
