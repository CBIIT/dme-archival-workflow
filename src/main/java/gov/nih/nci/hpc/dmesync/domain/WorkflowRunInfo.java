package gov.nih.nci.hpc.dmesync.domain;

import javax.persistence.*;
import java.sql.Timestamp;

@Entity
@Table(name = "WORKFLOW_RUN_INFO")
public class WorkflowRunInfo {

	@Id
	@Column(name = "ID", nullable = false, precision = 0)
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "WORKFLOW_RUN_INFO_SEQ")
	@SequenceGenerator(name = "WORKFLOW_RUN_INFO_SEQ", sequenceName = "WORKFLOW_RUN_INFO_SEQ", allocationSize = 1)
	private Long id;

	@Column(name = "RUN_ID")
	private String runId;

	@Column(name = "WORKFLOW_ID", nullable = false)
	private String workflowId;

	@Column(name = "USER_ID")
	private String userId;

	@Column(name = "SERVER_ID", nullable = false)
	private String serverId;

	@Column(name = "STATUS", nullable = false)
	private String status;

	@Column(name = "RUN_START_TIMESTAMP", nullable = false)
	private Timestamp runStartTimestamp;

	@Column(name = "RUN_END_TIMESTAMP")
	private Timestamp runEndTimestamp;

	@Column(name = "DURATION")
	private Long duration;

	@Column(name = "UPLOADED_SIZE")
	private String uploadedSize;

	@Column(name = "SOURCE_PATH")
	private String sourcePath;

	@Column(name = "SETTINGS_HASH")
	private String settingsHash;

	@Column(name = "ERROR_MESSAGE")
	private String errorMessage;

	@Column(name = "RUN_LAST_HEARTBEAT_TIMESTAMP")
	private Timestamp runLastHeartbeatTimestamp;

	@Column(name = "THREADS")
	private Integer threads;

	@Column(name = "DME_SERVER_ID")
	private String dmeServerId;
	
	 @Column(name = "CRON_EXPRESSION")
	  private String cronExpression;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getRunId() {
		return runId;
	}

	public void setRunId(String runId) {
		this.runId = runId;
	}

	public String getWorkflowId() {
		return workflowId;
	}

	public void setWorkflowId(String workflowId) {
		this.workflowId = workflowId;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getServerId() {
		return serverId;
	}

	public void setServerId(String serverId) {
		this.serverId = serverId;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public Timestamp getRunStartTimestamp() {
		return runStartTimestamp;
	}

	public void setRunStartTimestamp(Timestamp runStartTimestamp) {
		this.runStartTimestamp = runStartTimestamp;
	}

	public Timestamp getRunEndTimestamp() {
		return runEndTimestamp;
	}

	public void setRunEndTimestamp(Timestamp runEndTimestamp) {
		this.runEndTimestamp = runEndTimestamp;
	}

	public Long getDuration() {
		return duration;
	}

	public void setDuration(Long duration) {
		this.duration = duration;
	}

	public String getUploadedSize() {
		return uploadedSize;
	}

	public void setUploadedSize(String uploadedSize) {
		this.uploadedSize = uploadedSize;
	}

	public String getSourcePath() {
		return sourcePath;
	}

	public void setSourcePath(String sourcePath) {
		this.sourcePath = sourcePath;
	}

	public String getSettingsHash() {
		return settingsHash;
	}

	public void setSettingsHash(String settingsHash) {
		this.settingsHash = settingsHash;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public Timestamp getRunLastHeartbeatTimestamp() {
		return runLastHeartbeatTimestamp;
	}

	public void setRunLastHeartbeatTimestamp(Timestamp runLastHeartbeatTimestamp) {
		this.runLastHeartbeatTimestamp = runLastHeartbeatTimestamp;
	}

	public Integer getThreads() {
		return threads;
	}

	public void setThreads(Integer threads) {
		this.threads = threads;
	}

	public String getDmeServerId() {
		return dmeServerId;
	}

	public void setDmeServerId(String dmeServerId) {
		this.dmeServerId = dmeServerId;
	}

	public String getCronExpression() {
		return cronExpression;
	}

	public void setCronExpression(String cronExpression) {
		this.cronExpression = cronExpression;
	}
	
	
}
