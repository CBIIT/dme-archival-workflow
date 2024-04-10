package gov.nih.nci.hpc.dmesync.domain;

import java.util.Date;
import javax.persistence.*;

import gov.nih.nci.hpc.dto.datamanagement.HpcArchivePermissionsRequestDTO;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;

@Entity
public class StatusInfo {
  private Long id;
  private String doc;
  private String runId;
  private String orginalFileName;
  private String originalFilePath;
  private String sourceFileName; //work directory, same as original if no pre-processing
  private String sourceFilePath; //work directory, same as original if no pre-processing
  private String fullDestinationPath;
  private Long filesize; //in bytes,
  private String checksum;
  private String status;
  private Date startTimestamp;
  private Date endTimestamp;
  private Date uploadStartTimestamp;
  private Date uploadEndTimestamp;
  private Date tarStartTimestamp;
  private Date tarEndTimestamp;
  private Long retryCount = 0L;
  private String error;
  private String moveDataObjectOrignalPath;
  private Integer tarContentsCount;
  private HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO;
  private HpcArchivePermissionsRequestDTO archivePermissionsRequestDTO;

  @Id
  @Column(name = "ID", nullable = false, precision = 0)
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "STATUS_INFO_SEQ")
  @SequenceGenerator(name = "STATUS_INFO_SEQ", sequenceName = "STATUS_INFO_SEQ", allocationSize = 1)
  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getDoc() {
    return doc;
  }

  public void setDoc(String doc) {
    this.doc = doc;
  }
  
  public String getRunId() {
    return runId;
  }

  public void setRunId(String runId) {
    this.runId = runId;
  }

  public String getOrginalFileName() {
    return orginalFileName;
  }

  public void setOrginalFileName(String orginalFileName) {
    this.orginalFileName = orginalFileName;
  }

  public String getOriginalFilePath() {
    return originalFilePath;
  }

  public void setOriginalFilePath(String originalFilePath) {
    this.originalFilePath = originalFilePath;
  }

  public String getSourceFileName() {
    return sourceFileName;
  }

  public void setSourceFileName(String sourceFileName) {
    this.sourceFileName = sourceFileName;
  }

  public String getSourceFilePath() {
    return sourceFilePath;
  }

  public void setSourceFilePath(String sourceFilePath) {
    this.sourceFilePath = sourceFilePath;
  }

  public String getFullDestinationPath() {
    return fullDestinationPath;
  }

  public void setFullDestinationPath(String fullDestinationPath) {
    this.fullDestinationPath = fullDestinationPath;
  }

  public Long getFilesize() {
    return filesize;
  }

  public void setFilesize(Long filesize) {
    this.filesize = filesize;
  }

  public String getChecksum() {
    return checksum;
  }

  public void setChecksum(String checksum) {
    this.checksum = checksum;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public Date getStartTimestamp() {
    return startTimestamp;
  }

  public void setStartTimestamp(Date startTimestamp) {
    this.startTimestamp = startTimestamp;
  }

  public Date getEndTimestamp() {
    return endTimestamp;
  }

  public void setEndTimestamp(Date endTimestamp) {
    this.endTimestamp = endTimestamp;
  }

  public Date getUploadStartTimestamp() {
    return uploadStartTimestamp;
  }

  public void setUploadStartTimestamp(Date uploadStartTimestamp) {
    this.uploadStartTimestamp = uploadStartTimestamp;
  }

  public Date getUploadEndTimestamp() {
    return uploadEndTimestamp;
  }

  public void setUploadEndTimestamp(Date uploadEndTimestamp) {
    this.uploadEndTimestamp = uploadEndTimestamp;
  }

  public Date getTarStartTimestamp() {
  return tarStartTimestamp;}

  public void setTarStartTimestamp(Date tarStartTimestamp) {
  this.tarStartTimestamp = tarStartTimestamp;}

  public Date getTarEndTimestamp() {
  return tarEndTimestamp;}

  public void setTarEndTimestamp(Date tarEndTimestamp) {
  this.tarEndTimestamp = tarEndTimestamp;}

  public Long getRetryCount() {
    return retryCount;
  }

  public void setRetryCount(Long retryCount) {
    this.retryCount = retryCount;
  }

  public String getError() {
    return error;
  }

  public void setError(String error) {
    this.error = error;
  }
  

	public Integer getTarContentsCount() {
		return tarContentsCount;
	}

	public void setTarContentsCount(Integer tarContentsCount) {
		this.tarContentsCount = tarContentsCount;
	}

@Transient
  public String getMoveDataObjectOrignalPath() {
	return moveDataObjectOrignalPath;
  }

  @Transient
  public void setMoveDataObjectOrignalPath(String moveDataObjectOrignalPath) {
	this.moveDataObjectOrignalPath = moveDataObjectOrignalPath;
  }

  @Transient
  public HpcDataObjectRegistrationRequestDTO getDataObjectRegistrationRequestDTO() {
    return dataObjectRegistrationRequestDTO;
  }

  @Transient
  public void setDataObjectRegistrationRequestDTO(
      HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO) {
    this.dataObjectRegistrationRequestDTO = dataObjectRegistrationRequestDTO;
  }

  @Transient
  public HpcArchivePermissionsRequestDTO getArchivePermissionsRequestDTO() {
	return archivePermissionsRequestDTO;
  }

  @Transient
  public void setArchivePermissionsRequestDTO(HpcArchivePermissionsRequestDTO archivePermissionsRequestDTO) {
	this.archivePermissionsRequestDTO = archivePermissionsRequestDTO;
  }

@Override
  public String toString() {
    return "StatusInfo [id="
        + id
        + ", runId="
        + runId
        + ", orginalFileName="
        + orginalFileName
        + ", originalFilePath="
        + originalFilePath
        + ", sourceFileName="
        + sourceFileName
        + ", sourceFilePath="
        + sourceFilePath
        + ", fullDestinationPath="
        + fullDestinationPath
        + ", filesize="
        + filesize
        + ", checksum="
        + checksum
        + ", status="
        + status
        + ", startTimestamp="
        + startTimestamp
        + ", endTimestamp="
        + endTimestamp
        + ", uploadStartTimestamp="
        + uploadStartTimestamp
        + ", uploadEndTimestamp="
        + uploadEndTimestamp
        + ", tarStartTimestamp="
        + tarStartTimestamp
        + ", tarEndTimestamp="
        + tarEndTimestamp
        + ", tarContentsCount="
        + tarContentsCount
        + ", retryCount="
        + retryCount
        + ", error="
        + error
        + ", dataObjectRegistrationRequestDTO="
        + dataObjectRegistrationRequestDTO
        + "]";
  }
}
