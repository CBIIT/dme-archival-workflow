package gov.nih.nci.hpc.dmesync.dto;

import java.io.Serializable;

public class DmeSyncMessageDto implements Serializable {

  private Long objectId;
  private Long configId;

  public DmeSyncMessageDto() {}

  public DmeSyncMessageDto(Long objectId) {
    this.objectId = objectId;
  }

  public Long getObjectId() {
    return objectId;
  }

  public void setObjectId(Long objectId) {
    this.objectId = objectId;
  }
  
  public Long getDocConfigId() {
	return configId;
  }
	
  public void setDocConfigId(Long configId) {
	this.configId = configId;
  }
  
  @Override
  public String toString() {
    return "SyncMessage [objectId=" + objectId.toString() + ", configId=" + configId.toString() + "]";
  }


}
