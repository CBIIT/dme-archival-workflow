package gov.nih.nci.hpc.dmesync.dto;

import java.io.Serializable;

public class DmeSyncMessageDto implements Serializable {

  private Long objectId;

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

  @Override
  public String toString() {
    return "SyncMessage [objectId=" + objectId.toString() + "]";
  }
}
