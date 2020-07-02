package gov.nih.nci.hpc.dmesync.domain;

import javax.persistence.*;

@Entity
public class MetadataInfo {
  @Id @GeneratedValue private Long id;
  private Long objectId; //StatusInfo.id
  private String metaDataKey;
  private String metaDataValue;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Long getObjectId() {
    return objectId;
  }

  public void setObjectId(Long objectId) {
    this.objectId = objectId;
  }

  public String getMetaDataKey() {
    return metaDataKey;
  }

  public void setMetaDataKey(String metaDataKey) {
    this.metaDataKey = metaDataKey;
  }

  public String getMetaDataValue() {
    return metaDataValue;
  }

  public void setMetaDataValue(String metaDataValue) {
    this.metaDataValue = metaDataValue;
  }
}
