package gov.nih.nci.hpc.dmesync.domain;

import javax.persistence.*;

@Entity
public class CollectionNameMapping {
  @Id @GeneratedValue private Long id;
  private String mapKey;
  private String collectionType;
  private String mapValue;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getMapKey() {
    return mapKey;
  }

  public void setMapKey(String mapKey) {
    this.mapKey = mapKey;
  }

  public String getCollectionType() {
    return collectionType;
  }

  public void setCollectionType(String collectionType) {
    this.collectionType = collectionType;
  }

  public String getMapValue() {
    return mapValue;
  }

  public void setMapValue(String mapValue) {
    this.mapValue = mapValue;
  }
}
