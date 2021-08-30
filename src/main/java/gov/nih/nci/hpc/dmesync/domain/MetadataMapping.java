package gov.nih.nci.hpc.dmesync.domain;

import javax.persistence.*;

@Entity
public class MetadataMapping {
  @Id @GeneratedValue private Long id;
  private String doc;
  private String collectionType;
  private String collectionName;
  private String metaDataKey;
  private String metaDataValue;

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

  public String getCollectionType() {
    return collectionType;
  }

  public void setCollectionType(String collectionType) {
    this.collectionType = collectionType;
  }

  public String getCollectionName() {
    return collectionName;
  }

  public void setCollectionName(String collectionName) {
    this.collectionName = collectionName;
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
