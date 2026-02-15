package gov.nih.nci.hpc.dmesync.dto.validation;

import java.util.List;

/**
 * Framework Rule Set DTO
 * 
 * @author GitHub Copilot
 */
public class FrameworkRuleSet {
  
  private String id;
  private String basePath;
  private List<FrameworkCollectionMetadataValidationRule> collectionMetadataValidationRules;
  
  public String getId() {
    return id;
  }
  
  public void setId(String id) {
    this.id = id;
  }
  
  public String getBasePath() {
    return basePath;
  }
  
  public void setBasePath(String basePath) {
    this.basePath = basePath;
  }
  
  public List<FrameworkCollectionMetadataValidationRule> getCollectionMetadataValidationRules() {
    return collectionMetadataValidationRules;
  }
  
  public void setCollectionMetadataValidationRules(
      List<FrameworkCollectionMetadataValidationRule> collectionMetadataValidationRules) {
    this.collectionMetadataValidationRules = collectionMetadataValidationRules;
  }
}
