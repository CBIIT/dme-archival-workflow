package gov.nih.nci.hpc.dmesync.dto.validation;

import java.util.List;

/**
 * Framework Collection Metadata Validation Rule DTO
 * 
 * @author GitHub Copilot
 */
public class FrameworkCollectionMetadataValidationRule {
  
  private String attribute;
  private Boolean mandatory;
  private List<String> validValues;
  private List<String> collectionTypes;
  private Boolean ruleEnabled;
  private String defaultValue;
  private Boolean encrypted;
  private String description;
  
  public String getAttribute() {
    return attribute;
  }
  
  public void setAttribute(String attribute) {
    this.attribute = attribute;
  }
  
  public Boolean getMandatory() {
    return mandatory;
  }
  
  public void setMandatory(Boolean mandatory) {
    this.mandatory = mandatory;
  }
  
  public List<String> getValidValues() {
    return validValues;
  }
  
  public void setValidValues(List<String> validValues) {
    this.validValues = validValues;
  }
  
  public List<String> getCollectionTypes() {
    return collectionTypes;
  }
  
  public void setCollectionTypes(List<String> collectionTypes) {
    this.collectionTypes = collectionTypes;
  }
  
  public Boolean getRuleEnabled() {
    return ruleEnabled;
  }
  
  public void setRuleEnabled(Boolean ruleEnabled) {
    this.ruleEnabled = ruleEnabled;
  }
  
  public String getDefaultValue() {
    return defaultValue;
  }
  
  public void setDefaultValue(String defaultValue) {
    this.defaultValue = defaultValue;
  }
  
  public Boolean getEncrypted() {
    return encrypted;
  }
  
  public void setEncrypted(Boolean encrypted) {
    this.encrypted = encrypted;
  }
  
  public String getDescription() {
    return description;
  }
  
  public void setDescription(String description) {
    this.description = description;
  }
}
