package gov.nih.nci.hpc.dmesync.dto.validation;

import java.util.List;

/**
 * Framework Doc Rules DTO
 * 
 * @author GitHub Copilot
 */
public class FrameworkDocRules {
  
  private String doc;
  private List<FrameworkRuleSet> rules;
  
  public String getDoc() {
    return doc;
  }
  
  public void setDoc(String doc) {
    this.doc = doc;
  }
  
  public List<FrameworkRuleSet> getRules() {
    return rules;
  }
  
  public void setRules(List<FrameworkRuleSet> rules) {
    this.rules = rules;
  }
}
