package gov.nih.nci.hpc.dmesync.dto.validation;

import java.util.List;

/**
 * Framework Doc Rules Response DTO
 * 
 * @author GitHub Copilot
 */
public class FrameworkDocRulesResponse {
  
  private List<FrameworkDocRules> docRules;
  
  public List<FrameworkDocRules> getDocRules() {
    return docRules;
  }
  
  public void setDocRules(List<FrameworkDocRules> docRules) {
    this.docRules = docRules;
  }
}
