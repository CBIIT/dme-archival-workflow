package gov.nih.nci.hpc.dmesync.validation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.dto.validation.FrameworkCollectionMetadataValidationRule;
import gov.nih.nci.hpc.dmesync.dto.validation.FrameworkDocRules;
import gov.nih.nci.hpc.dmesync.dto.validation.FrameworkDocRulesResponse;
import gov.nih.nci.hpc.dmesync.dto.validation.FrameworkRuleSet;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntries;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntry;
import gov.nih.nci.hpc.domain.metadata.HpcMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;

/**
 * Framework Metadata Validator
 * Validates HpcDataObjectRegistrationRequestDTO against framework rules from DME API
 * 
 * @author GitHub Copilot
 */
@Component
public class FrameworkMetadataValidator {
  
  protected Logger logger = LoggerFactory.getLogger(this.getClass());
  
  /**
   * Validate metadata against framework rules
   * 
   * @param statusInfo Status information
   * @param doc Document/DOC name
   * @param request HpcDataObjectRegistrationRequestDTO to validate
   * @param rulesResponse Framework rules from DME API
   * @param validateObjectMetadata Whether to validate object metadata
   * @throws DmeSyncMappingException if validation fails
   */
  public void validate(
      StatusInfo statusInfo,
      String doc, 
      HpcDataObjectRegistrationRequestDTO request,
      FrameworkDocRulesResponse rulesResponse,
      boolean validateObjectMetadata) throws DmeSyncMappingException {
    
    logger.debug("Starting framework metadata validation for doc: {}", doc);
    
    if (rulesResponse == null || rulesResponse.getDocRules() == null) {
      logger.warn("No framework rules found in response, skipping validation");
      return;
    }
    
    // Find rules for this doc
    FrameworkDocRules docRules = rulesResponse.getDocRules().stream()
        .filter(dr -> doc.equals(dr.getDoc()))
        .findFirst()
        .orElse(null);
    
    if (docRules == null || docRules.getRules() == null || docRules.getRules().isEmpty()) {
      logger.info("No framework rules configured for doc: {}, skipping validation", doc);
      return;
    }
    
    // Normalize request data
    Map<String, Map<String, List<String>>> collectionMetadata = normalizeCollectionMetadata(request);
    Map<String, List<String>> objectMetadata = validateObjectMetadata ? normalizeObjectMetadata(request) : null;
    
    // Collect all validation errors
    List<String> errors = new ArrayList<>();
    
    // Validate against each rule set
    for (FrameworkRuleSet ruleSet : docRules.getRules()) {
      if (ruleSet.getCollectionMetadataValidationRules() == null) {
        continue;
      }
      
      validateRuleSet(ruleSet, collectionMetadata, objectMetadata, errors);
    }
    
    // If there are validation errors, throw exception
    if (!errors.isEmpty()) {
      String errorMessage = buildErrorMessage(doc, statusInfo, errors);
      logger.error("Framework metadata validation failed: {}", errorMessage);
      throw new DmeSyncMappingException(errorMessage);
    }
    
    logger.info("Framework metadata validation passed for doc: {}", doc);
  }
  
  /**
   * Normalize collection metadata from request into a map structure
   */
  private Map<String, Map<String, List<String>>> normalizeCollectionMetadata(
      HpcDataObjectRegistrationRequestDTO request) {
    
    Map<String, Map<String, List<String>>> result = new HashMap<>();
    
    HpcBulkMetadataEntries entries = request.getParentCollectionsBulkMetadataEntries();
    if (entries == null || entries.getPathsMetadataEntries() == null) {
      return result;
    }
    
    for (HpcBulkMetadataEntry bulkEntry : entries.getPathsMetadataEntries()) {
      String path = bulkEntry.getPath();
      Map<String, List<String>> metadata = new HashMap<>();
      
      if (bulkEntry.getPathMetadataEntries() != null) {
        for (HpcMetadataEntry entry : bulkEntry.getPathMetadataEntries()) {
          String attr = entry.getAttribute();
          String value = entry.getValue();
          
          metadata.computeIfAbsent(attr, k -> new ArrayList<>()).add(value);
        }
      }
      
      result.put(path, metadata);
    }
    
    return result;
  }
  
  /**
   * Normalize object metadata from request into a map structure
   */
  private Map<String, List<String>> normalizeObjectMetadata(HpcDataObjectRegistrationRequestDTO request) {
    Map<String, List<String>> result = new HashMap<>();
    
    // Add metadata entries
    if (request.getMetadataEntries() != null) {
      for (HpcMetadataEntry entry : request.getMetadataEntries()) {
        result.computeIfAbsent(entry.getAttribute(), k -> new ArrayList<>()).add(entry.getValue());
      }
    }
    
    // Add extracted metadata entries
    if (request.getExtractedMetadataEntries() != null) {
      for (HpcMetadataEntry entry : request.getExtractedMetadataEntries()) {
        result.computeIfAbsent(entry.getAttribute(), k -> new ArrayList<>()).add(entry.getValue());
      }
    }
    
    return result;
  }
  
  /**
   * Validate a single rule set
   */
  private void validateRuleSet(
      FrameworkRuleSet ruleSet,
      Map<String, Map<String, List<String>>> collectionMetadata,
      Map<String, List<String>> objectMetadata,
      List<String> errors) {
    
    for (FrameworkCollectionMetadataValidationRule rule : ruleSet.getCollectionMetadataValidationRules()) {
      // Skip disabled rules
      if (Boolean.FALSE.equals(rule.getRuleEnabled())) {
        continue;
      }
      
      // Validate each collection path
      for (Map.Entry<String, Map<String, List<String>>> entry : collectionMetadata.entrySet()) {
        String path = entry.getKey();
        Map<String, List<String>> metadata = entry.getValue();
        
        // Check if this rule applies to this collection based on collectionTypes
        if (!isRuleApplicable(rule, metadata)) {
          continue;
        }
        
        validateRule(rule, path, metadata, ruleSet.getId(), errors);
      }
      
      // Validate object metadata if enabled
      if (objectMetadata != null) {
        validateRule(rule, "[object]", objectMetadata, ruleSet.getId(), errors);
      }
    }
  }
  
  /**
   * Check if a rule is applicable to a collection based on collectionTypes filter
   */
  private boolean isRuleApplicable(
      FrameworkCollectionMetadataValidationRule rule,
      Map<String, List<String>> metadata) {
    
    // If no collectionTypes specified, rule applies to all collections
    if (rule.getCollectionTypes() == null || rule.getCollectionTypes().isEmpty()) {
      return true;
    }
    
    // Get the collection_type value from metadata
    List<String> collectionTypeValues = metadata.get("collection_type");
    if (collectionTypeValues == null || collectionTypeValues.isEmpty()) {
      // No collection_type in metadata, so we can't filter by it
      // Rule does not apply if collectionTypes filter is specified
      return false;
    }
    
    // Check if any of the collection_type values match the rule's collectionTypes
    for (String collectionType : collectionTypeValues) {
      if (rule.getCollectionTypes().contains(collectionType)) {
        return true;
      }
    }
    
    return false;
  }
  
  /**
   * Validate a single rule against metadata
   */
  private void validateRule(
      FrameworkCollectionMetadataValidationRule rule,
      String path,
      Map<String, List<String>> metadata,
      String ruleSetId,
      List<String> errors) {
    
    String attribute = rule.getAttribute();
    List<String> values = metadata.get(attribute);
    
    // Check mandatory
    if (Boolean.TRUE.equals(rule.getMandatory())) {
      if (values == null || values.isEmpty() || values.stream().allMatch(StringUtils::isBlank)) {
        errors.add(String.format(
            "RuleSet[%s] Path[%s]: Mandatory attribute '%s' is missing or blank",
            ruleSetId, path, attribute));
        return; // No point checking validValues if attribute is missing
      }
    }
    
    // Check validValues
    if (rule.getValidValues() != null && !rule.getValidValues().isEmpty()) {
      if (values != null) {
        for (String value : values) {
          if (StringUtils.isNotBlank(value) && !rule.getValidValues().contains(value)) {
            errors.add(String.format(
                "RuleSet[%s] Path[%s]: Attribute '%s' has invalid value '%s'. Valid values: %s",
                ruleSetId, path, attribute, value, rule.getValidValues()));
          }
        }
      }
    }
  }
  
  /**
   * Build a readable error message from validation errors
   */
  private String buildErrorMessage(String doc, StatusInfo statusInfo, List<String> errors) {
    StringBuilder sb = new StringBuilder();
    sb.append("Framework metadata validation failed for doc '").append(doc).append("'");
    
    if (statusInfo != null && statusInfo.getOriginalFilePath() != null) {
      sb.append(" (file: ").append(statusInfo.getOriginalFilePath()).append(")");
    }
    
    sb.append(":\n");
    
    for (String error : errors) {
      sb.append("  - ").append(error).append("\n");
    }
    
    return sb.toString();
  }
}
