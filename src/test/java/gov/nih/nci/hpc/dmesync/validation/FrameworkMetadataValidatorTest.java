package gov.nih.nci.hpc.dmesync.validation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;

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
 * Test class for FrameworkMetadataValidator
 * 
 * @author GitHub Copilot
 */
@RunWith(SpringRunner.class)
public class FrameworkMetadataValidatorTest {
  
  private FrameworkMetadataValidator validator;
  
  @Before
  public void setup() {
    validator = new FrameworkMetadataValidator();
  }
  
  /**
   * Test that validation passes when all mandatory attributes are present
   */
  @Test
  public void testValidationPassesWithValidMetadata() throws DmeSyncMappingException {
    // Setup rules
    FrameworkDocRulesResponse rulesResponse = createRulesResponse("TEST",
        createRule("data_owner", true, null, Arrays.asList("PI_Lab")),
        createRule("collection_type", true, Arrays.asList("Project", "PI_Lab"), null)
    );
    
    // Setup request with valid metadata
    HpcDataObjectRegistrationRequestDTO request = new HpcDataObjectRegistrationRequestDTO();
    HpcBulkMetadataEntries bulkEntries = new HpcBulkMetadataEntries();
    List<HpcBulkMetadataEntry> entries = new ArrayList<>();
    
    // PI_Lab collection with required attributes
    HpcBulkMetadataEntry piLabEntry = new HpcBulkMetadataEntry();
    piLabEntry.setPath("/TEST_Archive/PI_001");
    List<HpcMetadataEntry> piLabMetadata = new ArrayList<>();
    piLabMetadata.add(createMetadataEntry("collection_type", "PI_Lab"));
    piLabMetadata.add(createMetadataEntry("data_owner", "John Doe"));
    piLabEntry.getPathMetadataEntries().addAll(piLabMetadata);
    entries.add(piLabEntry);
    
    bulkEntries.getPathsMetadataEntries().addAll(entries);
    request.setParentCollectionsBulkMetadataEntries(bulkEntries);
    
    StatusInfo statusInfo = new StatusInfo();
    statusInfo.setOriginalFilePath("/test/file.txt");
    
    // Should not throw exception
    validator.validate(statusInfo, "TEST", request, rulesResponse, false);
  }
  
  /**
   * Test that validation fails when mandatory attribute is missing
   */
  @Test
  public void testValidationFailsWithMissingMandatoryAttribute() {
    // Setup rules - data_owner is mandatory for PI_Lab
    FrameworkDocRulesResponse rulesResponse = createRulesResponse("TEST",
        createRule("data_owner", true, null, Arrays.asList("PI_Lab")),
        createRule("collection_type", true, Arrays.asList("Project", "PI_Lab"), null)
    );
    
    // Setup request without data_owner
    HpcDataObjectRegistrationRequestDTO request = new HpcDataObjectRegistrationRequestDTO();
    HpcBulkMetadataEntries bulkEntries = new HpcBulkMetadataEntries();
    List<HpcBulkMetadataEntry> entries = new ArrayList<>();
    
    HpcBulkMetadataEntry piLabEntry = new HpcBulkMetadataEntry();
    piLabEntry.setPath("/TEST_Archive/PI_001");
    List<HpcMetadataEntry> piLabMetadata = new ArrayList<>();
    piLabMetadata.add(createMetadataEntry("collection_type", "PI_Lab"));
    // Missing data_owner
    piLabEntry.getPathMetadataEntries().addAll(piLabMetadata);
    entries.add(piLabEntry);
    
    bulkEntries.getPathsMetadataEntries().addAll(entries);
    request.setParentCollectionsBulkMetadataEntries(bulkEntries);
    
    StatusInfo statusInfo = new StatusInfo();
    statusInfo.setOriginalFilePath("/test/file.txt");
    
    try {
      validator.validate(statusInfo, "TEST", request, rulesResponse, false);
      fail("Expected DmeSyncMappingException to be thrown");
    } catch (DmeSyncMappingException e) {
      assertTrue(e.getMessage().contains("data_owner"));
      assertTrue(e.getMessage().contains("missing or blank"));
    }
  }
  
  /**
   * Test that validation fails when value is not in validValues list
   */
  @Test
  public void testValidationFailsWithInvalidValue() {
    // Setup rules with validValues constraint
    FrameworkDocRulesResponse rulesResponse = createRulesResponse("TEST",
        createRule("collection_type", true, Arrays.asList("Project", "PI_Lab", "Sample"), null)
    );
    
    // Setup request with invalid collection_type value
    HpcDataObjectRegistrationRequestDTO request = new HpcDataObjectRegistrationRequestDTO();
    HpcBulkMetadataEntries bulkEntries = new HpcBulkMetadataEntries();
    List<HpcBulkMetadataEntry> entries = new ArrayList<>();
    
    HpcBulkMetadataEntry entry = new HpcBulkMetadataEntry();
    entry.setPath("/TEST_Archive/PI_001");
    List<HpcMetadataEntry> metadata = new ArrayList<>();
    metadata.add(createMetadataEntry("collection_type", "InvalidType"));
    entry.getPathMetadataEntries().addAll(metadata);
    entries.add(entry);
    
    bulkEntries.getPathsMetadataEntries().addAll(entries);
    request.setParentCollectionsBulkMetadataEntries(bulkEntries);
    
    StatusInfo statusInfo = new StatusInfo();
    
    try {
      validator.validate(statusInfo, "TEST", request, rulesResponse, false);
      fail("Expected DmeSyncMappingException to be thrown");
    } catch (DmeSyncMappingException e) {
      assertTrue(e.getMessage().contains("invalid value"));
      assertTrue(e.getMessage().contains("InvalidType"));
    }
  }
  
  /**
   * Test that rules with collectionTypes filter only apply to matching collections
   */
  @Test
  public void testRuleWithCollectionTypesOnlyAppliesWhenMatching() throws DmeSyncMappingException {
    // Setup rules - data_owner is mandatory only for PI_Lab
    FrameworkDocRulesResponse rulesResponse = createRulesResponse("TEST",
        createRule("data_owner", true, null, Arrays.asList("PI_Lab")),
        createRule("collection_type", true, Arrays.asList("Project", "PI_Lab"), null)
    );
    
    // Setup request with Project collection (no data_owner required)
    HpcDataObjectRegistrationRequestDTO request = new HpcDataObjectRegistrationRequestDTO();
    HpcBulkMetadataEntries bulkEntries = new HpcBulkMetadataEntries();
    List<HpcBulkMetadataEntry> entries = new ArrayList<>();
    
    HpcBulkMetadataEntry projectEntry = new HpcBulkMetadataEntry();
    projectEntry.setPath("/TEST_Archive/Project_001");
    List<HpcMetadataEntry> projectMetadata = new ArrayList<>();
    projectMetadata.add(createMetadataEntry("collection_type", "Project"));
    // No data_owner - should be OK for Project
    projectEntry.getPathMetadataEntries().addAll(projectMetadata);
    entries.add(projectEntry);
    
    bulkEntries.getPathsMetadataEntries().addAll(entries);
    request.setParentCollectionsBulkMetadataEntries(bulkEntries);
    
    StatusInfo statusInfo = new StatusInfo();
    
    // Should not throw exception because data_owner rule only applies to PI_Lab
    validator.validate(statusInfo, "TEST", request, rulesResponse, false);
  }
  
  /**
   * Test that disabled rules are skipped
   */
  @Test
  public void testDisabledRulesAreSkipped() throws DmeSyncMappingException {
    // Setup rules with one disabled rule
    FrameworkCollectionMetadataValidationRule disabledRule = createRule("data_owner", true, null, null);
    disabledRule.setRuleEnabled(false); // Disable the rule
    
    FrameworkDocRulesResponse rulesResponse = createRulesResponse("TEST", disabledRule);
    
    // Setup request without data_owner (which would fail if rule was enabled)
    HpcDataObjectRegistrationRequestDTO request = new HpcDataObjectRegistrationRequestDTO();
    HpcBulkMetadataEntries bulkEntries = new HpcBulkMetadataEntries();
    List<HpcBulkMetadataEntry> entries = new ArrayList<>();
    
    HpcBulkMetadataEntry entry = new HpcBulkMetadataEntry();
    entry.setPath("/TEST_Archive/PI_001");
    List<HpcMetadataEntry> metadata = new ArrayList<>();
    metadata.add(createMetadataEntry("collection_type", "PI_Lab"));
    // Missing data_owner - but rule is disabled
    entry.getPathMetadataEntries().addAll(metadata);
    entries.add(entry);
    
    bulkEntries.getPathsMetadataEntries().addAll(entries);
    request.setParentCollectionsBulkMetadataEntries(bulkEntries);
    
    StatusInfo statusInfo = new StatusInfo();
    
    // Should not throw exception because rule is disabled
    validator.validate(statusInfo, "TEST", request, rulesResponse, false);
  }
  
  /**
   * Test validation with no rules configured for doc
   */
  @Test
  public void testValidationPassesWhenNoRulesForDoc() throws DmeSyncMappingException {
    // Setup rules for different doc
    FrameworkDocRulesResponse rulesResponse = createRulesResponse("OTHER_DOC",
        createRule("data_owner", true, null, null)
    );
    
    // Setup any request
    HpcDataObjectRegistrationRequestDTO request = new HpcDataObjectRegistrationRequestDTO();
    StatusInfo statusInfo = new StatusInfo();
    
    // Should not throw exception - no rules for "TEST" doc
    validator.validate(statusInfo, "TEST", request, rulesResponse, false);
  }
  
  // Helper methods
  
  private FrameworkDocRulesResponse createRulesResponse(String doc, FrameworkCollectionMetadataValidationRule... rules) {
    FrameworkDocRulesResponse response = new FrameworkDocRulesResponse();
    List<FrameworkDocRules> docRulesList = new ArrayList<>();
    
    FrameworkDocRules docRules = new FrameworkDocRules();
    docRules.setDoc(doc);
    
    List<FrameworkRuleSet> ruleSets = new ArrayList<>();
    FrameworkRuleSet ruleSet = new FrameworkRuleSet();
    ruleSet.setId("test-rule-set-1");
    ruleSet.setBasePath("/TEST_Archive");
    ruleSet.setCollectionMetadataValidationRules(Arrays.asList(rules));
    ruleSets.add(ruleSet);
    
    docRules.setRules(ruleSets);
    docRulesList.add(docRules);
    
    response.setDocRules(docRulesList);
    return response;
  }
  
  private FrameworkCollectionMetadataValidationRule createRule(
      String attribute, 
      Boolean mandatory, 
      List<String> validValues,
      List<String> collectionTypes) {
    FrameworkCollectionMetadataValidationRule rule = new FrameworkCollectionMetadataValidationRule();
    rule.setAttribute(attribute);
    rule.setMandatory(mandatory);
    rule.setValidValues(validValues);
    rule.setCollectionTypes(collectionTypes);
    rule.setRuleEnabled(true); // Enabled by default
    return rule;
  }
  
  private HpcMetadataEntry createMetadataEntry(String attribute, String value) {
    HpcMetadataEntry entry = new HpcMetadataEntry();
    entry.setAttribute(attribute);
    entry.setValue(value);
    return entry;
  }
}
