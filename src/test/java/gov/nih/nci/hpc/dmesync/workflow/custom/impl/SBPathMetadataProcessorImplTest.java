package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

import gov.nih.nci.hpc.dmesync.DmeSyncWorkflowServiceFactory;
import gov.nih.nci.hpc.dmesync.domain.CollectionNameMapping;
import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import gov.nih.nci.hpc.dmesync.domain.MetadataMapping;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.service.DmeSyncWorkflowService;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntry;
import gov.nih.nci.hpc.domain.metadata.HpcMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;
import gov.nih.nci.hpc.dmesync.workflow.impl.HpcEncryptor;

public class SBPathMetadataProcessorImplTest {

  //The service under test.
  SBPathMetadataProcessorImpl sbPathMetadataProcessorImpl;
  DocConfig config;
  DmeSyncWorkflowServiceFactory dmeSyncWorkflowServiceFactory;
  @Mock
  DmeSyncWorkflowService dmeSyncWorkflowService;
  @Mock
  HpcEncryptor encryptor;
  Map<String, String> dataMap;
  
  @BeforeEach
  public void init() throws DmeSyncMappingException, Exception {
    populateDataMap();
    MockitoAnnotations.openMocks(this);

    //Create the statusInfo object with the test Path
    DocConfig.SourceConfig sourceConfig = new DocConfig.SourceConfig("/data/CCRSB2/data", "/work", "/CCR_SB_Archive", 1);
    DocConfig.SourceRule sourceRule = new DocConfig.SourceRule(null, null, null, "", null, false, false, null, null, false, null, false, false, false, 0);
    config = new DocConfig(null, null, null, null, null, null, null, false, null, 0, null, null, sourceConfig, sourceRule, null, null, null, null);
    sbPathMetadataProcessorImpl = Mockito.spy(new SBPathMetadataProcessorImpl());
    // Use a mock for the service factory and service
    dmeSyncWorkflowServiceFactory = Mockito.mock(DmeSyncWorkflowServiceFactory.class);
    dmeSyncWorkflowService = Mockito.mock(DmeSyncWorkflowService.class);
    sbPathMetadataProcessorImpl.dmeSyncWorkflowService = dmeSyncWorkflowServiceFactory;
    Mockito.when(dmeSyncWorkflowServiceFactory.getService(any())).thenReturn(dmeSyncWorkflowService); 
    //Simulate the 2 CollectionNameMetadata rows that you will be retrieving from the DB.

    CollectionNameMapping piMapping = new CollectionNameMapping();
    piMapping.setMapValue("CCRSB2");
    Mockito.when(sbPathMetadataProcessorImpl.dmeSyncWorkflowService.getService("local")
            .findCollectionNameMappingByMapKeyAndCollectionTypeAndDoc("CCRSB2", "PI_Lab", "sb"))
        .thenReturn(piMapping);

    CollectionNameMapping userMapping = new CollectionNameMapping();
    userMapping.setMapValue("Surgery_Branch_NGS");
    Mockito.when(sbPathMetadataProcessorImpl.dmeSyncWorkflowService.getService("local")
            .findCollectionNameMappingByMapKeyAndCollectionTypeAndDoc("CCRSB2", "Project", "sb"))
        .thenReturn(userMapping);
    
    // Mock getAttrValueWithParitallyMatchingKey
    Mockito.doAnswer((Answer<String>) invocation -> {
    	Object[] args = invocation.getArguments();
    	String key = args[1].toString();
	    String partialKey = args[2].toString();
        return dataMap.get(key);
    }).when(sbPathMetadataProcessorImpl).getAttrValueWithParitallyMatchingKey(any(), any(), any(), any());
    
    Mockito.doReturn("").when(sbPathMetadataProcessorImpl).getCreationDate(any());
    Mockito.doReturn("").when(sbPathMetadataProcessorImpl).getModifiedDate(any());
    
    // Remove invalid doNothing() for non-void method
    // If needed, stub the value-returning overload to return null
    Mockito.doReturn(null).when(sbPathMetadataProcessorImpl).loadMetadataFile(Mockito.anyString(), Mockito.anyString());
  }

  private void populateDataMap() {
	dataMap = new HashMap<>();
    dataMap.put("data_owner", "Steven A. Rosenberg");
    dataMap.put("pi_lab", "NCI, CCR");
    dataMap.put("project_name", "Surgery_Branch_NGS");
    dataMap.put("project_title", "Surgery Branch Patient Sequencing");
    dataMap.put("project_description", "Contains all of the sequencing data for patients treated in the Surgery Branch");
    dataMap.put("lab_contact", "Paul Robbins");
    dataMap.put("bioinformatics_contact", "Jared Gartner");
    dataMap.put("affiliation", "NCI, CCR");
    //dataMap.put("patient_name", "Jane Doe");
    //dataMap.put("date_of_birth", "07/15/1999");
    //dataMap.put("patient_sex", "Female");
    dataMap.put("patient_name", "47qL+evVTqwGY8HECIb/ww==");
    dataMap.put("date_of_birth", "47qL+evVTqwGY8HECIb/ww==");
    dataMap.put("patient_sex", "Unknown");
    dataMap.put("description", "Sample patient description");
    dataMap.put("access", "Closed Access");
    dataMap.put("patient_id", "Z3VyOTZpWGo5NDErMHVpUngvOWhxUT09");
    dataMap.put("subject_id", "bam_files");
    dataMap.put("patient_key", "1d0f7543");
    dataMap.put("histology", "BREAST");
    //dataMap.put("sequencing_center", "SB");
    //dataMap.put("run_id", "Exome_November_04_2019_550A");
    dataMap.put("run_date", "November_04_2019");
    dataMap.put("run_type", "Exome");
    dataMap.put("sample_info", "Metastasis");
    dataMap.put("sample_name", "4390-1Met-Frag12_FrTu_November_04_2019");
    dataMap.put("tissue", "NOS");
    dataMap.put("strain", "Human");
    dataMap.put("age", "Unknown");
    dataMap.put("sequencer", "550A");
    dataMap.put("kit_used", "SureSelectXT_HSV7");
    dataMap.put("piArchivePath", "/CCR_SB_Archive/PI_CCRSB2");
    dataMap.put("projectArchivePath", "/CCR_SB_Archive/PI_CCRSB2/Project_Surgery_Branch_NGS");
    dataMap.put("patientArchivePath", "/CCR_SB_Archive/PI_CCRSB2/Project_Surgery_Branch_NGS/Patient_bam_files");
    dataMap.put(
        "runArchivePath", "/CCR_SB_Archive/PI_CCRSB2/Project_Surgery_Branch_NGS/Patient_bam_files/Sample_4390-1Met-Frag12_FrTu_November_04_2019");
  }
    
  private StatusInfo setupStatusInfo(String originalFilePath, String sourceFilePath) {
    StatusInfo statusInfoObj = new StatusInfo();
    statusInfoObj.setOriginalFilePath(originalFilePath);
    statusInfoObj.setSourceFilePath(sourceFilePath);
    statusInfoObj.setSourceFileName("4390-1Met-Frag12_FrTu_November_04_2019_recal.bam");
    
    return statusInfoObj;
  }

  @Test
  public void testGetArchivePath() throws DmeSyncMappingException {

    StatusInfo statusInfoNS =
        setupStatusInfo(
            "/data/CCRSB2/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam",
            "/data/CCRSB2/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam");

    //Determine the expected and actual archive path
    ///data/CCRSB2/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam
    String expectedArchivePath =
        "/CCR_SB_Archive/PI_CCRSB2/Project_Surgery_Branch_NGS/Patient_bam_files/Sample_4390-1Met-Frag12_FrTu_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam";
    String computedArchivePath = sbPathMetadataProcessorImpl.getArchivePath(statusInfoNS, config);

    //Confirm they are same
    assertEquals(expectedArchivePath, computedArchivePath);
  }

  @Test
  public void testGetMetadataJson()
      throws DmeSyncMappingException, DmeSyncWorkflowException {

    Mockito.doReturn("1d0f7543").when(sbPathMetadataProcessorImpl).getPatientKey(any(),any());
    // Mock encryptor and inject into processor
    Mockito.doReturn("gur96iXj941+0uiRx/9hqQ==".getBytes()).when(encryptor).encrypt(any());
    sbPathMetadataProcessorImpl.encryptor = encryptor;
	    
    StatusInfo statusInfoNS =
        setupStatusInfo(
            "/data/CCRSB2/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam",
            "/data/CCRSB2/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam");

    setupDataForMetadataJsonTest("CCRSB2", "Surgery_Branch_NGS", "8021351");

    //Execute the method to test
    HpcDataObjectRegistrationRequestDTO requestDto =
        sbPathMetadataProcessorImpl.getMetaDataJson(statusInfoNS, config);

    //Validate collection metadata results
    validateCollectionMetadataResults(requestDto, dataMap);

    //Validate object metadata results
    List<HpcMetadataEntry> entries = requestDto.getMetadataEntries();
    assertEquals(8, entries.size());
    assertEquals("object_name", entries.get(0).getAttribute());
    assertEquals("4390-1Met-Frag12_FrTu_November_04_2019_recal.bam", entries.get(0).getValue());
    assertEquals("file_type", entries.get(1).getAttribute());
    assertEquals("bam", entries.get(1).getValue());
    assertEquals("source_path", entries.get(4).getAttribute());
    assertEquals("/data/CCRSB2/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam", entries.get(4).getValue());
    assertEquals("pii_content", entries.get(5).getAttribute());
    assertEquals("Unspecified", entries.get(5).getValue());
    assertEquals("data_encryption_status", entries.get(6).getAttribute());
    assertEquals("Unspecified", entries.get(6).getValue());
    assertEquals("data_compression_status", entries.get(7).getAttribute());
    assertEquals("Compressed", entries.get(7).getValue());
  }

  private MetadataMapping populateMetadataMapping(
      String collectionName, String collectionType, String metadataKey, String metadataValue) {
    MetadataMapping nameMapping = new MetadataMapping();
    nameMapping.setCollectionName(collectionName);
    nameMapping.setCollectionType(collectionType);
    nameMapping.setMetaDataKey(metadataKey);
    nameMapping.setMetaDataValue(metadataValue);
    return nameMapping;
  }

  private void setupDataForMetadataJsonTest(
      String piCollectionName, String projectCollectionName, String patientCollectionName) {

    List<MetadataMapping> piNameMetaMappings = new ArrayList<>();
    piNameMetaMappings.add(populateMetadataMapping(piCollectionName, "PI_Lab", "data_owner", "Steven A. Rosenberg"));
    piNameMetaMappings.add(populateMetadataMapping(piCollectionName, "PI_Lab", "pi_lab", "NCI, CCR"));

    Mockito.when(sbPathMetadataProcessorImpl.dmeSyncWorkflowService.getService("local")
            .findAllMetadataMappingByCollectionTypeAndCollectionNameAndDoc("PI_Lab", piCollectionName, "sb"))
        .thenReturn(piNameMetaMappings);

    List<MetadataMapping> projectNameMetaMappings = new ArrayList<>();

    projectNameMetaMappings.add(populateMetadataMapping(projectCollectionName, "Project", "project_name", "Surgery_Branch_NGS"));
    projectNameMetaMappings.add(populateMetadataMapping(projectCollectionName, "Project", "project_title", "Surgery Branch Patient Sequencing"));
    projectNameMetaMappings.add(populateMetadataMapping(projectCollectionName, "Project", "project_description", "Contains all of the sequencing data for patients treated in the Surgery Branch"));
    projectNameMetaMappings.add(populateMetadataMapping(projectCollectionName, "Project", "lab_contact", "Paul Robbins"));
    projectNameMetaMappings.add(populateMetadataMapping(projectCollectionName, "Project", "bioinformatics_contact", "Jared Gartner"));
    projectNameMetaMappings.add(populateMetadataMapping(projectCollectionName, "Project", "affiliation", "NCI, CCR"));

    Mockito.when(sbPathMetadataProcessorImpl.dmeSyncWorkflowService.getService("local")
            .findAllMetadataMappingByCollectionTypeAndCollectionNameAndDoc(
                "Project", projectCollectionName, "sb"))
        .thenReturn(projectNameMetaMappings);
    
    List<MetadataMapping> patientNameMetaMappings = new ArrayList<>();

    //patientNameMetaMappings.add(populateMetadataMapping(patientCollectionName, "Patient", "patient_name", "Jane Doe"));
    //patientNameMetaMappings.add(populateMetadataMapping(patientCollectionName, "Patient", "date_of_birth", "07/15/1999"));
    //patientNameMetaMappings.add(populateMetadataMapping(patientCollectionName, "Patient", "patient_sex", "Female"));
    patientNameMetaMappings.add(populateMetadataMapping(patientCollectionName, "Patient", "description", "Sample patient description"));

    Mockito.when(sbPathMetadataProcessorImpl.dmeSyncWorkflowService.getService("local")
            .findAllMetadataMappingByCollectionTypeAndCollectionNameAndDoc(
                "Patient", patientCollectionName, "sb"))
        .thenReturn(patientNameMetaMappings);
    
  }

  private void validateCollectionMetadataResults(
      HpcDataObjectRegistrationRequestDTO requestDto, Map<String, String> dataMap) {

    assertNotNull(requestDto);
    assertEquals(true, requestDto.getGenerateUploadRequestURL());
    assertEquals(true, requestDto.getCreateParentCollections());
    List<HpcBulkMetadataEntry> bulkMetadataEntries =
        requestDto.getParentCollectionsBulkMetadataEntries().getPathsMetadataEntries();
    assertEquals(4, bulkMetadataEntries.size());

    for(HpcBulkMetadataEntry bulkEntry: bulkMetadataEntries) {
      List<HpcMetadataEntry> metadataEntries = bulkEntry.getPathMetadataEntries();
      HpcMetadataEntry collTypeEntry = metadataEntries.get(0);
      assertEquals("collection_type", collTypeEntry.getAttribute());
      if("PI_Lab".equals(collTypeEntry.getValue())) {
        assertEquals(dataMap.get("piArchivePath"), bulkEntry.getPath());
        assertEquals(3, metadataEntries.size());
      }
      if("Project".equals(collTypeEntry.getValue())) {
        assertEquals(dataMap.get("projectArchivePath"), bulkEntry.getPath());
        assertEquals(8, metadataEntries.size());
      }
      if("Patient".equals(collTypeEntry.getValue())) {
        assertEquals(dataMap.get("patientArchivePath"), bulkEntry.getPath());
        assertEquals(4, metadataEntries.size());
      }
      if("Run".equals(collTypeEntry.getValue())) {
        assertEquals(dataMap.get("runArchivePath"), bulkEntry.getPath());
        assertEquals(7, metadataEntries.size());
      }
      for (HpcMetadataEntry entry: metadataEntries) {
        if("collection_type".equals(entry.getAttribute()))
          continue;
        if("description".equals(entry.getAttribute()))
          continue;
        assertEquals(dataMap.get(entry.getAttribute()), entry.getValue());
      }
    }
  }
}
