package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import gov.nih.nci.hpc.dmesync.domain.CollectionNameMapping;
import gov.nih.nci.hpc.dmesync.domain.MetadataMapping;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.service.DmeSyncWorkflowService;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntry;
import gov.nih.nci.hpc.domain.metadata.HpcMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;

@Ignore
@RunWith(SpringRunner.class)
@SpringBootTest({
  "hpc.server.url=https://fr-s-hpcdm-gp-d.ncifcrf.gov:7738/hpc-server",
  "auth.token=xxxx"
})
public class SBPathMetadataProcessorImplTest {

  //The service under test.
  @Autowired SBPathMetadataProcessorImpl sbPathMetadataProcessorImpl;

  @Before
  public void init() {
    //Create the statusInfo object with the test Path
    sbPathMetadataProcessorImpl.destinationBaseDir = "/CCR_SB_Archive";
    //Simulate the 2 CollectionNameMetadata rows that you will be retrieving from the DB.

    sbPathMetadataProcessorImpl.dmeSyncWorkflowService = Mockito.mock(DmeSyncWorkflowService.class);
    CollectionNameMapping piMapping = new CollectionNameMapping();
    piMapping.setMapValue("XXX");
    when(sbPathMetadataProcessorImpl.dmeSyncWorkflowService
            .findCollectionNameMappingByMapKeyAndCollectionType("exome", "PI_Lab"))
        .thenReturn(piMapping);

    CollectionNameMapping userMapping = new CollectionNameMapping();
    userMapping.setMapValue("Surgery_Branch_NGS");
    when(sbPathMetadataProcessorImpl.dmeSyncWorkflowService
            .findCollectionNameMappingByMapKeyAndCollectionType("exome", "Project"))
        .thenReturn(userMapping);
  }

  private StatusInfo setupStatusInfo(String originalFilePath, String sourceFilePath) {
    StatusInfo statusInfoObj = new StatusInfo();
    statusInfoObj.setOriginalFilePath(originalFilePath);
    statusInfoObj.setSourceFilePath(sourceFilePath);
    sbPathMetadataProcessorImpl.destinationBaseDir = "/CCR_SB_Archive";

    return statusInfoObj;
  }

  @Test
  public void testGetArchivePath() throws DmeSyncMappingException {

    StatusInfo statusInfoNS =
        setupStatusInfo(
            "/data/CCRSB/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam",
            "/data/CCRSB/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam");

    //Determine the expected and actual archive path
    ///data/CCRSB/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam
    String expectedArchivePath =
        "/CCR_SB_Archive/PI_XXX/Project_Surgery_Branch_NGS/Patient_1d0f7543/Run_Exome_November_04_2019_550A/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam";
    String computedArchivePath = sbPathMetadataProcessorImpl.getArchivePath(statusInfoNS);

    //Confirm they are same
    assertEquals(expectedArchivePath, computedArchivePath);
  }

  @Test
  public void testGetMetadataJson()
      throws DmeSyncMappingException, DmeSyncWorkflowException {

    StatusInfo statusInfoNS =
        setupStatusInfo(
            "/data/CCRSB/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam",
            "/data/CCRSB/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam");

    setupDataForMetadataJsonTest("XXX", "Surgery_Branch_NGS", "8021351");

    //Execute the method to test
    HpcDataObjectRegistrationRequestDTO requestDto =
        sbPathMetadataProcessorImpl.getMetaDataJson(statusInfoNS);

    //Validate collection metadata results
    Map<String, String> dataMap = new HashMap<>();
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
    dataMap.put("patient_id", "gur96iXj941+0uiRx/9hqQ==");
    dataMap.put("patient_key", "1d0f7543");
    dataMap.put("histology", "BREAST");
    dataMap.put("sequencing_center", "SB");
    dataMap.put("run_id", "Exome_November_04_2019_550A");
    dataMap.put("run_date", "November_04_2019");
    dataMap.put("run_type", "Exome");
    dataMap.put("sequencer", "550A");
    dataMap.put("kit_used", "SureSelectXT_HSV7");
    dataMap.put("piArchivePath", "/CCR_SB_Archive/PI_XXX");
    dataMap.put("projectArchivePath", "/CCR_SB_Archive/PI_XXX/Project_Surgery_Branch_NGS");
    dataMap.put("patientArchivePath", "/CCR_SB_Archive/PI_XXX/Project_Surgery_Branch_NGS/Patient_1d0f7543");
    dataMap.put(
        "runArchivePath", "/CCR_SB_Archive/PI_XXX/Project_Surgery_Branch_NGS/Patient_1d0f7543/Run_Exome_November_04_2019_550A");
    validateCollectionMetadataResults(requestDto, dataMap);

    //Validate object metadata results
    List<HpcMetadataEntry> entries = requestDto.getMetadataEntries();
    assertEquals(9, entries.size());
    assertEquals("object_name", entries.get(0).getAttribute());
    assertEquals("4390-1Met-Frag12_FrTu_November_04_2019_recal.bam", entries.get(0).getValue());
    assertEquals("file_type", entries.get(1).getAttribute());
    assertEquals("bam", entries.get(1).getValue());
    assertEquals("source_path", entries.get(2).getAttribute());
    assertEquals("/data/CCRSB/data/bam_files/exome/SB/1d0f7543_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam", entries.get(2).getValue());
    assertEquals("sample_info", entries.get(3).getAttribute());
    assertEquals("Metastasis", entries.get(3).getValue());
    assertEquals("sample_source", entries.get(4).getAttribute());
    assertEquals("FrTu", entries.get(4).getValue());
    assertEquals("resection_date", entries.get(5).getAttribute());
    assertEquals("October_02_2019", entries.get(5).getValue());
    assertEquals("pii_content", entries.get(6).getAttribute());
    assertEquals("Unspecified", entries.get(6).getValue());
    assertEquals("data_encryption_status", entries.get(7).getAttribute());
    assertEquals("Unspecified", entries.get(7).getValue());
    assertEquals("data_compression_status", entries.get(8).getAttribute());
    assertEquals("Compressed", entries.get(8).getValue());
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

    when(sbPathMetadataProcessorImpl.dmeSyncWorkflowService
            .findAllMetadataMappingByCollectionTypeAndCollectionName("PI_Lab", piCollectionName))
        .thenReturn(piNameMetaMappings);

    List<MetadataMapping> projectNameMetaMappings = new ArrayList<>();

    projectNameMetaMappings.add(populateMetadataMapping(projectCollectionName, "Project", "project_name", "Surgery_Branch_NGS"));
    projectNameMetaMappings.add(populateMetadataMapping(projectCollectionName, "Project", "project_title", "Surgery Branch Patient Sequencing"));
    projectNameMetaMappings.add(populateMetadataMapping(projectCollectionName, "Project", "project_description", "Contains all of the sequencing data for patients treated in the Surgery Branch"));
    projectNameMetaMappings.add(populateMetadataMapping(projectCollectionName, "Project", "lab_contact", "Paul Robbins"));
    projectNameMetaMappings.add(populateMetadataMapping(projectCollectionName, "Project", "bioinformatics_contact", "Jared Gartner"));
    projectNameMetaMappings.add(populateMetadataMapping(projectCollectionName, "Project", "affiliation", "NCI, CCR"));

    when(sbPathMetadataProcessorImpl.dmeSyncWorkflowService
            .findAllMetadataMappingByCollectionTypeAndCollectionName(
                "Project", projectCollectionName))
        .thenReturn(projectNameMetaMappings);
    
    List<MetadataMapping> patientNameMetaMappings = new ArrayList<>();

    //patientNameMetaMappings.add(populateMetadataMapping(patientCollectionName, "Patient", "patient_name", "Jane Doe"));
    //patientNameMetaMappings.add(populateMetadataMapping(patientCollectionName, "Patient", "date_of_birth", "07/15/1999"));
    //patientNameMetaMappings.add(populateMetadataMapping(patientCollectionName, "Patient", "patient_sex", "Female"));
    patientNameMetaMappings.add(populateMetadataMapping(patientCollectionName, "Patient", "description", "Sample patient description"));

    when(sbPathMetadataProcessorImpl.dmeSyncWorkflowService
            .findAllMetadataMappingByCollectionTypeAndCollectionName(
                "Patient", patientCollectionName))
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
        assertEquals(8, metadataEntries.size());
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
