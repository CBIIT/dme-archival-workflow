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
import gov.nih.nci.hpc.dmesync.workflow.custom.impl.CompassPathMetadataProcessorImpl;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntry;
import gov.nih.nci.hpc.domain.metadata.HpcMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;

@Ignore
@RunWith(SpringRunner.class)
@SpringBootTest({
  "hpc.server.url=https://fr-s-hpcdm-gp-d.ncifcrf.gov:7738/hpc-server",
  "auth.token=xxxx"
})
public class CompassPathMetadataProcessorImplTest {

  //The service under test.
  @Autowired
  CompassPathMetadataProcessorImpl compassPathMetadataProcessorImpl;

  @Before
  public void init() {

    //Create the statusInfo object with the test Path
    compassPathMetadataProcessorImpl.destinationBaseDir = "/Compass_Test_Archive";
    //Simulate the 2 CollectionNameMetadata rows that you will be retrieving from the DB.

    compassPathMetadataProcessorImpl.dmeSyncWorkflowService = Mockito.mock(DmeSyncWorkflowService.class);
    CollectionNameMapping piMapping = new CollectionNameMapping();
    piMapping.setMapValue("Compass");
    when(compassPathMetadataProcessorImpl.dmeSyncWorkflowService
            .findCollectionNameMappingByMapKeyAndCollectionType("Compass", "PI_Lab"))
        .thenReturn(piMapping);
    CollectionNameMapping projectMapping = new CollectionNameMapping();
    projectMapping.setMapValue("Compass");
    when(compassPathMetadataProcessorImpl.dmeSyncWorkflowService
            .findCollectionNameMappingByMapKeyAndCollectionType("Compass", "Project"))
        .thenReturn(projectMapping);
  }

  private StatusInfo setupStatusInfo(String originalFilePath, String sourceFilePath) {
    StatusInfo statusInfoObj = new StatusInfo();
    statusInfoObj.setOriginalFilePath(originalFilePath);
    statusInfoObj.setSourceFilePath(sourceFilePath);
    compassPathMetadataProcessorImpl.destinationBaseDir = "/Compass_Test_Archive";

    return statusInfoObj;
  }

  @Test
  public void testGetArchivePath() throws DmeSyncMappingException {

    StatusInfo statusInfoNS =
        setupStatusInfo(
            "/data/Compass/DATA/NextSeq/FastqFolder/NA18487_100ng_N1D_PS2/NA18487_100ng_N1D_PS2/NA18487_100ng_N1D_PS2_S5_L001_R1_001.fastq.gz",
            "/data/Compass/DATA/NextSeq/FastqFolder/NA18487_100ng_N1D_PS2/NA18487_100ng_N1D_PS2/NA18487_100ng_N1D_PS2_S5_L001_R1_001.fastq.gz");

    //Determine the expected and actual archive path
    // /data/Compass/DATA/NextSeq/FastqFolder/NA18487_100ng_N1D_PS2
    String expectedArchivePath =
        "/Compass_Test_Archive/PI_Compass/Project_TSO500/Sample_NA18487_100ng_N1D_PS2/Sequence_Data/NA18487_100ng_N1D_PS2_S5_L001_R1_001.fastq.gz";
    String computedArchivePath = compassPathMetadataProcessorImpl.getArchivePath(statusInfoNS);

    //Confirm they are same
    assertEquals(expectedArchivePath, computedArchivePath);
  }

  @Test
  public void testGetMetadataJson() throws DmeSyncMappingException, DmeSyncWorkflowException {

    StatusInfo statusInfoNS =
        setupStatusInfo(
            "/data/Compass/DATA/NextSeq/FastqFolder/NA18487_100ng_N1D_PS2/NA18487_100ng_N1D_PS2/NA18487_100ng_N1D_PS2_S5_L001_R1_001.fastq.gz",
            "/data/Compass/DATA/NextSeq/FastqFolder/NA18487_100ng_N1D_PS2/NA18487_100ng_N1D_PS2/NA18487_100ng_N1D_PS2_S5_L001_R1_001.fastq.gz");

    setupDataForMetadataJsonTest("Compass");

    //For directory stream
    CompassPathMetadataProcessorImpl compassPathMetadataProcessorImpl2 = Mockito.spy(compassPathMetadataProcessorImpl);
    Mockito.doReturn("190627_NDX550200_0010_AHFL2KBGXB_DNA.done").when(compassPathMetadataProcessorImpl2).getDoneFileName(statusInfoNS);    

    //Execute the method to test
    HpcDataObjectRegistrationRequestDTO requestDto =
        compassPathMetadataProcessorImpl2.getMetaDataJson(statusInfoNS);

    //Validate collection metadata results
    Map<String, String> dataMap = new HashMap<String, String>();
    dataMap.put("data_owner", "Compass PI name");
    dataMap.put("affiliation", "Compass PI_Lab affiliation");
    dataMap.put("piArchivePath", "/Compass_Test_Archive/PI_Compass");
    dataMap.put("origin", "Placeholder for origin");
    dataMap.put("project_title", "Placeholder for project_title");
    dataMap.put("project_description", "Placeholder for project_description");
    dataMap.put("method", "Placeholder for method");
    dataMap.put("access", "Placeholder for access");
    dataMap.put("summary_of_samples", "Placeholder for summary_of_samples");
    dataMap.put("source_organism", "Placeholder for source_organism");
    dataMap.put("projectArchivePath", "/Compass_Test_Archive/PI_Compass/Project_TSO500");
    dataMap.put("sequencing_application_type", "Placeholder for sequencing_application_type");
    dataMap.put("patient_id", "NA18487_100ng");
    dataMap.put("sample_type", "DNA");
    dataMap.put("flowcell_id", "AHFL2KBGXB");
    dataMap.put("run_date", "190627");
    dataMap.put("library_name", "NA18487_100ng_N1D_PS2");
    dataMap.put("sampleArchivePath", "/Compass_Test_Archive/PI_Compass/Project_TSO500/Sample_NA18487_100ng_N1D_PS2");
    validateCollectionMetadataResults(requestDto, dataMap);

    //Validate object metadata results
    List<HpcMetadataEntry> entries = requestDto.getMetadataEntries();
    assertEquals(2, entries.size());
    assertEquals("object_name", entries.get(0).getAttribute());
    assertEquals("NA18487_100ng_N1D_PS2_S5_L001_R1_001.fastq.gz", entries.get(0).getValue());
    assertEquals("source_path", entries.get(1).getAttribute());
    assertEquals("/data/Compass/DATA/NextSeq/FastqFolder/NA18487_100ng_N1D_PS2/NA18487_100ng_N1D_PS2/NA18487_100ng_N1D_PS2_S5_L001_R1_001.fastq.gz", entries.get(1).getValue());
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
      String piCollectionName) {

    List<MetadataMapping> piNameMetaMappings = new ArrayList<>();
    piNameMetaMappings.add(populateMetadataMapping(piCollectionName, "PI_Lab", "data_owner", "Compass PI name"));
    piNameMetaMappings.add(populateMetadataMapping(piCollectionName, "PI_Lab", "affiliation", "Compass PI_Lab affiliation"));
    when(compassPathMetadataProcessorImpl.dmeSyncWorkflowService
            .findAllMetadataMappingByCollectionTypeAndCollectionName("PI_Lab", piCollectionName))
        .thenReturn(piNameMetaMappings);   
    
    List<MetadataMapping> projectMetaMappings = new ArrayList<>();
    projectMetaMappings.add(populateMetadataMapping(piCollectionName, "Project", "origin", "Placeholder for origin"));
    projectMetaMappings.add(populateMetadataMapping(piCollectionName, "Project", "project_title", "Placeholder for project_title"));
    projectMetaMappings.add(populateMetadataMapping(piCollectionName, "Project", "project_description", "Placeholder for project_description"));
    projectMetaMappings.add(populateMetadataMapping(piCollectionName, "Project", "method", "Placeholder for method"));
    projectMetaMappings.add(populateMetadataMapping(piCollectionName, "Project", "access", "Placeholder for access"));
    projectMetaMappings.add(populateMetadataMapping(piCollectionName, "Project", "summary_of_samples", "Placeholder for summary_of_samples"));
    projectMetaMappings.add(populateMetadataMapping(piCollectionName, "Project", "source_organism", "Placeholder for source_organism"));
    when(compassPathMetadataProcessorImpl.dmeSyncWorkflowService
            .findAllMetadataMappingByCollectionTypeAndCollectionName("Project", piCollectionName))
        .thenReturn(projectMetaMappings);   
    
    List<MetadataMapping> sampleMetaMappings = new ArrayList<>();
    sampleMetaMappings.add(populateMetadataMapping(piCollectionName, "Sample", "sequencing_application_type", "Placeholder for sequencing_application_type"));
    when(compassPathMetadataProcessorImpl.dmeSyncWorkflowService
            .findAllMetadataMappingByCollectionTypeAndCollectionName("Sample", piCollectionName))
        .thenReturn(sampleMetaMappings);   
  }
  
  private void validateCollectionMetadataResults(
      HpcDataObjectRegistrationRequestDTO requestDto, Map<String, String> dataMap) {

    assertNotNull(requestDto);
    assertEquals(true, requestDto.getGenerateUploadRequestURL());
    assertEquals(true, requestDto.getCreateParentCollections());
    List<HpcBulkMetadataEntry> bulkMetadataEntries =
        requestDto.getParentCollectionsBulkMetadataEntries().getPathsMetadataEntries();
    assertEquals(3, bulkMetadataEntries.size());

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
        assertEquals(1, metadataEntries.size());
      }
      if("Sample".equals(collTypeEntry.getValue())) {
        assertEquals(dataMap.get("sampleArchivePath"), bulkEntry.getPath());
        assertEquals(7, metadataEntries.size());
      }
      for (HpcMetadataEntry entry: metadataEntries) {
        if("collection_type".equals(entry.getAttribute()))
          continue;
        assertEquals(dataMap.get(entry.getAttribute()), entry.getValue());
      }
    }
  }
}
