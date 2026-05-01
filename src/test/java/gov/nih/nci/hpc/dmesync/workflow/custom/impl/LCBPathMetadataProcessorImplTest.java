package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
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


public class LCBPathMetadataProcessorImplTest {

  //The service under test.
  LCBPathMetadataProcessorImpl lcbPathMetadataProcessorImpl;
  DocConfig config;
  DmeSyncWorkflowServiceFactory dmeSyncWorkflowServiceFactory;
  @Mock
  DmeSyncWorkflowService dmeSyncWorkflowService;

  @BeforeEach
  public void init() {
	lcbPathMetadataProcessorImpl = new LCBPathMetadataProcessorImpl();
	DocConfig.SourceConfig sourceConfig = new DocConfig.SourceConfig(null, null, "/CCR_LCB_SubramaniamLab_Archive", 1);
    DocConfig.SourceRule sourceRule = new DocConfig.SourceRule(null, null, null, "", null, false, false, null, null, false, null, false, false, false, 0);
    config = new DocConfig(null, null, null, null, null, null, null, false, null, 0, null, null, sourceConfig, sourceRule, null, null, null, null);
    
    //Simulate the 2 CollectionNameMetadata rows that you will be retrieving from the DB.
    // Use a mock for the service factory and service
 	dmeSyncWorkflowServiceFactory = Mockito.mock(DmeSyncWorkflowServiceFactory.class);
 	dmeSyncWorkflowService = Mockito.mock(DmeSyncWorkflowService.class);
 	lcbPathMetadataProcessorImpl.dmeSyncWorkflowService = dmeSyncWorkflowServiceFactory;
 	Mockito.when(dmeSyncWorkflowServiceFactory.getService(any())).thenReturn(dmeSyncWorkflowService);
 	 
    CollectionNameMapping piMapping = new CollectionNameMapping();
    piMapping.setMapValue("Subramaniam");
    Mockito.when(lcbPathMetadataProcessorImpl.dmeSyncWorkflowService.getService("local")
            .findCollectionNameMappingByMapKeyAndCollectionTypeAndDoc("Livlab", "PI_Lab", "lcb"))
        .thenReturn(piMapping);
  }

  private StatusInfo setupStatusInfo(String originalFilePath, String sourceFilePath) {
    StatusInfo statusInfoObj = new StatusInfo();
    statusInfoObj.setOriginalFilePath(originalFilePath);
    statusInfoObj.setSourceFilePath(sourceFilePath);

    return statusInfoObj;
  }

  @Test
  public void testGetArchivePath() throws DmeSyncMappingException {

    StatusInfo statusInfoNS =
        setupStatusInfo(
            "/data/Livlab/projects/GluK2",
            "/work/lcb/projects/GluK2.tar");

    //Determine the expected and actual archive path
    ////data/Livlab/projects/GluK2
    String expectedArchivePath =
        "/CCR_LCB_SubramaniamLab_Archive/PI_Subramaniam/projects/GluK2.tar";
    String computedArchivePath = lcbPathMetadataProcessorImpl.getArchivePath(statusInfoNS, config);

    //Confirm they are same
    assertEquals(expectedArchivePath, computedArchivePath);
  }

  @Test
  public void testGetMetadataJson()
      throws DmeSyncMappingException, DmeSyncWorkflowException {

    StatusInfo statusInfoNS =
        setupStatusInfo(
            "/data/Livlab/projects/GluK2",
            "/work/lcb/projects/GluK2.tar");

    setupDataForMetadataJsonTest("Subramaniam");

    //Execute the method to test
    HpcDataObjectRegistrationRequestDTO requestDto =
        lcbPathMetadataProcessorImpl.getMetaDataJson(statusInfoNS, config);

    //Validate collection metadata results
    Map<String, String> dataMap = new HashMap<>();
    dataMap.put("data_owner", "Sriram Subramaniam");
    dataMap.put("piArchivePath", "/CCR_LCB_SubramaniamLab_Archive/PI_Subramaniam");
    validateCollectionMetadataResults(requestDto, dataMap);

    //Validate object metadata results
    List<HpcMetadataEntry> entries = requestDto.getMetadataEntries();
    assertEquals(3, entries.size());
    assertEquals("object_name", entries.get(0).getAttribute());
    assertEquals("GluK2.tar", entries.get(0).getValue());
    assertEquals("source_path", entries.get(1).getAttribute());
    assertEquals("/data/Livlab/projects/GluK2", entries.get(1).getValue());
    assertEquals("modified_date", entries.get(2).getAttribute());
    assertEquals("09-18-2020 09:12:11", entries.get(2).getValue());
  }

  private MetadataMapping populateMetadataMapping(
      String collectionName, String collectionType, String metadataKey, String metadataValue) {
    MetadataMapping nameMapping = new MetadataMapping();
    nameMapping.setDoc("lcb");
    nameMapping.setCollectionName(collectionName);
    nameMapping.setCollectionType(collectionType);
    nameMapping.setMetaDataKey(metadataKey);
    nameMapping.setMetaDataValue(metadataValue);
    return nameMapping;
  }

  private void setupDataForMetadataJsonTest(
      String piCollectionName) {

    List<MetadataMapping> piNameMetaMappings = new ArrayList<>();
    piNameMetaMappings.add(populateMetadataMapping(piCollectionName, "PI_Lab", "data_owner", "Sriram Subramaniam"));

    when(lcbPathMetadataProcessorImpl.dmeSyncWorkflowService.getService("local")
            .findAllMetadataMappingByCollectionTypeAndCollectionNameAndDoc("PI_Lab", piCollectionName, "lcb"))
        .thenReturn(piNameMetaMappings);   
  }

  private void validateCollectionMetadataResults(
      HpcDataObjectRegistrationRequestDTO requestDto, Map<String, String> dataMap) {

    assertNotNull(requestDto);
    assertEquals(true, requestDto.getGenerateUploadRequestURL());
    assertEquals(true, requestDto.getCreateParentCollections());
    List<HpcBulkMetadataEntry> bulkMetadataEntries =
        requestDto.getParentCollectionsBulkMetadataEntries().getPathsMetadataEntries();
    assertEquals(2, bulkMetadataEntries.size());

    for(HpcBulkMetadataEntry bulkEntry: bulkMetadataEntries) {
      List<HpcMetadataEntry> metadataEntries = bulkEntry.getPathMetadataEntries();
      HpcMetadataEntry collTypeEntry = metadataEntries.get(0);
      assertEquals("collection_type", collTypeEntry.getAttribute());
      if("PI_Lab".equals(collTypeEntry.getValue())) {
        assertEquals(dataMap.get("piArchivePath"), bulkEntry.getPath());
        assertEquals(2, metadataEntries.size());
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
