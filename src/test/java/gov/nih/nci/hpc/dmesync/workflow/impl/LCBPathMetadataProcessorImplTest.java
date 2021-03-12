package gov.nih.nci.hpc.dmesync.workflow.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
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

@RunWith(SpringRunner.class)
@SpringBootTest({
  "hpc.server.url=https://fr-s-hpcdm-gp-d.ncifcrf.gov:7738/hpc-server",
  "auth.token=xxxx"
})
public class LCBPathMetadataProcessorImplTest {

  //The service under test.
  @Autowired LCBPathMetadataProcessorImpl lcbPathMetadataProcessorImpl;

  @Before
  public void init() {
    //Create the statusInfo object with the test Path
    lcbPathMetadataProcessorImpl.destinationBaseDir = "/CCR_LCB_SubramaniamLab_Archive";
    //Simulate the 2 CollectionNameMetadata rows that you will be retrieving from the DB.

    lcbPathMetadataProcessorImpl.dmeSyncWorkflowService = Mockito.mock(DmeSyncWorkflowService.class);
    CollectionNameMapping piMapping = new CollectionNameMapping();
    piMapping.setMapValue("Subramaniam");
    when(lcbPathMetadataProcessorImpl.dmeSyncWorkflowService
            .findCollectionNameMappingByMapKeyAndCollectionType("Livlab", "PI_Lab"))
        .thenReturn(piMapping);
  }

  private StatusInfo setupStatusInfo(String originalFilePath, String sourceFilePath) {
    StatusInfo statusInfoObj = new StatusInfo();
    statusInfoObj.setOriginalFilePath(originalFilePath);
    statusInfoObj.setSourceFilePath(sourceFilePath);
    lcbPathMetadataProcessorImpl.destinationBaseDir = "/CCR_LCB_SubramaniamLab_Archive";

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
    String computedArchivePath = lcbPathMetadataProcessorImpl.getArchivePath(statusInfoNS);

    //Confirm they are same
    assertEquals(expectedArchivePath, computedArchivePath);
  }

  @Test
  public void testGetMetadataJson()
      throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {

    StatusInfo statusInfoNS =
        setupStatusInfo(
            "/data/Livlab/projects/GluK2",
            "/work/lcb/projects/GluK2.tar");

    setupDataForMetadataJsonTest("Subramaniam");

    //Execute the method to test
    HpcDataObjectRegistrationRequestDTO requestDto =
        lcbPathMetadataProcessorImpl.getMetaDataJson(statusInfoNS);

    //Validate collection metadata results
    Map<String, String> dataMap = new HashMap<String, String>();
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

    when(lcbPathMetadataProcessorImpl.dmeSyncWorkflowService
            .findAllMetadataMappingByCollectionTypeAndCollectionName("PI_Lab", piCollectionName))
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
