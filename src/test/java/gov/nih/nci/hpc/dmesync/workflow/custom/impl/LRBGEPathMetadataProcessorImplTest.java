package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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
public class LRBGEPathMetadataProcessorImplTest {

  //The service under test.
  @Autowired LRBGEPathMetadataProcessorImpl lrbgePathMetadataProcessorImpl;

  @Before
  public void init() {
    //Create the statusInfo object with the test Path
    lrbgePathMetadataProcessorImpl.destinationBaseDir = "/CCR_LRBGE_Archive";
    lrbgePathMetadataProcessorImpl.dmeSyncWorkflowService = Mockito.mock(DmeSyncWorkflowService.class);
  }

  private StatusInfo setupStatusInfo(String originalFilePath, String sourceFilePath) {
    StatusInfo statusInfoObj = new StatusInfo();
    statusInfoObj.setOriginalFilePath(originalFilePath);
    statusInfoObj.setSourceFilePath(sourceFilePath);
    lrbgePathMetadataProcessorImpl.destinationBaseDir = "/CCR_LRBGE_Archive";

    return statusInfoObj;
  }

  @Test
  public void testGetArchivePath() throws DmeSyncMappingException {

    StatusInfo statusInfoNS =
        setupStatusInfo(
            "lrbge/FLIM-ModuloAlongC.ome.tiff",
            "lrbge/FLIM-ModuloAlongC.ome.tiff");

    //Determine the expected and actual archive path
    ///data/LRBGE/Run_20200116/FLIM-ModuloAlongC.ome.tiff
    String expectedArchivePath =
        "/CCR_LRBGE_Archive/PI_GordonHager/User_Diana_Stavreva/Project_Cells_in_gels/Run_MyGelSoakedInHormones25/FLIM-ModuloAlongC.ome.tiff";
    String computedArchivePath = lrbgePathMetadataProcessorImpl.getArchivePath(statusInfoNS);

    //Confirm they are same
    assertEquals(expectedArchivePath, computedArchivePath);
  }

  @Test
  public void testGetMetadataJson()
      throws DmeSyncMappingException, DmeSyncWorkflowException {

    StatusInfo statusInfoNS =
        setupStatusInfo(
            "lrbge/FLIM-ModuloAlongC.ome.tiff",
            "lrbge/FLIM-ModuloAlongC.ome.tiff");

    String computedArchivePath = lrbgePathMetadataProcessorImpl.getArchivePath(statusInfoNS);
    
    //Execute the method to test
    HpcDataObjectRegistrationRequestDTO requestDto =
        lrbgePathMetadataProcessorImpl.getMetaDataJson(statusInfoNS);

    //Validate collection metadata results
    Map<String, String> dataMap = new HashMap<>();
    dataMap.put("data_curator", "Tatiana Karpova");
    dataMap.put("data_owner", "GordonHager");
    dataMap.put("affiliation", "CCR/LRBGE/HAO");
    dataMap.put("user_name", "Diana Stavreva");
    dataMap.put("user_affiliation", "LRBGE");
    dataMap.put("project_title", "Cells in gels");
    dataMap.put("project_description", "This data can be used for testing.");
    dataMap.put("access", "Closed Access");
    dataMap.put("microscope_type", "Lattice Light Sheet");
    dataMap.put("start_date", "17-Jan-20");
    dataMap.put("organism", "mouse");
    dataMap.put("run_id", "MyGelSoakedInHormones25");
    dataMap.put("run_date", "17-Jan-20");
    dataMap.put("treatment", "Cells treated with dex and then with heat shock");
    dataMap.put("piArchivePath", "/CCR_LRBGE_Archive/PI_GordonHager");
    dataMap.put("userArchivePath", "/CCR_LRBGE_Archive/PI_GordonHager/User_Diana_Stavreva");
    dataMap.put("projectArchivePath", "/CCR_LRBGE_Archive/PI_GordonHager/User_Diana_Stavreva/Project_Cells_in_gels");
    dataMap.put("runArchivePath", "/CCR_LRBGE_Archive/PI_GordonHager/User_Diana_Stavreva/Project_Cells_in_gels/Run_MyGelSoakedInHormones25");
    validateCollectionMetadataResults(requestDto, dataMap);

    //Validate object metadata results
    List<HpcMetadataEntry> entries = requestDto.getMetadataEntries();
    assertEquals(7, entries.size());
    assertEquals("image_name", entries.get(0).getAttribute());
    assertEquals("FLIM-ModuloAlongC.ome.tiff", entries.get(0).getValue());
    assertEquals("source_path", entries.get(1).getAttribute());
    assertEquals("lrbge/FLIM-ModuloAlongC.ome.tiff", entries.get(1).getValue());
    assertEquals("data_type", entries.get(2).getAttribute());
    assertEquals("Raw", entries.get(2).getValue());
    assertEquals("reconstruction_date", entries.get(3).getAttribute());
    assertEquals("18-Jan-20", entries.get(3).getValue());
    assertEquals("quantification_date", entries.get(4).getAttribute());
    assertEquals("19-Jan-20", entries.get(4).getValue());
    assertEquals("quantification_program", entries.get(5).getAttribute());
    assertEquals("mATLAB", entries.get(5).getValue());
    assertEquals("publication_status", entries.get(6).getAttribute());
    assertEquals("Court et al, Science, 359, 339-343, Dec 21 2016", entries.get(6).getValue());
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
      if("PI".equals(collTypeEntry.getValue())) {
        assertEquals(dataMap.get("piArchivePath"), bulkEntry.getPath());
        assertEquals(3, metadataEntries.size());
      }
      if("User".equals(collTypeEntry.getValue())) {
        assertEquals(dataMap.get("userArchivePath"), bulkEntry.getPath());
        assertEquals(3, metadataEntries.size());
      }
      if("Project".equals(collTypeEntry.getValue())) {
        assertEquals(dataMap.get("projectArchivePath"), bulkEntry.getPath());
        assertEquals(7, metadataEntries.size());
      }
      if("Run".equals(collTypeEntry.getValue())) {
        assertEquals(dataMap.get("runArchivePath"), bulkEntry.getPath());
        assertEquals(4, metadataEntries.size());
      }
      for (HpcMetadataEntry entry: metadataEntries) {
        if("collection_type".equals(entry.getAttribute()))
          continue;
        assertEquals(dataMap.get(entry.getAttribute()), entry.getValue());
      }
    }
  }
}
