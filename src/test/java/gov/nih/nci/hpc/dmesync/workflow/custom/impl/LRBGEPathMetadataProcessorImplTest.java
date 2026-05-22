package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.Arrays;
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
import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.service.DmeSyncWorkflowService;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntry;
import gov.nih.nci.hpc.domain.metadata.HpcMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;


public class LRBGEPathMetadataProcessorImplTest {

  //The service under test.
  LRBGEPathMetadataProcessorImpl lrbgePathMetadataProcessorImpl;
  DocConfig config;
  DmeSyncWorkflowServiceFactory dmeSyncWorkflowServiceFactory;
  @Mock
  DmeSyncWorkflowService dmeSyncWorkflowService;
  Map<String, String> dataMap;
  
  @BeforeEach
  public void init() {
	populateDataMap();
    MockitoAnnotations.openMocks(this);
    DocConfig.SourceConfig sourceConfig = new DocConfig.SourceConfig(null, null, "/CCR_LRBGE_Archive", 1);
    DocConfig.SourceRule sourceRule = new DocConfig.SourceRule(null, null, null, "", "", false, false, null, null, false, null, false, false, false, 0);
    config = new DocConfig(null, null, null, null, null, null, null, false, null, 0, null, null, sourceConfig, sourceRule, null, null, null, null);
    // Use a testable subclass to mock directory stream
    lrbgePathMetadataProcessorImpl = Mockito.spy(new TestableLRBGEPathMetadataProcessorImpl());
    dmeSyncWorkflowServiceFactory = Mockito.mock(DmeSyncWorkflowServiceFactory.class);
    dmeSyncWorkflowService = Mockito.mock(DmeSyncWorkflowService.class);
    lrbgePathMetadataProcessorImpl.dmeSyncWorkflowService = dmeSyncWorkflowServiceFactory;
    Mockito.when(dmeSyncWorkflowServiceFactory.getService(any())).thenReturn(dmeSyncWorkflowService);
    // Mock the directory stream to return a fake .xlsx file
    Path fakeXlsx = Mockito.mock(Path.class);
    Mockito.when(fakeXlsx.toString()).thenReturn("");
    DirectoryStream<Path> mockStream = Mockito.mock(DirectoryStream.class);
    Mockito.when(mockStream.iterator()).thenReturn(Arrays.asList(fakeXlsx).iterator());
    ((TestableLRBGEPathMetadataProcessorImpl)lrbgePathMetadataProcessorImpl).setMockDirectoryStream(mockStream);

    // Mock getAttrValueWithKey with different params
    // Only mock the correct signatures for getAttrValueWithKey
    Mockito.doAnswer((Answer<String>) invocation -> {
      Object[] args = invocation.getArguments();
      if (args.length == 2) {
    	  return dataMap.get(args[1]);
      }
      return "Mock Value";
    }).when(lrbgePathMetadataProcessorImpl).getAttrValueWithKey(any(), any());
  }

  // Testable subclass to override directory stream logic
  static class TestableLRBGEPathMetadataProcessorImpl extends LRBGEPathMetadataProcessorImpl {
    private DirectoryStream<Path> mockDirectoryStream;
    public void setMockDirectoryStream(DirectoryStream<Path> stream) { this.mockDirectoryStream = stream; }
    @Override
    protected DirectoryStream<Path> getDirectoryStream(Path dir, DirectoryStream.Filter<Path> filter) {
      return mockDirectoryStream;
    }
  }

  private void populateDataMap() {
	  dataMap = new HashMap<>();
	    dataMap.put("data_generator", "Tatiana Karpova");
	    dataMap.put("pi_name", "GordonHager");
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
            "lrbge/FLIM-ModuloAlongC.ome.tiff",
            "lrbge/FLIM-ModuloAlongC.ome.tiff");

    //Determine the expected and actual archive path
    ///data/LRBGE/Run_20200116/FLIM-ModuloAlongC.ome.tiff
    String expectedArchivePath =
        "/CCR_LRBGE_Archive/PI_GordonHager/User_Diana_Stavreva/Project_Cells_in_gels/Run_MyGelSoakedInHormones25/FLIM-ModuloAlongC.ome.tiff";
    String computedArchivePath = lrbgePathMetadataProcessorImpl.getArchivePath(statusInfoNS, config);

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

    String computedArchivePath = lrbgePathMetadataProcessorImpl.getArchivePath(statusInfoNS, config);
    
    //Execute the method to test
    HpcDataObjectRegistrationRequestDTO requestDto =
        lrbgePathMetadataProcessorImpl.getMetaDataJson(statusInfoNS, config);

    //Validate collection metadata results
    validateCollectionMetadataResults(requestDto, dataMap);

    //Validate object metadata results
    List<HpcMetadataEntry> entries = requestDto.getMetadataEntries();
    assertEquals(3, entries.size());
    assertEquals("image_name", entries.get(0).getAttribute());
    assertEquals("FLIM-ModuloAlongC.ome.tiff", entries.get(0).getValue());
    assertEquals("source_path", entries.get(1).getAttribute());
    assertEquals("lrbge/FLIM-ModuloAlongC.ome.tiff", entries.get(1).getValue());
    assertEquals("data_type", entries.get(2).getAttribute());
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