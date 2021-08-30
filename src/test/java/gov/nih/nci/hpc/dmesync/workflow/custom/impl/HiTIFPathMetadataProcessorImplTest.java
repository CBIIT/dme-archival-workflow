package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.runner.RunWith;
import org.junit.Before;
import org.junit.Test;
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
@SpringBootTest({"hpc.server.url=https://fr-s-hpcdm-gp-d.ncifcrf.gov:7738/hpc-server", "auth.token=xxxx"})
public class HiTIFPathMetadataProcessorImplTest {


//The service under test.
@Autowired
HiTIFPathMetadataProcessorImpl hitifPathMetadataProcessorImpl = null;	

StatusInfo statusInfo = null;



 @Before
 public void init() {
     //Create the statusInfo object with the test Path
	 statusInfo = new StatusInfo();
	 String sourcePath = "/Opera_Data/CV7000_Images/MeasurementData/Ziad/180712-U2F-20x-CyclinA488opti-FUCCI-Q670N1-DAPIsat_20180712_142846/FUCCIOpti-SantaCruz1in50-FISHandIF-working-wells";
	 if(System.getProperty ("os.name").toLowerCase().contains("win"))
       sourcePath = sourcePath.replace("/", "\\");
	 statusInfo.setOriginalFilePath(sourcePath); 
	 statusInfo.setSourceFilePath(sourcePath);
	 
	 hitifPathMetadataProcessorImpl.dmeSyncWorkflowService = Mockito.mock(DmeSyncWorkflowService.class);
	 
	 //Simulate the 2 CollectionNameMetadata rows that you will be retrieving from the DB. 
	 
	 CollectionNameMapping piMapping = new CollectionNameMapping();
	 piMapping.setMapValue("Tom_Misteli");
	 when(hitifPathMetadataProcessorImpl.dmeSyncWorkflowService.findCollectionNameMappingByMapKeyAndCollectionTypeAndDoc("Ziad", "PI_Lab", "hitif")).thenReturn(piMapping);
	  
	 CollectionNameMapping userMapping = new CollectionNameMapping();
	 userMapping.setMapValue("Ziad_Jowhar");
	 when(hitifPathMetadataProcessorImpl.dmeSyncWorkflowService.findCollectionNameMappingByMapKeyAndCollectionTypeAndDoc("Ziad", "User", "hitif")).thenReturn(userMapping);
	 
	 hitifPathMetadataProcessorImpl.destinationBaseDir = "/HiTIF_Archive";
	  
 }
  
  @Test
  public void testGetArchivePath() throws DmeSyncMappingException {
	  
	  //Determine the expected and actual archive path
	  String expectedArchivePath = "/HiTIF_Archive/PI_Tom_Misteli/User_Ziad_Jowhar/Exp_180712-U2F-20x-CyclinA488opti-FUCCI-Q670N1-DAPIsat_20180712_142846/FUCCIOpti-SantaCruz1in50-FISHandIF-working-wells";
	  String computedArchivePath = hitifPathMetadataProcessorImpl.getArchivePath(statusInfo);
	  
	  //Confirm they are same
	  assertEquals(expectedArchivePath, computedArchivePath);
    
  }
  
  @Test
  public void testGetMetadataJson() throws DmeSyncMappingException, DmeSyncWorkflowException {
	  
	  //Setup test data
	  
	  List<MetadataMapping> nameMetaMappings = new ArrayList<>();
	  MetadataMapping nameMapping = new MetadataMapping();
	  nameMapping.setCollectionName("Tom_Misteli");
	  nameMapping.setCollectionType("PI_Lab");
	  nameMapping.setMetaDataKey("data_owner");
	  nameMapping.setMetaDataValue("Tom Misteli");
	  nameMetaMappings.add(nameMapping);	 
	  when(hitifPathMetadataProcessorImpl.dmeSyncWorkflowService.findAllMetadataMappingByCollectionTypeAndCollectionNameAndDoc("PI_Lab", "Tom_Misteli", "hitif")).thenReturn(nameMetaMappings);
	  
	  List<MetadataMapping> userNameMetaMappings = new ArrayList<>();
	  MetadataMapping userNameMapping = new MetadataMapping();
	  userNameMapping.setCollectionName("Ziad_Jowhar");
	  userNameMapping.setCollectionType("User");
	  userNameMapping.setMetaDataKey("name");
	  userNameMapping.setMetaDataValue("Ziad Jowhar");
	  userNameMetaMappings.add(userNameMapping);
	  when(hitifPathMetadataProcessorImpl.dmeSyncWorkflowService.findAllMetadataMappingByCollectionTypeAndCollectionNameAndDoc("User", "Ziad_Jowhar", "hitif")).thenReturn(userNameMetaMappings);
	  
	  List<MetadataMapping> expMetaMappings = new ArrayList<>();
	  MetadataMapping expMapping = new MetadataMapping();
	  expMapping.setCollectionName("180712-U2F-20x-CyclinA488opti-FUCCI-Q670N1-DAPIsat_20180712_142846");
	  expMapping.setCollectionType("Exp");
	  expMapping.setMetaDataKey("experiment_name");
	  expMapping.setMetaDataValue("180712-U2F-20x-CyclinA488opti-FUCCI-Q670N1-DAPIsat_20180712_142846");
	  expMetaMappings.add(expMapping);
	  when(hitifPathMetadataProcessorImpl.dmeSyncWorkflowService.findAllMetadataMappingByCollectionTypeAndCollectionNameAndDoc(
			  "Exp", "180712-U2F-20x-CyclinA488opti-FUCCI-Q670N1-DAPIsat_20180712_142846", "hitif")).thenReturn(expMetaMappings);
	  
	  //Execute the method to test
	  
	  HpcDataObjectRegistrationRequestDTO requestDto = hitifPathMetadataProcessorImpl.getMetaDataJson(statusInfo);
	  
	  //validate results
	  
	  assertNotNull(requestDto);
	  assertEquals(true, requestDto.getGenerateUploadRequestURL());
	  assertEquals(true, requestDto.getCreateParentCollections());
	  List<HpcBulkMetadataEntry> bulkMetadataEntries = requestDto.getParentCollectionsBulkMetadataEntries().getPathsMetadataEntries();
	  assertEquals(3, bulkMetadataEntries.size());
	  
	  HpcBulkMetadataEntry bulkEntry  = bulkMetadataEntries.get(0);
	  assertEquals(bulkEntry.getPath(), "/HiTIF_Archive/PI_Tom_Misteli");
	  List<HpcMetadataEntry> metadataEntries = bulkEntry.getPathMetadataEntries();
	  assertEquals(1, metadataEntries.size());
	  HpcMetadataEntry entry = metadataEntries.get(0);
	  assertEquals("data_owner", entry.getAttribute());
	  assertEquals("Tom Misteli", entry.getValue());
	  
	  bulkEntry  = bulkMetadataEntries.get(1);
	  assertEquals(bulkEntry.getPath(), "/HiTIF_Archive/PI_Tom_Misteli/User_Ziad_Jowhar");
	  metadataEntries = bulkEntry.getPathMetadataEntries();
	  assertEquals(1, metadataEntries.size());
	  entry = metadataEntries.get(0);
	  assertEquals("name", entry.getAttribute());
	  assertEquals("Ziad Jowhar", entry.getValue());
	  
	  bulkEntry  = bulkMetadataEntries.get(2);
	  assertEquals("/HiTIF_Archive/PI_Tom_Misteli/User_Ziad_Jowhar/Exp_180712-U2F-20x-CyclinA488opti-FUCCI-Q670N1-DAPIsat_20180712_142846", bulkEntry.getPath());
	  metadataEntries = bulkEntry.getPathMetadataEntries();
	  assertEquals(metadataEntries.size(), 2);
	  entry = metadataEntries.get(0);
	  assertEquals(entry.getAttribute(), "experiment_name");
	  assertEquals(entry.getValue(), "180712-U2F-20x-CyclinA488opti-FUCCI-Q670N1-DAPIsat_20180712_142846");
	  
	  
  }
  
}
