package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.junit.runner.RunWith;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
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
public class CMMPathMetadataProcessorImplTest {


//The service under test.
@Autowired
CMMPathMetadataProcessorImpl cmmPathMetadataProcessorImpl;	



 @Before
 public void init() {
     //Create the statusInfo object with the test Path
	 cmmPathMetadataProcessorImpl.destinationBaseDir = "/CCR_CMM_Archive";
 }
 
 private StatusInfo setupStatusInfo(String sourcePath) {
	 StatusInfo statusInfoObj = new StatusInfo();
	 if(System.getProperty ("os.name").toLowerCase().contains("win"))
	   sourcePath = sourcePath.replace("/", "\\");
	 statusInfoObj.setOriginalFilePath(sourcePath); 
	 statusInfoObj.setSourceFilePath(sourcePath); 
	 return statusInfoObj;
 }
  
  @Test
  public void testGetArchivePathforNS() throws DmeSyncMappingException {
	  
	  StatusInfo statusInfoNS = setupStatusInfo("/data/CMM_CryoEM/CMM_Data/0022/HIV_Trimer/Trimer02/Negative_Stain/T20/Trimer02_January_10_2018.tar");
		

	  
	  //Determine the expected and actual archive path
	  //data/CMM_CryoEM/CMM_Data/0022/HIV_Trimer/Trimer02/Negative_Stain/T20/Trimer02_January_10_2018.tar 
	  String expectedArchivePath = "/CCR_CMM_Archive/PI_0022/Project_HIV_Trimer/Variant_Trimer02/Negative_Stain/Raw_Data/Trimer02_January_10_2018.tar";
	  String computedArchivePath = cmmPathMetadataProcessorImpl.getArchivePath(statusInfoNS);
	  
	  //Confirm they are same
	  assertEquals(expectedArchivePath, computedArchivePath);
    
  }
  
 
  @Test
  public void testGetMetadataJsonForNS() throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {
	  
	  StatusInfo statusInfoNS = setupStatusInfo("/data/CMM_CryoEM/CMM_Data/0022/HIV_Trimer/Trimer02/Negative_Stain/T20/Trimer02_January_10_2018.tar");
	  
	  cmmPathMetadataProcessorImpl.dmeSyncWorkflowService = Mockito.mock(DmeSyncWorkflowService.class);
	  
	  List<MetadataMapping> piNameMetaMappings = new ArrayList<>();
	  MetadataMapping nameMapping = new MetadataMapping();
	  nameMapping.setCollectionName("0022");
	  nameMapping.setCollectionType("PI_Lab");
	  nameMapping.setMetaDataKey("data_owner");
	  nameMapping.setMetaDataValue("Richard Wyatt");
	  piNameMetaMappings.add(nameMapping);	 
	  when(cmmPathMetadataProcessorImpl.dmeSyncWorkflowService.findAllMetadataMappingByCollectionTypeAndCollectionName(
			  "PI_Lab", "0022")).thenReturn(piNameMetaMappings);
	  
	  List<MetadataMapping> projectNameMetaMappings = new ArrayList<>();
	  MetadataMapping projectNameMapping = new MetadataMapping();
	  projectNameMapping.setCollectionName("HIV_Primer");
	  projectNameMapping.setCollectionType("Project");
	  projectNameMapping.setMetaDataKey("project_name");
	  projectNameMapping.setMetaDataValue("HIV-1 env in complex with bn Abs");
	  projectNameMetaMappings.add(projectNameMapping);
	  when(cmmPathMetadataProcessorImpl.dmeSyncWorkflowService.findAllMetadataMappingByCollectionTypeAndCollectionName(
			  "Project", "HIV_Trimer")).thenReturn(projectNameMetaMappings);
	  
	 
	  //Execute the method to test
	  
	  HpcDataObjectRegistrationRequestDTO requestDto = 
		cmmPathMetadataProcessorImpl.getMetaDataJson(statusInfoNS);
	  
	  //validate results
	  
	  assertNotNull(requestDto);
	  assertEquals(true, requestDto.getGenerateUploadRequestURL());
	  assertEquals(true, requestDto.getCreateParentCollections());
	  List<HpcBulkMetadataEntry> bulkMetadataEntries = requestDto.getParentCollectionsBulkMetadataEntries().getPathsMetadataEntries();
	  assertEquals(5, bulkMetadataEntries.size());
	  
	  HpcBulkMetadataEntry bulkEntry  = bulkMetadataEntries.get(0);
	  assertEquals("/CCR_CMM_Archive/PI_0022", bulkEntry.getPath());
	  List<HpcMetadataEntry> metadataEntries = bulkEntry.getPathMetadataEntries();
	  assertEquals(1, metadataEntries.size());
	  HpcMetadataEntry entry = metadataEntries.get(0);
	  assertEquals("data_owner", entry.getAttribute());
	  assertEquals("Richard Wyatt", entry.getValue());
	  
	  bulkEntry  = bulkMetadataEntries.get(1);
	  assertEquals("/CCR_CMM_Archive/PI_0022/Project_HIV_Trimer", bulkEntry.getPath());
	  metadataEntries = bulkEntry.getPathMetadataEntries();
	  assertEquals(2, metadataEntries.size());
	  entry = metadataEntries.get(0);
	  assertEquals("project_name", entry.getAttribute());
	  assertEquals("HIV-1 env in complex with bn Abs", entry.getValue());
	  entry = metadataEntries.get(1);
	  assertEquals("project_id", entry.getAttribute());
	  assertEquals("HIV_Trimer", entry.getValue());
	  
	  bulkEntry  = bulkMetadataEntries.get(2);
	  assertEquals("/CCR_CMM_Archive/PI_0022/Project_HIV_Trimer/Variant_Trimer02", bulkEntry.getPath());
	  metadataEntries = bulkEntry.getPathMetadataEntries();
	  assertEquals(2, metadataEntries.size());
	  entry = metadataEntries.get(0);
	  assertEquals("variant_name", entry.getAttribute());
	  assertEquals("Trimer02", entry.getValue());
	  
	  bulkEntry  = bulkMetadataEntries.get(3);
	  assertEquals("/CCR_CMM_Archive/PI_0022/Project_HIV_Trimer/Variant_Trimer02/Negative_Stain", bulkEntry.getPath());
	  metadataEntries = bulkEntry.getPathMetadataEntries();
	  assertEquals(metadataEntries.size(), 2);
	  entry = metadataEntries.get(0);
	  assertEquals("instrument", entry.getAttribute());
	  assertEquals("T20", entry.getValue());
	 
	  List<HpcMetadataEntry> entries = requestDto.getMetadataEntries();
	  assertEquals(1, entries.size());
	  assertEquals("object_name", entries.get(0).getAttribute());
	  assertEquals("Trimer02_January_10_2018.tar", entries.get(0).getValue());
	  
  }
  
  
  @Test
  public void testGetArchivePathForCryoEM() throws DmeSyncMappingException {
	  
	  StatusInfo statusInfoCryoEM = setupStatusInfo("/data/CMM_CryoEM/CMM_Data/0010/Project-B/Project-B/CryoEM/Latitude_runs/20180310_0568/DataImages/Stack/image_file.tif");
		 
	  
	  //Determine the expected and actual archive path
	  //data/CMM_CryoEM/CMM_Data/0022/HIV_Trimer/Trimer02/Negative_Stain/T20/Trimer02_January_10_2018.tar 
	  String expectedArchivePath = "/CCR_CMM_Archive/PI_0010/Project_Project-B/Variant_Project-B/CryoEM/Latitude_Run_20180310_0568/Raw_Data/image_file.tif";
	  String computedArchivePath = cmmPathMetadataProcessorImpl.getArchivePath(statusInfoCryoEM);
	  
	  //Confirm they are same
	  assertEquals(expectedArchivePath, computedArchivePath);
    
  }
  
  
  @Test
  public void testGetMetadataJsonForCryoEM() throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {
	  
	  StatusInfo statusInfoCryoEM = setupStatusInfo("/data/CMM_CryoEM/CMM_Data/0010/Project-B/Project-B/CryoEM/Latitude_runs/20180310_0568/DataImages/Stack/image_file.tif");
		
	  cmmPathMetadataProcessorImpl.dmeSyncWorkflowService = Mockito.mock(DmeSyncWorkflowService.class);
	  
	  List<MetadataMapping> piNameMetaMappings = new ArrayList<>();
	  MetadataMapping nameMapping = new MetadataMapping();
	  nameMapping.setCollectionName("0010");
	  nameMapping.setCollectionType("PI_Lab");
	  nameMapping.setMetaDataKey("data_owner");
	  nameMapping.setMetaDataValue("Wei Yang");
	  piNameMetaMappings.add(nameMapping);	 
	  when(cmmPathMetadataProcessorImpl.dmeSyncWorkflowService.findAllMetadataMappingByCollectionTypeAndCollectionName(
			  "PI_Lab", "0010")).thenReturn(piNameMetaMappings);
	  
	  List<MetadataMapping> projectNameMetaMappings = new ArrayList<>();
	  MetadataMapping projectNameMapping = new MetadataMapping();
	  projectNameMapping.setCollectionName("Project-B");
	  projectNameMapping.setCollectionType("Project");
	  projectNameMapping.setMetaDataKey("project_name");
	  projectNameMapping.setMetaDataValue("Gp5 DNA polymerase and Gp4 studies");
	  projectNameMetaMappings.add(projectNameMapping);
	  when(cmmPathMetadataProcessorImpl.dmeSyncWorkflowService.findAllMetadataMappingByCollectionTypeAndCollectionName(
			  "Project", "Project-B")).thenReturn(projectNameMetaMappings);
	  
	 
	  //Execute the method to test
	  
	  HpcDataObjectRegistrationRequestDTO requestDto = 
		cmmPathMetadataProcessorImpl.getMetaDataJson(statusInfoCryoEM);
	  
	  //validate results
	  
	  assertNotNull(requestDto);
	  assertEquals(true, requestDto.getGenerateUploadRequestURL());
	  assertEquals(true, requestDto.getCreateParentCollections());
	  List<HpcBulkMetadataEntry> bulkMetadataEntries = requestDto.getParentCollectionsBulkMetadataEntries().getPathsMetadataEntries();
	  assertEquals(6, bulkMetadataEntries.size());
	  
	  HpcBulkMetadataEntry bulkEntry  = bulkMetadataEntries.get(0);
	  assertEquals("/CCR_CMM_Archive/PI_0010", bulkEntry.getPath());
	  List<HpcMetadataEntry> metadataEntries = bulkEntry.getPathMetadataEntries();
	  assertEquals(1, metadataEntries.size());
	  HpcMetadataEntry entry = metadataEntries.get(0);
	  assertEquals("data_owner", entry.getAttribute());
	  assertEquals("Wei Yang", entry.getValue());
	  
	  bulkEntry  = bulkMetadataEntries.get(1);
	  assertEquals( "/CCR_CMM_Archive/PI_0010/Project_Project-B", bulkEntry.getPath());
	  metadataEntries = bulkEntry.getPathMetadataEntries();
	  assertEquals(2, metadataEntries.size());
	  entry = metadataEntries.get(0);
	  assertEquals("project_name", entry.getAttribute());
	  assertEquals("Gp5 DNA polymerase and Gp4 studies", entry.getValue());
	  entry = metadataEntries.get(1);
	  assertEquals("project_id", entry.getAttribute());
	  assertEquals("Project-B", entry.getValue());
	  
	  bulkEntry  = bulkMetadataEntries.get(2);
	  assertEquals("/CCR_CMM_Archive/PI_0010/Project_Project-B/Variant_Project-B", bulkEntry.getPath());
	  metadataEntries = bulkEntry.getPathMetadataEntries();
	  assertEquals(2, metadataEntries.size());
	  entry = metadataEntries.get(0);
	  assertEquals("variant_name", entry.getAttribute());
	  assertEquals("Project-B", entry.getValue());
	  
	  bulkEntry  = bulkMetadataEntries.get(4);
	  assertEquals("/CCR_CMM_Archive/PI_0010/Project_Project-B/Variant_Project-B/CryoEM/Latitude_Run_20180310_0568", bulkEntry.getPath());
	  metadataEntries = bulkEntry.getPathMetadataEntries();
	  assertEquals(4, metadataEntries.size());
	  entry = metadataEntries.get(0);
	  assertEquals("pipeline_number", entry.getAttribute());
	  assertEquals("20180310_0568", entry.getValue());
	  entry = metadataEntries.get(1);
	  assertEquals("run_date", entry.getAttribute());
	  assertEquals("03/10/2018", entry.getValue());
	  entry = metadataEntries.get(2);
      assertEquals("software", entry.getAttribute());
      assertEquals("Latitude", entry.getValue());
	  
	  List<HpcMetadataEntry> entries = requestDto.getMetadataEntries();
	  assertEquals(1, entries.size());
	  assertEquals("object_name", entries.get(0).getAttribute());
	  assertEquals("image_file.tif", entries.get(0).getValue());
	  
  }
  
}
