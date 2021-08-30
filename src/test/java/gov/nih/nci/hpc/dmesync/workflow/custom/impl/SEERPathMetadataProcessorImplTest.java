package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
public class SEERPathMetadataProcessorImplTest {


//The service under test.
@Autowired
SEERPathMetadataProcessorImpl seerPathMetadataProcessorImpl;	



 @Before
 public void init() {
	 
	 seerPathMetadataProcessorImpl.dmeSyncWorkflowService = Mockito.mock(DmeSyncWorkflowService.class);
	
	 //Simulate the 2 CollectionNameMetadata rows that you will be retrieving from the DB. 
	 
	 CollectionNameMapping piMapping = new CollectionNameMapping();
	 piMapping.setMapValue("Alison_Van_Dyke");
	 when(seerPathMetadataProcessorImpl.dmeSyncWorkflowService.findCollectionNameMappingByMapKeyAndCollectionTypeAndDoc("BCT", "PI_Lab", "seer")).thenReturn(piMapping);
	  
	 CollectionNameMapping projectMapping = new CollectionNameMapping();
	 projectMapping.setMapValue("BCT_Pilot");
	 when(seerPathMetadataProcessorImpl.dmeSyncWorkflowService.findCollectionNameMappingByMapKeyAndCollectionTypeAndDoc("BCT", "Project", "seer")).thenReturn(projectMapping);
	 
	 
     //Create the statusInfo object with the test Path
	 seerPathMetadataProcessorImpl.destinationBaseDir = "/SEER_VTR_SRP_Archive";
	 
	 
 }
 
 private StatusInfo setupStatusInfo(String originalFilePath, String sourceFilePath) {
	 StatusInfo statusInfoObj = new StatusInfo();
	 statusInfoObj.setOriginalFilePath(originalFilePath); 
	 statusInfoObj.setSourceFilePath(sourceFilePath); 
	 
	 return statusInfoObj;
 }
  
  @Test
  public void testGetArchivePath() throws DmeSyncMappingException {
	  
	  StatusInfo statusInfoNS = setupStatusInfo("/seer-VTR/BCT/VTRBCT_BC7001_BL3_XX.svs", 
	  		 "/mnt/IRODsScratch/work/VTRBCT_BC7001_BL3_XX.svs");
	  
	  //Determine the expected and actual archive path
	  String expectedArchivePath = "/SEER_VTR_SRP_Archive/PI_Alison_Van_Dyke/Project_BCT_Pilot/Dataset_BC7001/VTRBCT_BC7001_BL3_XX.svs";
	  String computedArchivePath = seerPathMetadataProcessorImpl.getArchivePath(statusInfoNS);
	  
	  //Confirm they are same
	  assertEquals(expectedArchivePath, computedArchivePath);
  }
  
 
  @Test
  public void testGetMetadataJson() throws DmeSyncMappingException, DmeSyncWorkflowException {
	  
	  
	  StatusInfo statusInfoNS = setupStatusInfo("/seer-VTR/BCT/VTRBCT_BC7001_BL3_XX.svs", 
		  		 "/mnt/IRODsScratch/work/VTRBCT_BC7001_BL3_XX.svs");
	  
	  setupDataForMetadataJsonTest("Alison_Van_Dyke", "Alison Van Dyke", "NCI, DCCP, SRP", 
			  "BCT_Pilot", "SEER VTR BCT Pilot", "whole-slide-images", "12/12/2018");
	 
	  //Execute the method to test
	  HpcDataObjectRegistrationRequestDTO requestDto = 
		seerPathMetadataProcessorImpl.getMetaDataJson(statusInfoNS);
	  
	  
	//Validate collection metadata results
	  Map<String, String> dataMap = new HashMap<>();
	  dataMap.put("data_owner", "Alison Van Dyke");
	  dataMap.put("affiliation", "NCI, DCCP, SRP");
	  dataMap.put("project_title", "SEER VTR BCT Pilot");
	  dataMap.put("access", "Closed Access");
	  dataMap.put("method", "whole-slide-images");
	  dataMap.put("start_date", "12/12/2018");
	  dataMap.put("project_description", "some description");
	  dataMap.put("origin", "some origin");
	  dataMap.put("summary_of_datasets", "some summary");
	  dataMap.put("organism", "Homo Sapiens (human)");
	  dataMap.put("study_id", "VTRBCT");
	  dataMap.put("person_id", "BC7001");
	  
	  dataMap.put("piArchivePath",  "/SEER_VTR_SRP_Archive/PI_Alison_Van_Dyke");
	  dataMap.put("projectArchivePath", "/SEER_VTR_SRP_Archive/PI_Alison_Van_Dyke/Project_BCT_Pilot");
	  dataMap.put("datasetArchivePath",  "/SEER_VTR_SRP_Archive/PI_Alison_Van_Dyke/Project_BCT_Pilot/Dataset_BC7001");
	  
	  
	  validateCollectionMetadataResults(requestDto, dataMap);
	  
	 
	  //Validate object metadata results
	 
	  List<HpcMetadataEntry> entries = requestDto.getMetadataEntries();
	  assertEquals(7, entries.size());
	  
	  assertEquals("source_path", entries.get(0).getAttribute());
	  assertEquals("/seer-VTR/BCT/VTRBCT_BC7001_BL3_XX.svs", entries.get(0).getValue());
	  assertEquals("object_name", entries.get(1).getAttribute());
	  assertEquals("VTRBCT_BC7001_BL3_XX.svs", entries.get(1).getValue());
	  assertEquals("file_type", entries.get(2).getAttribute());
	  assertEquals("svs", entries.get(2).getValue());
	  assertEquals("block_id", entries.get(3).getAttribute());
	  assertEquals("BL3", entries.get(3).getValue());
	  assertEquals("block_description", entries.get(4).getAttribute());
	  assertEquals("Normal breast tissue block", entries.get(4).getValue());
	  assertEquals("slide_id", entries.get(5).getAttribute());
	  assertEquals("XX", entries.get(5).getValue());
	  assertEquals("slide_description", entries.get(6).getAttribute());
	  assertEquals("last slide of the tissue block", entries.get(6).getValue());
	 
	  
  }
  
  
  
  private void setupDataForMetadataJsonTest(String piCollectionName, String piName, String affiliation, 
		  String projectCollectionName, String projectTitle, String method, String startDate) {
      
	  
	  //Database entries for PI Collection metadata
	  List<MetadataMapping> piNameMetaMappings = new ArrayList<>();
	  MetadataMapping nameMapping = new MetadataMapping();
	  nameMapping.setCollectionName(piCollectionName);
	  nameMapping.setCollectionType("PI_Lab");
	  nameMapping.setMetaDataKey("data_owner");
	  nameMapping.setMetaDataValue(piName);
	  piNameMetaMappings.add(nameMapping);	 
	  
	  nameMapping = new MetadataMapping();
	  nameMapping.setCollectionName(piCollectionName);
	  nameMapping.setCollectionType("PI_Lab");
	  nameMapping.setMetaDataKey("affiliation");
	  nameMapping.setMetaDataValue(affiliation);
	  piNameMetaMappings.add(nameMapping);	 
	  
	  when(seerPathMetadataProcessorImpl.dmeSyncWorkflowService.findAllMetadataMappingByCollectionTypeAndCollectionNameAndDoc(
			  "PI_Lab", piCollectionName, "seer")).thenReturn(piNameMetaMappings);
	  
	  //Database entries for Project Collection metadata
	  List<MetadataMapping> projectNameMetaMappings = new ArrayList<>();
	  
	  MetadataMapping projectNameMapping = new MetadataMapping();
	  projectNameMapping.setCollectionName(projectCollectionName);
	  projectNameMapping.setCollectionType("Project");
	  projectNameMapping.setMetaDataKey("project_title");
	  projectNameMapping.setMetaDataValue(projectTitle);
	  projectNameMetaMappings.add(projectNameMapping); 
	  
	  projectNameMapping = new MetadataMapping();
	  projectNameMapping.setCollectionName(projectCollectionName);
	  projectNameMapping.setCollectionType("Project");
	  projectNameMapping.setMetaDataKey("access");
	  projectNameMapping.setMetaDataValue("Closed Access");
	  projectNameMetaMappings.add(projectNameMapping); 
	  
	  projectNameMapping = new MetadataMapping();
	  projectNameMapping.setCollectionName(projectCollectionName);
	  projectNameMapping.setCollectionType("Project");
	  projectNameMapping.setMetaDataKey("method");
	  projectNameMapping.setMetaDataValue(method);
	  projectNameMetaMappings.add(projectNameMapping); 
	  
	  projectNameMapping = new MetadataMapping();
	  projectNameMapping.setCollectionName(projectCollectionName);
	  projectNameMapping.setCollectionType("Project");
	  projectNameMapping.setMetaDataKey("start_date");
	  projectNameMapping.setMetaDataValue(startDate);
	  projectNameMetaMappings.add(projectNameMapping);
	  
	  projectNameMapping = new MetadataMapping();
	  projectNameMapping.setCollectionName(projectCollectionName);
	  projectNameMapping.setCollectionType("Project");
	  projectNameMapping.setMetaDataKey("project_description");
	  projectNameMapping.setMetaDataValue("some description");
	  projectNameMetaMappings.add(projectNameMapping);
	  
	  projectNameMapping = new MetadataMapping();
	  projectNameMapping.setCollectionName(projectCollectionName);
	  projectNameMapping.setCollectionType("Project");
	  projectNameMapping.setMetaDataKey("origin");
	  projectNameMapping.setMetaDataValue("some origin");
	  projectNameMetaMappings.add(projectNameMapping); 
	  
	  projectNameMapping = new MetadataMapping();
	  projectNameMapping.setCollectionName(projectCollectionName);
	  projectNameMapping.setCollectionType("Project");
	  projectNameMapping.setMetaDataKey("summary_of_datasets");
	  projectNameMapping.setMetaDataValue("some summary");
	  projectNameMetaMappings.add(projectNameMapping); 
	  
	  projectNameMapping = new MetadataMapping();
	  projectNameMapping.setCollectionName(projectCollectionName);
	  projectNameMapping.setCollectionType("Project");
	  projectNameMapping.setMetaDataKey("organism");
	  projectNameMapping.setMetaDataValue("Homo Sapiens (human)");
	  projectNameMetaMappings.add(projectNameMapping);
	  
	  projectNameMapping = new MetadataMapping();
	  projectNameMapping.setCollectionName(projectCollectionName);
	  projectNameMapping.setCollectionType("Project");
	  projectNameMapping.setMetaDataKey("study_id");
	  projectNameMapping.setMetaDataValue("VTRBCT");
	  projectNameMetaMappings.add(projectNameMapping);
	  
	  when(seerPathMetadataProcessorImpl.dmeSyncWorkflowService.findAllMetadataMappingByCollectionTypeAndCollectionNameAndDoc(
			  "Project", projectCollectionName, "seer")).thenReturn(projectNameMetaMappings);
	  
  }
  
  private void validateCollectionMetadataResults(HpcDataObjectRegistrationRequestDTO requestDto, Map<String, String> dataMap) {
			  
	  assertNotNull(requestDto);
	  assertEquals(true, requestDto.getGenerateUploadRequestURL());
	  assertEquals(true, requestDto.getCreateParentCollections());
	  List<HpcBulkMetadataEntry> bulkMetadataEntries = requestDto.getParentCollectionsBulkMetadataEntries().getPathsMetadataEntries();
	  assertEquals(3, bulkMetadataEntries.size());
			  
	  HpcBulkMetadataEntry bulkEntry  = bulkMetadataEntries.get(0);
	  assertEquals(dataMap.get("piArchivePath"), bulkEntry.getPath());
	  List<HpcMetadataEntry> metadataEntries = bulkEntry.getPathMetadataEntries();
	  assertEquals(3, metadataEntries.size());
	  HpcMetadataEntry entry = metadataEntries.get(0);
	  assertEquals("collection_type", entry.getAttribute());
	  assertEquals("PI_Lab", entry.getValue());
	  entry = metadataEntries.get(1);
	  assertEquals("data_owner", entry.getAttribute());
	  assertEquals(dataMap.get("data_owner"), entry.getValue());
	  entry = metadataEntries.get(2);
	  assertEquals("affiliation", entry.getAttribute());
	  assertEquals(dataMap.get("affiliation"), entry.getValue());
	  
	  bulkEntry  = bulkMetadataEntries.get(1);
	  assertEquals(dataMap.get("projectArchivePath"), bulkEntry.getPath());
	  metadataEntries = bulkEntry.getPathMetadataEntries();
	  assertEquals(10, metadataEntries.size());
	  entry = metadataEntries.get(0);
	  assertEquals("collection_type", entry.getAttribute());
	  assertEquals("Project", entry.getValue());
	  entry = metadataEntries.get(1);
	  assertEquals("project_title", entry.getAttribute());
	  assertEquals(dataMap.get("project_title"), entry.getValue());
	  entry = metadataEntries.get(2);
	  assertEquals("access", entry.getAttribute());
	  assertEquals(dataMap.get("access"), entry.getValue());
	  entry = metadataEntries.get(3);
	  assertEquals("method", entry.getAttribute());
	  assertEquals(dataMap.get("method"), entry.getValue());
	  entry = metadataEntries.get(4);
	  assertEquals("start_date", entry.getAttribute());
	  assertEquals(dataMap.get("start_date"), entry.getValue());
	  entry = metadataEntries.get(5);
	  assertEquals("project_description", entry.getAttribute());
	  assertEquals(dataMap.get("project_description"), entry.getValue());
	  entry = metadataEntries.get(6);
	  assertEquals("origin", entry.getAttribute());
	  assertEquals(dataMap.get("origin"), entry.getValue());
	  entry = metadataEntries.get(7);
	  assertEquals("summary_of_datasets", entry.getAttribute());
	  assertEquals(dataMap.get("summary_of_datasets"), entry.getValue());
	  entry = metadataEntries.get(8);
	  assertEquals("organism", entry.getAttribute());  
	  assertEquals(dataMap.get("organism"), entry.getValue());
	  entry = metadataEntries.get(9);
	  assertEquals("study_id", entry.getAttribute());
	  assertEquals(dataMap.get("study_id"), entry.getValue());
	  
	   
			  
	  bulkEntry  = bulkMetadataEntries.get(2);
	  assertEquals(dataMap.get("datasetArchivePath"), bulkEntry.getPath());
	  metadataEntries = bulkEntry.getPathMetadataEntries();
	  assertEquals(2, metadataEntries.size());
	  entry = metadataEntries.get(0);
	  assertEquals("collection_type", entry.getAttribute());
	  assertEquals("Dataset", entry.getValue());
	  entry = metadataEntries.get(1);
	  assertEquals("person_id", entry.getAttribute());
	  assertEquals(dataMap.get("person_id"), entry.getValue());	
  }
 
  
}
