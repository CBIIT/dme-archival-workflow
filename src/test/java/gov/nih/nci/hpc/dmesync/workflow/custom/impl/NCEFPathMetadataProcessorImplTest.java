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
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import gov.nih.nci.hpc.dmesync.DmeSyncWorkflowServiceFactory;
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
@SpringBootTest({"hpc.server.url=https://fr-s-hpcdm-gp-d.ncifcrf.gov:7738/hpc-server", "auth.token=xxxx"})
public class NCEFPathMetadataProcessorImplTest {


//The service under test.
@Autowired
NCEFPathMetadataProcessorImpl ncefPathMetadataProcessorImpl;	



 @Before
 public void init() {
     //Create the statusInfo object with the test Path
	 ncefPathMetadataProcessorImpl.destinationBaseDir = "/FNL_NCEF_Archive";
 }
 
 private StatusInfo setupStatusInfo(String originalFilePath, String sourceFilePath) {
	 StatusInfo statusInfoObj = new StatusInfo();
	 statusInfoObj.setOriginalFilePath(originalFilePath); 
	 statusInfoObj.setSourceFilePath(sourceFilePath); 
	 
     /*ncefPathMetadataProcessorImpl.dmeSyncWorkflowService = Mockito.mock(DmeSyncWorkflowService.class);
	 
	 //Simulate the CollectionNameMetadata rows that you will be retrieving from the DB. 
	 
	 CollectionNameMapping piMapping = new CollectionNameMapping();
	 piMapping.setMapValue("Ronen Marmorstein");
	 when(ncefPathMetadataProcessorImpl.dmeSyncWorkflowService.findCollectionNameMappingByMapKeyAndCollectionType("RMarmorstein", "PI")).thenReturn(piMapping);*/
	  
	 ncefPathMetadataProcessorImpl.destinationBaseDir = "/FNL_NCEF_Archive";
	 
	 
	 return statusInfoObj;
 }
  
  @Test
  public void testGetArchivePath() throws DmeSyncMappingException {
	  
	  StatusInfo statusInfoNS = setupStatusInfo("/mnt/NCEF-CryoEM/Archive_Staging/SChakrapani-NCEF-010-019-10025.tar", 
	  		 "/mnt/IRODsScratch/work/SChakrapani-NCEF-010-019-10025.tar");
	  
	  //Determine the expected and actual archive path
	  ///mnt/NCEF-CryoEM/RMarmorstein-NCEF-033-007-10031/RMarmorstein-NCEF-033-007-10031-A.tar
	  String expectedArchivePath = "/FNL_NCEF_Archive/PI_SChakrapani/Project_NCEF-010-019/Run_10025/SChakrapani-NCEF-010-019-10025.tar";
	  String computedArchivePath = ncefPathMetadataProcessorImpl.getArchivePath(statusInfoNS);
	  
	  //Confirm they are same
	  assertEquals(expectedArchivePath, computedArchivePath);
	  
	  
      
    
  }
  
 
  @Test
  public void testGetMetadataJson() throws DmeSyncMappingException, DmeSyncWorkflowException {
	  
	  StatusInfo statusInfoNS = setupStatusInfo("/mnt/NCEF-CryoEM/Archive_Staging/SChakrapani-NCEF-010-019-10025.tar", 
			  "/mnt/IRODsScratch/work/SChakrapani-NCEF-010-019-10025.tar");
	  
	  setupDataForMetadataJsonTest("SChakrapani", "Sudha Chakrapani", "CWRU", "NCEF-010-019", "6/1/2018");
	 
	  //Execute the method to test
	  HpcDataObjectRegistrationRequestDTO requestDto = 
		ncefPathMetadataProcessorImpl.getMetaDataJson(statusInfoNS);
	  
	  //Validate collection metadata results
	//Validate collection metadata results
	  Map<String, String> dataMap = new HashMap<>();
	  dataMap.put("data_owner", "Sudha Chakrapani");
	  dataMap.put("affiliation", "CWRU");
	  dataMap.put("start_date", "4/1/18");
	  dataMap.put("project_title", "NCEF-010-019");
	  dataMap.put("submission_id", "019");
	  dataMap.put("run_number", "10025");
	  dataMap.put("instrument_name", "Instrument2");
	  dataMap.put("piArchivePath",  "/FNL_NCEF_Archive/PI_SChakrapani");
	  dataMap.put("projectArchivePath", "/FNL_NCEF_Archive/PI_SChakrapani/Project_NCEF-010-019");
	  dataMap.put("runArchivePath",  "/FNL_NCEF_Archive/PI_SChakrapani/Project_NCEF-010-019/Run_10025");
	  validateCollectionMetadataResults(requestDto, dataMap);
	  
	 
	  //Validate object metadata results
	 
	  List<HpcMetadataEntry> entries = requestDto.getMetadataEntries();
	  assertEquals(2, entries.size());
	  assertEquals("object_name", entries.get(0).getAttribute());
	  assertEquals("SChakrapani-NCEF-010-019-10025.tar", entries.get(0).getValue());
	  assertEquals("source_path", entries.get(1).getAttribute());
	  assertEquals("/mnt/NCEF-CryoEM/SChakrapani-NCEF-010-019-10025.tar", entries.get(1).getValue());
	  
  }
  
  
  @Test
  public void testGetArchivePathforWithRunFolder() throws DmeSyncMappingException {
	  
	  StatusInfo statusInfoNS = setupStatusInfo("/mnt/NCEF-CryoEM/Archive_Staging/RMarmorstein-NCEF-033-007-10031/RMarmorstein-NCEF-033-007-10031-A.tar",
			  "/mnt/IRODsScratch/work/RMarmorstein-NCEF-033-007-10031/RMarmorstein-NCEF-033-007-10031-A.tar");
	  
	  //Determine the expected and actual archive path
	  ///mnt/NCEF-CryoEM/RMarmorstein-NCEF-033-007-10031/RMarmorstein-NCEF-033-007-10031-A.tar
	  String expectedArchivePath = "/FNL_NCEF_Archive/PI_RMarmorstein/Project_NCEF-033-007/Run_10031-A/RMarmorstein-NCEF-033-007-10031-A.tar";
	  String computedArchivePath = ncefPathMetadataProcessorImpl.getArchivePath(statusInfoNS);
	  
	  //Confirm they are same
	  assertEquals(expectedArchivePath, computedArchivePath);
	  
	  statusInfoNS = setupStatusInfo("/mnt/NCEF-CryoEM/Archive_Staging/RMarmorstein-NCEF-033-007-10031/RMarmorstein-NCEF-033-007-10032.tar",
			  "/mnt/IRODsScratch/work/RMarmorstein-NCEF-033-007-10031/RMarmorstein-NCEF-033-007-10032.tar");
	  
	  //Determine the expected and actual archive path
	  ///mnt/NCEF-CryoEM/RMarmorstein-NCEF-033-007-10031/RMarmorstein-NCEF-033-007-10031-A.tar
	  expectedArchivePath = "/FNL_NCEF_Archive/PI_RMarmorstein/Project_NCEF-033-007/Run_10032/RMarmorstein-NCEF-033-007-10032.tar";
	  computedArchivePath = ncefPathMetadataProcessorImpl.getArchivePath(statusInfoNS);
	  
	  //Confirm they are same
	  assertEquals(expectedArchivePath, computedArchivePath);
    
  }
  
  
  @Test
  public void testGetMetadataJsonForWithRunFolder() throws DmeSyncMappingException, DmeSyncWorkflowException {
	  
	  StatusInfo statusInfoNS = setupStatusInfo("/mnt/NCEF-CryoEM/Archive_Staging/RMarmorstein-NCEF-033-007-10031/RMarmorstein-NCEF-033-007-10031-A.tar",
			  "/mnt/IRODsScratch/work/RMarmorstein-NCEF-033-007-10031/RMarmorstein-NCEF-033-007-10031-A.tar");
	  
	  setupDataForMetadataJsonTest("RMarmorstein", "Ronen Marmorstein", "UPENN", "NCEF-033-007", "4/1/2018");
	 
	  //Execute the method to test
	  HpcDataObjectRegistrationRequestDTO requestDto = 
				ncefPathMetadataProcessorImpl.getMetaDataJson(statusInfoNS);
	  
	  //Validate collection metadata results
	  Map<String, String> dataMap = new HashMap<>();
	  dataMap.put("data_owner", "Ronen Marmorstein");
	  dataMap.put("affiliation", "UPENN");
	  dataMap.put("start_date", "4/1/18");
	  dataMap.put("project_title", "NCEF-033-007");
	  dataMap.put("submission_id", "007");
	  dataMap.put("run_number", "10031-A");
	  dataMap.put("instrument_name", "Instrument2");
	  dataMap.put("piArchivePath",  "/FNL_NCEF_Archive/PI_RMarmorstein");
	  dataMap.put("projectArchivePath",  "/FNL_NCEF_Archive/PI_RMarmorstein/Project_NCEF-033-007");
	  dataMap.put("runArchivePath",  "/FNL_NCEF_Archive/PI_RMarmorstein/Project_NCEF-033-007/Run_10031-A");
	  validateCollectionMetadataResults(requestDto, dataMap);
	  
	  //Validate object metadata results
	  
	  List<HpcMetadataEntry> entries = requestDto.getMetadataEntries();
	  assertEquals(2, entries.size());
	  assertEquals("object_name", entries.get(0).getAttribute());
	  assertEquals("RMarmorstein-NCEF-033-007-10031-A.tar", entries.get(0).getValue());
	  assertEquals("source_path", entries.get(1).getAttribute());
	  assertEquals("/mnt/NCEF-CryoEM/Archive_Staging/RMarmorstein-NCEF-033-007-10031/RMarmorstein-NCEF-033-007-10031-A.tar", entries.get(1).getValue());
  }
  
  
  
  private void setupDataForMetadataJsonTest(String piCollectionName, String piName, String affiliation, String projectCollectionName, String startDate) {
      
	  ncefPathMetadataProcessorImpl.dmeSyncWorkflowService = Mockito.mock(DmeSyncWorkflowServiceFactory.class);
	  when(ncefPathMetadataProcessorImpl.dmeSyncWorkflowService.getService("local")).thenReturn(Mockito.mock(DmeSyncWorkflowService.class));
	  List<MetadataMapping> piNameMetaMappings = new ArrayList<>();
	  MetadataMapping nameMapping = new MetadataMapping();
	  nameMapping.setCollectionName(piCollectionName);
	  nameMapping.setCollectionType("PI");
	  nameMapping.setMetaDataKey("data_owner");
	  nameMapping.setMetaDataValue(piName);
	  piNameMetaMappings.add(nameMapping);	 
	  
	  nameMapping = new MetadataMapping();
	  nameMapping.setCollectionName(piCollectionName);
	  nameMapping.setCollectionType("PI");
	  nameMapping.setMetaDataKey("affiliation");
	  nameMapping.setMetaDataValue(affiliation);
	  piNameMetaMappings.add(nameMapping);	 
	  
	  when(ncefPathMetadataProcessorImpl.dmeSyncWorkflowService.getService("local").findAllMetadataMappingByCollectionTypeAndCollectionNameAndDoc(
			  "PI_Lab", piCollectionName, "ncef")).thenReturn(piNameMetaMappings);
	  
	  List<MetadataMapping> projectNameMetaMappings = new ArrayList<>();
	  
	  MetadataMapping projectNameMapping = new MetadataMapping();
	  projectNameMapping.setCollectionName(projectCollectionName);
	  projectNameMapping.setCollectionType("Project");
	  projectNameMapping.setMetaDataKey("project_description");
	  projectNameMapping.setMetaDataValue("some description");
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
	  projectNameMapping.setMetaDataKey("summary_of_samples");
	  projectNameMapping.setMetaDataValue("some summary of samples");
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
	  projectNameMapping.setMetaDataKey("origin");
	  projectNameMapping.setMetaDataValue("some origin");
	  projectNameMetaMappings.add(projectNameMapping);
	  
	  
	  when(ncefPathMetadataProcessorImpl.dmeSyncWorkflowService.getService("local").findAllMetadataMappingByCollectionTypeAndCollectionNameAndDoc(
			  "Project", projectCollectionName, "ncef")).thenReturn(projectNameMetaMappings);
	  
  }
  
  private void validateCollectionMetadataResults(HpcDataObjectRegistrationRequestDTO requestDto, Map<String, String> dataMap) {
			  
	  assertNotNull(requestDto);
	  assertEquals(true, requestDto.getGenerateUploadRequestURL());
	  assertEquals(true, requestDto.getCreateParentCollections());
	  List<HpcBulkMetadataEntry> bulkMetadataEntries = requestDto.getParentCollectionsBulkMetadataEntries().getPathsMetadataEntries();
	  assertEquals(4, bulkMetadataEntries.size());
			  
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
	  assertEquals("submission_id", entry.getAttribute());
	  assertEquals(dataMap.get("submission_id"), entry.getValue());
	  entry = metadataEntries.get(3);
	  assertEquals("access", entry.getAttribute());
	  assertEquals("Closed Access", entry.getValue());
	  entry = metadataEntries.get(4);
	  assertEquals("method", entry.getAttribute());
	  assertEquals("CryoEM", entry.getValue());
	  entry = metadataEntries.get(5);
	  assertEquals("start_date", entry.getAttribute());
	  assertEquals(dataMap.get("start_date"), entry.getValue());
	  entry = metadataEntries.get(6);
	  assertEquals("project_description", entry.getAttribute());
	  assertEquals("some description", entry.getValue());
	  entry = metadataEntries.get(7);
	  assertEquals("origin", entry.getAttribute());
	  assertEquals("some origin", entry.getValue());
	  entry = metadataEntries.get(8);
	  assertEquals("summary_of_datasets", entry.getAttribute());
	  assertEquals("some summary of samples", entry.getValue());
	  entry = metadataEntries.get(9);
	  assertEquals("organism", entry.getAttribute());
	  assertEquals("Homo Sapiens (human)", entry.getValue());
	  
	  	   
			  
	  bulkEntry  = bulkMetadataEntries.get(2);
	  assertEquals(dataMap.get("runArchivePath"), bulkEntry.getPath());
	  metadataEntries = bulkEntry.getPathMetadataEntries();
	  assertEquals(3, metadataEntries.size());
	  entry = metadataEntries.get(0);
	  assertEquals("collection_type", entry.getAttribute());
	  assertEquals("Run", entry.getValue());
	  entry = metadataEntries.get(1);
	  assertEquals("run_number", entry.getAttribute());
	  assertEquals(dataMap.get("run_number"), entry.getValue());
	  entry = metadataEntries.get(2);
	  assertEquals("instrument_name", entry.getAttribute());
	  assertEquals(dataMap.get("instrument_name"), entry.getValue());		
  }
 
  
}
