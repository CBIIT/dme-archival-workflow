package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncPathMetadataProcessor;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntries;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;

/**
 * NCEF DME Path and Meta-data Processor Implementation
 * 
 * @author menons2
 *
 */
@Service("ncef")
public class NCEFPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
    implements DmeSyncPathMetadataProcessor {

  //NCEF Custom logic for DME path construction and meta data creation

	@Value("${dmesync.additional.metadata.excel:}")
	private String metadataFile;

	@Value("${dmesync.additional.pi.metadata.excel:}")
	private String piMetadataFile;

	@Override
    public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

    logger.info("[PathMetadataTask] NCEFgetArchivePath called");

    //extract the user name from the source Path
    //Example source path - /mnt/IRODsScratch/work/RMarmorstein-NCEF-033-007-10031/RMarmorstein-NCEF-033-007-10031-A.tar
    String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
        
    //Get the PI collection value from the CollectionNameMetadata
    //Example row - mapKey - RMarmorstein, collectionType - PI, mapValue - Ronen Marmorstein
    //Extract the Project value from the Path

    String archivePath =
        destinationBaseDir
            + "/PI_"
            + getPiCollectionName(object)
            + "/Project_"
            + getProjectCollectionName(object)
            + "/Run_"
            + getRunCollectionName(object)
            + "/"
            + fileName;
    
    //replace spaces with underscore
    archivePath = archivePath.replace(" ", "_");
    
    logger.info("Archive path for {} : {}", object.getOriginalFilePath(), archivePath);

    return archivePath;
  }
  

  @Override
  public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object) throws DmeSyncMappingException, DmeSyncWorkflowException {


	  
      HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();
	  try {
		  String path = object.getOriginalFilePath();
	      

		  //Add to HpcBulkMetadataEntries for path attributes
		  HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();
		  
		    
	      //Add path metadata entries for "PI_XXX" collection
		  //Example row: collectionType - PI, collectionName - Marmostein (derived)
		  //key = pi_name, value = Ronen Marmorstein (supplied)
		  //key = affiliation, value = UPENN (supplied)
		  // load the PI metadata from the externally placed excel
		  threadLocalMap.set(loadMetadataFile(piMetadataFile, "pi_collection_name"));
	      String piCollectionName = getPiCollectionName(object);
	      String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;
	      HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
	      pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "PI_Lab"));
	      pathEntriesPI.setPath(piCollectionPath);
		  pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner", getAttrValueWithKey(piCollectionName,"data_owner")));
		  pathEntriesPI.getPathMetadataEntries().add(
					createPathEntry("data_generator", getAttrValueWithKey(piCollectionName, "data_generator")));
		  pathEntriesPI.getPathMetadataEntries().add(createPathEntry("affiliation",
					getAttrValueWithKey(piCollectionName, "affiliation")));
		  hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPI);
	      
	      //Add path metadata entries for "Project_XXX" collection
		  //Example row: collectionType - Project, collectionName - NCEF-033-007 (derived), 
		  //key = project_title, value = NCEF-033-007 (derived)
	      //key submission_id, value = 007 (derived)
	      //key = access, value = "Closed Access" (constant)
	      //key = method, value = "CryoEM" (constant)
		  //key = project_start_date, value = (supplied)
		  //key = project_description, value = (supplied)
	      //key = origin, value = (supplied)
	      //key = summary_of_samples, value = (supplied)
	      //key = organism, value = (supplied)
	     
		  // load the user metadata from the externally placed excel
		  threadLocalMap.set(loadMetadataFile(metadataFile, "path"));
	      String projectCollectionName = getProjectCollectionName(object);
	      String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;
	      HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_title", projectCollectionName));
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("submission_id", getSubmissionId(projectCollectionName)));
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Closed Access"));
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("method", "CryoEM"));
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_start_date", getAttrValueWithKey(path, "start_date"), "MM/dd/yy"));
	      String projectDescription = getAttrValueWithKey(path, "project_description");
	      if(projectDescription != null && projectDescription.length() > 2500) {
	    	  pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_description", projectDescription.substring(0, 2500)));
	    	  pathEntriesProject.getPathMetadataEntries().add(createPathEntry("additional_project_description", projectDescription.substring(2500)));
	      }
	      else
	    	  pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_description", getAttrValueWithKey(path, "project_description")));
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("origin", "NCEF"));
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("summary_of_datasets", getAttrValueWithKey(path, "summary_of_datasets")));
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("organism", getAttrValueWithKey(path, "organism")));
	      pathEntriesProject.setPath(projectCollectionPath);
	      hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);
	 
	      
	      //Add path metadata entries for "Run_XXX" collection
	      //Example row: collectionType - Run, collectionName - Run_007 (derived)
		  //key = run_number, value = 007 (derived)
	      //key instrument_id, value = 1 (derived)
	      //key instrument_name, value = Instrument2 (derived)
	      String runCollectionName = getRunCollectionName(object);
	      String runCollectionPath = projectCollectionPath + "/Run_" + runCollectionName;
	      HpcBulkMetadataEntry pathEntriesRun = new HpcBulkMetadataEntry();
	      pathEntriesRun.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Run"));
	      pathEntriesRun.getPathMetadataEntries().add(createPathEntry("run_number", runCollectionName));
	      pathEntriesRun.getPathMetadataEntries().add(createPathEntry("instrument_name", getInstrumentName(runCollectionName)));
	      pathEntriesRun.setPath(runCollectionPath);
	      hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesRun);
			     
	      String rawDataCollectionPath = runCollectionPath + "/Raw_Data";
	      HpcBulkMetadataEntry pathEntriesRawData = new HpcBulkMetadataEntry();
	      pathEntriesRawData.setPath(rawDataCollectionPath);
	      //Add path metadata entries for "Raw_Data" collection
	      pathEntriesRawData.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Raw_Data"));
	      hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesRawData);
	      
	      //Set it to dataObjectRegistrationRequestDTO
	      dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
	      dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
	      dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);
	
	      //Add object metadata
	      dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("object_name", Paths.get(object.getSourceFilePath()).toFile().getName()));
	      dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("source_path", object.getOriginalFilePath()));
	      
  } finally {
	threadLocalMap.remove();
  }
      logger.info(
        "NCEF custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
      return dataObjectRegistrationRequestDTO;
  }


  private String getCollectionNameFromParent(StatusInfo object, String parentName) {
	  //Example originalFilepath - /mnt/NCEF-CryoEM/Archive_Staging/RMarmorstein-NCEF-033-007-10031/RMarmorstein-NCEF-033-007-10031-A.tar
	  Path fullFilePath = Paths.get(object.getOriginalFilePath());
	  logger.info("Full File Path = {}", fullFilePath);
	  int count = fullFilePath.getNameCount();
	  for (int i = 0; i <= count; i++) {
	    if (fullFilePath.getParent().getFileName().toString().equals(parentName)) {
	      return fullFilePath.getFileName().toString();
	    }
	    fullFilePath = fullFilePath.getParent();
	  }
    return null;
  }


  private String getUserId(StatusInfo object) {
	  
	  String userDirName = getCollectionNameFromParent(object, "Archive_Staging");
	  //Example: If fullFilePath is /mnt/NCEF-CryoEM/Archive_Staging/RMarmorstein-NCEF-033-007-10031/RMarmorstein-NCEF-033-007-10031-A.tar
	  //then the userDirName will be RMarmorstein
	  return userDirName.substring(0, userDirName.indexOf("-"));
  }


  private String getPiCollectionName(StatusInfo object) throws DmeSyncMappingException {
	  String piCollectionName = null;
	  //Example: If originalFilePath is /mnt/NCEF-CryoEM/Archive_Staging/RMarmorstein-NCEF-033-007-10031/RMarmorstein-NCEF-033-007-10031-A.tar
	  //then the projectDirName will be RMarmorstein-NCEF-033-007-10031
	  String piDirName = getCollectionNameFromParent(object, "Archive_Staging");
	  logger.info("PI Directory Name: {}", piDirName);
	  
	  if(piDirName != null) {
		  //return RMarmorstein
		  piCollectionName =  piDirName.substring(0, piDirName.indexOf("-"));
	  } else {
		  piCollectionName = getCollectionMappingValue(getUserId(object), "PI", "ncef");
	  }
	  logger.info("PI Collection Name: {}", piCollectionName);
	  return piCollectionName;
	  
  }
  
  
  private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {
	  String projectCollectionName = null;
	  //Example: If originalFilePath is /mnt/NCEF-CryoEM/Archive_Staging/RMarmorstein-NCEF-033-007-10031/RMarmorstein-NCEF-033-007-10031-A.tar
	  //then the projectDirName will be RMarmorstein-NCEF-033-007-10031
	  String projectDirName = getCollectionNameFromParent(object, "Archive_Staging");
	  logger.info("Project Directory Name: {}", projectDirName);
	  if(projectDirName != null) {
		  int startIndex = projectDirName.indexOf("NCEF");
		  if(startIndex != -1) {
			  //return NCEF-033-007
			  int endIndex = projectDirName.lastIndexOf('-');
			  projectCollectionName =  projectDirName.substring(startIndex, endIndex);
		  } else {
			  projectCollectionName =  getCollectionMappingValue(getUserId(object), "Project", "ncef");
		  }
	  }
	  
	  logger.info("projectCollectionName: {}", projectCollectionName);
    return projectCollectionName;
  }


  private String getSubmissionId(String projectCollectionName) {
	  return projectCollectionName.substring(projectCollectionName.lastIndexOf("-") + 1);
  }
  
  
  private String getRunCollectionName(StatusInfo object) throws DmeSyncMappingException {
	  String runCollectionName = null;
	  //Example: If sourceFilePath is /mnt/IRODsScratch/work/RMarmorstein-NCEF-033-007-10031/RMarmorstein-NCEF-033-007-10031-A.tar
	  //then the runDirName will be RMarmorstein-NCEF-033-007-10031-A.tar
	  String runDirName = Paths.get(object.getSourceFilePath()).toFile().getName();
	  logger.info("Run Directory Name: {}", runDirName);
	  if(runDirName != null) {
		  String projectDirName = getCollectionNameFromParent(object, "Archive_Staging");
		  int startIndex = projectDirName.lastIndexOf('-');
		  if(startIndex != -1) {
			  //returns 10031-A
			  runCollectionName =  runDirName.substring(startIndex + 1, runDirName.indexOf(".tar"));
		  } else {
			  runCollectionName = getCollectionMappingValue(getUserId(object), "Run", "ncef");
		  }
	  }
	 
	 logger.info("runCollectionName: {}", runCollectionName);
    return runCollectionName;
  }

  private String getInstrumentName(String runNumber) {
	  //Example: If runNumber is 10031-A then the instrumentId will be 1
	  //TBD get the instrument names.
	  String instrumentId =  runNumber.substring(0, 1);
	  if(instrumentId.equals("0")) {
		  return "Instrument1";
	  }
	  
	  return "Instrument2";
  }
	  
  

}
