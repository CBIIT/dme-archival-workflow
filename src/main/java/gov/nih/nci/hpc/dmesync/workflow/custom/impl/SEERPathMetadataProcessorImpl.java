package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.springframework.stereotype.Service;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncPathMetadataProcessor;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntries;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;

/**
 * SEER DME Path and Meta-data Processor Implementation
 * 
 * @author menons2
 *
 */
@Service("seer")
public class SEERPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
    implements DmeSyncPathMetadataProcessor {

  //SEER Custom logic for DME path construction and meta data creation

  @Override
  public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

    logger.info("[PathMetadataTask] SEERgetArchivePath called");

    //extract the file name from the source Path
    //Example source path - /seer-VTR/PDAC/VTRPDAC_Test_PC2428_BL1_00.svs
    ///mnt/IRODsScratch/work/RMarmorstein-NCEF-033-007-10031/RMarmorstein-NCEF-033-007-10031-A.tar
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
            + "/Dataset_"
            + getDatasetCollectionName(object)
            + "/"
            + fileName;
    
    //replace spaces with underscore
    archivePath = archivePath.replace(" ", "_");
    
    logger.info("Archive path for {} : {}", object.getOriginalFilePath(), archivePath);

    return archivePath;
  }
  

  @Override
  public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object) throws DmeSyncMappingException, DmeSyncWorkflowException {
	  
	  //Add to HpcBulkMetadataEntries for path attributes
	  HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();
	  
	    
      //Add path metadata entries for "PI_XXX" collection
	  //Example row: collectionType - PI, collectionName - Alison_Van_Dyke (supplied)
	  //key = pi_name, value = Alison Van Dyke (supplied)
	  //key = affiliation, value = NCI, DCCPS, SRP (supplied)
     
      String piCollectionName = getPiCollectionName(object);
      String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;
      HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
      pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "PI_Lab"));
      pathEntriesPI.setPath(piCollectionPath);
      hpcBulkMetadataEntries.getPathsMetadataEntries().add(populateStoredMetadataEntries(pathEntriesPI, "PI_Lab", piCollectionName, "seer"));
      
      logger.info("created PI_Lab metadata");
      
      //Add path metadata entries for "Project_XXX" collection
	  //Example row: collectionType - Project, collectionName - PDAC_Pilot (supplied), 
	  //key = project_title, value = SEER VTR PDAC Pilot (supplied)
      //key = access, value = "Closed Access" (supplied)
      //key = method, value = "whole-slide images" (supplied)
	  //key = start_date, value = 12/12/18 (supplied)
	  //key = project_description, value = (supplied)
      //key = origin, value = (supplied)
      //key = summary_of_datasets, value = (supplied)
      //key = organism, value = (supplied)
      //key = study_id, value = (supplied)
     
      String projectCollectionName = getProjectCollectionName(object);
      String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;
      HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
      pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));

      pathEntriesProject.setPath(projectCollectionPath);
      hpcBulkMetadataEntries.getPathsMetadataEntries().add(populateStoredMetadataEntries(pathEntriesProject, "Project", projectCollectionName, "seer"));
      
      //Add path metadata entries for "Dataset_XXX" collection
      //Example row: collectionType - Dataset, collectionName - Dataset_PC7001 (derived)
	  //key = person_id, value = PC7001 (derived)
      String datasetCollectionName = getDatasetCollectionName(object);
      String datasetCollectionPath = projectCollectionPath + "/Dataset_" + datasetCollectionName;
      HpcBulkMetadataEntry pathEntriesRun = new HpcBulkMetadataEntry();
      pathEntriesRun.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Dataset"));
      pathEntriesRun.getPathMetadataEntries().add(createPathEntry("person_id", getPersonId(object)));
      pathEntriesRun.setPath(datasetCollectionPath);
      hpcBulkMetadataEntries.getPathsMetadataEntries().add(populateStoredMetadataEntries(pathEntriesRun, "Dataset", datasetCollectionName, "seer"));
		     
      //Set it to dataObjectRegistrationRequestDTO
      HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();
      dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
      dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
      dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);

      //Add object metadata
      dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("source_path", object.getOriginalFilePath()));
      dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("object_name", Paths.get(object.getOriginalFilePath()).toFile().getName()));
      dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("file_type", getFileType(object)));
      dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("block_id", getBlockId(object)));
      dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("block_description", getBlockDescription(object)));
      dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("slide_id", getSlideId(object)));
      dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("slide_description", getSlideDescription(object)));
      
      logger.info(
        "SEER custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
      return dataObjectRegistrationRequestDTO;
  }



  private String getPiCollectionName(StatusInfo object) throws DmeSyncMappingException {
	  String piCollectionName = getCollectionMappingValue(getProjectId(object), "PI_Lab", "seer");
	  logger.info("PI Collection Name: {}", piCollectionName);
	  return piCollectionName;
	  
  }
  
  
 
  private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {

	  String projectCollectionName =  getCollectionMappingValue(getProjectId(object), "Project", "seer");
	  logger.info("projectCollectionName: {}", projectCollectionName);
    return projectCollectionName;
  }


  
  private String getDatasetCollectionName(StatusInfo object) {
      String datasetCollectionName = getPersonId(object);
	  logger.info("datasetCollectionName: {}", datasetCollectionName);
      return datasetCollectionName;
  }
  
  private String getPersonId(StatusInfo object) {
	  String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
	  int startIndex = fileName.indexOf("_BC") + 1;
	  String personIdSubstring = fileName.substring(startIndex);
	  return fileName.substring(startIndex, startIndex + personIdSubstring.indexOf('_'));
	  	
  }

  private String getBlockId(StatusInfo object) {
	  String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
	  return fileName.substring(fileName.indexOf("_BL") + 1, fileName.lastIndexOf("_"));
  }
  
  private String getBlockDescription(StatusInfo object) {
	  String blockId = getBlockId(object);
	  String desc = null;
	  switch (blockId) {
		 case "BL1":
			  desc = "Breast tumor tissue block 1 with the highest tumor area and cellularity (≥20%) and least tumor necrosis (<20%)";
			  break;
		 case "BL2":
	    	  desc = "Breast tumor tissue block 2 with the second-highest tumor area and cellularity";
	    	  break;
		 case "BL3":
	    	  desc = "Normal breast tissue block";
	    	  break;
		 case "BL4":
			  desc = "Normal lymph node free of tumor";
			  break;
		 case "default":
	    	  logger.error("Cannot obtain description for block Id in {}", object.getSourceFilePath());
	  }
	  return desc;
	  
  }
  
  private String getSlideId(StatusInfo object) {
	  String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
	  return fileName.substring(fileName.lastIndexOf("_") + 1, fileName.indexOf('.'));
  }
  
  private String getSlideDescription(StatusInfo object) {
	  String slideId = getSlideId(object);
	  String desc = null;
	  switch (slideId) {
		 case "00":
			  desc = "first slide of the tissue block";
			  break;
		 case "AA":
	    	  desc = "first replacement H&E slide";
	    	  break;
		 case "YY":
	    	  desc = "second replacement H&E slide";
	    	  break;	  
		 case "XX":
	    	  desc = "last slide of the tissue block";
	    	  break;
		 case "11":
			  desc = "HER2 immunohistochemistry slide";
			  break;
		 case "12":
			  desc = "HER2 immunohistochemistry slide";
			  break;
		 case "default":
	    	  logger.error("Cannot obtain description for slide Id in {}", object.getSourceFilePath());
	  }
	  return desc;
	  
  }

  private String getFileType(StatusInfo object) {
	  String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
	  return fileName.substring(fileName.indexOf('.') + 1);
  }
  
  private String getProjectId(StatusInfo object) {
      Path fullFilePath = Paths.get(object.getOriginalFilePath());
      return fullFilePath.getParent().toFile().getName();
  }


}
