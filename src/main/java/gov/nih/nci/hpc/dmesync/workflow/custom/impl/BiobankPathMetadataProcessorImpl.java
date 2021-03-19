package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.lang3.StringUtils;
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
 * CMB DME Path and Meta-data Processor Implementation
 * 
 * @author dinhys
 *
 */
@Service("biobank")
public class BiobankPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
    implements DmeSyncPathMetadataProcessor {

  //CMB Custom logic for DME path construction and meta data creation

  @Value("${dmesync.additional.metadata.excel:}")
  private String metadataFile;
	
  @Override
  public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

    logger.info("[PathMetadataTask] Biobank getArchivePath called");
      
    //Get the PI collection name from the PI column from metadata file using path
    String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
   
    // load the user metadata from the externally placed excel
    threadLocalMap.set(loadMetadataFile(metadataFile, "path"));
  
    String archivePath =
        destinationBaseDir
            + "/PI_"
            + getPiCollectionName()
            + "/Project_"
            + getProjectCollectionName()
            + "/"
            + getCollectionNameFromParent(object, "CMB")
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

		  //Add to HpcBulkMetadataEntries for path attributes
		  HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();
		  
		    
	      //Add path metadata entries for "PI_Helen_Moore" collection
		  //Example row: collectionType - PI_Lab, collectionName - Helen_Moore (supplied)
		  //key = pi_name, value = ? (supplied)
		  //key = affiliation, value = ? (supplied)
	     
	      String piCollectionName = getPiCollectionName();
	      String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName.replace(" ", "_");
	      HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
	      pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "PI_Lab"));
	      pathEntriesPI.setPath(piCollectionPath);
	      hpcBulkMetadataEntries.getPathsMetadataEntries().add(populateStoredMetadataEntries(pathEntriesPI, "PI_Lab", piCollectionName));
	      
	      //Add path metadata entries for "Project_XXX" collection
		  //Example row: collectionType - Project, collectionName - CMB (supplied)
		  //key = project_title, value = ? (supplied)
	      //key = access, value = "Controlled Access" (constant)
	      //key = project_status, value = ? (supplied)
		  //key = start_date, value = (supplied)
		  //key = summary_of_samples, value = (supplied)
	      //key = origin, value = (supplied)
	      //key = description, value = (supplied)
	      //key = method, value = (supplied)
	     
	      String projectCollectionName = getProjectCollectionName();
	      String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;
	      HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Controlled Access"));
	      pathEntriesProject.setPath(projectCollectionPath);
	      hpcBulkMetadataEntries.getPathsMetadataEntries().add(populateStoredMetadataEntries(pathEntriesProject, "Project", projectCollectionName));
	 
	      
	      //Add path metadata entries for "Genomic_Reports" or "Molecular_Data" collection
	      //Example row: collectionType - Report or Molecular, collectionName - Genomic_Reports (derived)
		  //key = description, value = (supplied)
	      String subfolderCollectionName = getCollectionNameFromParent(object, "CMB");
	      String subfolderCollectionPath = projectCollectionPath + "/" + subfolderCollectionName;
	      String subfolderCollectionType = StringUtils.contains(subfolderCollectionName, "Molecular") ? "Molecular" : "Report";
	      HpcBulkMetadataEntry pathEntriesSubFolder = new HpcBulkMetadataEntry();
	      pathEntriesSubFolder.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, subfolderCollectionType));
	      pathEntriesSubFolder.setPath(subfolderCollectionPath);
	      hpcBulkMetadataEntries.getPathsMetadataEntries().add(populateStoredMetadataEntries(pathEntriesSubFolder, subfolderCollectionType, subfolderCollectionName));
			     
	      
	      //Set it to dataObjectRegistrationRequestDTO
	      dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
	      dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
	      dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);
	
	      //Add object metadata
	      dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("object_name", Paths.get(object.getSourceFilePath()).toFile().getName()));
	      dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("source_path", object.getOriginalFilePath()));
	      dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("patient_id", "Unknown"));
	      dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("report_id", "Unknown"));
  } finally {
	threadLocalMap.remove();
  }
      logger.info(
        "Biobank custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
      return dataObjectRegistrationRequestDTO;
  }


  private String getCollectionNameFromParent(StatusInfo object, String parentName) {
	  //Example originalFilepath - /mnt/10323_Data_files_for_upload/CMB/Molecular_Data
	  Path fullFilePath = Paths.get(object.getOriginalFilePath());
	  int count = fullFilePath.getNameCount();
	  for (int i = 0; i <= count; i++) {
	    if (fullFilePath.getParent().getFileName().toString().equals(parentName)) {
	      return fullFilePath.getFileName().toString();
	    }
	    fullFilePath = fullFilePath.getParent();
	  }
    return null;
  }


  private String getPiCollectionName() {
	  return "Helen_Moore_Test";
  }
  
  
  private String getProjectCollectionName() {
	  return "CMB";
  }
	  
  

}
