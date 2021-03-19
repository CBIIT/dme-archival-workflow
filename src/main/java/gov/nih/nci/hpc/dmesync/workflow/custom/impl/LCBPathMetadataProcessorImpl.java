package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.springframework.stereotype.Service;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncPathMetadataProcessor;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntries;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;

/**
 * LCB DME Path and Meta-data Processor Implementation
 * 
 * @author dinhys
 *
 */
@Service("lcb")
public class LCBPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
    implements DmeSyncPathMetadataProcessor {

  //CCR LCB SubramaniamLab Custom logic for DME path construction and meta data creation

  @Override
  public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

    logger.info("[PathMetadataTask] LCBgetArchivePath called");

    //extract the user name from the source Path
    //Example source path - /data/Livlab/projects/GluK2.tar
    String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
    
    //Get the PI collection value from the CollectionNameMetadata
    //Example row - mapKey - Livlab, collectionType - PI_Lab, mapValue - Sriram Subramaniam
    //Extract the Project value from the Path

    String archivePath =
        destinationBaseDir
            + "/PI_"
            + getPiCollectionName(object)
            + "/"
            + getProjectCollectionName(object)
            + "/"
            + fileName;
    
    //replace spaces with underscore
    archivePath = archivePath.replaceAll(" ", "_");
    
    logger.info("Archive path for {} : {}", object.getOriginalFilePath(), archivePath);

    return archivePath;
  }
  

  @Override
  public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object) throws DmeSyncMappingException, DmeSyncWorkflowException {

	  //Add to HpcBulkMetadataEntries for path attributes
	  HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();
	  
	    
      //Add path metadata entries for "PI_XXX" collection
	  //Example row: collectionType - PI_Lab, collectionName - Subramaniam (derived)
	  //key = pi_name, value = Sriram Subramaniam (supplied)

      String piCollectionName = getPiCollectionName(object);
      String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;
      HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
      pathEntriesPI.getPathMetadataEntries().add(createPathEntry("collection_type", "PI_Lab"));
      pathEntriesPI.setPath(piCollectionPath);
      hpcBulkMetadataEntries.getPathsMetadataEntries().add(populateStoredMetadataEntries(pathEntriesPI, "PI_Lab", piCollectionName));
        
      String projectCollectionName = getProjectCollectionName(object);
      String projectCollectionPath = piCollectionPath + "/" + projectCollectionName;
      HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("collection_type", "Project"));
      pathEntriesProject.setPath(projectCollectionPath);
      hpcBulkMetadataEntries.getPathsMetadataEntries().add(populateStoredMetadataEntries(pathEntriesProject, "Project", projectCollectionName));
      
      //Set it to dataObjectRegistrationRequestDTO
      HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();
      dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
      dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
      dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);

      //Add object metadata
      dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("object_name", Paths.get(object.getSourceFilePath()).toFile().getName()));
      dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("source_path", object.getOriginalFilePath()));
      dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("modified_date", getModifiedDate(object)));
      
      logger.info(
        "LCB custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
      return dataObjectRegistrationRequestDTO;
  }

  private String getPiCollectionName(StatusInfo object) throws DmeSyncMappingException {
	  //Example: If originalFilePath is /data/Livlab/projects/GluK2.tar
	  //then the piDirName will be Livlab
	  String piDirName = "Livlab";
	  logger.info("PI Directory Name: {}", piDirName);
	  String piCollectionName = getCollectionMappingValue(piDirName, "PI_Lab");
	  logger.info("PI Collection Name: {}", piCollectionName);
	  return piCollectionName;
	  
  }
  
  private String getCollectionNameFromParent(StatusInfo object, String parentName) {
    // Example originalFilepath -
    // /data/CCRSB/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam
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
  
  private String getProjectCollectionName(StatusInfo object) {
	  //Example: If originalFilePath is /data/Livlab/projects/GluK2.tar
	  //then the projectDirName will be projects
	  String projectDirName = getCollectionNameFromParent(object, "Livlab");
	  logger.info("Project Directory Name: {}", projectDirName);
    return projectDirName;
  }

  private String getModifiedDate(StatusInfo object) {
      //Example: If originalFilePath is /data/Livlab/projects/GluK2
      //then return the modified_date of the folder
      File folder = new File(object.getOriginalFilePath());
      long lastChanged = folder.lastModified();
      DateFormat sdf = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss"); 
      String lastUpdated = sdf.format(new Date(lastChanged));
      logger.info("directory {} was last updated at {}",
          folder.getName(), lastUpdated);
	  return lastUpdated;
  }

}
