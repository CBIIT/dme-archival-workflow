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
import gov.nih.nci.hpc.domain.metadata.HpcMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;

/**
 * HiTIF DME Path and Meta-data Processor Implementation
 * 
 * @author dinhys
 *
 */
@Service("hitif")
public class HiTIFPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
    implements DmeSyncPathMetadataProcessor {

  //HiTIF Custom logic for DME path construction and meta data creation

  @Override
  public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

    logger.info("[PathMetadataTask] HiTIF getArchivePath called");

    //extract the user name from the Path
    //Example path - /Opera_Data/CV7000_Images/MeasurementData/Carmen/180712-U2F-20x-CyclinA488opti-FUCCI-Q670N1-DAPIsat_20180712_142846/FUCCIOpti-SantaCruz1in50-FISHandIF-working-wells
    String userId = getUserId(object);
    String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
    
    //Get the PI collection value from the CollectionNameMetadata
    //Example row - mapKey - Ziad, collectionType - PI, mapValue - Tom_Mistelli
    //Get the User collection value from the CollectionNameMetadata
    //Example row - mapkey - Ziad, collectionType - User, mapvalue - Ziad Jowhar
    //Extract the Experiment value from the Path

    String archivePath =
        destinationBaseDir
            + "/PI_"
            + getPiCollectionName(userId)
            + "/User_"
            + getUserCollectionName(userId)
            + "/Exp_"
            + getExpCollectionName(object)
            + "/"
            + fileName;
    
    //replace spaces with underscore
    archivePath = archivePath.replace(" ", "_");
    
    logger.info("Archive path for {} : {}", object.getOriginalFilePath(), archivePath);

    return archivePath;
  }
  

  @Override
  public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object) throws DmeSyncMappingException, DmeSyncWorkflowException {

	  String userId = getUserId(object);
	  
	  //Add to HpcBulkMetadataEntries for path attributes
	  HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();
	  
	  //Add default path entry
	  HpcMetadataEntry defaultEntry = new HpcMetadataEntry();
	  defaultEntry.setAttribute(COLLECTION_TYPE_ATTRIBUTE);
	  defaultEntry.setValue("Folder");
	  hpcBulkMetadataEntries.getDefaultCollectionMetadataEntries().add(defaultEntry);
	    
      //Add path metadata entries for "PI_XXX" collection
	  //Example row: collectionType - PI, collectionName - Tom_Mistelli, 
	  //key = pi_name, value = Tom Mistelli
	  //key = pi_email, value = tom.mistelli@nih.gov, 
	  //key = institute, value = NCI
	  //key = lab, value = CCR
      HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
      String piCollectionName = getPiCollectionName(userId);
      pathEntriesPI.setPath(destinationBaseDir + "/PI_" + piCollectionName);
      hpcBulkMetadataEntries.getPathsMetadataEntries().add(
    		  populateStoredMetadataEntries(pathEntriesPI, "PI_Lab", piCollectionName, "hitif"));
      
      //Add path metadata entries for "USER_XXX" collection
	  //Example row: collectionType - User, collectionName - Tom_Mistelli, 
	  //key = name, value = Ziad Jowhar
	  //key = email, value = ziad.jowhar@nih.gov, 
	  //key = branch, value = CCR
	  //key = comment, value = Test
      HpcBulkMetadataEntry pathEntriesUser = new HpcBulkMetadataEntry();
      String userCollectionName = getUserCollectionName(userId);
      pathEntriesUser.setPath(destinationBaseDir + "/PI_" + piCollectionName + "/User_" + userCollectionName);
      hpcBulkMetadataEntries.getPathsMetadataEntries().add(
    		  populateStoredMetadataEntries(pathEntriesUser, "User", userCollectionName, "hitif"));
      
      //Add path metadata entries for "EXP_XXX" collection
      HpcBulkMetadataEntry pathEntriesExp = new HpcBulkMetadataEntry();
      //Extract the Experiment value from the Path
      String expCollectionName = getExpCollectionName(object);
      logger.debug("exp_collection_name = {}", expCollectionName);
      String expPath = destinationBaseDir + "/PI_" + piCollectionName + "/User_" + userCollectionName + "/Exp_" + expCollectionName;
      pathEntriesExp.setPath(expPath.replace(" ", "_"));
      pathEntriesExp.getPathMetadataEntries().add(createPathEntry("experiment_name", expCollectionName));
      pathEntriesExp.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Exp"));
      hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesExp);
		     
      //Set it to dataObjectRegistrationRequestDTO
      HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();
      dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
      dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
      dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);

      //Add object metadata
      dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("object_name", object.getOrginalFileName()));
      dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("source_path", object.getOriginalFilePath()));
      
      
      logger.info(
        "HiTIF custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
      return dataObjectRegistrationRequestDTO;
  }


  private String getUserId(StatusInfo object) {
      Path fullFilePath = Paths.get(object.getOriginalFilePath());
      return fullFilePath.subpath(0, fullFilePath.getNameCount()-2).toFile().getName();
  }


  private String getPiCollectionName(String userId) throws DmeSyncMappingException {
	  return getCollectionMappingValue(userId, "PI_Lab", "hitif");
  }


  private String getUserCollectionName(String userId) throws DmeSyncMappingException {
	  return getCollectionMappingValue(userId, "User", "hitif");
  }


  private String getExpCollectionName(StatusInfo object) {
	  return Paths.get(object.getOriginalFilePath()).getParent().toFile().getName();
  }
}
