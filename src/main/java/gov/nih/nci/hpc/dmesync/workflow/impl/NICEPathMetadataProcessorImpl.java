package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FilenameUtils;
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
 * NICE DME Path and Meta-data Processor Implementation
 * 
 * @author dinhys
 *
 */
@Service("nice")
public class NICEPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
    implements DmeSyncPathMetadataProcessor {

  //NICE Custom logic for DME path construction and meta data creation

  @Value("${dmesync.additional.metadata.excel:}")
  private String metadataFile;
	
  @Override
  public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

    logger.info("[PathMetadataTask] NICEgetArchivePath called");
      
    //Get the PI collection name from the PI column from metadata file using path
    String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
   
    // load the user metadata from the externally placed excel
    threadLocalMap.set(loadMetadataFile(metadataFile, "path"));
  
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
    archivePath = archivePath.replaceAll(" ", "_");
    
    logger.info("Archive path for {} : {}", object.getOriginalFilePath(), archivePath);

    return archivePath;
  }
  

  @Override
  public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object) throws DmeSyncMappingException, DmeSyncWorkflowException {


      HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();
	  try {
		  String path = FilenameUtils.separatorsToUnix(object.getOriginalFilePath());
	      

		  //Add to HpcBulkMetadataEntries for path attributes
		  HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();
		  
		    
	      //Add path metadata entries for "PI_XXX" collection
		  //Example row: collectionType - PI, collectionName - Di Xia (supplied)
		  //key = pi_name, value = Di Xia (supplied)
		  //key = affiliation, value = ? (supplied)
		  //key = poc_name, value = ? (supplied)
	     
	      String piCollectionName = getPiCollectionName(object);
	      String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName.replaceAll(" ", "_");
	      HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
	      pathEntriesPI.getPathMetadataEntries().add(createPathEntry("collection_type", "PI_Lab"));
	      pathEntriesPI.setPath(piCollectionPath);
	      hpcBulkMetadataEntries.getPathsMetadataEntries().add(populateStoredMetadataEntries(pathEntriesPI, "PI_Lab", piCollectionName.replaceAll(" ", "_")));
	      
	      //Add path metadata entries for "Project_XXX" collection
		  //Example row: collectionType - Project, collectionName - apof (derived), 
		  //key = project_title, value = apof (derived)
	      //key = access, value = "Closed Access" (constant)
	      //key = method, value = "CryoEM" (constant)
		  //key = start_date, value = (supplied)
		  //key = project_description, value = (supplied)
	      //key = origin, value = (fixed)
	      //key = grid_type, value = (supplied)
	      //key = freezing_conditions, value = (supplied)
	      //key = imaging_parameters, value = (supplied)

	      //key = summary_of_samples, value = (supplied)
	      //key = organism, value = (supplied)
	     
	      String projectCollectionName = getProjectCollectionName(object);
	      String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;
	      HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("collection_type", "Project"));
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_title", projectCollectionName));
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Closed Access"));
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("method", "CryoEM"));
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("start_date", getAttrValueWithKey(path, "start_date")));
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_description", getAttrValueWithKey(path, "project_description")));
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("origin", "NICE"));
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("organism", "Homo Sapiens (Human)"));
	      pathEntriesProject.setPath(projectCollectionPath);
	      hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);
	 
	      
	      //Add path metadata entries for "Run_XXX" collection
	      //Example row: collectionType - Run, collectionName - 20191101 (derived)
		  //key = run_date, value = 20191101 (derived)
	      String runCollectionName = getRunCollectionName(object);
	      String runCollectionPath = projectCollectionPath + "/Run_" + runCollectionName;
	      HpcBulkMetadataEntry pathEntriesRun = new HpcBulkMetadataEntry();
	      pathEntriesRun.getPathMetadataEntries().add(createPathEntry("collection_type", "Run"));
	      pathEntriesRun.getPathMetadataEntries().add(createPathEntry("run_number", runCollectionName));
	      pathEntriesRun.getPathMetadataEntries().add(createPathEntry("run_date", getAttrValueWithKey(path, "run_date")));
	      pathEntriesRun.getPathMetadataEntries().add(createPathEntry("grid_type", getAttrValueWithKey(path, "grid_type")));
	      pathEntriesRun.getPathMetadataEntries().add(createPathEntry("freezing_conditions", getAttrValueWithKey(path, "freezing_conditions")));
	      pathEntriesRun.getPathMetadataEntries().add(createPathEntry("imaging_parameters", getAttrValueWithKey(path, "imaging_parameters") != null ? getAttrValueWithKey(path, "imaging_parameters") : "Unknown"));
	      pathEntriesRun.getPathMetadataEntries().add(createPathEntry("summary_of_datasets", getAttrValueWithKey(path, "summary_of_datasets")));
          pathEntriesRun.setPath(runCollectionPath);
	      hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesRun);
			     
	      
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
        "NICE custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
      return dataObjectRegistrationRequestDTO;
  }


  private String getCollectionNameFromParent(StatusInfo object, String parentName) {
	  //Example originalFilepath - /mnt/NCEF-CryoEM/RMarmorstein-NCEF-033-007-10031/RMarmorstein-NCEF-033-007-10031-A.tar
	  Path fullFilePath = Paths.get(object.getOriginalFilePath());
	  logger.info("Full File Path = " + fullFilePath);
	  int count = fullFilePath.getNameCount();
	  for (int i = 0; i <= count; i++) {
	    if (fullFilePath.getParent().getFileName().toString().equals(parentName)) {
	      return fullFilePath.getFileName().toString();
	    }
	    fullFilePath = fullFilePath.getParent();
	  }
    return null;
  }


  private String getPiCollectionName(StatusInfo object) throws DmeSyncMappingException {
	  String piCollectionName = null;
	  //Example: If originalFilePath is /nice-nci-cryoem/NCI/PI_DXia/apof/20191101
	  //then the pi name will be Di Xia
	  String path = FilenameUtils.separatorsToUnix(object.getOriginalFilePath());
	  piCollectionName = getAttrValueWithKey(path, "PI");
	  logger.info("PI Collection Name: " + piCollectionName);
	  return piCollectionName;
	  
  }
  
  
  private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {
	  String projectCollectionName = null;
	  //Example: If originalFilePath is /nice-nci-cryoem/NCI/PI_DXia/apof/20191101
	  //then the projectDirName will be apof
	  String piDirName = getCollectionNameFromParent(object, "NCI");
	  logger.info("PI Directory Name: " + piDirName);
	  if(piDirName != null) {
		  projectCollectionName = getCollectionNameFromParent(object, piDirName);
	  }
	  logger.info("projectCollectionName: " + projectCollectionName);
    return projectCollectionName;
  }

  
  
  private String getRunCollectionName(StatusInfo object) throws DmeSyncMappingException {
	  String runCollectionName = null;
	  //Example: If sourceFilePath is /mnt/IRODsScratch/work/RMarmorstein-NCEF-033-007-10031/RMarmorstein-NCEF-033-007-10031-A.tar
	  //then the runDirName will be RMarmorstein-NCEF-033-007-10031-A.tar
	  runCollectionName = getCollectionNameFromParent(object, getProjectCollectionName(object));	 
	  logger.info("runCollectionName: " + runCollectionName);
	  return runCollectionName;
  }
	  
  

}
