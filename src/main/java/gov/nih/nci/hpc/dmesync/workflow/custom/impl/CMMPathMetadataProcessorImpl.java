package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
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
 * CMM DME Path and Meta-data Processor Implementation
 * 
 * @author menons2
 *
 */
@Service("cmm")
public class CMMPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
    implements DmeSyncPathMetadataProcessor {

  
  //CMM Custom logic for DME path construction and meta data creation

  @Override
  public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

    logger.info("CMM DmeSyncPathMetadataProcessor getArchivePath for object {}", object.getId());

    //extract the PI id from the Path
    //Example path 1 - /data/CMM_CryoEM/CMM_Data/0022/HIV_Trimer/Trimer02/Negative_Stain/T20/Trimer02_January_10_2018.tar
    //Example path 2 - cryo_EM - /data/CMM_CryoEM/CMM_Data/0005/CryoEM/Latitude_runs/20170914_1849/GridSurveyImages
    Path fullFilePath = Paths.get(object.getOriginalFilePath());
    
    String methodSpecificPath = "";
    if(object.getOriginalFilePath().contains("Negative_Stain")) {
    	methodSpecificPath = getNegativeStainArchivePath(object);
    } else {
    	methodSpecificPath = getCryoEMArchivePath(object);
    }
    
    //Get the PI collection path
    String piCollectionPath = "/PI_" + getPiCollectionName(object);
    
    //Get the Project collection path
    String projectCollectionPath = "/Project_" + getProjectCollectionName(object);
    
    //Get the Variant collection path
    String variantCollectionPath = "";
    String variantCollectionName = getVariantCollectionName(object);
    if(variantCollectionName != null && !variantCollectionName.isEmpty()) {
    	variantCollectionPath = "/Variant_" + variantCollectionName;
    }
    
    		
    String archivePath =
        destinationBaseDir
            + piCollectionPath
            + projectCollectionPath
            + variantCollectionPath
            + methodSpecificPath;
    
    logger.info("Archive path for {} : {}", fullFilePath, archivePath);
    return archivePath;
  }
  
  
  @Override
  public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object) 
		  throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {
    
	  HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();
	  
	  //Add default path entry
      HpcMetadataEntry defaultEntry = new HpcMetadataEntry();
      defaultEntry.setAttribute("collection_type");
      defaultEntry.setValue("Folder");
      hpcBulkMetadataEntries.getDefaultCollectionMetadataEntries().add(defaultEntry);
      
	  //Add path metadata entries for "PI_XXX" collection
	  //Example row: collectionType - PI, collectionName - 0022, 
	  //key = pi_name, value = Richard Wyatt
	  //key = affiliation, value = TSRI
      String piCollectionName = getPiCollectionName(object);
      String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;
      HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
      pathEntriesPI.setPath(piCollectionPath);
      hpcBulkMetadataEntries.getPathsMetadataEntries().add(
  		  populateStoredMetadataEntries(pathEntriesPI, "PI_Lab", piCollectionName));
    
    
      //Add path metadata entries for "Project_XXX" collection
      //Example row: collectionType - Project, collectionName - HIV_Trimer, 
	  //key = project_name, value = "HIV-1 env in complex with bn Abs"
	  //key = start_date, value = 4/1/2018 
	  //key = description, value = "Some description"
      //key = publications, value = "Some publication"
      String projectCollectionName = getProjectCollectionName(object);
      String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;
      HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
      pathEntriesProject.setPath(projectCollectionPath);
      pathEntriesProject = populateStoredMetadataEntries(pathEntriesProject, "Project", projectCollectionName);
      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_id", projectCollectionName));
      hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);
      
      
      //Add path metadata entries for "Variant_XXX" collection
      String variantCollectionName = getVariantCollectionName(object); 
      String variantCollectionPath = projectCollectionPath + "/Variant_" + variantCollectionName;
      HpcBulkMetadataEntry pathEntriesVariant = new HpcBulkMetadataEntry();
      pathEntriesVariant.setPath(variantCollectionPath);
      pathEntriesVariant.getPathMetadataEntries().add(createPathEntry("variant_name", variantCollectionName));
      pathEntriesVariant.getPathMetadataEntries().add(createPathEntry("collection_type", "Variant"));
      hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesVariant);
      
      
      //Add path metadata entries for "Negative_Stain" or CryoEM collection
      String methodName = getMethodName(object); 
      if(methodName.equals("Negative_Stain")) {
    	  setupMetadataForNegativeStain(object, hpcBulkMetadataEntries, variantCollectionPath);
      } else {
    	  setupMetadataForCryoEM(object, hpcBulkMetadataEntries, variantCollectionPath);
      }
      
      //Set it to dataObjectRegistrationRequestDTO
      HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();
      dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
      dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
      dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);
      
      //Add object metadata
	  dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("object_name", getFileName(object)));
    
    
    logger.info(
        "CMM DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
    return dataObjectRegistrationRequestDTO;
  }
  
  
  private String getNegativeStainArchivePath(StatusInfo object)  {
	  return "/Negative_Stain/Raw_Data/" + getFileName(object);
	 
  }
  
  
  private String getCryoEMArchivePath(StatusInfo object) {
	  
	  //Example path 2 - cryo_EM - /data/CMM_CryoEM/CMM_Data/0005/CryoEM/Latitude_runs/20170914_1849/GridSurveyImages.tar
	//Example path 2 - cryo_EM - /data/CMM_CryoEM/CMM_Data/0005/CryoEM/Latitude_runs/20170914_1849/DataImages/AverageImages.tar
	  
	  //Split at Latitude_run. Pick up the date folder, and append it to Latitude_run. 

	  return "/CryoEM/" + getSoftware(object) + "_Run_"  + getPipelineNumber(object) + "/Raw_Data/" + getFileName(object);

  }
  
  
  private String getPipelineNumber(StatusInfo object) {
	  return getCollectionNameFromParent(object, getCollectionNameFromParent(object, "CryoEM"));
  }
  
  private String getSoftware(StatusInfo object) {
    String runPath = getCollectionNameFromParent(object, "CryoEM");
    return runPath.substring(0, runPath.indexOf('_'));
}
  
  
  private String getFileName(StatusInfo object) {
	  return Paths.get(object.getSourceFilePath()).toFile().getName();
  }
  
  
  private String getPiCollectionName(StatusInfo object) {
	  
	  return getCollectionNameFromParent(object, "CMM_Data");
  }
  
  
  private String getProjectCollectionName(StatusInfo object) {
	 
	  String piCollectionName = getPiCollectionName(object);
	  logger.debug("pi name = {}", piCollectionName);
	  String projectCollectionName = getCollectionNameFromParent(object, piCollectionName);
	  logger.debug("project name = {}", projectCollectionName);
	  if(getMethodName(object).equals(projectCollectionName)) {
	    	//No projectId is specified, since the method sub-path comes instead of project sub-path
	    	//Hence set projectId to be same as piId, 
	    	projectCollectionName = piCollectionName;
	    }
	  
	  return projectCollectionName;
  }
  
  
  private String getVariantCollectionName(StatusInfo object) {
	 
	  
	  String methodName = getMethodName(object);

	  String projectCollectionName = getProjectCollectionName(object);
	 
	  String variantCollectionName = getCollectionNameFromParent(object, projectCollectionName);
  	  if(methodName.equals(variantCollectionName)) {
  	      //No variant is specified, since the method sub-path comes instead of variant sub-path
  		  //Hence set variant name to be same as project name
  		  return projectCollectionName;
  	  }
  	  
  	  return variantCollectionName;
  }
  
  private String getCollectionNameFromParent(StatusInfo object, String parentName) {
	  
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
  
  
  private String getMethodName(StatusInfo object) {
	  if(object.getOriginalFilePath().contains("Negative_Stain")) {
	    	return "Negative_Stain";
	  }
	  
	  return "CryoEM";	
  }
  
  
  private String getInstrumentName(StatusInfo object) throws DmeSyncMappingException {
	  
	  if(object.getOriginalFilePath().contains("T20")) {
		  return "T20";
	  } else if(object.getOriginalFilePath().contains("Hitachi")) {
		  return "Hitachi";
	  }
	  
	  throw new DmeSyncMappingException(
	"Could not locate instrument name in path " + object.getOriginalFilePath());
  }
  
  
  private String getRunDate(StatusInfo object) throws DmeSyncWorkflowException {
      SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
	  String pipelineNumber = getPipelineNumber(object);
	  String dateString = pipelineNumber.split("_")[0];
	  try {
		  Date date = new SimpleDateFormat("yyyyMMdd").parse(dateString);
		  return sdf.format(date);
	  } catch (ParseException e) {
		  throw new DmeSyncWorkflowException(e);
	  }
  }
  
  
  private HpcBulkMetadataEntries setupMetadataForNegativeStain(
	  StatusInfo object, HpcBulkMetadataEntries hpcBulkMetadataEntries, String variantPath) throws DmeSyncMappingException {
	  
	  String methodCollectionPath = variantPath + "/Negative_Stain";
	  HpcBulkMetadataEntry pathEntriesMethod = new HpcBulkMetadataEntry();
	  pathEntriesMethod.setPath(methodCollectionPath);
	  //Add path metadata entries for "Method_XXX" collection
	  pathEntriesMethod.getPathMetadataEntries().add(createPathEntry("instrument", getInstrumentName(object)));
	  pathEntriesMethod.getPathMetadataEntries().add(createPathEntry("collection_type", "Negative_Stain"));
	  hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesMethod);
	  
	  String rawDataCollectionPath = methodCollectionPath + "/Raw_Data";
      HpcBulkMetadataEntry pathEntriesRawData = new HpcBulkMetadataEntry();
      pathEntriesRawData.setPath(rawDataCollectionPath);
      //Add path metadata entries for "Raw_Data" collection
      pathEntriesRawData.getPathMetadataEntries().add(createPathEntry("collection_type", "Raw_Data"));
      hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesRawData);
	  
	  return hpcBulkMetadataEntries;
  }
  
  
  private HpcBulkMetadataEntries setupMetadataForCryoEM (
      StatusInfo object, HpcBulkMetadataEntries hpcBulkMetadataEntries, String variantPath) throws DmeSyncWorkflowException {
	  
      String cryoEMCollectionPath = variantPath + "/CryoEM";
      HpcBulkMetadataEntry pathEntriesCryoEM = new HpcBulkMetadataEntry();
      pathEntriesCryoEM.setPath(cryoEMCollectionPath);
      //Add path metadata entries for "CryoEM" collection
      pathEntriesCryoEM.getPathMetadataEntries().add(createPathEntry("collection_type", "CryoEM"));
      hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesCryoEM);
      
	  String runCollectionPath = cryoEMCollectionPath + "/" + getSoftware(object) + "_Run_" + getPipelineNumber(object);
	  HpcBulkMetadataEntry pathEntriesRun = new HpcBulkMetadataEntry();
      pathEntriesRun.setPath(runCollectionPath);
      //Add path metadata entries for "Latitude_Run_XXX" collection
      pathEntriesRun.getPathMetadataEntries().add(createPathEntry("pipeline_number", getPipelineNumber(object)));
      pathEntriesRun.getPathMetadataEntries().add(createPathEntry("run_date", getRunDate(object)));
      pathEntriesRun.getPathMetadataEntries().add(createPathEntry("software", getSoftware(object)));
      pathEntriesRun.getPathMetadataEntries().add(createPathEntry("collection_type", "Run"));
      hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesRun);
      
      String rawDataCollectionPath = runCollectionPath + "/Raw_Data";
      HpcBulkMetadataEntry pathEntriesRawData = new HpcBulkMetadataEntry();
      pathEntriesRawData.setPath(rawDataCollectionPath);
      //Add path metadata entries for "Raw_Data" collection
      pathEntriesRawData.getPathMetadataEntries().add(createPathEntry("collection_type", "Raw_Data"));
      hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesRawData);
      
	  return hpcBulkMetadataEntries;
  }
  
  
  
  
}
