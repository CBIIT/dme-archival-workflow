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
 * NCEP DME Path and Meta-data Processor Implementation
 *
 * @author dinhys
 */
@Service("ncep")
public class NCEPPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
    implements DmeSyncPathMetadataProcessor {

  //NCEP Custom logic for DME path construction and meta data creation

  @Value("${dmesync.additional.metadata.excel:}")
  private String metadataFile;
  
  @Value("${dmesync.doc.name}")
  private String doc;
  
  @Override
  public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

    logger.info("[PathMetadataTask] NCEPgetArchivePath called");

    // load the user metadata from the externally placed excel
 	threadLocalMap.set(loadMetadataFile(metadataFile, "path"));
 	 
    //extract the user name from the source Path
    //Example source path - /mnt/NCEP-CryoEM/active/Data/Archive/20200107_1_Glacios/20200107_1_Glacios.tar
    String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();

    //Extract the Project and Run value from the Path

    String archivePath =
        destinationBaseDir
            + "/PI_Ognjenovic"
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
  public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
      throws DmeSyncMappingException, DmeSyncWorkflowException {

    HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO =
        new HpcDataObjectRegistrationRequestDTO();

    //Add to HpcBulkMetadataEntries for path attributes
    HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

    //Add path metadata entries for "PI_XXX" collection
    //Example row: collectionType - PI, collectionName - Ognjenovic (static)
    //key = pi_name, value = Jana Ognjenovic (supplied)
    //key = affiliation, value = NCEP, CRTP, FNL (supplied)

    String piCollectionName = "Ognjenovic";
    String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;
    HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
    pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "PI_Lab"));
    pathEntriesPI.setPath(piCollectionPath);
    hpcBulkMetadataEntries
        .getPathsMetadataEntries()
        .add(populateStoredMetadataEntries(pathEntriesPI, "PI_Lab", piCollectionName));

    //Add path metadata entries for "Project_XXX" collection
    //Example row: collectionType - Project, collectionName - Glacios (derived),
    //key = project_title, value = Glacios (derived)
    //key = short_title, value = (supplied)
    //key = unique_identifier, value = 001 (supplied)
    //key = access, value = "Closed Access" (default)
    //key = unique_project_identifier, value = (supplied)
    //key = start_date, value = (supplied)
    //key = project_description, value = (supplied)
    //key = project_poc, value = Alan Merk (supplied)
    //key = status, value = Active (supplied)

    String path = object.getOriginalFilePath();
    String projectCollectionName = getProjectCollectionName(object);
    String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;
    HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
    pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
    pathEntriesProject.getPathMetadataEntries().add(createPathEntry("origin", "NCEP"));
    pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_title", getAttrValueWithKey(path, "project_identifier")));
    pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_description", getAttrValueWithKey(path, "project_description")));
    pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_identifier", getAttrValueWithKey(path, "project_identifier")));
    pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_start_date", getAttrValueWithKey(path, "project_start_date")));
    pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc", getAttrValueWithKey(path, "project_poc")));
    pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_status", "Active"));
    if(getAttrValueWithKey(path, "electron_detector_model") != null)
    	pathEntriesProject.getPathMetadataEntries().add(createPathEntry("electron_detector_model", getAttrValueWithKey(path, "electron_detector_model")));
    if(getAttrValueWithKey(path, "accelerating_voltage") != null)
    	pathEntriesProject.getPathMetadataEntries().add(createPathEntry("accelerating_voltage", getAttrValueWithKey(path, "accelerating_voltage")));
    if(getAttrValueWithKey(path, "electron_source") != null)
    	pathEntriesProject.getPathMetadataEntries().add(createPathEntry("electron_source", getAttrValueWithKey(path, "electron_source")));
    if(getAttrValueWithKey(path, "illumination_mode") != null)
    	pathEntriesProject.getPathMetadataEntries().add(createPathEntry("illumination_mode", getAttrValueWithKey(path, "illumination_mode")));
    if(getAttrValueWithKey(path, "microscope_model") != null)
    	pathEntriesProject.getPathMetadataEntries().add(createPathEntry("microscope_model", getAttrValueWithKey(path, "microscope_model")));
    if(getAttrValueWithKey(path, "em_specimen") != null)
    	pathEntriesProject.getPathMetadataEntries().add(createPathEntry("em_specimen",getAttrValueWithKey(path, "em_specimen")));
    pathEntriesProject.setPath(projectCollectionPath);
    hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);

    //Add path metadata entries for "Run_XXX" collection
    //Example row: collectionType - Run, collectionName - 20200107 (derived)
    String runCollectionName = getRunCollectionName(object);
    String runCollectionPath = projectCollectionPath + "/Run_" + runCollectionName;
    HpcBulkMetadataEntry pathEntriesRun = new HpcBulkMetadataEntry();
    pathEntriesRun.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Run"));
    pathEntriesRun.getPathMetadataEntries().add(createPathEntry("run_number", projectCollectionName+"_"+runCollectionName));
    if(getAttrValueWithKey(path, "project_author") != null)
    	pathEntriesRun.getPathMetadataEntries().add(createPathEntry("project_author",getAttrValueWithKey(path, "project_author")));
    pathEntriesRun.setPath(runCollectionPath);
    hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesRun);

    //Set it to dataObjectRegistrationRequestDTO
    dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
    dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
    dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(
        hpcBulkMetadataEntries);

    //Add object metadata
    dataObjectRegistrationRequestDTO
        .getMetadataEntries()
        .add(
            createPathEntry(
                "object_name", Paths.get(object.getSourceFilePath()).toFile().getName()));
    dataObjectRegistrationRequestDTO
        .getMetadataEntries()
        .add(createPathEntry("source_path", object.getOriginalFilePath()));

    logger.info(
        "NCEP custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
    return dataObjectRegistrationRequestDTO;
  }

  private String getCollectionNameFromParent(StatusInfo object, String parentName) {
    //Example originalFilepath - /mnt/NCEP-CryoEM/active/Data/Archive/20200107_1_Glacios
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

  private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {
    String projectCollectionName = null;
    //Example: If originalFilePath is /mnt/NCEP-CryoEM/active/Data/Archive/20200107_1_Glacios
    //then the projectDirName will be Glacios
    String path = object.getOriginalFilePath();
    projectCollectionName = getAttrValueWithKey(path, "project_identifier");
    logger.info("Project collection Name: {}", projectCollectionName);
    if (projectCollectionName == null)
      throw new DmeSyncMappingException(
          "Project collection name can't be derived for " + object.getOriginalFilePath());
    logger.info("projectCollectionName: {}", projectCollectionName);
    return projectCollectionName;
  }

  private String getRunCollectionName(StatusInfo object) throws DmeSyncMappingException {
    String runCollectionName = null;
    //Example: If originalFilePath is /mnt/NCEP-CryoEM/active/Data/Archive/20200107_1_Glacios
    //then the runCollectionName will be 20200107
    String path = object.getOriginalFilePath();
    runCollectionName = getAttrValueWithKey(path, "sub_project_identifier");
    if (runCollectionName == null)
      throw new DmeSyncMappingException(
          "Run collection name can't be derived for " + object.getOriginalFilePath());
    logger.info("runCollectionName: {}", runCollectionName);
    return runCollectionName;
  }
  
}
