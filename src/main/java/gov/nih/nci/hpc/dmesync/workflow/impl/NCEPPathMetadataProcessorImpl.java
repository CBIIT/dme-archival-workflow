package gov.nih.nci.hpc.dmesync.workflow.impl;

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
 * NCEP DME Path and Meta-data Processor Implementation
 *
 * @author dinhys
 */
@Service("ncep")
public class NCEPPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
    implements DmeSyncPathMetadataProcessor {

  //NCEP Custom logic for DME path construction and meta data creation

  @Override
  public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

    logger.info("[PathMetadataTask] NCEPgetArchivePath called");

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
    archivePath = archivePath.replaceAll(" ", "_");

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
    pathEntriesPI.getPathMetadataEntries().add(createPathEntry("collection_type", "PI_Lab"));
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

    String projectCollectionName = getProjectCollectionName(object);
    String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;
    HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
    pathEntriesProject.getPathMetadataEntries().add(createPathEntry("collection_type", "Project"));
    pathEntriesProject.getPathMetadataEntries().add(createPathEntry("origin", "NCEP"));
    pathEntriesProject.setPath(projectCollectionPath);
    hpcBulkMetadataEntries.getPathsMetadataEntries().add(populateStoredMetadataEntries(pathEntriesProject, "Project", projectCollectionName));

    //Add path metadata entries for "Run_XXX" collection
    //Example row: collectionType - Run, collectionName - 20200107 (derived)
    String runCollectionName = getRunCollectionName(object);
    String runCollectionPath = projectCollectionPath + "/Run_" + runCollectionName;
    HpcBulkMetadataEntry pathEntriesRun = new HpcBulkMetadataEntry();
    pathEntriesRun.getPathMetadataEntries().add(createPathEntry("collection_type", "Run"));
    pathEntriesRun.getPathMetadataEntries().add(createPathEntry("run_number", projectCollectionName+"_"+runCollectionName));
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
    String projectDirName = getCollectionNameFromParent(object, "Archive");
    logger.info("Project Directory Name: {}", projectDirName);
    if (projectDirName != null) {
      projectCollectionName = projectDirName.substring(projectDirName.lastIndexOf('_') + 1);
    }
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
    String runDirName = getCollectionNameFromParent(object, "Archive");
    if (runDirName != null) {
      runCollectionName = runDirName.substring(0, runDirName.indexOf('_'));
    }
    if (runCollectionName == null)
      throw new DmeSyncMappingException(
          "Run collection name can't be derived for " + object.getOriginalFilePath());
    logger.info("runCollectionName: {}", runCollectionName);
    return runCollectionName;
  }
}
