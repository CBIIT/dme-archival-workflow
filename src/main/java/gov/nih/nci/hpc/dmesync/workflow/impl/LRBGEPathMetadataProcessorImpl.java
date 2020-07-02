package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Service;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncPathMetadataProcessor;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntries;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;

/**
 * LRBGE DME Path and Meta-data Processor Implementation
 *
 * @author dinhys
 */
@Service("lrbge")
public class LRBGEPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
    implements DmeSyncPathMetadataProcessor {

  // LRBGE Custom logic for DME path construction and meta data creation
  
  @Override
  public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

    logger.info("[PathMetadataTask] LRBGE getArchivePath called");

    // Example source path -
    // /data/LRBGE/Run_20200116/FLIM-ModuloAlongC.ome.tiff
    Path filePath = Paths.get(object.getOriginalFilePath());
    String fileName = filePath.getFileName().toString();
    
    String archivePath = null;
    
    // extract the derived metadata from the excel that exists in the folder (assuming one excel)
    String metadataFile;
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(filePath.getParent(), path -> path.toString().endsWith(".xlsx"))) {
      Iterator<Path> it = stream.iterator();
      if(it.hasNext()) {
        metadataFile = it.next().toString();
      } else {
        logger.error("Metadata excel file not found for {}", object.getOriginalFilePath());
        throw new DmeSyncMappingException("Metadata excel file not found for " + object.getOriginalFilePath());
      }
    } catch (IOException e) {
      logger.error("Metadata excel file not found for {}", object.getOriginalFilePath());
      throw new DmeSyncMappingException("Metadata excel file not found for " + object.getOriginalFilePath());
    }
    
    threadLocalMap.set(loadMetadataFile(metadataFile, "image_name"));
   
    // Get the PI collection value from pi_name attribute
    // Example - image_name - FLIM-ModuloAlongC.ome.tiff, attrValue - GordonHager - PI_GordonHager (Spaces will be replaced with _)
    // Get the User collection name from user_name attribute
    // Example - image_name - FLIM-ModuloAlongC.ome.tiff, attrValue - Diana Stavreva - User_Diana_Stavreva
    // Get the Project collection name from project_title attribute
    // Example - image_name - FLIM-ModuloAlongC.ome.tiff, attrValue - Cells in gels - Project_Cells_in_gels
    // Get the Run collection name from run_id attribute
    // Example - image_name - FLIM-ModuloAlongC.ome.tiff, attrValue - MyGelSoakedInHormones25 - Run_MyGelSoakedInHormones25
    try {
      archivePath =
          destinationBaseDir
              + "/PI_"
              + getPiCollectionName(fileName)
              + "/User_"
              + getUserCollectionName(fileName)
              + "/Project_"
              + getProjectCollectionName(fileName)
              + "/Run_"
              + getRunCollectionName(fileName)
              + "/"
              + fileName;
  
      // replace spaces with underscore
      archivePath = archivePath.replaceAll(" ", "_");
    } catch (Exception e) {
      threadLocalMap.remove();
      throw e;
    }

    logger.info("Archive path for {} : {}", object.getOriginalFilePath(), archivePath);

    return archivePath;
  }

  @Override
  public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
      throws DmeSyncMappingException, DmeSyncWorkflowException {

    HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO =
        new HpcDataObjectRegistrationRequestDTO();
    try {
      String fileName = Paths.get(object.getOriginalFilePath()).toFile().getName();
      
      // Add to HpcBulkMetadataEntries for path attributes
      HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();
  
      // Add path metadata entries for "PI_XXX" collection
      // key = pi_name, value = GordonHager (supplied)
      // key = affiliation, value = CCR/LRBGE/HAO (supplied)
  
      String piCollectionName = getPiCollectionName(fileName);
      String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;
      HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
      pathEntriesPI.getPathMetadataEntries().add(createPathEntry("collection_type", "PI"));
      pathEntriesPI.getPathMetadataEntries().add(createPathEntry("pi_name", getAttrValueWithKey(fileName, "pi_name")));
      pathEntriesPI.getPathMetadataEntries().add(createPathEntry("affiliation", getAttrValueWithKey(fileName, "affiliation")));
      pathEntriesPI.setPath(piCollectionPath);
      hpcBulkMetadataEntries
          .getPathsMetadataEntries()
          .add(pathEntriesPI);
  
      // Add path metadata entries for "User_XXX" collection
      // key = user_name, value = Diana Stavreva (supplied)
      // key = affiliation, value = CCR/LRBGE/HAO (supplied)
  
      String userCollectionName = getUserCollectionName(fileName);
      String userCollectionPath = piCollectionPath + "/User_" + userCollectionName;
      HpcBulkMetadataEntry pathEntriesUser = new HpcBulkMetadataEntry();
      pathEntriesUser.getPathMetadataEntries().add(createPathEntry("collection_type", "User"));
      pathEntriesUser.getPathMetadataEntries().add(createPathEntry("user_name", getAttrValueWithKey(fileName, "user_name")));
      pathEntriesUser
          .getPathMetadataEntries()
          .add(
              createPathEntry(
                  "user_affiliation",
                  StringUtils.isEmpty(getAttrValueWithKey(fileName, "user_affiliation"))
                      ? getAttrValueWithKey(fileName, "affiliation")
                      : getAttrValueWithKey(fileName, "user_affiliation")));
      pathEntriesUser.setPath(userCollectionPath);
      hpcBulkMetadataEntries
          .getPathsMetadataEntries()
          .add(pathEntriesUser);
      
      // Add path metadata entries for "Project_XXX" collection
      // key = project_title, value = Cells in gels (supplied)
      // key = project_description, value = Some description
      // key = access, value = "Closed Access" (constant)
      // key = microscope_type, value = "Lattice Light Sheet" (supplied)
      // key = start_date, value = "1/17/2020" (supplied)
      // key = organism, value = "mouse" (supplied)
  
      String projectCollectionName = getProjectCollectionName(fileName);
      String projectCollectionPath = userCollectionPath + "/Project_" + projectCollectionName;
      HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("collection_type", "Project"));
      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Closed Access"));
      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_title", getAttrValueWithKey(fileName, "project_title")));
      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_description", getAttrValueWithKey(fileName, "project_description")));
      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("microscope_type", getAttrValueWithKey(fileName, "microscope_type")));
      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("start_date", getAttrValueWithKey(fileName, "start_date")));
      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("organism", getAttrValueWithKey(fileName, "organism")));
      pathEntriesProject.setPath(projectCollectionPath);
      hpcBulkMetadataEntries
          .getPathsMetadataEntries()
          .add(pathEntriesProject);
  
      // Add path metadata entries for "Run_XXX" collection
      // key = run_id, value = "MyGelSoakedInHormones25" (supplied)
      // key = run_date, value = "1/17/2020" (supplied, not mandatory)
      // key = treatment, value = "Cells treated with dex and then with heat shock" (supplied, not mandatory)
      String runCollectionName = getRunCollectionName(fileName);
      String runCollectionPath = projectCollectionPath + "/Run_" + runCollectionName;
      HpcBulkMetadataEntry pathEntriesRun = new HpcBulkMetadataEntry();
      pathEntriesRun.getPathMetadataEntries().add(createPathEntry("collection_type", "Run"));
      pathEntriesRun.getPathMetadataEntries().add(createPathEntry("run_id", getAttrValueWithKey(fileName, "run_id")));
      if(getAttrValueWithKey(fileName, "run_date") != null)
        pathEntriesRun.getPathMetadataEntries().add(createPathEntry("run_date", getAttrValueWithKey(fileName, "run_date")));
      if(getAttrValueWithKey(fileName, "treatment") != null)
        pathEntriesRun.getPathMetadataEntries().add(createPathEntry("treatment", getAttrValueWithKey(fileName, "treatment")));
      pathEntriesRun.setPath(runCollectionPath);
      hpcBulkMetadataEntries
          .getPathsMetadataEntries()
          .add(pathEntriesRun);
  
      // Set it to dataObjectRegistrationRequestDTO
      
      dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
      dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
      dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(
          hpcBulkMetadataEntries);
  
      // Add object metadata
      // key = image_name, value = (derived)
      // key = source_path, value = (derived)
      // key = data_type, value = Raw, Reconstructed, Quantified (supplied)
      // key = comments, value =  (supplied, not mandatory)
      // key = reconstruction_date, value = "1/18/2020" (supplied, not mandatory)
      // key = reconstruction_program, value =  (supplied, not mandatory)
      // key = quantification_date, value = "1/19/2020" (supplied, not mandatory)
      // key = quantification_program, value = "mATLAB" (supplied, not mandatory)
      // key = publication_status, value = "Court et al, Science, 359, 339-343, Dec 21 2016"  (supplied, not mandatory)
      dataObjectRegistrationRequestDTO
          .getMetadataEntries()
          .add(createPathEntry("image_name", fileName));
      dataObjectRegistrationRequestDTO
          .getMetadataEntries()
          .add(createPathEntry("source_path", object.getOriginalFilePath()));
      dataObjectRegistrationRequestDTO
          .getMetadataEntries()
          .add(createPathEntry("data_type", getAttrValueWithKey(fileName, "data_type")));
      if(getAttrValueWithKey(fileName, "comments") != null)
        dataObjectRegistrationRequestDTO
            .getMetadataEntries()
            .add(createPathEntry("comments", getAttrValueWithKey(fileName, "comments")));
      if(getAttrValueWithKey(fileName, "reconstruction_date") != null)
        dataObjectRegistrationRequestDTO
            .getMetadataEntries()
            .add(createPathEntry("reconstruction_date", getAttrValueWithKey(fileName, "reconstruction_date")));
      if(getAttrValueWithKey(fileName, "reconstruction_program") != null)
        dataObjectRegistrationRequestDTO
            .getMetadataEntries()
            .add(createPathEntry("reconstruction_program", getAttrValueWithKey(fileName, "reconstruction_program")));
      if(getAttrValueWithKey(fileName, "quantification_date") != null)
        dataObjectRegistrationRequestDTO
            .getMetadataEntries()
            .add(createPathEntry("quantification_date", getAttrValueWithKey(fileName, "quantification_date")));
      if(getAttrValueWithKey(fileName, "quantification_program") != null)
        dataObjectRegistrationRequestDTO
            .getMetadataEntries()
            .add(createPathEntry("quantification_program", getAttrValueWithKey(fileName, "quantification_program")));
      if(getAttrValueWithKey(fileName, "publication_status") != null)
        dataObjectRegistrationRequestDTO
            .getMetadataEntries()
            .add(createPathEntry("publication_status", getAttrValueWithKey(fileName, "publication_status")));
    } finally {
      threadLocalMap.remove();
    }
    logger.info(
        "LRBGE custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
    return dataObjectRegistrationRequestDTO;
  }


  private String getPiCollectionName(String fileName) throws DmeSyncMappingException {
    String piCollectionName = null;
    piCollectionName = getAttrValueWithKey(fileName, "pi_name");
    piCollectionName = piCollectionName.trim().replaceAll(" ", "_");
    piCollectionName = piCollectionName.replaceAll("\\.", "");
    logger.info("PI Collection Name: {}", piCollectionName);
    return piCollectionName;
  }

  private String getUserCollectionName(String fileName) throws DmeSyncMappingException {
    String userCollectionName = null;
    userCollectionName = getAttrValueWithKey(fileName, "user_name");
    //Replace any spaces and period.
    userCollectionName = userCollectionName.trim().replaceAll(" ", "_");
    userCollectionName = userCollectionName.replaceAll("\\.", "");
    logger.info("User Collection Name: {}", userCollectionName);
    return userCollectionName;
  }
  
  private String getProjectCollectionName(String fileName) throws DmeSyncMappingException {
    String projectCollectionName = null;
    projectCollectionName = getAttrValueWithKey(fileName, "project_title");
    projectCollectionName = projectCollectionName.trim().replaceAll(" ", "_");
    logger.info("Project Collection Name: {}", projectCollectionName);
    return projectCollectionName;
  }
  
  private String getRunCollectionName(String fileName) throws DmeSyncMappingException {
    String runCollectionName = null;
    runCollectionName = getAttrValueWithKey(fileName, "run_id");
    runCollectionName = runCollectionName.trim().replaceAll(" ", "_");
    logger.info("Run Collection Name: {}", runCollectionName);
    return runCollectionName;
  }

}
