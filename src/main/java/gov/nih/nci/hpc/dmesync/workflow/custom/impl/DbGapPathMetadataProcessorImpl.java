package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
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
 * dbGaP DME Path and Meta-data Processor Implementation
 *
 * @author dinhys
 */
@Service("dbgap")
public class DbGapPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
    implements DmeSyncPathMetadataProcessor {

  //dbGap Custom logic for DME path construction and meta data creation

  @Autowired private DbGapMetadataMapper mapper;

  @Value("${dmesync.additional.metadata.excel:}")
  private String metadataFile;

  @Override
  public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

    logger.info("[PathMetadataTask] dbGap getArchivePath called");

    String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();

    String dataType = getDataType(object.getOriginalFilePath());

    String archivePath = null;
        
    if(dataType.equals("SRA_Read")) {
      // load the user metadata from the externally placed excel
      threadLocalMap.set(loadMetadataFile(metadataFile, "Run"));
      String runId = getRunId(fileName);

      archivePath =
        destinationBaseDir
            + "/Database_DbGap"
            + "/Study_"
            + getProjectCollectionName(runId)
            + "/Dataset_"
            + getAttrValueWithKey(runId, "Consent")
            + "/"
            + dataType + "_Data"
            + "/Run_"
            + runId
            + "/"
            + fileName;
    } else {
      archivePath =
          destinationBaseDir
              + "/Database_DbGap"
              + "/Study_"
              + getCollectionMappingValue("refseq", "Project")
              + "/Dataset_"
              + getCollectionMappingValue("refseq", "Dataset")
              + "/"
              + dataType + "_Data"
              + "/"
              + fileName;
    }

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
    try {
      String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
      String dataType = getDataType(object.getOriginalFilePath());

      //Add to HpcBulkMetadataEntries for path attributes
      HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

      //Add path metadata entries for "Database_DbGap" collection
      //key = database_type, value = dbGaP (fixed)

      String dbCollectionPath = destinationBaseDir + "/Database_DbGap";
      HpcBulkMetadataEntry pathEntriesDB = new HpcBulkMetadataEntry();
      pathEntriesDB.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Database"));
      pathEntriesDB.getPathMetadataEntries().add(createPathEntry("database_type", "dbGaP"));
      pathEntriesDB.setPath(dbCollectionPath);
      hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesDB);

      //Add path metadata entries for "Study_XXX" collection
      //Example row: collectionType - Project, collectionName - PRJNA82747 (derived),
      //key = project_title, value = PRJNA82747 (derived)
      //dbGaP project metadata attributes

      String runId = null;
      String projectCollectionName = null;
      if(dataType.equals("SRA_Read")) {
        runId = getRunId(fileName);
        projectCollectionName = getProjectCollectionName(runId);
      } else {
        projectCollectionName = getCollectionMappingValue("refseq", "Project");
      }
      String projectCollectionPath = dbCollectionPath + "/Study_" + projectCollectionName;
      HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
      pathEntriesProject
          .getPathMetadataEntries()
          .add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Closed Access"));
      if(dataType.equals("SRA_Read")) {
        for (String metadataAttrName : mapper.getProjectMetadataAttributeNames()) {
          String metadataAttrValue = getAttrValueWithKey(runId, metadataAttrName);
          if (metadataAttrValue == null) metadataAttrValue = "Unknown";
          if (getAttrValueWithKey(runId, metadataAttrName) != null)
            pathEntriesProject
                .getPathMetadataEntries()
                .add(createPathEntry(metadataAttrName, metadataAttrValue));
        }
      }
      pathEntriesProject.setPath(projectCollectionPath);
      hpcBulkMetadataEntries.getPathsMetadataEntries().add(populateStoredMetadataEntries(pathEntriesProject, "Project", projectCollectionName));
      
      
      //Add path metadata entries for "Dataset_XXX" collection
      //Example row: collectionType - Dataset, collectionName - GRU (derived),
      //key = Consent, value = GRU (derived)
      //dbGaP dataset metadata attributes

      String datasetCollectionName = null;
      if(dataType.equals("SRA_Read")) {
        datasetCollectionName = getDatasetCollectionName(runId);
      } else {
        datasetCollectionName = getCollectionMappingValue("refseq", "Dataset");
      }
      String datasetCollectionPath = projectCollectionPath + "/Dataset_" + datasetCollectionName;
      HpcBulkMetadataEntry pathEntriesDataset = new HpcBulkMetadataEntry();
      pathEntriesDataset
          .getPathMetadataEntries()
          .add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Dataset"));
      if(dataType.equals("SRA_Read")) {
        for (String metadataAttrName : mapper.getDatasetMetadataAttributeNames()) {
          String metadataAttrValue = getAttrValueWithKey(runId, metadataAttrName);
          if (metadataAttrValue == null) metadataAttrValue = "Unknown";
          if (getAttrValueWithKey(runId, metadataAttrName) != null)
            pathEntriesDataset
                .getPathMetadataEntries()
                .add(createPathEntry(metadataAttrName, metadataAttrValue));
        }
      }
      pathEntriesDataset.setPath(datasetCollectionPath);
      hpcBulkMetadataEntries.getPathsMetadataEntries().add(populateStoredMetadataEntries(pathEntriesDataset, "Dataset", datasetCollectionName));
      
      //Add path metadata entries for "XXX_Data" collection
      //Example row: collectionType - SRA_Read or Alignment or Trait
      //             collectionName - SRA_Read_Data or Alignment_Data or Trait_Data

      String dataTypeCollectionPath = datasetCollectionPath + "/" + dataType + "_Data";
      HpcBulkMetadataEntry pathEntriesDataType = new HpcBulkMetadataEntry();
      pathEntriesDataType
          .getPathMetadataEntries()
          .add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, dataType));
      pathEntriesDataType.setPath(dataTypeCollectionPath);
      hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesDataType);
      
      
      //Add path metadata entries for "Run_XXX" collection
      //Example row: collectionType - Run, collectionName - SRR481988 (derived)
      //dbGaP run metadata attributes
      if(dataType.equals("SRA_Read")) {
        String runCollectionName = runId;
        String runCollectionPath = dataTypeCollectionPath + "/Run_" + runCollectionName;
        HpcBulkMetadataEntry pathEntriesRun = new HpcBulkMetadataEntry();
        pathEntriesRun.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Run"));
        pathEntriesRun.getPathMetadataEntries().add(createPathEntry("Run", runId));
        for (String metadataAttrName : mapper.getRunMetadataAttributeNames()) {
          String metadataAttrValue = getAttrValueWithKey(runId, metadataAttrName);
          if (metadataAttrValue == null) metadataAttrValue = "Unknown";
          pathEntriesRun
              .getPathMetadataEntries()
              .add(createPathEntry(metadataAttrName, metadataAttrValue));
        }
        pathEntriesRun.setPath(runCollectionPath);
        hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesRun);
      }
      
      //Set it to dataObjectRegistrationRequestDTO
      dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
      dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
      dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(
          hpcBulkMetadataEntries);

      //Add object metadata
      dataObjectRegistrationRequestDTO
          .getMetadataEntries()
          .add(createPathEntry("object_name", fileName));
      dataObjectRegistrationRequestDTO
          .getMetadataEntries()
          .add(createPathEntry("file_type", FilenameUtils.getExtension(fileName)));
      
      if (dataType.equals("Trait")) {
        Path filePath = Paths.get(object.getOriginalFilePath());
        String traitMetadataFile = null;
        try (DirectoryStream<Path> stream =
            Files.newDirectoryStream(
                filePath.getParent(), path -> path.toString().endsWith(".xlsx"))) {
          Iterator<Path> it = stream.iterator();
          if (it.hasNext()) {
            traitMetadataFile = it.next().toString();
          } else {
            logger.error("Metadata excel file not found for {}", object.getOriginalFilePath());
            throw new DmeSyncMappingException(
                "Metadata excel file not found for " + object.getOriginalFilePath());
          }
        } catch (IOException e) {
          logger.error("Metadata excel file not found for {}", object.getOriginalFilePath());
          throw new DmeSyncMappingException(
              "Metadata excel file not found for " + object.getOriginalFilePath());
        }
        threadLocalMap.set(loadMetadataFile(traitMetadataFile, "File Name"));
        if(threadLocalMap.get().get(fileName) != null) {
          for (String metadataAttrName: threadLocalMap.get().get(fileName).keySet()) {
            String metadataAttrValue = threadLocalMap.get().get(fileName).get(metadataAttrName);
            if (metadataAttrValue != null)
              dataObjectRegistrationRequestDTO
              .getMetadataEntries()
              .add(createPathEntry(metadataAttrName, metadataAttrValue));
          }
        }
      }
      
    } finally {
      threadLocalMap.remove();
    }
    logger.info(
        "dbGap custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
    return dataObjectRegistrationRequestDTO;
  }

  private String getDataType(String path) {
    String dataType = null;
    //Example: If parent folder is sra, then the dataType will be SRA_Read
    //         If parent folder is refseq, then the dataType will be Alignment
    //         else dataType will be Trait.
    Path filePath = Paths.get(path);
    if(StringUtils.equals(filePath.getParent().getFileName().toString(), "sra"))
      dataType =  "SRA_Read";
    else if(StringUtils.equals(filePath.getParent().getFileName().toString(), "refseq"))
      dataType = "Alignment";
    else
      dataType = "Trait";
    logger.info("dataType: {}", dataType);
    return dataType;
  }
  
  private String getRunId(String filename) {
    String runId = null;
    //Example: If filename is SRR481988_dbGaP-25281.sra.*
    //then the runId will be SRR481988
    if (filename.indexOf('_') != -1) runId = StringUtils.substringBefore(filename, "_");
    else runId = StringUtils.substringBefore(filename, ".");
    logger.info("runId: {}", runId);
    return runId;
  }

  private String getProjectCollectionName(String runId) throws DmeSyncMappingException {
    String projectCollectionName = null;
    //Example: If runId is SRR481988
    //then the projectCollectionName will be PRJNA82747
    projectCollectionName = getAttrValueWithKey(runId, "BioProject");
    if (projectCollectionName == null)
      throw new DmeSyncMappingException("Excel mapping (BioProject) not found for " + runId);
    logger.info("projectCollectionName: {}", projectCollectionName);
    return projectCollectionName;
  }
  
  private String getDatasetCollectionName(String runId) throws DmeSyncMappingException {
    String datasetCollectionName = null;
    //Example: If runId is SRR481988
    //then the projectCollectionName will be PRJNA82747
    datasetCollectionName = getAttrValueWithKey(runId, "Consent");
    if (datasetCollectionName == null)
      throw new DmeSyncMappingException("Excel mapping (Consent) not found for " + runId);
    logger.info("datasetCollectionName: {}", datasetCollectionName);
    return datasetCollectionName;
  }
}
