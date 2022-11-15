package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import javax.annotation.PostConstruct;
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
import gov.nih.nci.hpc.dmesync.util.ExcelUtil;

/**
 * MoCha DME Path and Meta-data Processor Implementation
 *
 * @author dinhys
 */
@Service("mocha")
public class MochaPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
    implements DmeSyncPathMetadataProcessor {

  // Mocha DME path construction and meta data creation

  @Value("${dmesync.additional.metadata.excel:}")
  private String metadataFile;
  
  @Value("${dmesync.doc.name}")
  private String doc;
  
  @Value("${dmesync.source.base.dir}")
  private String sourceDir;
  
  @Value("${dmesync.tar:false}")
  private boolean tar;
  
  Map<String, Map<String, String>> metadataMap = null;
  
  @Override
  public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

    logger.info("[PathMetadataTask] Mocha getArchivePath called");

    // Example source path -
    // /mnt/mocha_static/NovaSeq/220113_A00424_0160_BHKJNWDSX2/Data/Intensities/BaseCalls/L001
    // /mnt/mocha_scratch/BW_transfers/2022_January/220113_A01063_0058_BHKJMGDSX2/Sample_RES210195_HKJMGDSX2/Sample_RES210195_HKJMGDSX2_R1.fastq.gz
    String fileName = Paths.get(object.getSourceFileName()).toFile().getName();
    String archivePath = null;
    
    if(isBCL(object)) {
    	archivePath =
	        destinationBaseDir
	            + "/Lab_"
	            + getPiCollectionName(object)
	            + "/Platform_"
	            + getPlatformCollectionName(object)
	            + "/Run_FC"
	            + "/Flowcell_"
	            + getFlowcellId(object)
	            + "/"
	            + getRunId(object) + ".tar";
    } else {
	    archivePath =
	        destinationBaseDir
	            + "/Lab_"
	            + getPiCollectionName(object)
	            + "/Platform_"
	            + getPlatformCollectionName(object)
	            + "/Project_"
	            + getProjectCollectionName(object)
	            + "/"
	            + getSampleCollectionName(object)
	            + "/"
	            + fileName;
    }

    // replace spaces with underscore
    archivePath = archivePath.replace(" ", "_");

    logger.info("Archive path for {} : {}", object.getOriginalFilePath(), archivePath);
    
    return archivePath;
  }

  @Override
  public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
      throws DmeSyncMappingException, DmeSyncWorkflowException {

    // Add to HpcBulkMetadataEntries for path attributes
    HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();
    String fileName = null;
    if(isBCL(object))
    	fileName = getRunId(object) + ".tar";
    else
    	fileName = Paths.get(object.getSourceFileName()).toFile().getName();
    
    // Add path metadata entries for "PI_XXX" collection
    // Example row: collectionType - PI, collectionName - XXX (derived)
    // key = data_owner, value = Mickey Williams (supplied)
    // key = data_owner_affiliation, value = Molecular Characterization Laboratory, FNLCR (supplied)
    String piCollectionName = getPiCollectionName(object);
    String piCollectionPath = destinationBaseDir + "/Lab_" + piCollectionName;
    HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
    pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "PI_Lab"));
    pathEntriesPI.setPath(piCollectionPath);
    hpcBulkMetadataEntries
        .getPathsMetadataEntries()
        .add(populateStoredMetadataEntries(pathEntriesPI, "PI_Lab", piCollectionName, "mocha"));

    // Add path metadata entries for "Platform" collection
    // Example row: collectionType - Platform, collectionName - HiSeq, NovaSeq
    // platform_name
    String platformCollectionName = getPlatformCollectionName(object);
    String platformCollectionPath = piCollectionPath + "/Platform_" + platformCollectionName;
    HpcBulkMetadataEntry pathEntriesPlatform = new HpcBulkMetadataEntry();
    pathEntriesPlatform.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Platform"));
    pathEntriesPlatform.getPathMetadataEntries().add(createPathEntry("platform_name", platformCollectionName));
    pathEntriesPlatform.getPathMetadataEntries().add(createPathEntry("access", "Closed Access"));
    pathEntriesPlatform.getPathMetadataEntries().add(createPathEntry("organism", "Human"));
    pathEntriesPlatform.setPath(platformCollectionPath);
    hpcBulkMetadataEntries
        .getPathsMetadataEntries()
        .add(pathEntriesPlatform);
    
    if(!isBCL(object)) {
    	// Add path metadata entries for "Project" collection
		// Example row: collectionType - Project, collectionName - Project_PDX
    	// project_id key = Project Name
    	// project_title key = Project Name
	    // project_description key = Project Description
    	// project_start_date key = Start Date
    	// project_status key = Status
	    // project_poc key = Project POC
	    // project_poc_affiliation key = POC email
	    // project_poc_email key = POC email
    	String flowcellId = getFlowcellId(object);
	    String sampleId = getSampleId(object);
	    String runId = getRunId(object);
		String projectCollectionName = getProjectCollectionName(object);
		String projectCollectionPath = platformCollectionPath + "/Project_" + projectCollectionName;
		HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_id", projectCollectionName));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_status", "Active"));
		pathEntriesProject.setPath(projectCollectionPath);
		hpcBulkMetadataEntries.getPathsMetadataEntries()
		.add(populateStoredMetadataEntries(pathEntriesProject, "Project", projectCollectionName, "mocha"));
	    
	    // Add path metadata entries for "Sample" collection
	    // Example row: collectionType - Sample, collectionName - Sample_<SampleId>
	    // sample_id, value = PDA01236 (derived)
	    // sample_name, value = PDA01236 (derived)
	    // flowcell_lane = Lane
	    
	    String sampleCollectionPath = projectCollectionPath + "/" + getSampleCollectionName(object);
	    sampleCollectionPath = sampleCollectionPath.replace(" ", "_");
	    HpcBulkMetadataEntry pathEntriesSample = new HpcBulkMetadataEntry();
	    pathEntriesSample.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Sample"));
	    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("flowcell_id", flowcellId));
	    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("run_id", getRunId(object))); 
	    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sample_id", sampleId));  
	    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sample_name", getAttrWithKey(runId, sampleId, "Mocha_ID")));
	    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("library_strategy", getAttrWithKey(runId, sampleId, "Library_Type")));
	    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("analyte_type", getAttrWithKey(runId, sampleId, "Analyte")));
	    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("flowcell_lane", getAttrWithKey(runId, sampleId, "Lane")));
	    if(StringUtils.isNotBlank(getAttrWithKey(runId, sampleId, "SubProject")))
	    	pathEntriesSample.getPathMetadataEntries().add(createPathEntry("subproject", getAttrWithKey(runId, sampleId, "SubProject")));  
	    pathEntriesSample.setPath(sampleCollectionPath);
	    hpcBulkMetadataEntries
	        .getPathsMetadataEntries()
	        .add(pathEntriesSample);
    } else  {
    	//BCL
	    String runFCCollectionPath = platformCollectionPath + "/Run_FC";
	    runFCCollectionPath = runFCCollectionPath.replace(" ", "_");
 	    HpcBulkMetadataEntry pathEntriesRunFC = new HpcBulkMetadataEntry();
 	    pathEntriesRunFC.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Run_FC"));     
 	    pathEntriesRunFC.setPath(runFCCollectionPath);
 	    hpcBulkMetadataEntries
 	        .getPathsMetadataEntries()
 	        .add(pathEntriesRunFC);
 	    
	    // Add path metadata entries for "Flowcell" collection
	    // Example row: collectionType - Flowcell
	    // flowcell_id
    	String flowcellId = getFlowcellId(object);
	    String flowcellCollectionPath = runFCCollectionPath + "/Flowcell_" + flowcellId;
	    HpcBulkMetadataEntry pathEntriesFlowcell = new HpcBulkMetadataEntry();
	    pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Flowcell"));
	    pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("flowcell_id", flowcellId));
	    pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("run_id", getRunId(object)));
	    pathEntriesFlowcell.setPath(flowcellCollectionPath);
	    hpcBulkMetadataEntries
	        .getPathsMetadataEntries()
	        .add(pathEntriesFlowcell);
 	    
    }
    
    // Set it to dataObjectRegistrationRequestDTO
    HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO =
        new HpcDataObjectRegistrationRequestDTO();
    dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
    dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
    dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(
        hpcBulkMetadataEntries);

    // Add object metadata
    // key = object_name, value = Sample_RES210195_HKJMGDSX2_R1.fastq.gz (derived)
    // key = file_type, value = fastq, bcl (derived)
    String fileType = getFileType(object);
    dataObjectRegistrationRequestDTO
        .getMetadataEntries()
        .add(createPathEntry("object_name", fileName));
    dataObjectRegistrationRequestDTO
        .getMetadataEntries()
        .add(createPathEntry("file_type", fileType));
    dataObjectRegistrationRequestDTO
        .getMetadataEntries()
        .add(createPathEntry("source_path",  object.getOriginalFilePath()));
    logger.info(
        "Metadata MoCha custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
    return dataObjectRegistrationRequestDTO;
  }

  private String getCollectionNameFromParent(StatusInfo object, String parentName) {
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

  private String getPiCollectionName(StatusInfo object) throws DmeSyncMappingException {
    String piCollectionName = null;
    // Example: If originalFilePath is
    // /mnt/mocha_static/NovaSeq/220113_A00424_0160_BHKJNWDSX2/Data/Intensities/BaseCalls/L001
    // then return the mapped PI from /mnt/mocha_static/NovaSeq
    piCollectionName = getCollectionMappingValue(sourceDir, "PI_Lab", "mocha");
    
    logger.info("PI Collection Name: {}", piCollectionName);
    return piCollectionName;
  }
  
  private String getPlatformCollectionName(StatusInfo object) throws DmeSyncMappingException {
	  String path = Paths.get(object.getOriginalFilePath()).toString();
	  String platform = null;
	  if (path.contains("NovaSeq") || path.contains("BW_transfers")) {
		  platform = "NovaSeq";
	  } else {
		  platform = "HiSeq";
	  }
	  return platform;
  }

  private String getFlowcellId(StatusInfo object) throws DmeSyncMappingException {
	  String runId = getRunId(object);
	return getAttrValueWithParitallyMatchingKey(runId, "Flowcell");
  }
  
  private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {
	  String runId = getRunId(object);
	  String sampleId = getSampleId(object);
	return getAttrWithKey(runId, sampleId, "Project");
  }
  
  private String getRunId(StatusInfo object) throws DmeSyncMappingException {
	  String path = Paths.get(object.getOriginalFilePath()).toString();
	  String flowcellCollectionName = null;
	if (path.contains("mocha_scratch") && path.contains("BW_transfers")) {
		flowcellCollectionName = getCollectionNameFromParent(object, getCollectionNameFromParent(object, "BW_transfers"));
	} else if (path.contains("mocha_static") && path.contains("NovaSeq")) {
		flowcellCollectionName = getCollectionNameFromParent(object, "NovaSeq");
	} else if (path.contains("mocha_static")) {
		flowcellCollectionName = getCollectionNameFromParent(object, "mocha_static");
	}
	return flowcellCollectionName;
  }
  
  private boolean isBCL(StatusInfo object) throws DmeSyncMappingException {
	  String path = Paths.get(object.getOriginalFilePath()).toString();
	  if (tar) {
		  //BCL files are tarred.
		  return true;
	  }
	  return false;
  }
  
  private String getSampleId(StatusInfo object) throws DmeSyncMappingException {
	  String path = Paths.get(object.getOriginalFilePath()).toString();
	  String sampleId = null;
	  // 1) Check if any sample entry for this folder is in the file path.
	  sampleId = getSampleFromFilePath(path, getRunId(object));
	  if(StringUtils.isEmpty(sampleId)) {
		  logger.error("Sample ID can't be extracted for {}", path);
		  throw new DmeSyncMappingException("Sample ID can't be extracted for " + path);
	  }
	// 1) If Sample_xxx folder exists in the path, then use the name after Sample_
	/*if (path.contains("Sample_")) {
	  sampleId = StringUtils.substringAfter(path, "Sample_");
	  if(StringUtils.isEmpty(sampleId)) {
		  logger.error("Sample ID can't be extracted for {}", path);
		  throw new DmeSyncMappingException("Sample ID can't be extracted for " + path);
	  }
	  sampleId = StringUtils.substringBeforeLast(StringUtils.substringBefore(sampleId, "/"), "_");
	}*/
	return sampleId;
  }
  
  private String getSampleCollectionName(StatusInfo object) throws DmeSyncMappingException {
	  String runId = getRunId(object);
	  String sampleId = getSampleId(object);
	  String mochaId = getAttrWithKey(runId, sampleId, "Mocha_ID");
	  String sequencingDate = getAttrWithKey(runId, sampleId, "sequencing_Date");
	  String flowcellId= getFlowcellId(object);
	return mochaId + "_" + sequencingDate + "_" + flowcellId;
  }
  
  private String getAttrWithKey(String key1, String key2, String attrKey) {
		if(StringUtils.isEmpty(key1) || StringUtils.isEmpty(key2)) {
	      logger.error("Excel mapping not found for {}", key1 + key2);
	      return null;
	    }
	    return (metadataMap.get(key1 + "_" + key2) == null? null : metadataMap.get(key1 + "_" + key2).get(attrKey));
  }
  
  private String getAttrValueWithParitallyMatchingKey(String partialKey, String attrKey) throws DmeSyncMappingException {
	    String key = null;
	    for (Map.Entry<String, Map<String, String>> entry : metadataMap.entrySet()) {
	        if(StringUtils.contains(entry.getKey(), partialKey)) {
	          //Partial key match.
	          key = entry.getKey();
	          break;
	        }
	    }
	    if(StringUtils.isEmpty(key)) {
	      logger.error("Excel mapping not found for partial key {}", partialKey);
	      throw new DmeSyncMappingException("Excel mapping not found for " + partialKey);
	    }
	    String attrValue = metadataMap.get(key).get(attrKey);
	    return attrValue;
  }
  
  private String getSampleFromFilePath(String path, String folderName) throws DmeSyncMappingException {
	    String sampleName = null;
	    for (Map.Entry<String, Map<String, String>> entry : metadataMap.entrySet()) {
	        if(StringUtils.startsWith(entry.getKey(), folderName)) {
	        	String sampleEntry = metadataMap.get(entry.getKey()).get("Sample");
		        if(StringUtils.contains(path, sampleEntry)) {
		          //Sample is present in the file path
		          sampleName = sampleEntry;
		          break;
		        }
	        }
	    }
	    if(StringUtils.isEmpty(sampleName)) {
	      logger.error("Can't extract Sample from path {}", path);
	      throw new DmeSyncMappingException("Can't extract Sample from path " + path);
	    }
	    return sampleName;
  }
  
  private String getFileType(StatusInfo object) throws DmeSyncMappingException {
	  String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
	  if(isBCL(object))
	    return "tar";
	  else if (fileName.contains(".fastq"))
		return "fastq";
	  return fileName.substring(fileName.indexOf('.') + 1);
  }
  
  @PostConstruct
  private void init() {
	if("mocha".equalsIgnoreCase(doc)) {
	    try {
	      metadataMap = ExcelUtil.parseBulkMetadataEntries(metadataFile, "Run_ID", "Sample");
	    } catch (DmeSyncMappingException e) {
	        logger.error(
	            "Failed to initialize metadata  path metadata processor", e);
	    }
	}
  }
}
