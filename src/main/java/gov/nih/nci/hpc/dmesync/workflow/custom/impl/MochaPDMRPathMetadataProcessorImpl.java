package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

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
 * MoCha PDMR data DME Path and Meta-data Processor Implementation
 *
 * @author dinhys
 */
@Service("mocha-pdmr")
public class MochaPDMRPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
    implements DmeSyncPathMetadataProcessor {

  // Mocha PDMR data DME path construction and meta data creation

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

    logger.info("[PathMetadataTask] Mocha PDMR data getArchivePath called");

    // Example source path -
    // /mnt/mocha_ngs/active/MoCha-NGS_BW_transfers/PDX_fastq_backup
    // /mnt/mocha_scratch/BW_transfers/processedDATA
    String fileName = Paths.get(object.getOrginalFileName()).toFile().getName();
    String archivePath =
        destinationBaseDir
            + "/Lab_"
            + getPiCollectionName()
            + "/Platform_"
            + getPlatformCollectionName()
            + "/Project_"
            + getProjectCollectionName()
            + "/Patient_"
            + getPatientId(object)
            + "/Sample_"
            + getSampleId(object)
            + "/"
            + fileName;

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
    String fileName = Paths.get(object.getOrginalFileName()).toFile().getName();
    
    // Add path metadata entries for "PI_XXX" collection
    // Example row: collectionType - PI, collectionName - XXX (derived)
    // key = data_owner, value = Mickey Williams (supplied)
    // key = data_owner_affiliation, value = Molecular Characterization Laboratory, FNLCR (supplied)
    String piCollectionName = getPiCollectionName();
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
    String platformCollectionName = getPlatformCollectionName();
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
    

	String projectCollectionName = getProjectCollectionName();
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
    String sampleId = getSampleId(object);
	String projectCollectionPath = platformCollectionPath + "/Project_" + projectCollectionName;
	HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
	pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
	pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_id", projectCollectionName));
	pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_status", "Active"));
	pathEntriesProject.setPath(projectCollectionPath);
	hpcBulkMetadataEntries.getPathsMetadataEntries()
	.add(populateStoredMetadataEntries(pathEntriesProject, "Project", projectCollectionName, "mocha"));
    
	// Add path metadata entries for "Patient" collection    
    String patientCollectionPath = projectCollectionPath + "/Patient_" + getPatientId(object);
    patientCollectionPath = patientCollectionPath.replace(" ", "_");
    HpcBulkMetadataEntry pathEntriesPatient = new HpcBulkMetadataEntry();
    pathEntriesPatient.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Patient"));
    pathEntriesPatient.getPathMetadataEntries().add(createPathEntry("patient_id", getPatientId(object)));  
    pathEntriesPatient.getPathMetadataEntries().add(createPathEntry("histology", getAttrWithKey(sampleId, "Histology")));  
    pathEntriesPatient.getPathMetadataEntries().add(createPathEntry("diagnosis", getAttrWithKey(sampleId, "Diagnosis")));
    pathEntriesPatient.setPath(patientCollectionPath);
    hpcBulkMetadataEntries
        .getPathsMetadataEntries()
        .add(pathEntriesPatient);
    
    // Add path metadata entries for "Sample" collection    
    String sampleCollectionPath = patientCollectionPath + "/Sample_" + getSampleId(object);
    sampleCollectionPath = sampleCollectionPath.replace(" ", "_");
    HpcBulkMetadataEntry pathEntriesSample = new HpcBulkMetadataEntry();
    pathEntriesSample.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Sample"));
    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sample_id", sampleId));
    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sample_type", getAttrWithKey(sampleId, "Sampletype")));  
    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("assey_type", getAttrWithKey(sampleId, "Assay")));
    pathEntriesSample.setPath(sampleCollectionPath);
    hpcBulkMetadataEntries
        .getPathsMetadataEntries()
        .add(pathEntriesSample);
    
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

  private String getPiCollectionName() throws DmeSyncMappingException {
    return "MoCha";
  }
  
  private String getPlatformCollectionName() throws DmeSyncMappingException {
	  return "NovaSeq";
  }

  private String getProjectCollectionName() throws DmeSyncMappingException {
	return "PDMR";
  }

  private String getPatientId(StatusInfo object) throws DmeSyncMappingException {
	String sampleId = getSampleId(object);
	return getAttrWithKey(sampleId, "Patient_ID");
  }
  
  private String getSampleId(StatusInfo object) throws DmeSyncMappingException {
	  String path = Paths.get(object.getOriginalFilePath()).toString();
	  String sampleId = getAttrKeyFromKeyInSearchString(path);
	  if(StringUtils.isEmpty(sampleId)) {
		  logger.error("Sample ID can't be extracted for {}", path);
	  }
	return sampleId;
  }
  
  private String getAttrWithKey(String key, String attrKey) {
		if(metadataMap.get(key) == null) {
	      logger.error("Excel mapping not found for {}", key);
	      return null;
	    }
	    return (metadataMap.get(key).get(attrKey));
  }
  
  private String getAttrKeyFromKeyInSearchString(String searchString) throws DmeSyncMappingException {
	    String key = null;
	    for (Map.Entry<String, Map<String, String>> entry : metadataMap.entrySet()) {
	        if(StringUtils.contains(searchString, entry.getKey())) {
	          //Partial key match.
	          key = entry.getKey();
	          break;
	        }
	    }
	    if(StringUtils.isEmpty(key)) {
	      logger.error("Excel mapping not found for search string {}", searchString);
	      throw new DmeSyncMappingException("Excel mapping not found for " + searchString);
	    }
	    return key;
  }
  
  private String getFileType(StatusInfo object) throws DmeSyncMappingException {
	  String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
	  if (fileName.contains(".fastq"))
		return "fastq";
	  return fileName.substring(fileName.indexOf('.') + 1);
  }
  
  @PostConstruct
  private void init() {
	if("mocha-pdmr".equalsIgnoreCase(doc)) {
	    try {
	      metadataMap = ExcelUtil.parseBulkMetadataEntries(metadataFile, "Sample_ID");
	    } catch (DmeSyncMappingException e) {
	        logger.error(
	            "Failed to initialize metadata  path metadata processor", e);
	    }
	}
  }
}
