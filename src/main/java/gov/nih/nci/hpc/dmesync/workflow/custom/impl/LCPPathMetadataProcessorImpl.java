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
 * LCP DME Path and Meta-data Processor Implementation
 *
 * @author dinhys
 */
@Service("lcp")
public class LCPPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
    implements DmeSyncPathMetadataProcessor {

  // LCP DME path construction and meta data creation

  @Value("${dmesync.additional.metadata.excel:}")
  private String metadataFile;
  
  @Value("${dmesync.doc.name}")
  private String doc;
  
  @Value("${dmesync.source.base.dir}")
  private String sourceDir;
  
  @Value("${dmesync.create.softlink:false}")
  private boolean createSoftlink;
  
  Map<String, Map<String, String>> sampleNameMap = null;
  
  @Override
  public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

    logger.info("[PathMetadataTask] LCP getArchivePath called");

    // Example source path -
    // /data/LCP_Omics/pilot_project/rawdata/exome_seq/fastq/LCP0051_Bx1PT1_ZN_A.R1.fastq.gz
    // /FNL_SF_Archive/PI_Lab_Xin_Wang/Project_XinWang_CS025208_241RNA_241RNA_080219/Flowcell_HKY7MDMXX/Sample_100_LCP0088_Bx1/100_LCP0088_Bx1_S78_R1_001.fastq.gz
    // /data/LCP_Omics/pilot_project/pipeliner/rna/initialQC/DEG_ALL/RSEM.genes.expected_count.all_samples.txt
    // /data/LCP_Omics/pilot_project/pipeliner/rna/initialQC/bams/LCP0036_BX1PT1_A.star_rg_added.sorted.dmark.bam
    // /data/LCP_Omics/pilot_project/pipeliner/rna/initialQC/Reports/multiqc_report.html
    String fileName = Paths.get(object.getSourceFileName()).toFile().getName();
    String archivePath = null;

    archivePath =
        destinationBaseDir
            + "/DataOwner_"
            + getDataOwnerCollectionName(object)
            + "/Project_"
            + getProjectCollectionName(object)
            + "/Application_Type_"
            + getApplicationTypeCollecionName(object)
            + "/Study_Site_"
            + getStudySite(object)
            + (isPipelinerMulti(object) ? "/Analysis/" + fileName: "/Sample_"
            + getSampleId(object)
            + "/"
            + (isAnalysis(object) ? "Analysis/" : "") 
            + (createSoftlink ? "Raw/" : "")
    		+ fileName);

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

    // Add path metadata entries for "DataOwner_XXX" collection
    // Example row: collectionType - DataOwner_Lab, collectionName - XXX (derived)
    // key = data_owner, value = Wang, Xin Wei (supplied)
    // key = data_owner_affiliation, value = Laboratory of Human Carcinogenesis (supplied)

    String dataOwnerCollectionName = getDataOwnerCollectionName(object);
    String dataOwnerCollectionPath = destinationBaseDir + "/DataOwner_" + dataOwnerCollectionName;
    HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
    pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "DataOwner_Lab"));
    pathEntriesPI.setPath(dataOwnerCollectionPath);
    hpcBulkMetadataEntries
        .getPathsMetadataEntries()
        .add(populateStoredMetadataEntries(pathEntriesPI, "DataOwner_Lab", dataOwnerCollectionName));

    // Add path metadata entries for "Project_XXX" collection
    String projectCollectionName = getProjectCollectionName(object);
    String projectCollectionPath = dataOwnerCollectionPath + "/Project_" + projectCollectionName;
    projectCollectionPath = projectCollectionPath.replace(" ", "_");
    HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
    pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
	pathEntriesProject.setPath(projectCollectionPath);
    hpcBulkMetadataEntries
	    .getPathsMetadataEntries()
	    .add(populateStoredMetadataEntries(pathEntriesProject, "Project", projectCollectionName));

    // Add path metadata entries for "Application_Type_XXX" collection
    String applicationTypeCollectionPath = projectCollectionPath + "/Application_Type_" + getApplicationTypeCollecionName(object);
    applicationTypeCollectionPath = applicationTypeCollectionPath.replace(" ", "_");
    HpcBulkMetadataEntry pathEntriesApplicationType = new HpcBulkMetadataEntry();
    pathEntriesApplicationType.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Application_Type"));
    pathEntriesApplicationType.setPath(applicationTypeCollectionPath);
    hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesApplicationType);
    
    // Add path metadata entries for "Study_Site_XXX" collection
    String studySiteCollectionPath = applicationTypeCollectionPath + "/Study_Site_" + getStudySite(object);
    studySiteCollectionPath = studySiteCollectionPath.replace(" ", "_");
    HpcBulkMetadataEntry pathEntriesStudySite = new HpcBulkMetadataEntry();
    pathEntriesStudySite.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Study_Site"));
    pathEntriesStudySite.setPath(studySiteCollectionPath);
    hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesStudySite);

    String fileName = Paths.get(object.getOriginalFilePath()).toFile().getName();
    if(isPipelinerMulti(object)) {
    	// Add path metadata entries for "Analysis" collection
        String analysisCollectionPath = studySiteCollectionPath + "/Analysis";
        HpcBulkMetadataEntry pathEntriesAnalysis = new HpcBulkMetadataEntry();
        pathEntriesAnalysis.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Analysis"));
        pathEntriesAnalysis.setPath(analysisCollectionPath);
        hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesAnalysis);
    } else {
	    // Add path metadata entries for "Sample" collection
	    String sampleId = getSampleId(object);
	    String applicationType = getApplicationType(object);
		String sampleCollectionPath = studySiteCollectionPath + "/Sample_" + sampleId;
	    sampleCollectionPath = sampleCollectionPath.replace(" ", "_");
	    HpcBulkMetadataEntry pathEntriesSample = new HpcBulkMetadataEntry();
	    pathEntriesSample.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Sample"));
	    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sample_name", getAttrWithKey(sampleId, applicationType, "Sample Name")));
	    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sample_id", sampleId));
	    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("library_strategy", applicationType));
	    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("analyte_type", getAttrWithKey(sampleId, applicationType, "Analyte Type")));
	    if(getAttrWithKey(sampleId, applicationType, "Disease") != null)
	    	pathEntriesSample.getPathMetadataEntries().add(createPathEntry("study_disease", getAttrWithKey(sampleId, applicationType, "Disease")));
	    if(getAttrWithKey(sampleId, applicationType, "Tissue") != null)
	    	pathEntriesSample.getPathMetadataEntries().add(createPathEntry("tissue", getAttrWithKey(sampleId, applicationType, "Tissue")));
	    if(getAttrWithKey(sampleId, applicationType, "Tissue Type") != null)
	    	pathEntriesSample.getPathMetadataEntries().add(createPathEntry("tissue_type", getAttrWithKey(sampleId, applicationType, "Tissue Type")));
	    if(getAttrWithKey(sampleId, applicationType, "Age") != null)
	    	pathEntriesSample.getPathMetadataEntries().add(createPathEntry("age", getAttrWithKey(sampleId, applicationType, "Age")));
	    if(getAttrWithKey(sampleId, applicationType, "Organism") != null)
	    	pathEntriesSample.getPathMetadataEntries().add(createPathEntry("organism_strain", getAttrWithKey(sampleId, applicationType, "Organism")));
	    if(getAttrWithKey(sampleId, applicationType, "Sex") != null)
	    	pathEntriesSample.getPathMetadataEntries().add(createPathEntry("gender", getAttrWithKey(sampleId, applicationType, "Sex")));
	    if(getAttrWithKey(sampleId, applicationType, "Is FFPE") != null)
	    	pathEntriesSample.getPathMetadataEntries().add(createPathEntry("is_ffpe", getAttrWithKey(sampleId, applicationType, "Is FFPE")));
	    if(getAttrWithKey(sampleId, applicationType, "Is Tumor") != null)
	    	pathEntriesSample.getPathMetadataEntries().add(createPathEntry("is_tumor", getAttrWithKey(sampleId, applicationType, "Is Tumor")));
	    if(getAttrWithKey(sampleId, applicationType, "Patient ID") != null)
	    	pathEntriesSample.getPathMetadataEntries().add(createPathEntry("patient_id", getAttrWithKey(sampleId, applicationType, "Patient ID")));
	    if(getAttrWithKey(sampleId, applicationType, "Used in retrospective manuscript?") != null)
	    	pathEntriesSample.getPathMetadataEntries().add(createPathEntry("is_used_in_retrospective_manuscript", getAttrWithKey(sampleId, applicationType, "Used in retrospective manuscript?")));
	    pathEntriesSample.setPath(sampleCollectionPath);
	    hpcBulkMetadataEntries
		    .getPathsMetadataEntries()
		    .add(pathEntriesSample);
	
	    if(isAnalysis(object)) {
	    	// Add path metadata entries for "Analysis" collection
	        String analysisCollectionPath = sampleCollectionPath + "/Analysis";
	        HpcBulkMetadataEntry pathEntriesAnalysis = new HpcBulkMetadataEntry();
	        pathEntriesAnalysis.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Analysis"));
	        pathEntriesAnalysis.setPath(analysisCollectionPath);
	        hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesAnalysis);
	    }
	    
	    if (createSoftlink) {
	    	// Add path metadata entries for "Raw" collection
	        String rawCollectionPath = isAnalysis(object) ? sampleCollectionPath + "/Analysis/Raw" : sampleCollectionPath + "/Raw";
	        HpcBulkMetadataEntry pathEntriesRaw = new HpcBulkMetadataEntry();
	        pathEntriesRaw.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Raw"));
	        pathEntriesRaw.setPath(rawCollectionPath);
	        hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesRaw);
	    }
    }
    
    // Set it to dataObjectRegistrationRequestDTO
    HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO =
        new HpcDataObjectRegistrationRequestDTO();
    dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
    dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
    dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(
        hpcBulkMetadataEntries);

    // Add object metadata
    String fileType = StringUtils.substringBefore(fileName, ".gz");
    fileType = fileType.substring(fileType.lastIndexOf('.') + 1);
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
        "LCP custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
    return dataObjectRegistrationRequestDTO;
  }

  private String getCollectionNameFromParent(String path, String parentName) {
    Path fullFilePath = Paths.get(path);
    int count = fullFilePath.getNameCount();
    for (int i = 0; i <= count; i++) {
      if (fullFilePath.getParent().getFileName().toString().equals(parentName)) {
        return fullFilePath.getFileName().toString();
      }
      fullFilePath = fullFilePath.getParent();
    }
    return null;
  }

  private String getDataOwnerCollectionName(StatusInfo object) throws DmeSyncMappingException {
    String dataOwnerCollectionName = null;
    // Example: If originalFilePath is
    // /data/LCP_Omics/pilot_project/rawdata/exome_seq/fastq/LCP0051_Bx1PT1_ZN_A.R1.fastq.gz
    // then return the mapped DataOwner from LCP_Omics
    dataOwnerCollectionName = getCollectionMappingValue("LCP_Omics", "DataOwner_Lab");

    logger.info("DataOwner Collection Name: {}", dataOwnerCollectionName);
    return dataOwnerCollectionName;
  }

  private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {
    String projectCollectionName = null;
    // Example: If originalFilePath is
    // /data/LCP_Omics/pilot_project/rawdata/exome_seq/fastq/LCP0051_Bx1PT1_ZN_A.R1.fastq.gz
    // then return the mapped Project for pilot_project
    // else if data/LCP_Omics/frozen_crc/pipeliner/rna/qc
    // then return the mapped Project for frozen_crc
    projectCollectionName = getCollectionMappingValue(getCollectionNameFromParent(sourceDir, "LCP_Omics"), "Project");

    logger.info("projectCollectionName: {}", projectCollectionName);
    return projectCollectionName;
  }

  private String getSampleId(StatusInfo object) throws DmeSyncMappingException {
	String path = Paths.get(object.getOriginalFilePath()).toString();
	String fileName = Paths.get(object.getOriginalFilePath()).getFileName().toString();
	String sampleId = null;
	String applicationType = getApplicationType(object);
	// Example: If DME path,
	// /FNL_SF_Archive/PI_Lab_Xin_Wang/Project_XinWang_CS025208_241RNA_241RNA_080219/Flowcell_HKY7MDMXX/Sample_100_LCP0088_Bx1/100_LCP0088_Bx1_S78_R1_001.fastq.gz
	// then get sampleId from sampleName.
	// Else if file name is LCP0051_Bx1PT1_ZN_A.R1.fastq.gz
	// then the sampleId will be LCP0051_Bx1PT1_ZN_A
	if (path.contains("Sample_")) {
		String sampleName = "";
		if(path.contains("FNL_SF_Archive")) {
			sampleName = path.substring(path.indexOf("Sample_"),
					path.indexOf(File.separator, path.indexOf("Sample_")));
		} else {
			sampleName = path.substring(path.indexOf("Sample_"),
					path.indexOf("_ZN", path.indexOf("Sample_")) + 3);
		}
		sampleId = getSampleIdWithKey(sampleName, applicationType, "Sample ID");
		if(sampleId == null)
			sampleId = getSampleIdWithKey(sampleName.replace("Bx1", "BX1"), applicationType, "Sample ID");
		if(sampleId == null)
			sampleId = getSampleIdWithKey(sampleName.replace("BX1", "Bx1"), applicationType, "Sample ID");
		
	} else {
		sampleId = StringUtils.substringBefore(fileName, ".");
	}
	return sampleId;
  }
  
  private String getApplicationTypeCollecionName(StatusInfo object) throws DmeSyncMappingException {
	// Example: If originalFilePath is
    // /data/LCP_Omics/pilot_project/rawdata/exome_seq/fastq/LCP0051_Bx1PT1_ZN_A.R1.fastq.gz
	// then the Application_Type will be WES
	// /FNL_SF_Archive/PI_Lab_Xin_Wang/Project_XinWang_CS025208_241RNA_241RNA_080219
	// /FNL_SF_Archive/PI_Lab_Xin_Wang/Project_XinWang_CS025209_241exome_090419
	String applicationType = null;
	if (StringUtils.containsIgnoreCase(object.getSourceFilePath(), "exome")) {
		applicationType = "WES";
	} else {
		applicationType = "WTS";
	}
	return applicationType;
  }
  
  private String getApplicationType(StatusInfo object) throws DmeSyncMappingException {
	// Example: If originalFilePath is
	// /data/LCP_Omics/pilot_project/rawdata/rna_seq/fastq/LCP0051_Bx1PT1_ZN_A.R1.fastq.gz
	// then the sampleId will be LCP0051_Bx1PT1_ZN_A
	String applicationType = null;
	if(isPipelinerMulti(object))
	  return "WTS";
	if (StringUtils.containsIgnoreCase(object.getSourceFilePath(), "exome")) {
		applicationType = "WES";
	} else {
		applicationType = "RNA-seq";
	}
	return applicationType;
  }
  
  private String getStudySite(StatusInfo object) throws DmeSyncMappingException {
	if(isPipelinerMulti(object))
	  return "NIH";
	String sampleId = getSampleId(object);
	String applicationType = getApplicationType(object);
	if(getAttrWithKey(sampleId, applicationType, "Site") == null)
		throw new DmeSyncMappingException("Can't find study site for sampleId " + sampleId + " and applicationType " + applicationType);	
	return getAttrWithKey(sampleId, applicationType, "Site");
  }
  
  private String getAttrWithKey(String key1, String key2, String attrKey) {
	if(StringUtils.isEmpty(key1) || StringUtils.isEmpty(key2)) {
	  logger.error("Excel mapping not found for {}", key1 + key2);
	  return null;
	}
	return (metadataMap.get(key1 + "_" + key2) == null? null : metadataMap.get(key1 + "_" + key2).get(attrKey));
  }
  
  private String getSampleIdWithKey(String key1, String key2, String attrKey) {
	if(StringUtils.isEmpty(key1) || StringUtils.isEmpty(key2)) {
	  logger.error("Excel mapping not found for {}", key1 + key2);
	  return null;
	}
	return (sampleNameMap.get(key1 + "_" + key2) == null? null : sampleNameMap.get(key1 + "_" + key2).get(attrKey));
  }
  
  private boolean isAnalysis(StatusInfo object) {
    return object.getOriginalFilePath().contains("Analysis") || isPipelinerBam(object);
  }
  
  private boolean isPipelinerBam(StatusInfo object) {
	// /data/LCP_Omics/pilot_project/pipeliner/rna/initialQC/bams/LCP0036_BX1PT1_A.star_rg_added.sorted.dmark.bam
	return object.getOriginalFilePath().contains("pipeliner") && object.getOriginalFilePath().contains("bams");
  }
  
  private boolean isPipelinerMulti(StatusInfo object) {
	// /data/LCP_Omics/pilot_project/pipeliner/rna/initialQC/DEG_ALL/RSEM.genes.expected_count.all_samples.txt
	// /data/LCP_Omics/pilot_project/pipeliner/rna/initialQC/Reports/multiqc_report.html
    return object.getOriginalFilePath().contains("pipeliner") && !object.getOriginalFilePath().contains("bams");
  }
  
  @PostConstruct
  private void init() throws DmeSyncMappingException {
	if("lcp".equalsIgnoreCase(doc)) {
		metadataMap = ExcelUtil.parseBulkMetadataEntries(metadataFile, "Sample ID", "Application Type");
		sampleNameMap = ExcelUtil.parseBulkMetadataEntries(metadataFile, "Sample Name", "Application Type");
	}
  }
}
