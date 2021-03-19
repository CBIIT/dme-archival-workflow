package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map;
import javax.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncPathMetadataProcessor;
import gov.nih.nci.hpc.dmesync.workflow.impl.HpcEncryptor;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntries;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;
import gov.nih.nci.hpc.dmesync.util.ExcelUtil;

/**
 * SB DME Path and Meta-data Processor Implementation
 *
 * @author dinhys
 */
@Service("sb")
public class SBPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
    implements DmeSyncPathMetadataProcessor {

  // SB Custom logic for DME path construction and meta data creation

  @Autowired
  HpcEncryptor encryptor = null;

  @Value("${dmesync.additional.metadata.excel:}")
  private String metadataFile;
  
  @Value("${dmesync.doc.name}")
  private String doc;
  
  Map<String, Map<String, String>> metadataMap = null;
  
  @Override
  public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

    logger.info("[PathMetadataTask] SB getArchivePath called");

    // extract the derived metadata from the source path
    // Example source path -
    // /data/CCRSB/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam
    String fileName = Paths.get(object.getOriginalFilePath()).toFile().getName();
    String archivePath = null;
    
    // Get the PI collection value from the CollectionNameMetadata
    // Example row - mapKey - exome, collectionType - PI_Lab, mapValue - PI_XXX
    // Extract the Project value from the Path
    String patientKey = getPatientKey(object);
    String patientId = getPatientId(object);

    archivePath =
        destinationBaseDir
            + "/PI_"
            + getPiCollectionName(object)
            + "/Project_"
            + getProjectCollectionName(object)
            + "/Patient_"
            + patientKey
            + "/Run_"
            + getRunId(object)
            + "/"
            + fileName.replace(patientId, patientKey);

    // replace spaces with underscore
    archivePath = archivePath.replaceAll(" ", "_");

    logger.info("Archive path for {} : {}", object.getOriginalFilePath(), archivePath);

    return archivePath;
  }

  @Override
  public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
      throws DmeSyncMappingException, DmeSyncWorkflowException {

    // Add to HpcBulkMetadataEntries for path attributes
    HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

    // Add path metadata entries for "PI_XXX" collection
    // Example row: collectionType - PI, collectionName - XXX (derived)
    // key = pi_name, value = Steven A. Rosenberg (supplied)
    // key = pi_lab, value = NCI, CCR (supplied)

    String piCollectionName = getPiCollectionName(object);
    String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;
    HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
    pathEntriesPI.getPathMetadataEntries().add(createPathEntry("collection_type", "PI_Lab"));
    pathEntriesPI.setPath(piCollectionPath);
    hpcBulkMetadataEntries
        .getPathsMetadataEntries()
        .add(populateStoredMetadataEntries(pathEntriesPI, "PI_Lab", piCollectionName));

    // Add path metadata entries for "Project_XXX" collection
    // Example row: collectionType - Project, collectionName - Surgery_Branch_NGS
    // (derived),
    // key = project_name, value = Surgery_Branch_NGS (supplied)
    // key = project_title, value = Surgery Branch Patient Sequencing (supplied)
    // key = project_description, value = Contains all of the sequencing data for patients
    // treated in the Surgery Branch (supplied)
    // key = access, value = "Closed Access" (constant)
    // key = affiliation, value = "NCI, CCR" (supplied)
    // key = lab_contact, value = (supplied)
    // key = bioinformatics_contact, value = (supplied)

    String projectCollectionName = getProjectCollectionName(object);
    String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;
    HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
    pathEntriesProject.getPathMetadataEntries().add(createPathEntry("collection_type", "Project"));
    pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Closed Access"));
    pathEntriesProject.setPath(projectCollectionPath);
    hpcBulkMetadataEntries
        .getPathsMetadataEntries()
        .add(populateStoredMetadataEntries(pathEntriesProject, "Project", projectCollectionName));

    // Add path metadata entries for "Patient_XXX" collection
    // Example row: collectionType - Patient, collectionName - Patient_8021351
    // (derived)
    // key = patient_id, value = 8021351 (derived)
    // key patient_name, value = (supplied, for now "Unknown")
    // key date_of_birth, value = (supplied, for now "Unknown")
    // key patient_sex, value = (supplied, for now "Unknown")
    // key histology, value = BREAST (derived)
    // key patient_description, value = (supplied)
    String patientKey = getPatientKey(object);
    String patientId = getPatientId(object);
    String patientCollectionPath = projectCollectionPath + "/Patient_" + patientKey;
    HpcBulkMetadataEntry pathEntriesPatient = new HpcBulkMetadataEntry();
    pathEntriesPatient.getPathMetadataEntries().add(createPathEntry("collection_type", "Patient"));
    String patientIdEncrypted = Base64.getEncoder().encodeToString(encryptor.encrypt(patientId));
    String unknownEncrypted = Base64.getEncoder().encodeToString(encryptor.encrypt("Unknown"));
    pathEntriesPatient
        .getPathMetadataEntries()
        .add(createPathEntry("patient_id", patientIdEncrypted));
    pathEntriesPatient
        .getPathMetadataEntries()
        .add(createPathEntry("patient_key", patientKey));
    pathEntriesPatient
        .getPathMetadataEntries()
        .add(createPathEntry("histology", getHistology(object)));
    
    // For now, we are adding mandatory metadata which are TBD
    pathEntriesPatient.getPathMetadataEntries().add(createPathEntry("patient_name", unknownEncrypted));
    pathEntriesPatient.getPathMetadataEntries().add(createPathEntry("date_of_birth", unknownEncrypted));
    pathEntriesPatient.getPathMetadataEntries().add(createPathEntry("patient_sex", "Unknown"));
    
    pathEntriesPatient.setPath(patientCollectionPath);
    hpcBulkMetadataEntries
        .getPathsMetadataEntries()
        .add(populateStoredMetadataEntries(pathEntriesPatient, "Patient", patientId));

    // Add path metadata entries for "Run" collection
    // Example row: collectionType - Run, collectionName - Run_Seq_<date>_<sequencer> (derived)
    // key = sequencing_center, value = SB,Axeq,PDGX,etc… (derived)
    // key = run_id, value = Exome_June_26_2019_550A (derived)
    // key = run_date, value = MONTH_DATE_YEAR (derived)
    // key = run_type, value = Exome, Genome, Transcriptome, TCR seq (derived)
    // key = sequencer, SB ones are 550 (derived)
    // key = kit_used, value = XT_HS_kitV7, RNA-access (derived)
    // key = run_description, value = RNA-seq performed using RNA-capture probes (TBD)
    String fileName = Paths.get(object.getOriginalFilePath()).toFile().getName();
    String runId = getRunId(object);
    String runCollectionPath = patientCollectionPath + "/Run_" + runId;
    HpcBulkMetadataEntry pathEntriesRun = new HpcBulkMetadataEntry();
    pathEntriesRun.getPathMetadataEntries().add(createPathEntry("collection_type", "Run"));
    pathEntriesRun.getPathMetadataEntries().add(createPathEntry("sequencing_center", getSequencingCenter(object)));
    pathEntriesRun.getPathMetadataEntries().add(createPathEntry("run_id", runId));
    pathEntriesRun.getPathMetadataEntries().add(createPathEntry("run_date", getRunDate(object)));
    pathEntriesRun.getPathMetadataEntries().add(createPathEntry("run_type", getRunType(object)));
    String sequencer = getSequencer(object);
    if (StringUtils.isNotEmpty(sequencer))
      pathEntriesRun.getPathMetadataEntries().add(createPathEntry("sequencer", sequencer));
    String kitUsed = getKitUsed(object);
    if (StringUtils.isNotEmpty(kitUsed))
      pathEntriesRun.getPathMetadataEntries().add(createPathEntry("kit_used", kitUsed));
    pathEntriesRun.setPath(runCollectionPath);
    hpcBulkMetadataEntries
        .getPathsMetadataEntries()
        .add(populateStoredMetadataEntries(pathEntriesRun, "Run", "Run"));

    // Set it to dataObjectRegistrationRequestDTO
    HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO =
        new HpcDataObjectRegistrationRequestDTO();
    dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
    dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
    dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(
        hpcBulkMetadataEntries);

    // Add object metadata
    // key = object_name, value = 4390-1Met-Frag12_FrTu_November_04_2019_recal.bam (derived)
    // key = file_type, value = fastq, bam (derived)
    // key = sample_info, value = Metastasis, Primary, Other "Met" (derived)
    // key = sample_source, value = FrTu,FFPE,Xeno,TC (derived)
    // key = resection_date, value = 9/28/1980 (derived)
    // key = pii_content, value = Unspecified (default)
    // key = data_encryption_status, value = Unspecified (default)
    // key = data_compression_status, value = Compressed, Fastq will be gzipped and bam files are already binary compressed
    String fileType = fileName.substring(fileName.lastIndexOf('.') + 1);
    String dataCompressionStatus = "Unspecified";
    if ("gz".equals(fileType)) {
      fileType = fileName.substring(fileName.indexOf('.') + 1, fileName.lastIndexOf('.'));
      dataCompressionStatus = "Compressed";
    } else if ("bam".equals(fileType)) {
      dataCompressionStatus = "Compressed";
    }
    dataObjectRegistrationRequestDTO
        .getMetadataEntries()
        .add(createPathEntry("object_name", fileName.replace(patientId, patientKey)));
    dataObjectRegistrationRequestDTO
        .getMetadataEntries()
        .add(createPathEntry("file_type", fileType));
    dataObjectRegistrationRequestDTO
        .getMetadataEntries()
        .add(createPathEntry("source_path", object.getOriginalFilePath().replaceAll(patientId, patientKey)));
    dataObjectRegistrationRequestDTO
        .getMetadataEntries()
        .add(createPathEntry("sample_info", getSampleInfo(fileName)));
    dataObjectRegistrationRequestDTO
        .getMetadataEntries()
        .add(createPathEntry("sample_source", getSampleSource(fileName)));
    dataObjectRegistrationRequestDTO
        .getMetadataEntries()
        .add(createPathEntry("resection_date", getResectionDate(object)));
    dataObjectRegistrationRequestDTO
        .getMetadataEntries()
        .add(createPathEntry("pii_content", "Unspecified"));
    dataObjectRegistrationRequestDTO
        .getMetadataEntries()
        .add(createPathEntry("data_encryption_status", "Unspecified"));
    dataObjectRegistrationRequestDTO
        .getMetadataEntries()
        .add(createPathEntry("data_compression_status", dataCompressionStatus));
    logger.info(
        "CCRSB custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
    return dataObjectRegistrationRequestDTO;
  }

  private String getCollectionNameFromParent(StatusInfo object, String parentName) {
    // Example originalFilepath -
    // /data/CCRSB/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam
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
    // /data/CCRSB/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam
    // then the projectDirName will be exome
    String parent = object.getOriginalFilePath().contains("bam_files") ? "bam_files" : "fastq_files";
    String piDirName = getCollectionNameFromParent(object, parent);
    logger.info("PI Directory Name: {}", piDirName);

    piCollectionName = getCollectionMappingValue(piDirName, "PI_Lab");

    logger.info("PI Collection Name: {}", piCollectionName);
    return piCollectionName;
  }

  private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {
    String projectCollectionName = null;

    // Example: If originalFilePath is
    // /data/CCRSB/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam
    // then the projectDirName will be exome
    String parent = object.getOriginalFilePath().contains("bam_files") ? "bam_files" : "fastq_files";
    String projectDirName = getCollectionNameFromParent(object, parent);
    logger.info("Project Directory Name: {}", projectDirName);

    projectCollectionName = getCollectionMappingValue(projectDirName, "Project");

    logger.info("projectCollectionName: {}", projectCollectionName);
    return projectCollectionName;
  }

  private String getPatientId(StatusInfo object) throws DmeSyncMappingException {
    String patientId = null;
    // Example: If originalFilePath is
    // /data/CCRSB/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam
    // then the patientCollectionName will be 8021351
    Path fullFilePath = Paths.get(object.getOriginalFilePath());
    String parentFolder = fullFilePath.getParent().getFileName().toString();
    if(StringUtils.equals(parentFolder, "Raw_fastq"))
      parentFolder = fullFilePath.getParent().getParent().getFileName().toString();
    patientId = parentFolder.substring(0, parentFolder.indexOf('_'));
    logger.info("patientId: {}", patientId);
    return patientId;
  }

  private String getPatientKey(StatusInfo object) throws DmeSyncMappingException {
    String patientKey = null;
    // Example: If originalFilePath is
    // /data/CCRSB/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam
    // then the patientKey will be derived from 8021351
    Path fullFilePath = Paths.get(object.getOriginalFilePath());
    String parentFolder = fullFilePath.getParent().getFileName().toString();
    if(StringUtils.equals(parentFolder, "Raw_fastq"))
      parentFolder = fullFilePath.getParent().getParent().getFileName().toString();
    patientKey = parentFolder.substring(0, parentFolder.indexOf('_'));
    try {
      patientKey = encryptor.digest(patientKey).substring(0, 8);
    } catch (NoSuchAlgorithmException e) {
      throw new DmeSyncMappingException("Cannot derive the patient key ", e);
    }
    logger.info("patientKey: {}", patientKey);
    return patientKey;
  }
  
  private String getHistology(StatusInfo object) {
    String histology = null;
    // Example: If originalFilePath is
    // /data/CCRSB/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam
    // then the histology will be BREAST
    Path fullFilePath = Paths.get(object.getOriginalFilePath());
    String parentFolder = fullFilePath.getParent().getFileName().toString();
    if(StringUtils.equals(parentFolder, "Raw_fastq"))
      parentFolder = fullFilePath.getParent().getParent().getFileName().toString();
    histology = parentFolder.substring(parentFolder.indexOf('_') + 1);
    histology = histology.substring(0, histology.indexOf('_'));
    logger.info("histology: {}", histology);
    return histology;
  }

  private String getRunId(StatusInfo object) throws DmeSyncMappingException {
    String runId = null;
    // Example: If originalFilePath is
    // /data/CCRSB/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam
    // then the RunId will be Exome_November_04_2019_550A
    runId = getRunType(object) + "_" + getRunDate(object) + "_" + getSequencer(object);
    logger.info("RunId: {}", runId);
    return runId;
  }

  private String getRunDate(StatusInfo object) throws DmeSyncMappingException {
    // Example: If originalFilePath is
    // /data/CCRSB/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam
    // then the RunDate will be November_04_2019 (This is based on excel column, "sequencer_run_date")
    return getAttrValueWithParitallyMatchingKey(metadataMap, object, "sequencer_run_date");
  }

  private String getRunDateFromPath(StatusInfo object) throws DmeSyncMappingException {
    // Example: If originalFilePath is
    // /data/CCRSB/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam
    // then the RunDate will be November_04_2019 (This is based on parent path.)
    Path fullFilePath = Paths.get(object.getOriginalFilePath());
    String parentFolder = fullFilePath.getParent().getFileName().toString();
    if(StringUtils.equals(parentFolder, "Raw_fastq"))
      parentFolder = fullFilePath.getParent().getParent().getFileName().toString();
    String runDate = parentFolder.substring(parentFolder.indexOf('_', parentFolder.indexOf('_') + 1) + 1);
    logger.info("runDate: {}", runDate);
    return runDate;
  }
  
  private String getSequencingCenter(StatusInfo object) {
    // Example: If originalFilePath is
    // /data/CCRSB/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam
    // then the sequencing_center will be SB
    String parent = object.getOriginalFilePath().contains("bam_files") ? "bam_files" : "fastq_files";
    String runType = getCollectionNameFromParent(object, parent);
    return getCollectionNameFromParent(object, runType);
  }
  
  private String getRunType(StatusInfo object) {
    // Example: If originalFilePath is
    // /data/CCRSB/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam
    // then the RunType will be Exome
    String parent = object.getOriginalFilePath().contains("bam_files") ? "bam_files" : "fastq_files";
    String runType = getCollectionNameFromParent(object, parent);
    return StringUtils.capitalize(runType);
  }

  private String getSequencer(StatusInfo object) throws DmeSyncMappingException {
    // Example: If originalFilePath is
    // /data/CCRSB/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam
    // then the Sequencer will be 550A (This is based on excel column, "sequencer")
    return getAttrValueWithParitallyMatchingKey(metadataMap, object, "sequencer");
  }

  private String getKitUsed(StatusInfo object) throws DmeSyncMappingException {
    // Example: If originalFilePath is
    // /data/CCRSB/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam
    // then the kit_used will be SureSelectXT_HSV7 (This is based on excel column, "capture_kit")
    return getAttrValueWithParitallyMatchingKey(metadataMap, object, "capture_kit");
  }


  private String getSampleInfo(String objectName) {
    // Example: If objectName is
    // 4390-1Met-Frag12_FrTu_November_04_2019_recal.bam
    // then the SampleInfo will be Metastasis from "Met" (Metastasis "Met", Primary "Pri", Other)
    // PBL will be Other.
    String sampleInfo = null;
    if (StringUtils.contains(objectName, "Met")) sampleInfo = "Metastasis";
    else if (StringUtils.contains(objectName, "Pri")) sampleInfo = "Primary";
    else sampleInfo = "Other";
    return sampleInfo;
  }

  private String getSampleSource(String objectName) {
    // Example: If objectName is
    // 4390-1Met-Frag12_FrTu_November_04_2019_recal.bam
    // then the SampleSource will be FrTu (FrTu,FFPE,Xeno,TC)
    // PBL will be PBL
    String sampleSource = null;
    if (StringUtils.contains(objectName, "FrTu")) sampleSource = "FrTu";
    else if (StringUtils.contains(objectName, "FFPE")) sampleSource = "FFPE";
    else if (StringUtils.contains(objectName, "Xeno")) sampleSource = "Xeno";
    else if (StringUtils.contains(objectName, "TC")) sampleSource = "TC";
    else if (StringUtils.contains(objectName, "PBL")) sampleSource = "PBL";
    else sampleSource = "Unknown";
    return sampleSource;
  }
  
  private String getResectionDate(StatusInfo object) throws DmeSyncMappingException {
    // Example: If originalFilePath is
    // /data/CCRSB/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam
    // then the resection_date will be October_02_2019 (This is based on excel column, "resection_date")
    String fileName = Paths.get(object.getOriginalFilePath()).toFile().getName();
    String resectionDate = null;
    if(fileName.contains("PBL")) {
      resectionDate = "Unknown";
    } else {
      resectionDate = getAttrValueWithParitallyMatchingKey(metadataMap, object, "resection_date");
    }
    return resectionDate;
  }
  
  private String getAttrValueWithParitallyMatchingKey(Map<String, Map<String, String>> map, StatusInfo object, String attrKey)
      throws DmeSyncMappingException {
    String key = null;
    String fullKey = Paths.get(object.getOriginalFilePath()).toFile().getName();
    //If it is a PBL file, find the PBL in Matched_Normal_used column with the matching sequencer_run_date if exists. Otherwise, take the first entry.
    if(getRunType(object).equalsIgnoreCase("exome")) {
      if(fullKey.contains("PBL")) {
        //Extract run_date from the PBL file name
        String runDate=StringUtils.substringAfterLast(fullKey, "PBL_");
        for (Map.Entry<String, Map<String, String>> entry : map.entrySet()) {
          if(StringUtils.contains(fullKey, entry.getValue().get("Matched_Normal_used"))) {
            //PBL match.
            key = entry.getKey();
            if(StringUtils.contains(runDate, entry.getValue().get("sequencer_run_date")))
              break;
          }
        }
      }
      else {
        //Find the key that matches (the key is contained in the full key passed)
        for (String partialKey : map.keySet()) {
          if (StringUtils.contains(fullKey, StringUtils.trim(partialKey))) {
            key = partialKey;
            break;
          }
        }
      }
    } else {
      //Extract run_date from the path
      String runDate=getRunDateFromPath(object);
      String partialName = fullKey.substring(0, fullKey.indexOf('_'));
      for (Map.Entry<String, Map<String, String>> entry : map.entrySet()) {
        if(StringUtils.equalsIgnoreCase(entry.getKey(), partialName)) {
          //Sample match.
          key = entry.getKey();
          if(StringUtils.contains(runDate, entry.getValue().get("sequencer_run_date")))
            break;
        }
      }
    }
    if(StringUtils.isEmpty(key)) {
      logger.error("Excel mapping not found for {}", fullKey);
      throw new DmeSyncMappingException("Excel mapping not found for " + fullKey);
    }
    String attrValue = map.get(key).get(attrKey);
    if(StringUtils.isEmpty(attrValue))
      attrValue = "Unknown";
    return attrValue;
  }
  
  @PostConstruct
  private void init() {
	if("sb".equalsIgnoreCase(doc)) {
	    try {
	      metadataMap = ExcelUtil.parseBulkMatadataEntries(metadataFile, "file_name");
	    } catch (DmeSyncMappingException e) {
	        logger.error(
	            "Failed to initialize SB path metadata processor", e);
	    }
	}
  }
}
