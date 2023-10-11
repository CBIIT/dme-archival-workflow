package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.Iterator;
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
  
  @Value("${dmesync.source.base.dir}")
  private String baseDir;
  
  @Value("${dmesync.work.base.dir}")
  private String workDir;
  
  @Value("${dmesync.search.dir:}")
  private String searchDir;
  
  Map<String, Map<String, String>> metadataMap = null;
  
  @Override
  public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

    logger.info("[PathMetadataTask] SB getArchivePath called");

    // extract the derived metadata from the source path
    // Example source path -
    // /data/CCRSB2/fastq_files/SB_4431Met_Frag1_FrTu_January_26_2021_rnaseq_1.fastq.gz
    // /data/CCRSB2/pipelineData/612161e212/Bams/SB_4431Met_Frag13_FrTu_December_15_2020_exome_recal.bam
    String fileName = Paths.get(object.getSourceFileName()).toFile().getName();
    String archivePath = null;
    
    // Get the PI collection value from the CollectionNameMetadata
    // Example row - mapKey - CCRSB2, collectionType - PI_Lab, mapValue - PI_XXX
    // Extract the Project value from the Path
    String patientId = getPatientId(object);

    archivePath =
        destinationBaseDir
            + "/PI_"
            + getPiCollectionName(object)
            + "/Project_"
            + getProjectCollectionName(object)
            + "/Patient_"
            + patientId
            + (isSingleCell() ? "/Run_" + getRunId(object) : "")
            + "/Sample_"
            + getSampleId(object)
            + "/"
            + fileName;

    // replace spaces with underscore
    archivePath = archivePath.replace(" ", "_");
    
    // replace "select" with "Select"
    archivePath = archivePath.replace("select", "Select");

    logger.info("Archive path for {} : {}", object.getOriginalFilePath(), archivePath);
    
    if (!isSingleCell()) {
	    Path metadataFilePath = Paths.get(workDir, "fastq_files", patientId, "successfulRun.xls");
	    if(object.getOriginalFilePath().contains("Bams")) {
		    // Find corresponding fastq files and create a symbolic link in the work dir if it doesn't already exist
		    String partialName = StringUtils.substringBefore(fileName, "_exome") + (fileName.contains("_exome") ? "_exome" : "");
		    partialName = StringUtils.substringBefore(partialName, "_rnaseq") + (partialName.contains("_rnaseq") ? "_rnaseq" : "");
		    partialName = StringUtils.substringBefore(partialName, "_1.");
		    partialName = StringUtils.substringBefore(partialName, "_2.");
		    partialName = StringUtils.substringBefore(partialName, ".");
		    partialName = StringUtils.substringBefore(partialName, "_recal");
		    final String fastqFilePartialName = partialName;
		    try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(searchDir),
					path -> path.getFileName().toString().startsWith(fastqFilePartialName))) {
				Iterator<Path> it = stream.iterator();
				while (it.hasNext()) {
					Path fastqFile = it.next();
					Path link = Paths.get(workDir, "fastq_files", patientId, fastqFile.getFileName().toString());
					if (!Files.exists(link)) {
						//Create parent folder if it doesn’t exist
			 			Files.createDirectories(link.getParent());
						Files.createSymbolicLink(link, fastqFile);
				    }
				}
			} catch (IOException e) {
				logger.info("Fastq file not found for {}", object.getOriginalFilePath());
			}
		    // Convert patient sample sheet to excel for loading if not already created or sample sheet is updated.
		    Path path = Paths.get(object.getOriginalFilePath());
    		Path sampleSheetPath = Paths.get(path.getParent().getParent().toString(), "successfulRun.txt");
    		try {
	    		BasicFileAttributes sampleSheetAttr = Files.readAttributes(sampleSheetPath, BasicFileAttributes.class);
	    		FileTime sampleSheetTime = sampleSheetAttr.lastModifiedTime();
	    		boolean createExcel = true;
	    		if(Files.exists(metadataFilePath)) {
	    			BasicFileAttributes metadataFileAttr = Files.readAttributes(metadataFilePath, BasicFileAttributes.class);
	        		FileTime metadataFileTime = metadataFileAttr.lastModifiedTime();
	        		//sampleSheet was created before metadataFile so it has not been updated.
	        		if(sampleSheetTime.compareTo(metadataFileTime) < 0)
	        			createExcel = false;
	    		}
		    	if(createExcel) {
		        	Path excelFilePath = Paths.get(workDir, "fastq_files", patientId, "successfulRun.xls");
		        	ExcelUtil.convertTextToExcel(new File(sampleSheetPath.toString()), new File(excelFilePath.toString()),"\t");
		        }
    		} catch (IOException e) {
        		throw new DmeSyncMappingException("Can't convert patient samplesheet to excel", e);
        	}
	    } else if(object.getOriginalFilePath().contains("fastq_files")) {
	    	//Set source path to the actual file.
	    	try {
	    		String fastqFile = Files.readSymbolicLink(Paths.get(object.getOriginalFilePath())).toString();
	    		object.setSourceFilePath(fastqFile);
	    	} catch (Exception e) {
	    		logger.info("File is not a symbolic link {}", object.getOriginalFilePath());
	    	}
	    }
	    threadLocalMap.set(loadMetadataFile(metadataFilePath.toString(), "SampleID"));
    }
    
    return archivePath;
  }

  @Override
  public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
      throws DmeSyncMappingException, DmeSyncWorkflowException {

    HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO =
            new HpcDataObjectRegistrationRequestDTO();
	try {   
    // Add to HpcBulkMetadataEntries for path attributes
    HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

    // Add path metadata entries for "PI_XXX" collection
    // Example row: collectionType - PI, collectionName - XXX (derived)
    // key = pi_name, value = Steven A. Rosenberg (supplied)
    // key = pi_lab, value = NCI, CCR (supplied)

    String piCollectionName = getPiCollectionName(object);
    String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;
    HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
    pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "PI_Lab"));
    pathEntriesPI.setPath(piCollectionPath);
    hpcBulkMetadataEntries
        .getPathsMetadataEntries()
        .add(populateStoredMetadataEntries(pathEntriesPI, "PI_Lab", piCollectionName, "sb"));

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
    pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
    pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Closed Access"));
    pathEntriesProject.setPath(projectCollectionPath);
    hpcBulkMetadataEntries
        .getPathsMetadataEntries()
        .add(populateStoredMetadataEntries(pathEntriesProject, "Project", projectCollectionName, "sb"));

    // Add path metadata entries for "Patient_XXX" collection
    // Example row: collectionType - Patient, collectionName - Patient_612161e212
    // (derived)
    // key = patient_id, value = 612161e212 (derived)
    // key patient_name, value = (supplied, for now "Unknown")
    // key date_of_birth, value = (supplied, for now "Unknown")
    // key patient_sex, value = (supplied, for now "Unknown")
    // key histology, value = BREAST (derived)
    // key patient_description, value = (supplied)
    String patientKey = getPatientKey(object);
    String patientId = getPatientId(object);
    String patientCollectionPath = projectCollectionPath + "/Patient_" + patientId;
    HpcBulkMetadataEntry pathEntriesPatient = new HpcBulkMetadataEntry();
    pathEntriesPatient.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Patient"));
    String patientIdEncrypted = Base64.getEncoder().encodeToString(encryptor.encrypt(patientKey));
    pathEntriesPatient
        .getPathMetadataEntries()
        .add(createPathEntry("patient_id", patientIdEncrypted));
    pathEntriesPatient
        .getPathMetadataEntries()
        .add(createPathEntry("subject_id", patientId));
    
    // For now, we are adding mandatory metadata which are TBD
    //pathEntriesPatient.getPathMetadataEntries().add(createPathEntry("patient_name", unknownEncrypted));
    //pathEntriesPatient.getPathMetadataEntries().add(createPathEntry("date_of_birth", unknownEncrypted));
    pathEntriesPatient.getPathMetadataEntries().add(createPathEntry("gender", getBiologicalSex(object)));
    
    pathEntriesPatient.setPath(patientCollectionPath);
    hpcBulkMetadataEntries
        .getPathsMetadataEntries()
        .add(pathEntriesPatient);

    // Add path metadata entries for "Run" collection for Single Cell fastq files
    String fileName = Paths.get(object.getOriginalFilePath()).toFile().getName();
    String runId = getRunId(object);
    String runCollectionPath = null;
    if(isSingleCell()) {
    	runCollectionPath = patientCollectionPath + "/Run_" + runId;
        HpcBulkMetadataEntry pathEntriesRun = new HpcBulkMetadataEntry();
        pathEntriesRun.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Run"));
        pathEntriesRun.getPathMetadataEntries().add(createPathEntry("run_id", runId));
        pathEntriesRun.getPathMetadataEntries().add(createPathEntry("run_date", getRunDateFromPath(object)));
        pathEntriesRun.setPath(runCollectionPath);
        hpcBulkMetadataEntries
        .getPathsMetadataEntries()
        .add(pathEntriesRun);
    }

    // Add path metadata entries for "Sample" collection
    // Example row: collectionType - Sample, collectionName - Sample_<RunID>
    // key = sequencing_center, value = SB,Axeq,PDGX,etc… (derived)
    // key = run_id, value = 210126_NB551078_353_HHKC2BGXH (derived)
    // key = run_date, value = MONTH_DATE_YEAR (derived)
    // key = run_type, value = Exome, Genome, Transcriptome, TCR seq (derived)
    // key = sequencer, SB ones are 550 (derived)
    // key = kit_used, value = XT_HS_kitV7, RNA-access (derived)
    // key = run_description, value = RNA-seq performed using RNA-capture probes (TBD)
    // key = metastasis_site, value = Metastasis, Primary, Other "Met" (derived)
    // key = tissue_type, value = FrTu,FFPE,Xeno,TC (derived)
    // key = resection_date, value = 9/28/1980 (derived)
    String sampleId = getSampleId(object);
    String sampleCollectionPath =  (isSingleCell() ? runCollectionPath : patientCollectionPath) +  "/Sample_" + sampleId;
    HpcBulkMetadataEntry pathEntriesSample = new HpcBulkMetadataEntry();
    pathEntriesSample.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Sample"));
    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sequencing_center", getSequencingCenter(object)));
    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("run_id", runId));
    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("run_date", getRunDateFromPath(object)));
    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("analyte_type", getSampleType(object)));
    String sequencer = getSequencer(object);
    if (StringUtils.isNotEmpty(sequencer))
    	pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sequencer", sequencer));
    String kitUsed = getKitUsed(object);
    if (StringUtils.isNotEmpty(kitUsed))
    	pathEntriesSample.getPathMetadataEntries().add(createPathEntry("kit_used", kitUsed));
    if (StringUtils.isNotEmpty(getSampleInfo(object)))
    	pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sample_info", getSampleInfo(object)));
    if (StringUtils.isNotEmpty(getTissueType(object)))
    	pathEntriesSample.getPathMetadataEntries().add(createPathEntry("tissue_type", getTissueType(object)));
    if (StringUtils.isNotEmpty(getResectionDate(object)))
    	pathEntriesSample.getPathMetadataEntries().add(createPathEntry("resection_date", getResectionDate(object)));
    if (StringUtils.isNotEmpty(getMatchedNormal(object)))
    	pathEntriesSample.getPathMetadataEntries().add(createPathEntry("matched_normal", getMatchedNormal(object)));
    if (StringUtils.isNotEmpty(getMatchedRnaseq(object)))
    	pathEntriesSample.getPathMetadataEntries().add(createPathEntry("matched_rnaseq", getMatchedRnaseq(object)));
    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sample_name", sampleId));
    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("library_strategy", getLibraryType(object)));
    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("tissue", "NOS"));
    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sequenced_material", getSequencedMaterial(object)));//is_cell_line
    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("disease_type", getHistology(object)));
    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("strain", getStrain(object)));
    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("age", "Unknown"));
    if(isSingleCell() && getAttrWithKey(runId, object.getOrginalFileName(), "Comment") != null)
    	pathEntriesSample.getPathMetadataEntries().add(createPathEntry("comment", getAttrWithKey(runId, object.getOrginalFileName(), "Comment")));
  
    pathEntriesSample.setPath(sampleCollectionPath);
    hpcBulkMetadataEntries
        .getPathsMetadataEntries()
        .add(pathEntriesSample);

    // Set it to dataObjectRegistrationRequestDTO
    dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
    dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
    dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(
        hpcBulkMetadataEntries);

    // Add object metadata
    // key = object_name, value = 4390-1Met-Frag12_FrTu_November_04_2019_recal.bam (derived)
    // key = file_type, value = fastq, bam (derived)
    // key = pii_content, value = Unspecified (default)
    // key = data_encryption_status, value = Unspecified (default)
    // key = data_compression_status, value = Compressed, Fastq will be gzipped and bam files are already binary compressed
    String fileType = StringUtils.substringBefore(fileName, ".tar");
    fileType = fileType.substring(fileType.lastIndexOf('.') + 1);
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
    dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("creation_date", getCreationDate(object)));
    dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("modified_date", getModifiedDate(object)));
    dataObjectRegistrationRequestDTO
        .getMetadataEntries()
        .add(createPathEntry("source_path",  object.getOriginalFilePath().contains("Bams") ? object.getOriginalFilePath(): object.getSourceFilePath()));
    dataObjectRegistrationRequestDTO
        .getMetadataEntries()
        .add(createPathEntry("pii_content", "Unspecified"));
    dataObjectRegistrationRequestDTO
        .getMetadataEntries()
        .add(createPathEntry("data_encryption_status", "Unspecified"));
    dataObjectRegistrationRequestDTO
        .getMetadataEntries()
        .add(createPathEntry("data_compression_status", dataCompressionStatus));
  } catch (IOException e){
	 throw new DmeSyncMappingException(e);
  } finally {
	if (!isSingleCell())
	  threadLocalMap.remove();
  }
    logger.info(
        "CCRSB custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
    return dataObjectRegistrationRequestDTO;
  }

  private String getCollectionNameFromParent(StatusInfo object, String parentName) {
    // Example originalFilepath -
    // /data/CCRSB/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam
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

  private String getPiCollectionName(StatusInfo object) throws DmeSyncMappingException {
    String piCollectionName = null;
    // Example: If originalFilePath is
    // /data/CCRSB2/fastq_files/SB_4431Met_Frag1_FrTu_January_26_2021_rnaseq_1.fastq.gz
    // then return the mapped PI from CCRSB2
    String projectDirName = "CCRSB2";
    piCollectionName = getCollectionMappingValue(projectDirName, "PI_Lab", "sb");

    logger.info("PI Collection Name: {}", piCollectionName);
    return piCollectionName;
  }

  private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {
    String projectCollectionName = null;
    // Example: If originalFilePath is
    // /data/CCRSB2/fastq_files/SB_4431Met_Frag1_FrTu_January_26_2021_rnaseq_1.fastq.gz
    // then return the mapped Project from CCRSB2
    String projectDirName = "CCRSB2";
    projectCollectionName = getCollectionMappingValue(projectDirName, "Project", "sb");

    logger.info("projectCollectionName: {}", projectCollectionName);
    return projectCollectionName;
  }

  private String getPatientId(StatusInfo object) throws DmeSyncMappingException {
    String patientId = null;
    // Example: If originalFilePath is
    // /data/CCRSB2/pipelineData/612161e212/Bams/SB_4431Met_Frag13_FrTu_December_15_2020_exome_recal.bam
    // then the patientCollectionName will be 612161e212
    if (isSingleCell()) {
    	patientId = getAttrWithKey(getRunId(object), object.getOrginalFileName(), "PatientID");
    	if(patientId == null)
    		throw new DmeSyncMappingException("Metadata entry not found for RunID: " + getRunId(object) + " Fastq: " + object.getOrginalFileName());
    	return patientId;
    }
    	
    try {
    	patientId = getCollectionNameFromParent(object, "pipelineData");
    } catch (Exception e) {
    	patientId = getCollectionNameFromParent(object, "fastq_files");
    }
    logger.info("patientId: {}", patientId);
    return patientId;
  }

  private String getPatientKey(StatusInfo object) throws DmeSyncMappingException {
    String patientKey = null;
    // Example: If originalFilePath is
    // /data/CCRSB2/pipelineData/612161e212/Bams/SB_4431Met_Frag13_FrTu_December_15_2020_exome_recal.bam
    // then the MRN will be extracted from metafile using sample SB_4431Met_Frag13_FrTu_December_15_2020_exome
    if (isSingleCell()) {
    	return getAttrWithKey(getRunId(object), object.getOrginalFileName(), "MRN");
    }
    patientKey = getAttrValueWithParitallyMatchingKey(threadLocalMap.get(), object, "MRN");
    logger.info("patientKey: {}", patientKey);
    return patientKey;
  }
  
  private String getHistology(StatusInfo object) throws DmeSyncMappingException {
    String histology = null;
    // Example: If originalFilePath is
    // /data/CCRSB2/pipelineData/612161e212/Bams/SB_4431Met_Frag13_FrTu_December_15_2020_exome_recal.bam
    // then the Histology will be extracted from metafile using sample SB_4431Met_Frag13_FrTu_December_15_2020_exome
    if (isSingleCell()) {
    	return getAttrWithKey(getRunId(object), object.getOrginalFileName(), "Histology");
    }
    histology = getAttrValueWithParitallyMatchingKey(threadLocalMap.get(), object, "Histology");
    logger.info("histology: {}", histology);
    return histology;
  }

  private String getRunId(StatusInfo object) throws DmeSyncMappingException {
    String runId = null;
    // Example: If originalFilePath is
    // /data/CCRSB2/pipelineData/612161e212/Bams/SB_4431Met_Frag13_FrTu_December_15_2020_exome_recal.bam
    // then the RunId will be extracted from metafile using sample SB_4431Met_Frag13_FrTu_December_15_2020_exome
    if (isSingleCell()) {
    	return getCollectionNameFromParent(object, "10X_Fastqs");
    }
    runId = getAttrValueWithParitallyMatchingKey(threadLocalMap.get(), object, "RunID");
    logger.info("RunId: {}", runId);
    return runId;
  }


  private String getRunDateFromPath(StatusInfo object) throws DmeSyncMappingException {
    // Example: If originalFilePath is
	// /data/CCRSB2/pipelineData/612161e212/Bams/SB_4431Met_Frag13_FrTu_December_15_2020_exome_recal.bam
    // then the RunDate will be December_15_2020
    Path fullFilePath = Paths.get(object.getOriginalFilePath());
    String fileName = fullFilePath.getFileName().toString();
    if (isSingleCell()) {
		return StringUtils.substringBefore(getRunId(object), "_");
	}
    String runDate = StringUtils.substringBefore(fileName, "_rnaseq");
    runDate = StringUtils.substringBefore(runDate, "_exome");
    runDate = StringUtils.substringBefore(runDate, "_1.");
    runDate = StringUtils.substringBefore(runDate, "_2.");
    runDate = StringUtils.substringBefore(runDate, ".");
    runDate = StringUtils.substringBefore(runDate, "_recal");
    runDate = StringUtils.isNotEmpty(StringUtils.substringAfter(runDate, "_FrTu_")) ? StringUtils.substringAfter(runDate, "_FrTu_") : runDate;
    runDate = StringUtils.isNotEmpty(StringUtils.substringAfter(runDate, "_PBL_")) ? StringUtils.substringAfter(runDate, "_PBL_") : runDate;
    logger.info("runDate: {}", runDate);
    return runDate;
  }
  
  private String getSequencingCenter(StatusInfo object) throws DmeSyncMappingException {
	// Example: If originalFilePath is
    // /data/CCRSB/data/bam_files/exome/SB/8021351_BREAST_November_04_2019/4390-1Met-Frag12_FrTu_November_04_2019_recal.bam
    // then the SequencingCenter will be SB (This is based on excel column, "SequencingCenter")
    if (isSingleCell()) {
    	return getAttrWithKey(getRunId(object), object.getOrginalFileName(), "SequencingCenter") == null ? "Unknown"
				: getAttrWithKey(getRunId(object), object.getOrginalFileName(), "SequencingCenter");
	}
    return getAttrValueWithParitallyMatchingKey(threadLocalMap.get(), object, "SequencingCenter");
  }
  
  private String getSampleType(StatusInfo object) throws DmeSyncMappingException {
    // Example: If originalFilePath is
    // /data/CCRSB2/pipelineData/612161e212/Bams/SB_4431Met_Frag13_FrTu_December_15_2020_exome_recal.bam
    // then the SampleType will be DNA (This is based on excel column, "analyte_type")
	if (isSingleCell()) {
		return getAttrWithKey(getRunId(object), object.getOrginalFileName(), "analyte_type") == null ? "Unknown"
				: getAttrWithKey(getRunId(object), object.getOrginalFileName(), "analyte_type");
	}
	return getAttrValueWithParitallyMatchingKey(threadLocalMap.get(), object, "analyte_type");
  }
  
  private String getStrain(StatusInfo object) throws DmeSyncMappingException {
    // Example: If originalFilePath is
    // /data/CCRSB2/pipelineData/612161e212/Bams/SB_4431Met_Frag13_FrTu_December_15_2020_exome_recal.bam
    // then the Strain will be Human (This is based on excel column, "strain" for SC)
	if (isSingleCell()) {
		return getAttrWithKey(getRunId(object), object.getOrginalFileName(), "strain") == null ? "Human"
				: getAttrWithKey(getRunId(object), object.getOrginalFileName(), "strain");
	}
	return "Human";
  }
  
  private String getTissueType(StatusInfo object) throws DmeSyncMappingException {
	    // Example: If originalFilePath is
	    // /data/CCRSB2/pipelineData/612161e212/Bams/SB_4431Met_Frag13_FrTu_December_15_2020_exome_recal.bam
	    // then the TissueType will be Tumor (This is based on excel column, "tissue_type")
	    if (isSingleCell()) {
		  return "Unknown";
		}
		return getAttrValueWithParitallyMatchingKey(threadLocalMap.get(), object, "tissue_type");
  }
  
  private String getBiologicalSex(StatusInfo object) throws DmeSyncMappingException {
	    // Example: If originalFilePath is
	    // /data/CCRSB2/pipelineData/612161e212/Bams/SB_4431Met_Frag13_FrTu_December_15_2020_exome_recal.bam
	    // then the BiologicalSex will be Male (This is based on excel column, "Gender")
	    if (isSingleCell()) {
	    	if(getAttrWithKey(getRunId(object), object.getOrginalFileName(), "Gender") == null )
	    		return "Unknown";
	    	else
	    		return getAttrWithKey(getRunId(object), object.getOrginalFileName(), "Gender").startsWith("M")? "Male" : "Female";
		}
		return getAttrValueWithParitallyMatchingKey(threadLocalMap.get(), object, "Gender");
  }
  
  private String getMatchedNormal(StatusInfo object) throws DmeSyncMappingException {
	    // Example: If originalFilePath is
	    // /data/CCRSB2/pipelineData/612161e212/Bams/SB_4431Met_Frag13_FrTu_December_15_2020_exome_recal.bam
	    // then the MatchedNormal will be SB_4431N_PBL_November_30_2020_exome (This is based on excel column, "MatchedNormal")
	    if (isSingleCell()) {
			return getAttrWithKey(getRunId(object), object.getOrginalFileName(), "MatchedNormal");
		}
		return getAttrValueWithParitallyMatchingKey(threadLocalMap.get(), object, "MatchedNormal");
  }
  
  private String getMatchedRnaseq(StatusInfo object) throws DmeSyncMappingException {
	    // Example: If originalFilePath is
	    // /data/CCRSB2/pipelineData/612161e212/Bams/SB_4431Met_Frag13_FrTu_December_15_2020_exome_recal.bam
	    // then the MatchedRnaseq will be SB_4431Met_Frag13_FrTu_January_26_2021_rnaseq (This is based on excel column, "MatchedRNASeq")
	    if (isSingleCell()) {
			return getAttrWithKey(getRunId(object), object.getOrginalFileName(), "MatchedRNASeq");
		}
		return getAttrValueWithParitallyMatchingKey(threadLocalMap.get(), object, "MatchedRNASeq");
  }
  
  private String getSequencedMaterial(StatusInfo object) throws DmeSyncMappingException {
	    // Example: If originalFilePath is
	    // /data/CCRSB2/pipelineData/612161e212/Bams/SB_4431Met_Frag13_FrTu_December_15_2020_exome_recal.bam
	    // then the SequencedMaterial will be FrTu (This is based on excel column, "SequencedMaterial")
	    if (isSingleCell()) {
			return getAttrWithKey(getRunId(object), object.getOrginalFileName(), "SequencedMaterial");
		}
		return getAttrValueWithParitallyMatchingKey(threadLocalMap.get(), object, "SequencedMaterial");
  }
  
  private String getLibraryType(StatusInfo object) throws DmeSyncMappingException {
	    // Example: If originalFilePath is
	    // /data/CCRSB2/pipelineData/612161e212/Bams/SB_4431Met_Frag13_FrTu_December_15_2020_exome_recal.bam
	    // then the LibraryType will be TD (This is based on excel column, "LibraryType")
	  	if (isSingleCell()) {
	    	return getAttrWithKey(getRunId(object), object.getOrginalFileName(), "LibraryType");
	    }
		return getAttrValueWithParitallyMatchingKey(threadLocalMap.get(), object, "LibraryType");
  }
 
  private String getIsPrimary(StatusInfo object) throws DmeSyncMappingException {
	    // Example: If originalFilePath is
	    // /data/CCRSB2/pipelineData/612161e212/Bams/SB_4431Met_Frag13_FrTu_December_15_2020_exome_recal.bam
	    // then the IsPrimary will be N (This is based on excel column, "IsPrimary")
	    if (isSingleCell()) {
	    	return getAttrWithKey(getRunId(object), object.getOrginalFileName(), "IsPrimary");
	    }
		return getAttrValueWithParitallyMatchingKey(threadLocalMap.get(), object, "IsPrimary");
  }
  
  private String getSequencer(StatusInfo object) throws DmeSyncMappingException {
    // Example: If originalFilePath is
    // /data/CCRSB2/pipelineData/612161e212/Bams/SB_4431Met_Frag13_FrTu_December_15_2020_exome_recal.bam
    // then the Sequencer will be NextSeq550A (This is based on excel column, "Sequencer")
	if (isSingleCell()) {
		return getAttrWithKey(getRunId(object), object.getOrginalFileName(), "Sequencer");
	}
    return getAttrValueWithParitallyMatchingKey(threadLocalMap.get(), object, "Sequencer");
  }

  private String getKitUsed(StatusInfo object) throws DmeSyncMappingException {
    // Example: If originalFilePath is
    // /data/CCRSB2/pipelineData/612161e212/Bams/SB_4431Met_Frag13_FrTu_December_15_2020_exome_recal.bam
    // then the kit_used will be SureSelect_RNA_XT_HS2 (This is based on excel column, "LibraryEnrichment")
    if (isSingleCell()) {
		return getAttrWithKey(getRunId(object), object.getOrginalFileName(), "LibraryEnrichment");
	}
    return getAttrValueWithParitallyMatchingKey(threadLocalMap.get(), object, "LibraryEnrichment");
  }


  private String getSampleInfo(StatusInfo object) throws DmeSyncMappingException {
    // Example: If objectName is
    // SB_4431Met_Frag1_FrTu_November_30_2020_exome_recal.bam
    // then the SampleInfo will be Metastasis from "Met" (Metastasis "Met", Primary "Pri", Other)
    // PBL will be Other.
	if (isSingleCell()) {
		return "";
	}
	String objectName = Paths.get(object.getOriginalFilePath()).toFile().getName();
    String sampleInfo = null;
    if (StringUtils.contains(objectName, "Met")) sampleInfo = "Metastasis";
    else if (getIsPrimary(object).equalsIgnoreCase("Y")) sampleInfo = "Primary";
    else if (getTissueType(object).equalsIgnoreCase("Normal")) sampleInfo = "Normal";
    else sampleInfo = "Other";
    return sampleInfo;
  }

  
  private String getResectionDate(StatusInfo object) throws DmeSyncMappingException {
    // Example: If originalFilePath is
    // /data/CCRSB2/pipelineData/612161e212/Bams/SB_4431Met_Frag13_FrTu_December_15_2020_exome_recal.bam
    // then the resection_date will be November_12_2020 (This is based on excel column, "Resectiondate")
	if (isSingleCell()) {
		return getAttrWithKey(getRunId(object), object.getOrginalFileName(), "Resectiondate") == null ? "Unknown"
				: getAttrWithKey(getRunId(object), object.getOrginalFileName(), "Resectiondate");
	}
    return getAttrValueWithParitallyMatchingKey(threadLocalMap.get(), object, "Resectiondate");
  }
 
  private String getSampleId(StatusInfo object) throws DmeSyncMappingException {
	  String fileName = Paths.get(object.getOriginalFilePath()).toFile().getName();
	  //Extract filename ending with _exome or _rnaseq from the path
	  if (isSingleCell()) {
	    return getAttrWithKey(getRunId(object), object.getOrginalFileName(), "SampleID");
	  }
      String sampleId = StringUtils.substringBefore(fileName, "_exome") + (fileName.contains("_exome") ? "_exome" : "");
      sampleId = StringUtils.substringBefore(sampleId, "_rnaseq") + (fileName.contains("_rnaseq") ? "_rnaseq" : "");
      sampleId = StringUtils.substringBefore(sampleId, "_1.");
      sampleId = StringUtils.substringBefore(sampleId, "_2.");
      sampleId = StringUtils.substringBefore(sampleId, ".");
      sampleId = StringUtils.substringBefore(sampleId, "_recal");
      return sampleId;
  }
  
  private String getAttrValueWithParitallyMatchingKey(Map<String, Map<String, String>> map, StatusInfo object, String attrKey)
      throws DmeSyncMappingException {
    String key = null;
    String fullKey = Paths.get(object.getOriginalFilePath()).toFile().getName();
    if(object.getOriginalFilePath().contains("Bams") || object.getOriginalFilePath().contains("fastq_files")) {
      //Extract filename ending with _exome or _rnaseq from the path
      String partialName = getSampleId(object);
      for (Map.Entry<String, Map<String, String>> entry : map.entrySet()) {
        if(StringUtils.contains(entry.getKey(), partialName)) {
          //Sample match.
          key = entry.getKey();
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
  
  private String getAttrWithKey(String key1, String key2, String attrKey) {
		if(StringUtils.isEmpty(key1) || StringUtils.isEmpty(key2)) {
	      logger.error("Excel mapping not found for {}", key1 + key2);
	      return null;
	    }
	    return (metadataMap.get(key1 + "_" + key2) == null? null : metadataMap.get(key1 + "_" + key2).get(attrKey));
  }
  
  private boolean isSingleCell() {
    return baseDir.contains("10X_Fastqs");
  }

  private String getModifiedDate(StatusInfo object) throws IOException {
      //Return the modified_date of the file/folder
      File file = new File(object.getOriginalFilePath());
      
      Path path = file.toPath();
      BasicFileAttributes fileAttribute = Files.readAttributes(path,
              BasicFileAttributes.class);
      
      long lastChanged = fileAttribute.lastModifiedTime().toMillis();
      DateFormat sdf = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss"); 
      String lastUpdated = sdf.format(new Date(lastChanged));
      logger.info("file {} was last updated at {}",
          file.getName(), lastUpdated);
	  return lastUpdated;
  }
  
  private String getCreationDate(StatusInfo object) throws IOException {
	//Return the creation_date of the file/folder
      File file = new File(object.getOriginalFilePath());
      
      Path path = file.toPath();
      BasicFileAttributes fileAttribute = Files.readAttributes(path,
              BasicFileAttributes.class);
      
      long created = fileAttribute.creationTime().toMillis();
      DateFormat sdf = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss"); 
      String createdDate = sdf.format(new Date(created));
      logger.info("file {} was created at {}",
          file.getName(), createdDate);
	  return createdDate;
  }
  
  @PostConstruct
  private void init() throws DmeSyncMappingException {
	if("sb".equalsIgnoreCase(doc) && isSingleCell()) {
    	Path excelFilePath = Paths.get(workDir, "10XMetaForUpload.xls");
    	try {
    		ExcelUtil.convertTextToExcel(new File(metadataFile), new File(excelFilePath.toString()), "\t");
    	} catch (IOException e) {
    		throw new DmeSyncMappingException("Can't convert 10XMetaForUpload.txt file to excel", e);
    	}
		metadataMap = ExcelUtil.parseBulkMetadataEntries(excelFilePath.toString(), "RunID", "Fastq");
	}
  }
}
