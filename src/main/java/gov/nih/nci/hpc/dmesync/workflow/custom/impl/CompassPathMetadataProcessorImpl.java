package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

/**
 * Compass DME Path and Meta-data Processor Implementation
 *
 * @author dinhys
 */
@Service("compass")
public class CompassPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
    implements DmeSyncPathMetadataProcessor {

  // Compass Custom logic for DME path construction and meta data creation

  @Value("${dmesync.additional.metadata.excel:}")
  private String metadataFile;
  
  @Override
  public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

    logger.info("[PathMetadataTask] Compass getArchivePath called");


    // Example source path -
    // /data/Compass/DATA/NextSeq/FastqFolder/NA18487_100ng_N1D_PS2
    String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
    String archivePath = null;

    // Get the PI collection value from the CollectionNameMetadata
    // Example row - mapKey - Compass, collectionType - PI_Lab, mapValue - PI_XXX
    // Extract the Project value from the Path
    String projectCollectionName = getProjectCollectionName(object);
    
    if (projectCollectionName.equals("TSO500v2") && isProcessedResults(object)) {
      archivePath =
          destinationBaseDir
              + "/PI_"
              + getPiCollectionName()
              + "/Project_"
              + projectCollectionName
              + "/Sample_"
              + getSampleId(object) + "/"
              + getCollectionNameFromParent(object, "TSO500_Results") + "/"
              + fileName;
    } else if ((projectCollectionName.equals("TSO500v2") || projectCollectionName.equals("ExomeRNA")) && !isProcessedResults(object)) {
      archivePath =
          destinationBaseDir
              + "/PI_"
              + getPiCollectionName()
              + "/Project_"
              + projectCollectionName
              + "/Sample_"
              + getSampleId(object)
              + "/Sequence_Data/"
              + fileName;
    } else if (projectCollectionName.equals("ExomeRNA")) {
    	archivePath =
          destinationBaseDir
              + "/PI_"
              + getPiCollectionName()
              + "/Project_"
              + projectCollectionName
              + "/Sample_"
              + getSampleId(object)
              + "/" + getExperimentId(object) + "/"
              + fileName;
    } else if (projectCollectionName.equals("Methylation") && !isProcessedResults(object)) {
      archivePath =
            destinationBaseDir
                + "/PI_"
                + getPiCollectionName()
                + "/Project_"
                + projectCollectionName
                + "/Sentrix_"
                + getMethylationSentrixId(object)
                + "/Raw_Data/"
                + fileName;
    }  else if (projectCollectionName.equals("Methylation")) {
      archivePath =
            destinationBaseDir
                + "/PI_"
                + getPiCollectionName()
                + "/Project_"
                + projectCollectionName
                + "/Sentrix_"
                + getMethylationSentrixId(object)
                + "/Sample_"
                + getSampleId(object)
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

    // Add path metadata entries for "PI_XXX" collection
    // Example row: collectionType - PI, collectionName - XXX (derived)
    // key = pi_name, value = Compass PI name (supplied)
    // key = affiliation, value = Compass PI_Lab affiliation (supplied)

    String piCollectionName = getPiCollectionName();
    String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;
    HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
    pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "PI_Lab"));
    pathEntriesPI.setPath(piCollectionPath);
    hpcBulkMetadataEntries
        .getPathsMetadataEntries()
        .add(populateStoredMetadataEntries(pathEntriesPI, "PI_Lab", piCollectionName));

    // Add path metadata entries for "Project_XXX" collection
    // Example row: collectionType - Project, collectionName - Compass
    // (derived),
    // key = origin, value = Surgery_Branch_NGS (supplied)
    // key = project_title, value = Surgery Branch Patient Sequencing (supplied)
    // key = project_description (supplied)
    // key = method (supplied)
    // key = start_date (supplied)
    // key = access, value = "Closed Access" (constant)
    // key = summary_of_samples
    // key = source_organism (supplied)

    String projectCollectionName = getProjectCollectionName(object);
    String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;
    HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
    pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
    pathEntriesProject.setPath(projectCollectionPath);
    hpcBulkMetadataEntries
        .getPathsMetadataEntries()
        .add(populateStoredMetadataEntries(pathEntriesProject, "Project", projectCollectionName));

    // Add path metadata entries for "Sample" collection
    // Example row: collectionType - Sample, collectionName - Sample_<sample id> (derived)
    // Example done file name: 190627_NDX550200_0010_AHFL2KBGXB_DNA
    // key = sequencing_application_type (supplied)
    // key = sample_id, value = NA18487_100ng_N1D_PS2 (derived)
    // key = sample_type, value = DNA or RNA (derived from the .done file)
    // key = flowcell_id, value = AHFL2KBGXB (derived)
    // key = run_date, 190627 (derived)
    // key = library_name, value = NA18487_100ng_N1D_PS2 (derived, same as sample id)
    String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
    HpcBulkMetadataEntry pathEntriesSample = new HpcBulkMetadataEntry();
    String sampleCollectionPath = null;
    pathEntriesSample.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Sample"));
    if(projectCollectionName.equals("TSO500v2") || projectCollectionName.equals("ExomeRNA")) {
    	String sampleId = getSampleId(object);
        sampleCollectionPath = projectCollectionPath + "/Sample_" + sampleId;
        pathEntriesSample.getPathMetadataEntries().add(createPathEntry("patient_id", sampleId));
    } else if(projectCollectionName.equals("Methylation")) {
    	String sentrixId = getMethylationSentrixId(object);
    	sampleCollectionPath = projectCollectionPath + "/Sentrix_" + sentrixId;
    	pathEntriesSample
	        .getPathMetadataEntries()
	        .add(createPathEntry("sentrix_id", sentrixId));
    }
    pathEntriesSample.setPath(sampleCollectionPath);
    hpcBulkMetadataEntries
        .getPathsMetadataEntries()
        .add(populateStoredMetadataEntries(pathEntriesSample, "Sample", "Compass"));

    // Add the case_id attribute for the ExomeRNA experiment folder
    if(projectCollectionName.equals("ExomeRNA") && isProcessedResults(object)) {
    	String experimentCollectionPath = null;
        HpcBulkMetadataEntry pathEntriesExperiment = new HpcBulkMetadataEntry();
        experimentCollectionPath = sampleCollectionPath + "/" + getExperimentId(object);
        pathEntriesExperiment.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Folder"));
    	pathEntriesExperiment.getPathMetadataEntries().add(createPathEntry("case_id", getExperimentId(object)));
    	pathEntriesExperiment.setPath(experimentCollectionPath);
        hpcBulkMetadataEntries
            .getPathsMetadataEntries()
            .add(pathEntriesExperiment);
    } else if(projectCollectionName.equals("Methylation") && isProcessedResults(object)) {
    	String methylationSampleCollectionPath = null;
    	String sampleId = getSampleId(object);
        HpcBulkMetadataEntry pathEntriesMethylationSample = new HpcBulkMetadataEntry();
        methylationSampleCollectionPath = sampleCollectionPath + "/Sample_" + sampleId;
        pathEntriesMethylationSample.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Folder"));
        pathEntriesMethylationSample.getPathMetadataEntries().add(createPathEntry("patient_id", sampleId));
        pathEntriesMethylationSample.getPathMetadataEntries().add(createPathEntry("library_name", sampleId));
        // load the user metadata from the externally placed excel
        if(StringUtils.isNotBlank(metadataFile)){
          String sentrixId = getMethylationSentrixId(object);
          String key = sentrixId + "_" + sampleId;
          threadLocalMap.set(loadMetadataFile(metadataFile, "Sentrix_ID", "Sample_Name"));
          String materialType = getAttrValueWithExactKey(key, "Material_Type") == null ? "Unspecified": getAttrValueWithExactKey(key, "Material_Type");
          pathEntriesMethylationSample.getPathMetadataEntries().add(createPathEntry("material_type", materialType));
          String gender = getAttrValueWithExactKey(key, "Gender") == null ? "Unspecified": getAttrValueWithExactKey(key, "Gender");
          pathEntriesMethylationSample.getPathMetadataEntries().add(createPathEntry("gender", gender));
          String surgicalCase = getAttrValueWithExactKey(key, "Surgical_Case") == null ? "Unspecified": getAttrValueWithExactKey(key, "Surgical_Case");
          pathEntriesMethylationSample.getPathMetadataEntries().add(createPathEntry("surgical_case", surgicalCase));
          String diagnosis = getAttrValueWithExactKey(key, "Diagnosis") == null ? "Unspecified": getAttrValueWithExactKey(key, "Diagnosis");
          pathEntriesMethylationSample.getPathMetadataEntries().add(createPathEntry("diagnosis", diagnosis));
          String piCollaborator = getAttrValueWithExactKey(key, "PI_Collaborator") == null ? "Unspecified": getAttrValueWithExactKey(key, "PI_Collaborator");
          pathEntriesMethylationSample.getPathMetadataEntries().add(createPathEntry("pi_collaborator", piCollaborator));
        }
        pathEntriesMethylationSample.setPath(methylationSampleCollectionPath);
        hpcBulkMetadataEntries
            .getPathsMetadataEntries()
            .add(pathEntriesMethylationSample);
    }
    
    // Set it to dataObjectRegistrationRequestDTO
    HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO =
        new HpcDataObjectRegistrationRequestDTO();
    dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
    dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(
        hpcBulkMetadataEntries);

    // Add object metadata
    dataObjectRegistrationRequestDTO
        .getMetadataEntries()
        .add(createPathEntry("object_name", fileName));
    dataObjectRegistrationRequestDTO
        .getMetadataEntries()
        .add(createPathEntry("source_path", object.getOriginalFilePath()));
    if(projectCollectionName.equals("ExomeRNA")) {
    	if(!isProcessedResults(object)) {
		    String doneFileName = getDoneFileName(object);
		    dataObjectRegistrationRequestDTO
			    .getMetadataEntries()
			    .add(createPathEntry("flowcell_id", getExomeRNAFlowcellId(object)));
		    dataObjectRegistrationRequestDTO
		        .getMetadataEntries()
		        .add(createPathEntry("run_date", getRunDate(doneFileName)));
    	}
		String libraryName = getExomeRNALibraryName(object);
		if(StringUtils.isNotBlank(libraryName)) {
			dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("library_name", libraryName));
			// load the user metadata from the externally placed excel
	        if(StringUtils.isNotBlank(metadataFile)){
	          threadLocalMap.set(loadMetadataFile(metadataFile, "Library ID"));
	          String flowcellId = getAttrValueWithExactKey(libraryName, "FCID") == null ? "Unspecified": getAttrValueWithExactKey(libraryName, "FCID");
	          dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("flowcell_id", flowcellId));
	          String runDate = getAttrValueWithExactKey(libraryName, "Sequencing Date") == null ? "Unspecified": getAttrValueWithExactKey(libraryName, "Sequencing Date");
	          dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("run_date", runDate));
	          String sampleType = getAttrValueWithExactKey(libraryName, "Sample Type") == null ? "Unspecified": getAttrValueWithExactKey(libraryName, "Sample Type");
	          dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("sample_type", sampleType));
	          String surgicalCase = getAttrValueWithExactKey(libraryName, "surgical specimen ID") == null ? "Unspecified": getAttrValueWithExactKey(libraryName, "surgical specimen ID");
	          dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("surgical_case", surgicalCase));
	          String dnaRnaId = getAttrValueWithExactKey(libraryName, "LP/Source #") == null ? "Unspecified": getAttrValueWithExactKey(libraryName, "LP/Source #");
	          dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("dna_rna_id", dnaRnaId));
	          String materialType = getAttrValueWithExactKey(libraryName, "Sample Source") == null ? "Unspecified": getAttrValueWithExactKey(libraryName, "Sample Source");
	          dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("material_type", materialType));
              String captureKit = getAttrValueWithExactKey(libraryName, "Test") == null ? "Unspecified": getAttrValueWithExactKey(libraryName, "Test");
              dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("capture_kit", captureKit));
              String diagnosis = getAttrValueWithExactKey(libraryName, "Diagnosis (Cancer Type)") == null ? "Unspecified": getAttrValueWithExactKey(libraryName, "Diagnosis (Cancer Type)");
              dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("diagnosis", diagnosis));
	          String piCollaborator = getAttrValueWithExactKey(libraryName, "PI_Collaborator") == null ? "Unspecified": getAttrValueWithExactKey(libraryName, "PI_Collaborator");
	          pathEntriesSample.getPathMetadataEntries().add(createPathEntry("pi_collaborator", piCollaborator));
	        }
		}
	} else if(projectCollectionName.equals("TSO500v2")) {
		String libraryName = getTSO500LibraryName(object);
		
        // load the user metadata from the externally placed excel
        if(StringUtils.isNotBlank(metadataFile)){
          threadLocalMap.set(loadMetadataFile(metadataFile, "Accession ID_CP #"));
          if(!threadLocalMap.get().containsKey(libraryName)) {
        	  throw new DmeSyncMappingException("Library name " + libraryName + " is not available in metafile.");
          }
          dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("library_name", libraryName));
          String flowcellId = getAttrValueWithExactKey(libraryName, "FCID") == null ? "Unspecified": getAttrValueWithExactKey(libraryName, "FCID");
          dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("flowcell_id", flowcellId));
          String runDate = getAttrValueWithExactKey(libraryName, "Sequencing Date") == null ? "Unspecified": getAttrValueWithExactKey(libraryName, "Sequencing Date");
          dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("run_date", runDate));

          String sampleType = getAttrValueWithExactKey(libraryName, "Sample Type") == null ? "Unspecified": getAttrValueWithExactKey(libraryName, "Sample Type");
          dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("sample_type", sampleType));
          String surgicalCase = getAttrValueWithExactKey(libraryName, "Specimen ID (surgical pathology case#_block ID)") == null ? "Unspecified": getAttrValueWithExactKey(libraryName, "Specimen ID (surgical pathology case#_block ID)");
          dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("surgical_case", surgicalCase));
          String dnaRnaId = getAttrValueWithExactKey(libraryName, "Accesson ID_(DNA#)") == null ? "Unspecified": getAttrValueWithExactKey(libraryName, "Accesson ID_(DNA#)");
          dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("dna_rna_id", dnaRnaId));
          String diagnosis = getAttrValueWithExactKey(libraryName, "Diagnosis (Cancer Type)") == null ? "Unspecified": getAttrValueWithExactKey(libraryName, "Diagnosis (Cancer Type)");
          dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("diagnosis", diagnosis));
          String piCollaborator = getAttrValueWithExactKey(libraryName, "Pi_Collaborator") == null ? "Unspecified": getAttrValueWithExactKey(libraryName, "Pi_Collaborator");
          pathEntriesSample.getPathMetadataEntries().add(createPathEntry("pi_collaborator", piCollaborator));
        }
	}
    logger.info(
        "Compass custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}",
        object.getId());
    return dataObjectRegistrationRequestDTO;
  }

  private String getCollectionNameFromParent(StatusInfo object, String parentName) {
    Path fullFilePath = Paths.get(object.getOriginalFilePath());
    String parent = null;
    logger.info("Full File Path = {}", fullFilePath);
    int count = fullFilePath.getNameCount();
    for (int i = 1; i < count; i++) {
      if (fullFilePath.getParent().getFileName().toString().equals(parentName)) {
    	  parent = fullFilePath.getFileName().toString();
      }
      fullFilePath = fullFilePath.getParent();
    }
    return parent;
  }
  
  private Path getCollectionPathFromParent(StatusInfo object, String parentName) {
    Path fullFilePath = Paths.get(object.getOriginalFilePath());
    logger.info("Full File Path = {}", fullFilePath);
    int count = fullFilePath.getNameCount();
    for (int i = 0; i <= count; i++) {
      if (fullFilePath.getParent().getFileName().toString().equals(parentName)) {
        return fullFilePath;
      }
      fullFilePath = fullFilePath.getParent();
    }
    return null;
  }

  private String getPiCollectionName() throws DmeSyncMappingException {
    return getCollectionMappingValue("Compass", "PI_Lab");
  }

  private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {
	// Get project collection name based on the path.
	// Project_Methylation:
	// /data/Compass/Methylation/ClassifierReports
	// /data/Compass/iScan_raw
	// Project_TSO500:
	// /data/Compass/Analysis/ProcessedResults_NexSeq/TSO500_Results
	// /data/Compass/DATA/NextSeq/FastqFolder/**/_R1_001.fastq.gz 
	// Project_ExomeRNA:
	// /data/Compass/Analysis/ProcessedResults_NexSeq/ExomeRNA_Results
	// /data/Compass/DATA/NextSeq/FastqFolder/**/_R1.fastq.gz
	String subfolder = getCollectionNameFromParent(object, "Compass");
	if(StringUtils.equals("Methylation", subfolder) || StringUtils.equals("iScan_raw", subfolder)) {
	  return "Methylation";
	} else if (StringUtils.equals("Analysis", subfolder)){
	  String resultFolder = getCollectionNameFromParent(object, "ProcessedResults_NexSeq");
	  if(StringUtils.equals("TSO500_Results", resultFolder)) {
	    return "TSO500v2";
	  } else {
	    return "ExomeRNA";
	  }
	} else if (StringUtils.equals("DATA", subfolder)){
		String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
		if(StringUtils.endsWith(fileName, "_001.fastq.gz") || StringUtils.endsWith(fileName, "_DNA.done") || StringUtils.endsWith(fileName, "_RNA.done"))
			return "TSO500v2";
		else 
			return "ExomeRNA";
	}
    return getCollectionMappingValue("Compass", "Project");
  }

  private String getSampleId(StatusInfo object) throws DmeSyncMappingException {
    String sampleId = null;
    // Example: If originalFilePath is
    // /data/Compass/DATA/NextSeq/FastqFolder/NA18487_100ng_N1D_PS2
    // /data/Compass/Analysis/ProcessedResults_NexSeq/NA18487_100ng_N1D_PS2/Logs_Intermediates/SmallVariantFilter
    // then the SampleId will be NA18487_100ng_N1D_PS2
    if ("TSO500v2".equals(getProjectCollectionName(object)) && isProcessedResults(object))
      sampleId = getPatientId(getCollectionNameFromParent(object, "TSO500_Results"));
    else if ("TSO500v2".equals(getProjectCollectionName(object)) && !isProcessedResults(object))
        sampleId = getPatientId(getCollectionNameFromParent(object, "FastqFolder"));
    else if ("ExomeRNA".equals(getProjectCollectionName(object)) && isProcessedResults(object))
      sampleId = getCollectionNameFromParent(object, "ExomeRNA_Results");
    else if ("ExomeRNA".equals(getProjectCollectionName(object)) && !isProcessedResults(object)) {
    	sampleId = getCollectionNameFromParent(object, "FastqFolder");
    	sampleId = StringUtils.substring(sampleId, 0, sampleId.indexOf('_'));
    }
    else if (isProcessedResults(object)) {
    	sampleId = getCollectionNameFromParent(object, "ClassifierReports");
    	sampleId = StringUtils.substring(sampleId, 0, sampleId.indexOf('_'));
    }
    logger.info("SampleId: {}", sampleId);
    return sampleId;
  }
  
  private String getExperimentId(StatusInfo object) {
    String experimentId = null;
    // Example: If originalFilePath is
    // /data/Compass/Analysis/ProcessedResults_NexSeq/ExomeRNA_Results/NA12878/Mix50
    // then the experimentId will be Mix50
    experimentId = getCollectionNameFromParent(object, getCollectionNameFromParent(object, "ExomeRNA_Results"));
    logger.info("experimentId: {}", experimentId);
    return experimentId;
  }
  
  private boolean isProcessedResults(StatusInfo object) {
    return object.getOriginalFilePath().contains("ProcessedResults_NexSeq") || object.getOriginalFilePath().contains("ClassifierReports") ;
  }

  public String getDoneFileName(StatusInfo object) throws DmeSyncMappingException {
    // Example: If originalFilePath is
    // /data/Compass/DATA/NextSeq/FastqFolder/NA18487_100ng_N1D_PS2
    // /data/Compass/Analysis/ProcessedResults_NexSeq/NA18487_100ng_N1D_PS2/Logs_Intermediates/SmallVariantFilter
    // then the done file resides under NA18487_100ng_N1D_PS2
    String doneFileName = null;
    Path dirPath = null;
    String doneFileExt = null;
    if(isProcessedResults(object)) {
      dirPath = getCollectionPathFromParent(object, "TSO500_Results");
      if(dirPath.getFileName().toString().endsWith("Pair"))
        doneFileExt = "Pair.done";
      else
    	doneFileExt = "NoPair.done";
    } else {
      dirPath = getCollectionPathFromParent(object, "FastqFolder");
      doneFileExt = ".done";
    }
    final String doneFileEndsWith = doneFileExt;
    try (DirectoryStream<Path> stream =
        Files.newDirectoryStream(
            dirPath, path -> path.toString().endsWith(doneFileEndsWith))) {

      Iterator<Path> pathItr = stream.iterator();
      if (pathItr.hasNext()) {
        doneFileName = pathItr.next().getFileName().toString();
      }
    } catch (IOException ex) {
      throw new DmeSyncMappingException(
          "Error while listing directory to get the done file for path: "
              + object.getOriginalFilePath(),
          ex);
    }
    if (doneFileName == null)
      throw new DmeSyncMappingException(
          ".done file is not found for path: " + object.getOriginalFilePath());
    return doneFileName;
  }

  private String getTSO500FlowcellId(String fileName) {
    // Example: If done file name is 190627_NDX550200_0010_AHFL2KBGXB_DNA
    // then the FlowCellId will be AHFL2KBGXB
    fileName = StringUtils.replace(fileName, "rsync.", "");
    fileName = StringUtils.replace(fileName, "Illumina_", "");
    return StringUtils.substring(
        fileName,
        StringUtils.ordinalIndexOf(fileName, "_", 3) + 1,
        StringUtils.ordinalIndexOf(fileName, "_", 4));
  }
  
  private String getExomeRNAFlowcellId(StatusInfo object) {
    // Example: If sample is NA18487_N1D_E5_H7F5YBGXC
    // then the FlowCellId will be H7F5YBGXC
	String sampleId = getCollectionNameFromParent(object, "FastqFolder");
    return StringUtils.substringAfterLast(sampleId, "_");
  }
  
  private String getMethylationSentrixId(StatusInfo object) {
    String sentrixId = null;
    // Example: If originalFilePath is
    // /data/Compass/Methylation/ClassifierReports/T213_202816900150_R02C01
    // then the SentrixId will be 202816900150
    if (isProcessedResults(object)) {
    	sentrixId = getCollectionNameFromParent(object, "ClassifierReports");
    	sentrixId = StringUtils.substringBeforeLast(StringUtils.substring(sentrixId, sentrixId.indexOf('_') + 1), "_");
    } else {
    	sentrixId = getCollectionNameFromParent(object, "iScan_raw");
    }
    logger.info("SentrixId: {}", sentrixId);
    return sentrixId;
  }
  
  private String getRunDate(String fileName) {
    // Example: If done file name is 190627_NDX550200_0010_AHFL2KBGXB_DNA
    // then the RunDate will be 190627
    fileName = StringUtils.replace(fileName, "rsync.", "");
    fileName = StringUtils.replace(fileName, "Illumina_", "");
    return StringUtils.substring(fileName, 0, StringUtils.indexOf(fileName, "_"));
  }
  
  private String getExomeRNALibraryName(StatusInfo object) {
    String libraryName = null;
    // Example: If originalFilePath is
    // /data/Compass/DATA/NextSeq/FastqFolder/NA18487_N1D_E6_H7F7TBGXC
    // /data/Compass/Analysis/ProcessedResults_NexSeq\ExomeRNA_Results\NA12878\Mix50\NA18740_N1D_E
    // then the library id will be NA18487_N1D_E6
    if (isProcessedResults(object)) {
    	String regex = "_[TN][1-9I][DR]";
        Pattern pattern = Pattern.compile(regex);
        Path fullFilePath = Paths.get(object.getOriginalFilePath());
        int count = fullFilePath.getNameCount();
        for (int i = 1; i < count; i++) {
          Matcher matcher = pattern.matcher(fullFilePath.getParent().getFileName().toString());
          if (matcher.find()) {
        	  libraryName = fullFilePath.getParent().getFileName().toString();
        	  break;
          }
          fullFilePath = fullFilePath.getParent();
        }
    }
    else {
    	libraryName = getCollectionNameFromParent(object, "FastqFolder");
    	libraryName = StringUtils.substring(libraryName, 0, libraryName.lastIndexOf('_'));
    }
    logger.info("LibraryName: {}", libraryName);
    return libraryName;
  }
  
  private String getTSO500LibraryName(StatusInfo object) {
    String libraryName = null;
    // Example: If originalFilePath is
    // /data/Compass/DATA/NextSeq/FastqFolder/NA18487_N1D_E6_H7F7TBGXC
    // /data/Compass/Analysis/ProcessedResults_NexSeq\ExomeRNA_Results\NA12878\Mix50\NA18740_N1D_E
    // then the library id will be NA18487_N1D_E6
    if (isProcessedResults(object)) {
    	String regex = "_[TN][1-9I][DR]";
        Pattern pattern = Pattern.compile(regex);
        Path fullFilePath = Paths.get(object.getOriginalFilePath());
        int count = fullFilePath.getNameCount();
        for (int i = 1; i < count; i++) {
          Matcher matcher = pattern.matcher(fullFilePath.getParent().getFileName().toString());
          if (matcher.find()) {
        	  libraryName = fullFilePath.getParent().getFileName().toString();
        	  break;
          }
          fullFilePath = fullFilePath.getParent();
        }
        if(libraryName != null && libraryName.chars().filter(num -> num == '_').count() > 2)
        	libraryName = StringUtils.substring(libraryName, 0, libraryName.lastIndexOf('_'));
    }
    else {
    	libraryName = getCollectionNameFromParent(object, "FastqFolder");
    }
    logger.info("LibraryName: {}", libraryName);
    return libraryName;
  }
  
  private String getPatientId(String libraryName) {
    // Example: If library name is NA18487_N1D_E,
    // then the patient id will be NA18487, look for a string before the pattern, _T?R or _T?D or _N?D
	String regex = "_[TN][1-9][DR]";
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(libraryName);
    matcher.find();
    return StringUtils.substring(libraryName, 0, matcher.start());
  }
  
  private String getExomeRNASampleType(String libraryName) {
    // Example: If library name is NA18487_N1D_E
    // then D is for DNA, R is for RNA
	String regex = "_[TN][1-9I][DR]";
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(libraryName);
    matcher.find();
    if(libraryName.charAt(matcher.start() + 3) == 'D') {
		return "DNA";
    }
    return "RNA";
  }
}
