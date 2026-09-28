package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.domain.DocConfig.PreprocessingConfig;
import gov.nih.nci.hpc.dmesync.domain.DocConfig.SourceConfig;
import gov.nih.nci.hpc.dmesync.domain.DocConfig.SourceRule;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncPathMetadataProcessor;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntries;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;
import gov.nih.nci.hpc.dmesync.util.DmeMetadataBuilder;

/**
 * MoCha DME Path and Meta-data Processor Implementation
 *
 * @author dinhys
 */
@Service("mocha")
public class MochaPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
    implements DmeSyncPathMetadataProcessor {

  // Mocha DME path construction and meta data creation
  
  @Autowired
  private DmeMetadataBuilder dmeMetadataBuilder;
  
  Map<String, Map<String, String>> metadataMap = null;
  
  @Override
  public String getArchivePath(StatusInfo object, DocConfig config) throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {

	SourceConfig sourceConfig = config.getSourceConfig();
	SourceRule sourceRule = config.getSourceRule();
    logger.info("[PathMetadataTask] Mocha getArchivePath called");
    
    // load the user metadata from the externally placed excel
    metadataMapWithTwoKeys = dmeMetadataBuilder.getMetadataMapWithTwoKeys(sourceRule.metadataFile, "Run_ID", "Sample");
    
    // Example source path -
    // /mnt/mocha_static/NovaSeq/220113_A00424_0160_BHKJNWDSX2/Data/Intensities/BaseCalls/L001
    // /mnt/mocha_scratch/BW_transfers/2022_January/220113_A01063_0058_BHKJMGDSX2/Sample_RES210195_HKJMGDSX2/Sample_RES210195_HKJMGDSX2_R1.fastq.gz
    String fileName = Paths.get(object.getOrginalFileName()).toFile().getName();
    String archivePath = null;
    
    if(isBCL(object, config)) {
    	archivePath =
    		sourceConfig.destinationBaseDir
	            + "/Lab_"
	            + getPiCollectionName(object, config)
	            + "/Platform_"
	            + getPlatformCollectionName(object, config)
	            + "/Run_FC"
	            + "/Flowcell_"
	            + getFlowcellId(object, config)
	            + "/"
	            + (fileName.equals("Reports")||fileName.equals("Stats") ? "FASTQ_Report/" + fileName : getRunId(object)) + ".tar";
    } else {
        String platform = getPlatformCollectionName(object, config);
        String project = getProjectCollectionName(object);
        if(StringUtils.isBlank(project)) {
		    archivePath =
		    		sourceConfig.destinationBaseDir
			            + "/Lab_"
			            + getPiCollectionName(object, config)
			            + "/Platform_"
			            + platform
			            + "/DME_Unassigned_FASTQ"
			            + "/"
			            + getSeqDateFlowcellCollectionName(object)
			            + "/"
			            + fileName;
        } else {
		    archivePath =
		    	sourceConfig.destinationBaseDir
		            + "/Lab_"
		            + getPiCollectionName(object, config)
		            + "/Platform_"
		            + platform
		            + "/Project_"
		            + project
		            + "/"
		            + getSampleCollectionName(object, config)
		            + "/"
		            + fileName;
        }
    }

    // replace spaces with underscore
    archivePath = archivePath.replace(" ", "_");

    logger.info("Archive path for {} : {}", object.getOriginalFilePath(), archivePath);
    
    return archivePath;
  }

  @Override
  public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object, DocConfig config)
      throws DmeSyncMappingException, DmeSyncWorkflowException {

	 SourceConfig sourceConfig = config.getSourceConfig();
	  
    // Add to HpcBulkMetadataEntries for path attributes
    HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();
    String fileName = Paths.get(object.getOrginalFileName()).toFile().getName();
    if (fileName.equals("Reports")||fileName.equals("Stats")) {
    	fileName = Paths.get(object.getOrginalFileName()).toFile().getName();
    } else if(isBCL(object, config))
    	fileName = getRunId(object) + ".tar";
    else
    	fileName = Paths.get(object.getSourceFileName()).toFile().getName();
    
    // Add path metadata entries for "PI_XXX" collection
    // Example row: collectionType - PI, collectionName - XXX (derived)
    // key = data_owner, value = Mickey Williams (supplied)
    // key = data_owner_affiliation, value = Molecular Characterization Laboratory, FNLCR (supplied)
    String piCollectionName = getPiCollectionName(object, config);
    String piCollectionPath = sourceConfig.destinationBaseDir + "/Lab_" + piCollectionName;
    HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
    pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "PI_Lab"));
    pathEntriesPI.setPath(piCollectionPath);
    hpcBulkMetadataEntries
        .getPathsMetadataEntries()
        .add(populateStoredMetadataEntries(pathEntriesPI, "PI_Lab", piCollectionName, "mocha"));

    // Add path metadata entries for "Platform" collection
    // Example row: collectionType - Platform, collectionName - HiSeq, NovaSeq
    // platform_name
    String platformCollectionName = getPlatformCollectionName(object, config);
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
    
    if(!isBCL(object, config)) {
		String projectCollectionName = getProjectCollectionName(object);
        if(StringUtils.isBlank(projectCollectionName)) {
        	//DME_Unassigned_FASTQ
    	    String unassignedCollectionPath = platformCollectionPath + "/DME_Unassigned_FASTQ";
     	    HpcBulkMetadataEntry pathEntriesUnassigned = new HpcBulkMetadataEntry();
     	    pathEntriesUnassigned.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Run_FC"));     
     	    pathEntriesUnassigned.setPath(unassignedCollectionPath);
     	    hpcBulkMetadataEntries
     	        .getPathsMetadataEntries()
     	        .add(pathEntriesUnassigned);
     	    
    	    // Add path metadata entries for "Flowcell" collection
    	    // Example row: collectionType - Flowcell
    	    // flowcell_id
     	    String seqDateFlowcell = getSeqDateFlowcellCollectionName(object);
        	String flowcellId = StringUtils.substringAfter(seqDateFlowcell, "_");
    	    String flowcellCollectionPath = unassignedCollectionPath + "/" + seqDateFlowcell;
    	    HpcBulkMetadataEntry pathEntriesFlowcell = new HpcBulkMetadataEntry();
    	    pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Flowcell"));
    	    pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("flowcell_id", flowcellId));
    	    pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("run_id", getRunId(object)));
    	    pathEntriesFlowcell.setPath(flowcellCollectionPath);
    	    hpcBulkMetadataEntries
    	        .getPathsMetadataEntries()
    	        .add(pathEntriesFlowcell);
        } else {
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
        	String runId = getRunId(object);
        	String flowcellId = getFlowcellId(object, config);
    	    String sampleId = getSampleId(object);
			String projectCollectionPath = platformCollectionPath + "/Project_" + projectCollectionName.replace(" ", "_");
			HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
			HpcBulkMetadataEntry hpcBulkMetadataProjectEntries = populateStoredMetadataEntries(pathEntriesProject,
					"Project", projectCollectionName, "mocha");

			if (hpcBulkMetadataProjectEntries == null || hpcBulkMetadataProjectEntries.getPathMetadataEntries() == null
					|| hpcBulkMetadataProjectEntries.getPathMetadataEntries().isEmpty()) {
				// It is null or empty means no mapping for project in database
				String msg = "No metadata entries were found for Collection Type " + "Project"
						+ " with Project Mapping Key: " + projectCollectionName;
				logger.error(msg);
				throw new DmeSyncMappingException(msg);
			}
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_id", projectCollectionName));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_status", "Active"));
			pathEntriesProject.setPath(projectCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(hpcBulkMetadataProjectEntries);
			
		    // Add path metadata entries for "Sample" collection
		    // Example row: collectionType - Sample, collectionName - Sample_<SampleId>
		    // sample_id, value = PDA01236 (derived)
		    // sample_name, value = PDA01236 (derived)
		    // flowcell_lane = Lane
		    
		    String sampleCollectionPath = projectCollectionPath + "/" + getSampleCollectionName(object, config);
		    sampleCollectionPath = sampleCollectionPath.replace(" ", "_");
		    HpcBulkMetadataEntry pathEntriesSample = new HpcBulkMetadataEntry();
		    pathEntriesSample.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Sample_Flowcell"));
		    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("flowcell_id", flowcellId));
		    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("run_id", getRunId(object))); 
		    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sample_id", sampleId));  
		    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sample_name", getAttrValueFromMetadataMap(runId, sampleId, "Mocha_ID")));
		    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("library_strategy", getAttrValueFromMetadataMap(runId, sampleId, "Library_Type")));
		    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("analyte_type", getAttrValueFromMetadataMap(runId, sampleId, "Analyte")));
		    pathEntriesSample.getPathMetadataEntries().add(createPathEntry("flowcell_lane", StringUtils.isBlank(getAttrValueFromMetadataMap(runId, sampleId, "Lane")) ? "std_mode" : getAttrValueFromMetadataMap(runId, sampleId, "Lane")));
		    if(StringUtils.isNotBlank(getAttrValueFromMetadataMap(runId, sampleId, "SubProject")))
		    	pathEntriesSample.getPathMetadataEntries().add(createPathEntry("subproject", getAttrValueFromMetadataMap(runId, sampleId, "SubProject")));  
		    pathEntriesSample.setPath(sampleCollectionPath);
		    hpcBulkMetadataEntries
		        .getPathsMetadataEntries()
		        .add(pathEntriesSample);
        }
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
    	String flowcellId = getFlowcellId(object, config);
	    String flowcellCollectionPath = runFCCollectionPath + "/Flowcell_" + flowcellId;
	    HpcBulkMetadataEntry pathEntriesFlowcell = new HpcBulkMetadataEntry();
	    pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Flowcell"));
	    pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("flowcell_id", flowcellId));
	    pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("run_id", getRunId(object)));
	    pathEntriesFlowcell.setPath(flowcellCollectionPath);
	    hpcBulkMetadataEntries
	        .getPathsMetadataEntries()
	        .add(pathEntriesFlowcell);
	    
	    if (fileName.equals("Reports")||fileName.equals("Stats")) {
	    	String reportCollectionPath = flowcellCollectionPath + "/FASTQ_Report";
	    	HpcBulkMetadataEntry pathEntriesReport = new HpcBulkMetadataEntry();
	    	pathEntriesReport.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Report"));
	    	pathEntriesReport.setPath(reportCollectionPath);
	    	hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesReport);
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
    // key = object_name, value = Sample_RES210195_HKJMGDSX2_R1.fastq.gz (derived)
    // key = file_type, value = fastq, bcl (derived)
    String fileType = getFileType(object, config);
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

  private String getPiCollectionName(StatusInfo object, DocConfig config) throws DmeSyncMappingException {
	SourceConfig sourceConfig = config.getSourceConfig();
    String piCollectionName = null;
    // Example: If originalFilePath is
    // /mnt/mocha_static/NovaSeq/220113_A00424_0160_BHKJNWDSX2/Data/Intensities/BaseCalls/L001
    // then return the mapped PI from /mnt/mocha_static/NovaSeq
    piCollectionName = getCollectionMappingValue(sourceConfig.sourceBaseDir, "PI_Lab", "mocha");
    
    logger.info("PI Collection Name: {}", piCollectionName);
    return piCollectionName;
  }
  
  private String getPlatformCollectionName(StatusInfo object, DocConfig config) throws DmeSyncMappingException {
	  String path = Paths.get(object.getOriginalFilePath()).toString();
	  String platform = null;
	  if(isBCL(object, config)) {
		  if (path.contains("NovaSeq") || path.contains("BW_transfers")) {
			  platform = "NovaSeq";
		  } else {
			  platform = "HiSeq";
		  }
	  } else {
		  // For fastq, get the Platform from the spreadsheet by using only Run_ID
		  String runId = getRunId(object);
		  try {
			  platform = getAttrValueWithParitallyMatchingKeyFromMapWithTwoKeys(runId, "Platform");
		  } catch (DmeSyncMappingException e) {
			  throw new DmeSyncMappingException("Run ID is missing from spreadsheet. Run_ID: " + runId);
		  }
		  if(StringUtils.containsIgnoreCase(platform, "HiSeq"))
			  platform = "HiSeq";
		  else if (StringUtils.containsIgnoreCase(platform, "NovaSeq"))
			  platform = "NovaSeq";
		  else
			  throw new DmeSyncMappingException("Platform cannot be determined from Run_ID " + runId);
	  }
	  return platform;
  }

  private String getFlowcellId(StatusInfo object, DocConfig config) throws DmeSyncMappingException {
	  String flowcellId = null;
	  String runId = getRunId(object);
	  if(isBCL(object, config)) {
		  //Get flowcell ID from run ID. (190501_D00719_0157_BHYJTMBCX2 will be HYJTMBCX2)
		  flowcellId = StringUtils.substringAfterLast(runId, "_");
		  flowcellId = StringUtils.substring(flowcellId, 1);
	  } else {
		  flowcellId = getAttrValueWithParitallyMatchingKeyFromMapWithTwoKeys(runId, "Flowcell");
	  }
	return flowcellId;
  }
  
  private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {
	  String runId = getRunId(object);
	  String sampleId = getSampleId(object);
	return getAttrValueFromMetadataMap(runId, sampleId, "Project");
  }
  
  private String getRunId(StatusInfo object) throws DmeSyncMappingException {
	  String path = Paths.get(object.getOriginalFilePath()).toString();
	  String flowcellCollectionName = null;
	if (path.contains("mocha_scratch") && path.contains("BW_transfers")) {
		flowcellCollectionName = getCollectionNameFromParent(object, getCollectionNameFromParent(object, "BW_transfers"));
	} else if (path.contains("mocha_ngs") && path.contains("BW_transfers")) {
		flowcellCollectionName = getCollectionNameFromParent(object, getCollectionNameFromParent(object, "MoCha-NGS_BW_transfers"));
	} else if (path.contains("mocha_ngs") && path.contains("FASTQ")) {
		flowcellCollectionName = getCollectionNameFromParent(object, "FASTQ");
	} else if (path.contains("static") && path.contains("NovaSeq")) {
		flowcellCollectionName = getCollectionNameFromParent(object, "NovaSeq");
	} else if (path.contains("static")) {
		flowcellCollectionName = getCollectionNameFromParent(object, "mocha_static");
	} else if (path.contains("mocha_ngs") && path.contains("Dragen_TSO500")) {
		flowcellCollectionName = getCollectionNameFromParent(object, "dragen_bcl2fastqconvert");
		 if (flowcellCollectionName != null)
		   flowcellCollectionName=flowcellCollectionName.replace("Dragen_BCL_", "DRAGEN_TSO500_V2_");
	}
	return flowcellCollectionName;
  }
  
  private boolean isBCL(StatusInfo object, DocConfig config) throws DmeSyncMappingException {
	  PreprocessingConfig pre = config.getPreprocessingConfig();
	  String path = Paths.get(object.getOriginalFilePath()).toString();
	  if (pre.tar) {
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
  
  private String getSampleCollectionName(StatusInfo object, DocConfig config) throws DmeSyncMappingException {
	  String runId = getRunId(object);
	  String sampleId = getSampleId(object);
	  String mochaId = getAttrValueFromMetadataMap(runId, sampleId, "Mocha_ID");
	  String sequencingDate = getAttrValueFromMetadataMap(runId, sampleId, "sequencing_Date");
	  if(sequencingDate.contains("/")) {
		  DateFormat outputFormatter = new SimpleDateFormat("yyyyMMdd");
		  SimpleDateFormat inputFormatter = new SimpleDateFormat("MM/dd/yy");
		  Date date = null;
		  try {
				date = inputFormatter.parse(sequencingDate);
		  } catch (ParseException e) {
				throw new DmeSyncMappingException(e);
		  }
		  sequencingDate = outputFormatter.format(date);
	  }
	  String flowcellId= getFlowcellId(object, config);
	return mochaId + "_" + sequencingDate + "_" + flowcellId;
  }
  
  private String getSeqDateFlowcellCollectionName(StatusInfo object) throws DmeSyncMappingException {
	  ////Get SeqDate and Flowcell from Run ID. (190501_D00719_0157_BHYJTMBCX2 will be 20190501_HYJTMBCX2)
	  String runId = getRunId(object);
	  String sequencingDate = StringUtils.substringBefore(runId, "_");
	  String flowcellId = StringUtils.substringAfterLast(runId, "_");
	  flowcellId = StringUtils.substring(flowcellId, 1);
	return "20" + sequencingDate + "_" + flowcellId;
  }
  
  private String getSampleFromFilePath(String path, String folderName) {
	    String sampleName = null;
	    for (Map.Entry<String, Map<String, String>> entry : metadataMapWithTwoKeys.entrySet()) {
	        if(StringUtils.startsWith(entry.getKey(), folderName)) {
	        	String sampleEntry = metadataMapWithTwoKeys.get(entry.getKey()).get("Sample");
		        if(StringUtils.containsIgnoreCase(path, sampleEntry)) {
		          //Sample is present in the file path
		          sampleName = sampleEntry;
		          break;
		        }
	        }
	    }
	    return sampleName;
  }
  
  private String getFileType(StatusInfo object, DocConfig config) throws DmeSyncMappingException {
	  String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
	  if(isBCL(object, config))
	    return "tar";
	  else if (fileName.contains(".fastq"))
		return "fastq";
	  return fileName.substring(fileName.indexOf('.') + 1);
  }
  
}
