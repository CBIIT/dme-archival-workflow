package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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

/**
 * Default GB OMICS Path and Meta-data Processor Implementation
 * 
 * @author dinhys
 *
 */
@Service("gb-omics")
public class GBOmicsPathMetadataProcessorImpl extends AbstractPathMetadataProcessor implements DmeSyncPathMetadataProcessor {

	@Value("${dmesync.doc.name}")
	  private String doc;
	@Value("${dmesync.source.base.dir}")
	protected String sourceBaseDir;

	@Value("${dmesync.create.softlink:false}")
	private boolean createSoftlink;
	
	@Value("${dmesync.additional.metadata.excel:}")
	private String metadataFile;
	
	@Value("${dmesync.source.softlink.file:}")
	private String softlinkFile;
	
	@Value("${dmesync.db.access:local}")
	  protected String access;
	
	// DOC GB OMICS logic for DME path construction and meta data creation

	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

		logger.info("[PathMetadataTask] GB OMICS getArchivePath called");

		String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
		String archivePath = "";
		if(isDATA() || isONT()) {
			String sampleId = "";
			try {
				sampleId = getSample(object);
				archivePath = destinationBaseDir + "/Lab_" + getPICollectionName(object) + "/DATA" + "/Year_" + getYear(object)
					+ "/Flowcell_" + getFlowcell(object) 
					+ (!sampleId.startsWith("Sample_") ? "/Sample_": "/") + sampleId + "/" + fileName;
			} catch (DmeSyncMappingException e) {
				if(StringUtils.isEmpty(sampleId)) {
					//This path might not be in the master file. If it is not, we want to ignore this path for now.
					try {
						searchPathInMasterFile(object.getOriginalFilePath());
					} catch (DmeSyncMappingException me) {
						//This path is not in the master file, so log the path and complete the workflow
						logger.info("No need to upload file : {}", object.getOriginalFilePath());
						// update the current status info row as completed so this workflow is completed and next task won't be processed.
						object.setRunId(object.getRunId() + "_IGNORED");
						object.setEndWorkflow(true);
						object.setError("No need to upload");
						object = dmeSyncWorkflowService.getService(access).saveStatusInfo(object);
						return "";
					}
					// If path is found, this means that the sample metadata is not available, so raise exception
					throw e;
				}
			}
		} else
			archivePath = destinationBaseDir + "/Lab_" + getPICollectionName(object) + "/Project_" + getProjectCollectionName(object)
			+ (createSoftlink ? "/Source_Data" + "/Flowcell_" + getFlowcellIdForProject(object) : "/Analysis") + "/" + fileName;

		// replace spaces with underscore
		archivePath = archivePath.replace(" ", "_");

		return archivePath;
	}

	@Override
	public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException {

		logger.info("[PathMetadataTask] GB OMICS getMetaDataJson called");

		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();

		//This is the case where this file is not listed in the master file path.
		if(StringUtils.isBlank(object.getFullDestinationPath())) {
			return dataObjectRegistrationRequestDTO;
		}
		
		// Add to HpcBulkMetadataEntries for path attributes
		HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

		// Add path metadata entries for "DataOwner_Lab" collection

		String piCollectionName = getPICollectionName(object);
		String piCollectionPath = destinationBaseDir + "/Lab_" + piCollectionName;
		HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "DataOwner_Lab"));
		pathEntriesPI.setPath(piCollectionPath);
		hpcBulkMetadataEntries.getPathsMetadataEntries()
				.add(populateStoredMetadataEntries(pathEntriesPI, "DataOwner_Lab", piCollectionName, "gb-omics"));

		if(isDATA() || isONT()) {
			// Add path metadata entries for "DATA" folder
			String dataCollectionPath = piCollectionPath + "/DATA";
			HpcBulkMetadataEntry pathEntriesDATA = new HpcBulkMetadataEntry();
			pathEntriesDATA.setPath(dataCollectionPath);
			pathEntriesDATA.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Raw_Data"));
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesDATA);
			
			// Add path metadata entries for "Date" folder
			String sampleId = getSample(object);
			String flowcellId = getFlowcell(object);
			String key = isONT() ? flowcellId : sampleId;
			String dateCollectionPath = dataCollectionPath + "/Year_" + getYear(object);
			HpcBulkMetadataEntry pathEntriesDate = new HpcBulkMetadataEntry();
			pathEntriesDate.setPath(dateCollectionPath);
			pathEntriesDate.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Date"));
			pathEntriesDate.getPathMetadataEntries().add(createPathEntry("year", getYear(object)));
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesDate);
			
			// Add path metadata entries for "Flowcell" collection
			String flowcellCollectionPath = dateCollectionPath + "/Flowcell_" + flowcellId;
			HpcBulkMetadataEntry pathEntriesFlowcell = new HpcBulkMetadataEntry();
			pathEntriesFlowcell.setPath(flowcellCollectionPath);
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Flowcell"));
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("flowcell_id", flowcellId));
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("data_generating_facility", getAttrValueWithExactKey(key, "Data generating facility")));
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("library_strategy", getAttrValueWithExactKey(key, "Library strategy")));
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("analyte_type", getAttrValueWithExactKey(key, "Analyte Type")));
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("platform_name", getAttrValueWithExactKey(key, "Platform")));
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("organism", getAttrValueWithExactKey(key, "Species")));
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("is_cell_line", getAttrValueWithExactKey(key, "Is cell line")));
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("enrichment_step", getAttrValueWithExactKey(key, "Enrichment step")));
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("reference_genome", getAttrValueWithExactKey(key, "SampleRef")));
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("sequenced_date", getAttrValueWithExactKey(key, "Run Start Date")));
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("sequencing_application_type", getAttrValueWithExactKey(key, "Type of sequencing")));
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("run_date", getRunDate(key)));
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesFlowcell);

			// Add path metadata entries for "Sample" collection
			String sampleCollectionPath = flowcellCollectionPath + (!sampleId.startsWith("Sample_") ? "/Sample_": "/") + sampleId;
			HpcBulkMetadataEntry pathEntriesSample = new HpcBulkMetadataEntry();
			pathEntriesSample.setPath(sampleCollectionPath);
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Sample"));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sample_id", sampleId));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sample_name", sampleId));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("patient_id", getAttrValueWithExactKey(key, "Patient ID")));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("library_id", getAttrValueWithExactKey(key, "Library ID")));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("case_name", getAttrValueWithExactKey(key, "Case Name")));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("diagnosis", getAttrValueWithExactKey(key, "Diagnosis")));
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesSample);
			
		} else {

			// Add path metadata entries for "Project" collection
			String projectCollectionName = getProjectCollectionName(object);
			String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;
			HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Closed Access"));
			pathEntriesProject.setPath(projectCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries()
					.add(populateStoredMetadataEntries(pathEntriesProject, "Project", projectCollectionName, "gb-omics"));
			
			// Add path metadata entries for "Source_Data" or "Analysis" folder
			String analysisCollectionPath = projectCollectionPath + (createSoftlink ? "/Source_Data" : "/Analysis");
			HpcBulkMetadataEntry pathEntriesAnalysis = new HpcBulkMetadataEntry();
			pathEntriesAnalysis.setPath(analysisCollectionPath);
			pathEntriesAnalysis.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, (createSoftlink ? "Source_Data" : "Analysis")));
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesAnalysis);
			
			if(createSoftlink) {
				// Add path metadata entries for "Flowcell" collection
				String flowcellId = getFlowcellIdForProject(object);
				String flowcellCollectionPath = analysisCollectionPath + "/Flowcell_" + flowcellId;
				HpcBulkMetadataEntry pathEntriesFlowcell = new HpcBulkMetadataEntry();
				pathEntriesFlowcell.setPath(flowcellCollectionPath);
				pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Flowcell_Link"));
				pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("flowcell_id", flowcellId));
				pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("flowcell_path", StringUtils.substringBefore(object.getOriginalFilePath(), flowcellId) + flowcellId));
				hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesFlowcell);
			}
	    }

		// Set it to dataObjectRegistrationRequestDTO
		dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
		dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);

		// Add object metadata
		String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
		String fileType = StringUtils.substringBefore(fileName, ".gz");
	    fileType = fileType.substring(fileType.lastIndexOf('.') + 1);
		dataObjectRegistrationRequestDTO.getMetadataEntries()
				.add(createPathEntry("object_name", fileName));
		dataObjectRegistrationRequestDTO.getMetadataEntries()
        		.add(createPathEntry("file_type", fileType));
		dataObjectRegistrationRequestDTO.getMetadataEntries()
				.add(createPathEntry("source_path", object.getOriginalFilePath()));

		return dataObjectRegistrationRequestDTO;
	}

	private String getCollectionNameFromParent(String path, String parentName) {
		Path fullFilePath = Paths.get(path);
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
	
	private String getPIFolder(StatusInfo object) throws DmeSyncMappingException {
		//For /data/khanlab3/gb_omics/DATA/caplen, it will return caplen.
		//For /data/khanlab3/gb_omics/projects/caplen, it will return caplen.
		String piFolder = null;
		if(isONT()) {
			String key = getFlowcell(object);
			piFolder = getAttrValueWithExactKey(key, "PI");
		}
		else if(createSoftlink)
			piFolder = getCollectionNameFromParent(softlinkFile, StringUtils.substringAfterLast(sourceBaseDir, File.separator));
		else
			piFolder = getCollectionNameFromParent(object.getOriginalFilePath(), StringUtils.substringAfterLast(sourceBaseDir, File.separator));
		return piFolder;
	}
	
	private String getPICollectionName(StatusInfo object) throws DmeSyncMappingException {
		//For /data/khanlab3/gb_omics/DATA/caplen, it will get mapped collection name for caplen.
		//For /data/khanlab3/gb_omics/projects/caplen, it will get mapped collection name for caplen.
		String piFolder = getPIFolder(object);
		return getCollectionMappingValue(piFolder.toLowerCase(), "DataOwner_Lab", "gb-omics");
	}

	private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {
		String projectId = null;
		if(createSoftlink)
			projectId = getCollectionNameFromParent(softlinkFile, getPIFolder(object));
		else
			projectId = getCollectionNameFromParent(object.getOriginalFilePath(), getPIFolder(object));
		return projectId.toUpperCase().replace('_', '-');
	}

	private String getFlowcell(StatusInfo object) throws DmeSyncMappingException {
		String flowcellId = "";
		if(!isONT()) {
			flowcellId = getAttrValueWithExactKey(getSample(object), "FCID");
		}
		else {
			flowcellId = getCollectionNameFromParent(object.getOriginalFilePath(), StringUtils.substringAfterLast(sourceBaseDir, File.separator));
		}
		return flowcellId;
	}
	
	private String getYear(StatusInfo object) throws DmeSyncMappingException {
		String key = isONT() ? getFlowcell(object) : getSample(object);
		String runDate = getAttrValueWithExactKey(key, "Run Start Date");
		String year = "";
		if(StringUtils.isNotBlank(runDate) && StringUtils.contains(runDate, "/")) {
			year = StringUtils.substringAfterLast(runDate, "/");
			if(year.length() == 2)
				year = "20" + year;
		} else if (StringUtils.isNotBlank(runDate)) {
			year = "20" + StringUtils.substring(runDate, 0, 2);
		}
		return year;
	}
	
	private String getRunDate(String key) {
		String runDate = getAttrValueWithExactKey(key, "Run Start Date");
		String date = "";
		DateFormat outputFormatter = new SimpleDateFormat("yyyy-MM-dd");
		if(StringUtils.isNotBlank(runDate) && StringUtils.contains(runDate, "/")) {
			SimpleDateFormat inputFormatter = new SimpleDateFormat("MM/dd/yy");
			try {
				date = outputFormatter.format(inputFormatter.parse(runDate));
			} catch (ParseException e) {
				logger.error("Can't parse run_date: ", runDate);
			}
		} else if (StringUtils.isNotBlank(runDate)) {
			SimpleDateFormat inputFormatter = new SimpleDateFormat("yyMMdd");
			try {
				date = outputFormatter.format(inputFormatter.parse(runDate));
			} catch (ParseException e) {
				logger.error("Can't parse run_date: ", runDate);
			}
		}
		return date;
	}
	
	private String getFlowcellIdForProject(StatusInfo object) {
		String flowcellId = "";
		if(object.getOriginalFilePath().contains("Flowcell_")) {
			String pathStartingFlowcellId = StringUtils.substringAfter(object.getOriginalFilePath(), "Flowcell_");
			return StringUtils.substringBefore(pathStartingFlowcellId, "/");
		}
		return flowcellId;
	}
	
	private String getSample(StatusInfo object) throws DmeSyncMappingException {
		String sampleId = "";
		if(!isONT()) {
			String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
			sampleId = getAttrKeyFromKeyInSearchString(fileName);
		} else {
			sampleId = getAttrValueWithExactKey(getFlowcell(object), "Sample name");
		}
		return sampleId;
	}
	
	public String getAttrValueWithExactKey(String key, String attrKey) {
		if(StringUtils.isEmpty(key)) {
	      logger.error("Excel mapping not found for {}", key);
	      return null;
	    }
	    return (metadataMap.get(key) == null? null : metadataMap.get(key).get(attrKey));
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
	
	private String searchPathInMasterFile(String searchString) throws DmeSyncMappingException {
	    String key = null;
	    for (Map.Entry<String, Map<String, String>> entry : metadataMap.entrySet()) {
	        if(StringUtils.contains(searchString, getAttrValueWithExactKey(entry.getKey(), "PI") + File.separator + getAttrValueWithExactKey(entry.getKey(), "Path"))) {
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
	
	private boolean isDATA() {
	    return (StringUtils.contains(sourceBaseDir, "DATA")? true : false);
	}
	
	private boolean isONT() {
		return (StringUtils.contains(sourceBaseDir, "GB_OMICS_ONT")? true : false);
	}
	
	@PostConstruct
	  private void init() throws IOException {
		if("gb-omics".equalsIgnoreCase(doc)) {
		    try {
		    	// load the user metadata from the externally placed excel
				if(StringUtils.isNotEmpty(metadataFile) && !isONT())
					metadataMap = loadMetadataFile(metadataFile, "Sample name");
				else if (StringUtils.isNotEmpty(metadataFile) && isONT())
					metadataMap = loadMetadataFile(metadataFile, "FCID");
		    } catch (DmeSyncMappingException e) {
		        logger.error(
		            "Failed to initialize metadata  path metadata processor", e);
		    }
		}
	  }
	
}
