package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.File;
import java.io.IOException;
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
	
	// DOC GB OMICS logic for DME path construction and meta data creation

	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

		logger.info("[PathMetadataTask] GB OMICS getArchivePath called");

		String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
		String archivePath;
		if(StringUtils.substringAfterLast(sourceBaseDir, File.separator).equals("DATA"))
			archivePath = destinationBaseDir + "/Lab_" + getPICollectionName(object) + "/DATA" + "/Flowcell_" + getFlowcell(getSample(object)) 
				+ "/Sample_" + getSample(object) + "/" + fileName;
		else
			archivePath = destinationBaseDir + "/Lab_" + getPICollectionName(object) + "/Project_" + getProjectCollectionName(object)
			+ (createSoftlink ? "/Source_Data" + "/Flowcell_" + getFlowcellId(object) : "/Analysis") + "/" + fileName;

		// replace spaces with underscore
		archivePath = archivePath.replace(" ", "_");

		return archivePath;
	}

	@Override
	public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException {

		logger.info("[PathMetadataTask] GB OMICS getMetaDataJson called");

		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();

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

		if(StringUtils.substringAfterLast(sourceBaseDir, File.separator).equals("DATA")) {
			// Add path metadata entries for "DATA" folder
			String dataCollectionPath = piCollectionPath + "/DATA";
			HpcBulkMetadataEntry pathEntriesDATA = new HpcBulkMetadataEntry();
			pathEntriesDATA.setPath(dataCollectionPath);
			pathEntriesDATA.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Raw_Data"));
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesDATA);
			
			// Add path metadata entries for "Flowcell" collection
			String sampleId = getSample(object);
			String flowcellId = getFlowcell(getSample(object));
			String flowcellCollectionPath = dataCollectionPath + "/Flowcell_" + flowcellId;
			HpcBulkMetadataEntry pathEntriesFlowcell = new HpcBulkMetadataEntry();
			pathEntriesFlowcell.setPath(flowcellCollectionPath);
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Flowcell"));
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("flowcell_id", flowcellId));
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("data_generating_facility", getAttrValueWithExactKey(sampleId, "Data generating facility")));
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("library_strategy", getAttrValueWithExactKey(sampleId, "Library strategy")));
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("analyte_type", getAttrValueWithExactKey(sampleId, "Analyte Type")));
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("platform_name", getAttrValueWithExactKey(sampleId, "Platform")));
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("organism", getAttrValueWithExactKey(sampleId, "Species")));
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("is_cell_line", getAttrValueWithExactKey(sampleId, "Is cell line")));
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("enrichment_step", getAttrValueWithExactKey(sampleId, "Enrichment step")));
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("reference_genome", getAttrValueWithExactKey(sampleId, "SampleRef")));
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("sequenced_date", getAttrValueWithExactKey(sampleId, "Run Start Date")));
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("sequencing_application_type", getAttrValueWithExactKey(sampleId, "Type of sequencing")));
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesFlowcell);

			// Add path metadata entries for "Sample" collection
			String sampleCollectionPath = flowcellCollectionPath + "/Sample_" + sampleId;
			HpcBulkMetadataEntry pathEntriesSample = new HpcBulkMetadataEntry();
			pathEntriesSample.setPath(sampleCollectionPath);
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Sample"));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sample_id", sampleId));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sample_name", sampleId));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("patient_id", getAttrValueWithExactKey(sampleId, "Patient ID")));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("library_id", getAttrValueWithExactKey(sampleId, "Library ID")));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("case_name", getAttrValueWithExactKey(sampleId, "Case Name")));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("diagnosis", getAttrValueWithExactKey(sampleId, "Diagnosis")));
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
				String flowcellId = getFlowcellId(object);
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
		if(createSoftlink)
			piFolder = getCollectionNameFromParent(softlinkFile, StringUtils.substringAfterLast(sourceBaseDir, File.separator));
		else
			piFolder = getCollectionNameFromParent(object.getOriginalFilePath(), StringUtils.substringAfterLast(sourceBaseDir, File.separator));
		return piFolder;
	}
	
	private String getPICollectionName(StatusInfo object) throws DmeSyncMappingException {
		//For /data/khanlab3/gb_omics/DATA/caplen, it will get mapped collection name for caplen.
		//For /data/khanlab3/gb_omics/projects/caplen, it will get mapped collection name for caplen.
		String piFolder = getPIFolder(object);
		return getCollectionMappingValue(piFolder, "DataOwner_Lab", "gb-omics");
	}

	private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {
		String projectId = null;
		if(createSoftlink)
			projectId = getCollectionNameFromParent(softlinkFile, getPIFolder(object));
		else
			projectId = getCollectionNameFromParent(object.getOriginalFilePath(), getPIFolder(object));
		return projectId.toUpperCase().replace('_', '-');
	}

	private String getFlowcell(String sampleId) {
		String flowcellId = getAttrValueWithExactKey(sampleId, "FCID");
		return flowcellId;
	}
	
	private String getFlowcellId(StatusInfo object) {
		if(object.getOriginalFilePath().contains("Flowcell_")) {
			String pathStartingFlowcellId = StringUtils.substringAfter(object.getOriginalFilePath(), "Flowcell_");
			return StringUtils.substringBefore(pathStartingFlowcellId, "/");
		}
		return null;
	}
	
	private String getSample(StatusInfo object) throws DmeSyncMappingException {
		String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
		String sampleId = getAttrKeyFromKeyInSearchString(fileName);
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
	
	@PostConstruct
	  private void init() throws IOException {
		if("gb-omics".equalsIgnoreCase(doc)) {
		    try {
		    	// load the user metadata from the externally placed excel
				if(StringUtils.isNotEmpty(metadataFile))
					metadataMap = loadMetadataFile(metadataFile, "Sample name");
		    } catch (DmeSyncMappingException e) {
		        logger.error(
		            "Failed to initialize metadata  path metadata processor", e);
		    }
		}
	  }
	
}
