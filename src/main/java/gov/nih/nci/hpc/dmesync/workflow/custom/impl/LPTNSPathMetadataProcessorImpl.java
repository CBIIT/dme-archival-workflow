package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
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
 * LP TNS DME Path and Meta-data Processor Implementation
 * 
 * @author dinhys
 *
 */
@Service("lp-tns")
public class LPTNSPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
		implements DmeSyncPathMetadataProcessor {

	// LP TNS Custom logic for DME path construction and meta data creation
	@Value("${dmesync.additional.metadata.excel:}")
	private String metadataFile;
	
	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

		logger.info("[PathMetadataTask] LP TNS getArchivePath called");

		// extract the user name from the Path
		// Example path - /data/EVset_RNAseq/PI_Lab_Jennifer_Jones/Project_EVsetRNAseq1/Experiment_DC24_20210914/Sample_FASTQ/*
		String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();

		String archivePath = destinationBaseDir + "/" + getPiCollectionName(object) + "/"
				+ getProjectCollectionName(object) + "/" + getExpCollectionName(object) 
				+ "/" + getCollectionNameFromParent(object, getExpCollectionName(object)) + "/" + fileName;

		// replace spaces with underscore
		archivePath = archivePath.replace(" ", "_");

		logger.info("Archive path for {} : {}", object.getOriginalFilePath(), archivePath);

		return archivePath;
	}

	@Override
	public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException {

		// load the user metadata from the externally placed excel
		threadLocalMap.set(loadMetadataFile(metadataFile, "path"));
		String path = "/data/EVset_RNAseq" + StringUtils.substringAfter(object.getOriginalFilePath().replace(File.separatorChar,'/'), "EVset_RNAseq");
		
		// Add to HpcBulkMetadataEntries for path attributes
		HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

		// Add path metadata entries for "PI_XXX" collection
		// Example row: collectionType - DataOwner_Lab collectionName -
		// PI_Jennifer_Jones,
		HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
		String piCollectionName = getPiCollectionName(object);
		pathEntriesPI.setPath(destinationBaseDir + "/" + piCollectionName);
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "DataOwner_Lab"));
		hpcBulkMetadataEntries.getPathsMetadataEntries()
				.add(populateStoredMetadataEntries(pathEntriesPI, "DataOwner_Lab", piCollectionName, "lp-tns"));

		// Add path metadata entries for "Project_XXX" collection
		HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
		String projectCollectionName = getProjectCollectionName(object);
		String projectPath = destinationBaseDir + "/" + piCollectionName + "/" + projectCollectionName;
		pathEntriesProject.setPath(projectPath.replace(" ", "_"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc", getAttrValueWithKey(path, "project_poc")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc_email", getAttrValueWithKey(path, "poc_email")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc_affiliation", getAttrValueWithKey(path, "poc_affiliation")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("data_generating_facility", getAttrValueWithKey(path, "data_generating_facility")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_id", getAttrValueWithKey(path, "project_id")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_title", getAttrValueWithKey(path, "project_title")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_description", getAttrValueWithKey(path, "project_description")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_start_date", getAttrValueWithKey(path, "project_start_date"), "MM/dd/yy"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("platform_name", getAttrValueWithKey(path, "platform_name")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("organism", getAttrValueWithKey(path, "organism")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("is_cell_line", getAttrValueWithKey(path, "is_cell_line")));
		if (StringUtils.isNotBlank(getAttrValueWithKey(path, "study_disease")))
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("study_disease", getAttrValueWithKey(path, "study_disease")));
		if (StringUtils.isNotBlank(getAttrValueWithKey(path, "project_status")))
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_status", getAttrValueWithKey(path, "project_status")));
		if (StringUtils.isNotBlank(getAttrValueWithKey(path, "project_completed_date")))
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_completed_date", getAttrValueWithKey(path, "project_completed_date"), "MM/dd/yy"));
		if (StringUtils.isNotBlank(getAttrValueWithKey(path, "collaborators")))
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("collaborators", getAttrValueWithKey(path, "collaborators")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Controlled Access"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);

		// Add path metadata entries for "Exp_XXX" collection
		String flowcellId = null;
		HpcBulkMetadataEntry pathEntriesExp = new HpcBulkMetadataEntry();
		String expCollectionName = getExpCollectionName(object);
		String expPath = projectPath + "/" + expCollectionName;
		pathEntriesExp.setPath(expPath.replace(" ", "_"));
		pathEntriesExp.getPathMetadataEntries().add(createPathEntry("experiment_name", getAttrValueWithKey(path, "experiment_name")));
		pathEntriesExp.getPathMetadataEntries().add(createPathEntry("experiment_type", getAttrValueWithKey(path, "experiment_type")));
		pathEntriesExp.getPathMetadataEntries().add(createPathEntry("experiment_id", getAttrValueWithKey(path, "experiment_id")));
		pathEntriesExp.getPathMetadataEntries().add(createPathEntry("experiment_date", getAttrValueWithKey(path, "experiment_date")));
		pathEntriesExp.getPathMetadataEntries().add(createPathEntry("cell_line", getAttrValueWithKey(path, "cell_line")));
		pathEntriesExp.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Experiment"));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesExp);

		// Add path metadata entries for "Sample, Raw_Data or Analysis" collection
		HpcBulkMetadataEntry pathEntriesData = new HpcBulkMetadataEntry();
		String dataCollectionName = getCollectionNameFromParent(object, getExpCollectionName(object));
		String dataPath = expPath + "/" + dataCollectionName;
		pathEntriesData.setPath(dataPath.replace(" ", "_"));
		String collectionType = getCollectionType(dataCollectionName);
		if(collectionType.equals("Raw_Data")) {
			flowcellId = getFlowcellId(object.getOrginalFileName());
		}
		pathEntriesData.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, collectionType));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesData);
		
		// Set it to dataObjectRegistrationRequestDTO
		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();
		dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
		dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
		dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);

		// Add object metadata
		String fileName = Paths.get(object.getOrginalFileName()).toFile().getName();
		String fileType = StringUtils.substringBefore(fileName, ".gz");
	    fileType = fileType.substring(fileType.lastIndexOf('.') + 1);
		dataObjectRegistrationRequestDTO.getMetadataEntries()
				.add(createPathEntry("object_name", fileName));
		dataObjectRegistrationRequestDTO.getMetadataEntries()
		.add(createPathEntry("file_type", fileType));
		dataObjectRegistrationRequestDTO.getMetadataEntries()
				.add(createPathEntry("source_path", object.getOriginalFilePath()));
		if(StringUtils.isNoneBlank(flowcellId))
			dataObjectRegistrationRequestDTO.getMetadataEntries()
			.add(createPathEntry("flowcell_id", flowcellId));

		logger.info("LP TNS custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
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
		return getCollectionNameFromParent(object, "EVset_RNAseq");
	}

	private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {
		return getCollectionNameFromParent(object, getPiCollectionName(object));
	}

	private String getExpCollectionName(StatusInfo object) throws DmeSyncMappingException {
		return getCollectionNameFromParent(object, getProjectCollectionName(object));
	}
	
	private String getCollectionType(String collectionName) {
		if (StringUtils.startsWith(collectionName, "Sample_")) {
			return "Sample";
		} else if (StringUtils.startsWith(collectionName, "Analysis_")) {
			return "Analysis";
		}
		return "Raw_Data";
	}
	
	private String getFlowcellId(String fileName) {
		String flowcellId = StringUtils.substringAfterLast(fileName, "_");
		return StringUtils.substringBefore(flowcellId, ".");
	}
}
