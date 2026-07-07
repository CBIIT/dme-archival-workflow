package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.util.DmeMetadataBuilder;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncPathMetadataProcessor;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntries;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;

/**
 * LCBG SDS DME Path and Meta-data Processor Implementation
 * 
 * @author dinhys
 *
 */
@Service("lcbg-sds")
public class LCBGSDSPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
		implements DmeSyncPathMetadataProcessor {

	// LCBG SDS Custom logic for DME path construction and meta data creation
	@Value("${dmesync.additional.metadata.excel:}")
	private String metadataFile;
	
	@Autowired
	private DmeMetadataBuilder dmeMetadataBuilder;
	
	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {

		logger.info("[PathMetadataTask] LCBG SDS getArchivePath called");

		
		// load the user metadata from the externally placed excel
	    metadataMap = dmeMetadataBuilder.getMetadataMap(metadataFile, "Folder");
				
		// extract the user name from the Path
		// Example path - /Cappell-Section/Adrijana/AC053_AXXAi_Gem_CDK2_sibTRCP/Raw/*
		String userId = getUserId(object);
		String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();

		// Get the DataOwner_Lab collection value from the CollectionNameMetadata
		// Example row - mapKey - Cappell-Section, collectionType - DataOwner_Lab,
		// mapValue - Steven_Cappell
		// Get the Researcher collection value from the CollectionNameMetadata
		// Example row - mapkey - Adrijana, collectionType - Researcher, mapvalue -
		// Adrijana_Crncec
		// Extract the Experiment value from the Path

		String archivePath = destinationBaseDir + "/PI_" + getPiCollectionName() + "/Researcher_"
				+ getResearcherCollectionName(userId) + "/Project_" + getProjectCollectionName(object) + "/Experiment_"
				+ getExpCollectionName(object) + "/" + fileName;

		// replace spaces with underscore
		archivePath = archivePath.replace(" ", "_");

		logger.info("Archive path for {} : {}", object.getOriginalFilePath(), archivePath);

		return archivePath;
	}

	@Override
	public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException {

		String userId = getUserId(object);
		String path = getPath(object);
		String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
		// Add to HpcBulkMetadataEntries for path attributes
		HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

		// Add path metadata entries for "PI_XXX" collection
		// Example row: collectionType - DataOwner_Lab collectionName -
		// PI_Steven_Cappell,
		HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
		String piCollectionName = getPiCollectionName();
		pathEntriesPI.setPath(destinationBaseDir + "/PI_" + piCollectionName);
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "DataOwner_Lab"));
		hpcBulkMetadataEntries.getPathsMetadataEntries()
				.add(populateStoredMetadataEntries(pathEntriesPI, "DataOwner_Lab", piCollectionName, "lcbg-sds"));

		// Add path metadata entries for "Researcher_XXX" collection
		// Example row: collectionType - Researcher, collectionName -
		// Researcher_James_Cornwell
		HpcBulkMetadataEntry pathEntriesResearcher = new HpcBulkMetadataEntry();
		String researcherCollectionName = getResearcherCollectionName(userId);
		String researcherPath = destinationBaseDir + "/PI_" + piCollectionName + "/Researcher_"
				+ researcherCollectionName;
		pathEntriesResearcher
				.setPath(destinationBaseDir + "/PI_" + piCollectionName + "/Researcher_" + researcherCollectionName);
		pathEntriesResearcher.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Researcher"));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(populateStoredMetadataEntries(pathEntriesResearcher,
				"Researcher", researcherCollectionName, "lcbg-sds"));

		// Add path metadata entries for "Project_XXX" collection
		HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
		String projectCollectionName = getProjectCollectionName(object);
		String projectPath = researcherPath + "/Project_" + projectCollectionName;
		pathEntriesProject.setPath(projectPath.replace(" ", "_"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_id", projectCollectionName));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_title", getAttrValueWithExactKeyFromMetadataMap(path, "project_title")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_description", getAttrValueWithExactKeyFromMetadataMap(path, "project_description")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_start_date", getAttrValueWithExactKeyFromMetadataMap(path, "project_start_date"), "yyyy_MM_dd"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("data_generating_facility", "LCBG SDS"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Closed Access"));
		if (StringUtils.isNotBlank(getAttrValueWithExactKeyFromMetadataMap(path, "key_collaborator")))
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("key_collaborator", getAttrValueWithExactKeyFromMetadataMap(path, "key_collaborator")));
		if (StringUtils.isNotBlank(getAttrValueWithExactKeyFromMetadataMap(path, "key_collaborator_affiliation")))
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("key_collaborator_affiliation", getAttrValueWithExactKeyFromMetadataMap(path, "key_collaborator_affiliation")));
		if (StringUtils.isNotBlank(getAttrValueWithExactKeyFromMetadataMap(path, "key_collaborator_email")))
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("key_collaborator_email", getAttrValueWithExactKeyFromMetadataMap(path, "key_collaborator_email")));		
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(populateStoredMetadataEntries(pathEntriesProject,
				"Project", researcherCollectionName, "lcbg-sds"));

		// Add path metadata entries for "Exp_XXX" collection
		HpcBulkMetadataEntry pathEntriesExp = new HpcBulkMetadataEntry();
		String expCollectionName = getExpCollectionName(object);
		String expPath = projectPath + "/Experiment_" + expCollectionName;
		pathEntriesExp.setPath(expPath.replace(" ", "_"));
		pathEntriesExp.getPathMetadataEntries().add(createPathEntry("experiment_name", expCollectionName));
		pathEntriesExp.getPathMetadataEntries().add(createPathEntry("experiment_id", getAttrValueWithExactKeyFromMetadataMap(path, "experiment_id")));
		pathEntriesExp.getPathMetadataEntries().add(createPathEntry("experiment_date", getAttrValueWithExactKeyFromMetadataMap(path, "experiment_date")));
		if (StringUtils.isNotBlank(getAttrValueWithExactKeyFromMetadataMap(path, "experiment_type")))
			pathEntriesExp.getPathMetadataEntries().add(createPathEntry("experiment_type", getAttrValueWithExactKeyFromMetadataMap(path, "experiment_type")));
		if (StringUtils.isNotBlank(getAttrValueWithExactKeyFromMetadataMap(path, "number_of_samples")))
			pathEntriesExp.getPathMetadataEntries().add(createPathEntry("number_of_samples", getAttrValueWithExactKeyFromMetadataMap(path, "number_of_samples")));
		if (StringUtils.isNotBlank(getAttrValueWithExactKeyFromMetadataMap(path, "cell_line")))
			pathEntriesExp.getPathMetadataEntries().add(createPathEntry("cell_line", getAttrValueWithExactKeyFromMetadataMap(path, "cell_line")));
		if (StringUtils.isNotBlank(getAttrValueWithExactKeyFromMetadataMap(path, "instrument_id")))
			pathEntriesExp.getPathMetadataEntries().add(createPathEntry("instrument_id", getAttrValueWithExactKeyFromMetadataMap(path, "instrument_id")));
		if (StringUtils.isNotBlank(getAttrValueWithExactKeyFromMetadataMap(path, "instrument_name")))
			pathEntriesExp.getPathMetadataEntries().add(createPathEntry("instrument_name", getAttrValueWithExactKeyFromMetadataMap(path, "instrument_name")));
		if (StringUtils.isNotBlank(getAttrValueWithExactKeyFromMetadataMap(path, "treatments")))
			pathEntriesExp.getPathMetadataEntries().add(createPathEntry("treatments", getAttrValueWithExactKeyFromMetadataMap(path, "treatments")));
		if (StringUtils.isNotBlank(getAttrValueWithExactKeyFromMetadataMap(path, "num_frames")))
			pathEntriesExp.getPathMetadataEntries().add(createPathEntry("num_frames", getAttrValueWithExactKeyFromMetadataMap(path, "num_frames")));
		if (StringUtils.isNotBlank(getAttrValueWithExactKeyFromMetadataMap(path, "frame_drug_added")))
			pathEntriesExp.getPathMetadataEntries().add(createPathEntry("frame_drug_added", getAttrValueWithExactKeyFromMetadataMap(path, "frame_drug_added")));
		if (StringUtils.isNotBlank(getAttrValueWithExactKeyFromMetadataMap(path, "frame_wash")))
			pathEntriesExp.getPathMetadataEntries().add(createPathEntry("frame_wash", getAttrValueWithExactKeyFromMetadataMap(path, "frame_wash")));
		if (StringUtils.isNotBlank(getAttrValueWithExactKeyFromMetadataMap(path, "binning")))
			pathEntriesExp.getPathMetadataEntries().add(createPathEntry("binning", getAttrValueWithExactKeyFromMetadataMap(path, "binning")));
		if (StringUtils.isNotBlank(getAttrValueWithExactKeyFromMetadataMap(path, "frame_rate")))
			pathEntriesExp.getPathMetadataEntries().add(createPathEntry("frame_rate", getAttrValueWithExactKeyFromMetadataMap(path, "frame_rate")));
		if (StringUtils.isNotBlank(getAttrValueWithExactKeyFromMetadataMap(path, "flourescent_protein_1")))
			pathEntriesExp.getPathMetadataEntries().add(createPathEntry("fluorescent_protein_1", getAttrValueWithExactKeyFromMetadataMap(path, "flourescent_protein_1")));
		if (StringUtils.isNotBlank(getAttrValueWithExactKeyFromMetadataMap(path, "flourescent_protein_2")))
			pathEntriesExp.getPathMetadataEntries().add(createPathEntry("fluorescent_protein_2", getAttrValueWithExactKeyFromMetadataMap(path, "flourescent_protein_2")));
		if (StringUtils.isNotBlank(getAttrValueWithExactKeyFromMetadataMap(path, "flourescent_protein_3")))
			pathEntriesExp.getPathMetadataEntries().add(createPathEntry("fluorescent_protein_3", getAttrValueWithExactKeyFromMetadataMap(path, "flourescent_protein_3")));
		if (StringUtils.isNotBlank(getAttrValueWithExactKeyFromMetadataMap(path, "flourescent_protein_4")))
			pathEntriesExp.getPathMetadataEntries().add(createPathEntry("fluorescent_protein_4", getAttrValueWithExactKeyFromMetadataMap(path, "flourescent_protein_4")));
		if (StringUtils.isNotBlank(getAttrValueWithExactKeyFromMetadataMap(path, "flourescent_protein_5")))
			pathEntriesExp.getPathMetadataEntries().add(createPathEntry("fluorescent_protein_5", getAttrValueWithExactKeyFromMetadataMap(path, "flourescent_protein_5")));
		pathEntriesExp.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Experiment"));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesExp);
		
		// Set it to dataObjectRegistrationRequestDTO
		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();
		dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
		dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
		dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);

		// Add object metadata
		dataObjectRegistrationRequestDTO.getMetadataEntries()
				.add(createPathEntry("object_name", fileName));
		dataObjectRegistrationRequestDTO.getMetadataEntries()
				.add(createPathEntry("source_path", object.getOriginalFilePath()));

		logger.info("LCBG SDS custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
		return dataObjectRegistrationRequestDTO;
	}

	@Override
    public boolean isMetadataAvailable(StatusInfo object) throws DmeSyncMappingException {
		
		   // Check if this project is in the metadata spreadsheet
			String experimentName = getExpCollectionName(object);
			return experimentName != null;
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

	private String getUserId(StatusInfo object) {
		return getCollectionNameFromParent(object, "Cappell-Section");
	}
	
	private String getPath(StatusInfo object) {
		return getCollectionNameFromParent(object, getUserId(object));
	}

	private String getPiCollectionName() throws DmeSyncMappingException {
		return getCollectionMappingValue("Cappell-Section", "DataOwner_Lab", "lcbg-sds");
	}

	private String getResearcherCollectionName(String userId) throws DmeSyncMappingException {
		return getCollectionMappingValue(userId, "Researcher", "lcbg-sds");
	}

	private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {
		return getAttrValueWithExactKeyFromMetadataMap(getPath(object), "project_id");
	}

	private String getExpCollectionName(StatusInfo object) {
		return getAttrValueWithExactKeyFromMetadataMap(getPath(object), "experiment_name");
	}
}
