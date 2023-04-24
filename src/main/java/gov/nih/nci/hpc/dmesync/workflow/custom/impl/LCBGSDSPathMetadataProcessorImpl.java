package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
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

	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

		logger.info("[PathMetadataTask] LCBG SDS getArchivePath called");

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
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Controlled Access"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
		hpcBulkMetadataEntries.getPathsMetadataEntries()
				.add(populateStoredMetadataEntries(pathEntriesProject, "Project", projectCollectionName, "lcbg-sds"));

		// Add path metadata entries for "Exp_XXX" collection
		HpcBulkMetadataEntry pathEntriesExp = new HpcBulkMetadataEntry();
		String expCollectionName = getExpCollectionName(object);
		String expPath = projectPath + "/Experiment_" + expCollectionName;
		pathEntriesExp.setPath(expPath.replace(" ", "_"));
		pathEntriesExp.getPathMetadataEntries().add(createPathEntry("experiment_name", expCollectionName));
		pathEntriesExp.getPathMetadataEntries().add(createPathEntry("experiment_id", projectCollectionName));
		pathEntriesExp.getPathMetadataEntries().add(createPathEntry("experiment_date", "Unknown"));
		pathEntriesExp.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Experiment"));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesExp);

		// Set it to dataObjectRegistrationRequestDTO
		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();
		dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
		dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
		dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);

		// Add object metadata
		dataObjectRegistrationRequestDTO.getMetadataEntries()
				.add(createPathEntry("object_name", object.getOrginalFileName()));
		dataObjectRegistrationRequestDTO.getMetadataEntries()
				.add(createPathEntry("source_path", object.getOriginalFilePath()));

		logger.info("LCBG SDS custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
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

	private String getUserId(StatusInfo object) {
		return getCollectionNameFromParent(object, "Cappell-Section");
	}

	private String getPiCollectionName() throws DmeSyncMappingException {
		return getCollectionMappingValue("Cappell-Section", "DataOwner_Lab", "lcbg-sds");
	}

	private String getResearcherCollectionName(String userId) throws DmeSyncMappingException {
		return getCollectionMappingValue(userId, "Researcher", "lcbg-sds");
	}

	private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {
		return StringUtils.substringAfterLast(getExpCollectionName(object), "_");
	}

	private String getExpCollectionName(StatusInfo object) {
		return getCollectionNameFromParent(object, getUserId(object));
	}
}
