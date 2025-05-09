package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

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
 * Default CCR LCO Path and Meta-data Processor Implementation
 * 
 * @author konerum3
 *
 */
@Service("lco")
public class LCOPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
		implements DmeSyncPathMetadataProcessor {

	@Value("${dmesync.doc.name}")
	private String doc;

	@Value("${dmesync.source.base.dir}")
	protected String sourceBaseDir;

	@Value("${dmesync.additional.metadata.excel:}")
	private String metadataFile;

	// DOC CCR CIO logic for DME path construction and meta data creation

	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

		// load the user metadata from the externally placed excel
		threadLocalMap.set(loadMetadataFile(metadataFile, "path"));
		logger.info("[PathMetadataTask] LCO getArchivePath called");
		String metadataFileKey = getMetadataKeyPath(object);

		String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
		String archivePath = destinationBaseDir + "/PI_" + getPICollectionName(object) + "/Study_"
				+ getStudyCollectionName(metadataFileKey) + "/Period_" + getPeriodCollectionName(metadataFileKey) +
				"/" + getJournalCollectionName(metadataFileKey) + "/" + fileName;
		// replace spaces with underscore
		archivePath = archivePath.replace(" ", "_");
		logger.info("[PathMetadataTask] LCO ArchivePath  " + archivePath);

		return archivePath;
	}

	@Override
	public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException {

		logger.info("[PathMetadataTask] LCO getMetaDataJson called");

		// load the user metadata from the externally placed excel
		threadLocalMap.set(loadMetadataFile(metadataFile, "path"));
		  
		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();

		// Add to HpcBulkMetadataEntries for path attributes
		HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

		// Add path metadata entries for "DataOwner_Lab" collection
		String piCollectionName = getPICollectionName(object);
		String metadataFileKey = getMetadataKeyPath(object);
		String studyCollectionName = getStudyCollectionName(metadataFileKey);

		String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;
		HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "DataOwner_Lab"));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner", getAttrValueWithKey(metadataFileKey, "data_owner")));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_affiliation", getAttrValueWithKey(metadataFileKey, "data_owner_affiliation")));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_email", getAttrValueWithKey(metadataFileKey, "data_owner_email")));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_designee", getAttrValueWithKey(metadataFileKey, "data_generator")));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_designee_affiliation", getAttrValueWithKey(metadataFileKey, "data_generator_affiliation")));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_designee_email", getAttrValueWithKey(metadataFileKey, "data_generator_email")));
		pathEntriesPI.setPath(piCollectionPath);
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPI);

		// Add path metadata entries for "Project" collection
		String projectCollectionPath = piCollectionPath + "/Study_" + studyCollectionName.replace(" ", "_");
		HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_poc", getAttrValueWithKey(metadataFileKey, "project_poc")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc_affiliation",
				getAttrValueWithKey(metadataFileKey, "project_poc_affiliation")));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_poc_email", getAttrValueWithKey(metadataFileKey, "project_poc_email")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_id", studyCollectionName));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_start_date", getAttrValueWithKey(metadataFileKey, "project_start_date"), "MM/dd/yy"));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_title", getAttrValueWithKey(metadataFileKey, "project_title")));
		pathEntriesProject.getPathMetadataEntries().add(
				createPathEntry("project_description", getAttrValueWithKey(metadataFileKey, "project_description")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("data_generating_facility",
				getAttrValueWithKey(metadataFileKey, "data_generating_facility")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("organism", getAttrValueWithKey(metadataFileKey, "organism")));
		pathEntriesProject.getPathMetadataEntries()
		.add(createPathEntry("study_disease", getAttrValueWithKey(metadataFileKey, "study_disease")));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("data_generator", getAttrValueWithKey(metadataFileKey, "data_generator")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("data_generator_affiliation",
				getAttrValueWithKey(metadataFileKey, "data_generator_affiliation")));
		pathEntriesProject.getPathMetadataEntries().add(
				createPathEntry("data_generator_email", getAttrValueWithKey(metadataFileKey, "data_generator_email")));
		pathEntriesProject.setPath(projectCollectionPath);
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);
		
		// Add path metadata entries for "Case" folder
		String period = getPeriodCollectionName(metadataFileKey);
		String periodCollectionPath = projectCollectionPath + "/Period_" + period;
		HpcBulkMetadataEntry pathEntriesCase = new HpcBulkMetadataEntry();
		pathEntriesCase.setPath(periodCollectionPath);
		pathEntriesCase.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Date"));
		pathEntriesCase.getPathMetadataEntries().add(createPathEntry("year", period));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesCase);
		
		// Add path metadata entries for "Journal" folder
		String journal = getJournalCollectionName(metadataFileKey);
		String journalCollectionPath = periodCollectionPath + "/" + journal.replace(" ", "_");
		HpcBulkMetadataEntry pathEntriesJournal = new HpcBulkMetadataEntry();
		pathEntriesJournal.setPath(journalCollectionPath);
		pathEntriesJournal.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Analysis"));
		pathEntriesJournal.getPathMetadataEntries().add(createPathEntry("journal_name", journal));
		pathEntriesJournal.getPathMetadataEntries().add(createPathEntry("pubmed_id", getAttrValueWithExactKey(metadataFileKey, "pubmed_id")));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesJournal);
		

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

	private Path getCollectionNameFromParent(String path, String parentName) {
		Path fullFilePath = Paths.get(path);
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

	private String getPICollectionName(StatusInfo object) throws DmeSyncMappingException {
		return "Hidetaka_Ohnuki";
	}

	private String getStudyCollectionName(String metadataPathKey) throws DmeSyncMappingException {
		String studyName = getAttrValueWithKey(metadataPathKey, "study_name");
		logger.info("Study name = {}", studyName);
		return studyName;
	}

	private String getPeriodCollectionName(String metadataPathKey) throws DmeSyncMappingException {
		String periodYear = getAttrValueWithKey(metadataPathKey, "period");
		logger.info("period year = {}", periodYear);
		return periodYear;
	}

	private String getJournalCollectionName(String metadataPathKey) throws DmeSyncMappingException {
		String journalName = getAttrValueWithKey(metadataPathKey, "journal_name");
		logger.info("Journal Name = {}", journalName);
		return journalName;
	}
	
	public String getMetadataKeyPath(StatusInfo object) {
		// path /mnt/OhnukihBiowulf/Until_2027/01_Nature_Biotechnology/filename 
		// Should return  /mnt/OhnukihBiowulf/Until_2027/01_Nature_Biotechnology/
		Path metadataKeyPath= getCollectionNameFromParent(object.getOriginalFilePath(),(getCollectionNameFromParent(object.getOriginalFilePath(),"OhnukihBiowulf").getFileName().toString()));
		logger.info("metadata key path = {}", metadataKeyPath.toString());
		return metadataKeyPath.toString();

	}

}
