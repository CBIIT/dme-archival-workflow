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

	// DOC CCR LCO logic for DME path construction and meta data creation

	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

		logger.info("[PathMetadataTask] LCO getArchivePath called");
		Path filePath = Paths.get(object.getSourceFilePath());
		String metadataFileKey = getMetadataKeyPath(object);

		String fileName = filePath.toFile().getName();
		String archivePath = destinationBaseDir + "/PI_" + getPICollectionName(object) + "/Study_"
				+ getStudyCollectionName(metadataFileKey) + "/Period_" + getPeriodCollectionName(metadataFileKey) + "/"
				+ getJournalCollectionName(metadataFileKey) + "/" + fileName; 
		
		/*String archivePath = destinationBaseDir + "/PI_" + getPICollectionName(object) + "/Study_"
				+ getStudyCollectionName(metadataFileKey) +"/Period_" + getPeriodCollectionName(metadataFileKey)+ "/" + fileName;*/
		// replace spaces with underscore
		archivePath = archivePath.replace(" ", "_");
		archivePath = archivePath.replace("-", "_");

		
		logger.info("[PathMetadataTask] LCO ArchivePath  " + archivePath);

		return archivePath;
	}

	@Override
	public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException {

		logger.info("[PathMetadataTask] LCO getMetaDataJson called");

		Path path = Paths.get(object.getOriginalFilePath());
		String metadataFileKey = getMetadataKeyPath(object);

		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();

		// Add to HpcBulkMetadataEntries for path attributes
		HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

		// Add path metadata entries for "DataOwner_Lab" collection
		String piCollectionName = getPICollectionName(object);
		String studyCollectionName = getStudyCollectionName(metadataFileKey);
		String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;
		HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "DataOwner_Lab"));
		pathEntriesPI.getPathMetadataEntries()
				.add(createPathEntry("data_owner", getAttrValueWithExactKey(metadataFileKey, "data_owner")));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_affiliation",
				getAttrValueWithExactKey(metadataFileKey, "data_owner_affiliation")));
		pathEntriesPI.getPathMetadataEntries()
				.add(createPathEntry("data_owner_email", getAttrValueWithExactKey(metadataFileKey, "data_owner_email")));
		pathEntriesPI.getPathMetadataEntries().add(
				createPathEntry("data_owner_designee", getAttrValueWithExactKey(metadataFileKey, "data_owner_designee")));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_designee_affiliation",
				getAttrValueWithExactKey(metadataFileKey, "data_owner_designee_affiliation")));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_designee_email",
				getAttrValueWithExactKey(metadataFileKey, "data_owner_designee_email")));
		pathEntriesPI.setPath(piCollectionPath);
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPI);

		// Add path metadata entries for "Project" collection
		String projectCollectionPath = piCollectionPath + "/Study_" + studyCollectionName;
		HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_poc", getAttrValueWithExactKey(metadataFileKey, "project_poc")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc_affiliation",
				getAttrValueWithExactKey(metadataFileKey, "project_poc_affiliation")));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_poc_email", getAttrValueWithExactKey(metadataFileKey, "project_poc_email")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_id", studyCollectionName));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_start_date", getAttrValueWithExactKey(metadataFileKey, "project_start_date")));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_title", getAttrValueWithExactKey(metadataFileKey, "project_title")));
		//pathEntriesProject.getPathMetadataEntries().add(
		//		createPathEntry("project_description", getAttrValueWithExactKey(metadataFileKey, "project_description")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("data_generating_facility",
				getAttrValueWithExactKey(metadataFileKey, "data_generating_facility")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("organism", getAttrValueWithExactKey(metadataFileKey, "organism")));
		pathEntriesProject.getPathMetadataEntries()
		.add(createPathEntry("study_disease", getAttrValueWithExactKey(metadataFileKey, "study_disease")));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("data_generator", getAttrValueWithExactKey(metadataFileKey, "data_generator")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("data_generator_affiliation",
				getAttrValueWithExactKey(metadataFileKey, "data_generator_affiliation")));
		pathEntriesProject.getPathMetadataEntries().add(
				createPathEntry("data_generator_email", getAttrValueWithExactKey(metadataFileKey, "data_generator_email")));
		pathEntriesProject.setPath(projectCollectionPath);
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);

		// Add path metadata entries for "Date" folder
		String period = getPeriodCollectionName(metadataFileKey);
		String periodCollectionPath = projectCollectionPath + "/Period_" + period;
		HpcBulkMetadataEntry pathEntriesPeriod = new HpcBulkMetadataEntry();
		pathEntriesPeriod.setPath(periodCollectionPath);
		pathEntriesPeriod.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Date"));
		pathEntriesPeriod.getPathMetadataEntries().add(createPathEntry("year", period));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPeriod);

		// Add path metadata entries for "journal" folder
		String journal = getJournalCollectionName(metadataFileKey);
		String journalCollectionPath = periodCollectionPath + "/" + journal;
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
		//dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("object_name", fileName));
		//dataObjectRegistrationRequestDTO.getMetadataEntries()
		//		.add(createPathEntry("source_path", object.getOriginalFilePath()));

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
		String studyName = getAttrValueWithExactKey(metadataPathKey, "study_name");
		logger.info("Study name = {}", studyName);
		return studyName;
	}

	private String getPeriodCollectionName(String metadataPathKey) throws DmeSyncMappingException {
		String periodYear = getAttrValueWithExactKey(metadataPathKey, "period");
		logger.info("period year = {}", periodYear);
		return periodYear;
	}

	private String getJournalCollectionName(String metadataPathKey) throws DmeSyncMappingException {
		String journalName = getAttrValueWithExactKey(metadataPathKey, "journal_name");
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

	public String getAttrValueWithExactKey(String key, String attrKey) {
		if (StringUtils.isEmpty(key)) {
			logger.error("Excel mapping not found for {}", key);
			return null;
		}
		return (metadataMap.get(key) == null ? null : metadataMap.get(key).get(attrKey));
	}

	@PostConstruct
	private void init() throws IOException {
		if ("lco".equalsIgnoreCase(doc)) {
			try {
				// load the user metadata from the externally placed excel
				metadataMap = loadMetadataFile(metadataFile, "path");
			} catch (DmeSyncMappingException e) {
				logger.error("Failed to initialize metadata  path metadata processor", e);
			}
		}
	}

}
