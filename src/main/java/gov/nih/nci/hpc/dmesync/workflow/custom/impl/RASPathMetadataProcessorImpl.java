package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

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
 * Default CCR RAS Path and Meta-data Processor Implementation
 * 
 * @author konerum3
 *
 */
@Service("ras")
public class RASPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
		implements DmeSyncPathMetadataProcessor {

	@Value("${dmesync.doc.name}")
	private String doc;

	@Value("${dmesync.source.base.dir}")
	protected String sourceBaseDir;

	@Value("${dmesync.additional.metadata.excel:}")
	private String metadataFile;

	// DOC CCR RAS logic for DME path construction and meta data creation

	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

		logger.info("[PathMetadataTask] RAS getArchivePath called");

		String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
		String tarFilePath = object.getOriginalFilePath();
		String archivePath = destinationBaseDir + "/PI_" + getPICollectionName(object) + "/Project_"
				+ getProjectCollectionName(object,tarFilePath) + "/Run_" + getRunID(object,tarFilePath) + "/" + fileName;
		// replace spaces with underscore
		archivePath = archivePath.replace(" ", "_");
		
		logger.info("RAS ArchviePath details ", archivePath);

		return archivePath;
	}

	@Override
	public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException {

		logger.info("[PathMetadataTask] RAS getMetaDataJson called");
		String tarFilePath = object.getOriginalFilePath();

		  
		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();

		// Add to HpcBulkMetadataEntries for path attributes
		HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

		// Add path metadata entries for "DataOwner_Lab" collection
		String piCollectionName = getPICollectionName(object);
		String projectCollectionName = getProjectCollectionName(object,tarFilePath);
		String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;
		HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "PI_Lab"));
		pathEntriesPI.setPath(piCollectionPath);
	    hpcBulkMetadataEntries.getPathsMetadataEntries().add(populateStoredMetadataEntries(pathEntriesPI, "PI_Lab", piCollectionName, "ras"));

		// Add path metadata entries for "Project" collection
		String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;
		projectCollectionPath = projectCollectionPath.replace(" ", "_");

		HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc", "Trent_Balius"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc_affiliation", "CRTP"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc_email", "trent.balius@nih.gov"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_start_date", getAttrValueWithExactKey(tarFilePath, "project_start_date")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_title", getAttrValueWithExactKey(tarFilePath, "project_title")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_description", getAttrValueWithExactKey(tarFilePath, "project_description")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Closed Access"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("origin", getAttrValueWithExactKey(tarFilePath, "origin")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("target_protein", getAttrValueWithExactKey(tarFilePath, "target_protein")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("method", getAttrValueWithExactKey(tarFilePath, "method")));
		pathEntriesProject.setPath(projectCollectionPath);
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);
		
		// Add path metadata entries for "Case" folder
		String runId = getRunID(object,tarFilePath);
		String runCollectionPath = projectCollectionPath + "/Run_" + runId;
		runCollectionPath = runCollectionPath.replace(" ", "_");

		HpcBulkMetadataEntry pathEntriesRun = new HpcBulkMetadataEntry();
		pathEntriesRun.setPath(runCollectionPath);
		pathEntriesRun.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Run"));
		pathEntriesRun.getPathMetadataEntries().add(createPathEntry("calculation_type", getAttrValueWithExactKey(tarFilePath, "calculation_type")));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesRun);

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
		dataObjectRegistrationRequestDTO.getMetadataEntries()
		.add(createPathEntry("run_date", getAttrValueWithExactKey(tarFilePath, "run_date")));
		dataObjectRegistrationRequestDTO.getMetadataEntries()
		.add(createPathEntry("run_description", getAttrValueWithExactKey(tarFilePath, "run_description")));
		dataObjectRegistrationRequestDTO.getMetadataEntries()
		.add(createPathEntry("software_version", getAttrValueWithExactKey(tarFilePath, "software_version")));
		dataObjectRegistrationRequestDTO.getMetadataEntries()
		.add(createPathEntry("database_size", getAttrValueWithExactKey(tarFilePath, "database_size")));
		dataObjectRegistrationRequestDTO.getMetadataEntries()
		.add(createPathEntry("database_date", getAttrValueWithExactKey(tarFilePath, "database_date")));

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

	private String getPICollectionName(StatusInfo object) throws DmeSyncMappingException {
		return "Trent_Balius";
	}

	private String getProjectCollectionName(StatusInfo object, String tarFilePath) throws DmeSyncMappingException {
		String projectId = getAttrValueWithExactKey(tarFilePath, "project_id");
        logger.info("project name {}",projectId);	
        return projectId;
	}

	private String getRunID(StatusInfo object, String tarFilePath) throws DmeSyncMappingException {
		String runId = getAttrValueWithExactKey(tarFilePath, "run_name");
		logger.info("Run Name {}",runId);
		//return runId;
		return "dock prep";
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
		if ("ras".equalsIgnoreCase(doc)) {
			try {
				// load the user metadata from the externally placed excel
				metadataMap = loadMetadataFile(metadataFile, "Tar_file_location");
			} catch (DmeSyncMappingException e) {
				logger.error("Failed to initialize metadata  path metadata processor", e);
			}
		}
	}

}
