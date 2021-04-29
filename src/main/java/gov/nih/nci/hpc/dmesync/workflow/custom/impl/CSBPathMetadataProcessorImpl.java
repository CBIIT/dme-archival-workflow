package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import org.springframework.beans.factory.annotation.Autowired;
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
 * CSB DME Path and Meta-data Processor Implementation
 *
 * @author dinhys
 */
@Service("csb")
public class CSBPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
		implements DmeSyncPathMetadataProcessor {

	// CSB Custom logic for DME path construction and meta data creation

	@Value("${dmesync.additional.metadata.excel:}")
	private String mappingFile;

	@Autowired
	private EGAMetadataMapper mapper;

	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

		logger.info("[PathMetadataTask] CSB getArchivePath called");

		threadLocalMap.set(loadMetadataFile(mappingFile, "FILE_ACCESSION"));

		Path path = Paths.get(object.getSourceFilePath());
		String fileName = path.getFileName().toString();

		String archivePath = destinationBaseDir + "/PI_" + getPiCollectionName(object) + "/Project_"
				+ getProjectCollectionName(object) + "/Run_Raw_" + getRunCollectionName(object) + "/" + fileName;

		// replace spaces with underscore
		archivePath = archivePath.replace(" ", "_");

		logger.info("Archive path for {} : {}", object.getOriginalFilePath(), archivePath);

		return archivePath;
	}

	@Override
	public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException {

		Path filePath = Paths.get(object.getSourceFilePath());
		// load the metadata from the json file
		String metadataFile;
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(filePath.getParent(),
				path -> path.toString().endsWith(".metadata.json"))) {
			Iterator<Path> it = stream.iterator();
			if (it.hasNext()) {
				metadataFile = it.next().toString();
			} else {
				logger.error("Metadata json file not found for {}", object.getOriginalFilePath());
				throw new DmeSyncMappingException("Metadata json file not found for " + object.getOriginalFilePath());
			}
		} catch (IOException e) {
			logger.error("Metadata excel file not found for {}", object.getOriginalFilePath());
			throw new DmeSyncMappingException("Metadata json file not found for " + object.getOriginalFilePath());
		}

		threadLocalMap.set(loadJsonMetadataFile(metadataFile, "run"));

		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();
		try {

			// Add to HpcBulkMetadataEntries for path attributes
			HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

			// Add path metadata entries for "PI_XXX" collection
			// Example row: collectionType - PI, collectionName - Susan_Lea
			// (derived)
			// key = data_owner, value = Susan Lea (supplied)
			// key = affiliation, value = CSB (supplied)

			String piCollectionName = getPiCollectionName(object);
			String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;
			HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "PI_Lab"));
			pathEntriesPI.setPath(piCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries()
					.add(populateStoredMetadataEntries(pathEntriesPI, "PI_Lab", piCollectionName));

			// Add path metadata entries for "Project_XXX" collection
			// Example row: collectionType - Project, collectionName -
			// CSB_CryoEM (supplied),
			// key = project_id, value = CSB_CryoEM (supplied)
			// key = project_title, value = CSB CryoEM (supplied)
			// key = project_description, value = (supplied)
			// key = start_date, value = (supplied)
			// key = access, value = "Closed Access" (constant)
			// key = project_scientist, value = Joseph Caesar (supplied)
			// key = project_status, value = Active (supplied)
			// key = data_submitter, value = Susan Lea (supplied)

			String projectCollectionName = getProjectCollectionName(object);
			String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;
			HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
			pathEntriesProject.setPath(projectCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries()
					.add(populateStoredMetadataEntries(pathEntriesProject, "Project", projectCollectionName));

			// Add path metadata entries for "Run_Raw_XXX" collection
			// Example row: collectionType - Run_Raw, collectionName -
			// Run_march1721 (derived)
			// key = instrument, value = B469 Microscope (json)
			// key = date, value = (json)
			// key = pi_group, value = (json)
			// key = short_title, value = (json)
			String runCollectionName = getRunCollectionName(object);
			String runCollectionPath = projectCollectionPath + "/Run_Raw_" + runCollectionName;
			HpcBulkMetadataEntry pathEntriesRun = new HpcBulkMetadataEntry();
			pathEntriesRun.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Run_Raw"));
			pathEntriesRun.getPathMetadataEntries()
					.add(createPathEntry("instrument", getAttrValueWithKey("run", "instrument")));
			pathEntriesRun.getPathMetadataEntries()
					.add(createPathEntry("date", getAttrValueWithKey("run", "date")));
			pathEntriesRun.getPathMetadataEntries()
					.add(createPathEntry("pi_group", getAttrValueWithKey("run", "pi_group")));
			pathEntriesRun.getPathMetadataEntries()
					.add(createPathEntry("short_title", getAttrValueWithKey("run", "short_title")));
			pathEntriesRun.setPath(runCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesRun);

			// Set it to dataObjectRegistrationRequestDTO
			dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
			dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
			dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);

			// Add object metadata
			dataObjectRegistrationRequestDTO.getMetadataEntries()
					.add(createPathEntry("object_name", Paths.get(object.getSourceFilePath()).toFile().getName()));
			dataObjectRegistrationRequestDTO.getMetadataEntries()
					.add(createPathEntry("source_path", object.getOriginalFilePath()));

		} finally {
			threadLocalMap.remove();
		}
		logger.info("CSB custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
		return dataObjectRegistrationRequestDTO;
	}

	private String getPiCollectionName(StatusInfo object) throws DmeSyncMappingException {
		String piCollectionName = "Susan_Lea";
		return piCollectionName;
	}

	private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {
		String projectCollectionName = "CSB_CryoEM";
		return projectCollectionName;
	}

	private String getRunCollectionName(StatusInfo object) throws DmeSyncMappingException {
		String runCollectionName = Paths.get(object.getSourceFilePath()).getParent().getFileName().toString();
		logger.info("runCollectionName: {}", runCollectionName);
		return runCollectionName;
	}
}