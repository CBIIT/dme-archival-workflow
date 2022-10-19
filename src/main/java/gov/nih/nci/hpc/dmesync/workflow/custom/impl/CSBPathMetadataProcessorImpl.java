package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
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

	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

		logger.info("[PathMetadataTask] CSB getArchivePath called");

		Path filePath = Paths.get(object.getSourceFilePath());
		// load the metadata from the json file
		String metadataFile;
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(filePath.getParent(),
				path -> path.toString().endsWith("archive.json"))) {
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

		Path path = Paths.get(object.getSourceFilePath());
		String fileName = path.getFileName().toString();

		String archivePath = destinationBaseDir + "/PI_" + getPiCollectionName() + "/Project_"
				+ getProjectCollectionName() + "/Run_Raw_" + getRunCollectionName() + "/" + fileName;

		// replace spaces with underscore
		archivePath = archivePath.replace(" ", "_");

		logger.info("Archive path for {} : {}", object.getOriginalFilePath(), archivePath);

		return archivePath;
	}

	@Override
	public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException {

		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();
		try {

			// Add to HpcBulkMetadataEntries for path attributes
			HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

			// Add path metadata entries for "PI_XXX" collection
			// Example row: collectionType - PI, collectionName (derived)
			// key = data_owner, value = (supplied)
			// key = affiliation, value = (supplied)
			// key = data_generator, value = Susan Lea (constant)

			String piCollectionName = getPiCollectionName();
			String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;
			HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "PI_Lab"));
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner", getAttrValueWithKey("run", "ownername")));
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry("affiliation", "placeholder for affiliation"));
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_generator", "Susan Lea"));
			pathEntriesPI.setPath(piCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPI);

			// Add path metadata entries for "Project_XXX" collection
			// Example row: collectionType - Project, collectionName (derived)
			// key = project_id, value = (supplied)
			// key = project_title, value = (supplied)
			// key = project_description, value = (supplied - Optional)
			// key = project_scientist, value = (supplied - Optional)
			// key = start_date, value = (derived)
			// key = access, value = "Closed Access" (default)
			// key = project_status, value = Active (default)
			// key = retention_years, value = 7 (default)

			String projectCollectionName = getProjectCollectionName();
			String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;
			HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
			DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
			String startDate = dateFormat.format(new Date());
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_id", getAttrValueWithKey("run", "description") + "_" + startDate));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_title", getAttrValueWithKey("run", "description")));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_scientist", getAttrValueWithKey("run", "scientist")));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("start_date", startDate));
			pathEntriesProject.setPath(projectCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);

			// Add path metadata entries for "Run_Raw_XXX" collection
			// Example row: collectionType - Run_Raw, collectionName (derived)
			// key = dataset_id, value = 2021_5_4_154630_Krios1_123456789 (supplied)
			// key = dataset_location, value = /data/location/dataset_X (supplied)
			// key = dataset_name, value = dataset_X (supplied)
			// key = microscope_name, value = Krios1 (supplied)
			// key = microscope_serial, value = 123456789 (supplied)
			String runCollectionName = getRunCollectionName();
			String runCollectionPath = projectCollectionPath + "/Run_Raw_" + runCollectionName;
			HpcBulkMetadataEntry pathEntriesRun = new HpcBulkMetadataEntry();
			pathEntriesRun.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Run_Raw"));
			pathEntriesRun.getPathMetadataEntries()
					.add(createPathEntry("dataset_id", getAttrValueWithKey("run", "datasetid")));
			pathEntriesRun.getPathMetadataEntries()
					.add(createPathEntry("dataset_location", getAttrValueWithKey("run", "datasetlocation")));
			pathEntriesRun.getPathMetadataEntries()
					.add(createPathEntry("dataset_name", getAttrValueWithKey("run", "datasetname")));
			pathEntriesRun.getPathMetadataEntries()
					.add(createPathEntry("microscope_name", getAttrValueWithKey("run", "microscopename")));
			pathEntriesRun.getPathMetadataEntries()
					.add(createPathEntry("microscope_serial", getAttrValueWithKey("run", "microscopeserial")));
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

	private String getPiCollectionName() {
		String piCollectionName = getAttrValueWithKey("run", "ownername");
		piCollectionName = piCollectionName.replace(" ", "_");
		return piCollectionName;
	}

	private String getProjectCollectionName() {
		String projectCollectionName = getAttrValueWithKey("run", "description");
		projectCollectionName = projectCollectionName.replace(" ", "_");
		return projectCollectionName;
	}

	private String getRunCollectionName() {
		String runCollectionName = getAttrValueWithKey("run", "datasetname");
		runCollectionName = runCollectionName.replace(" ", "_");
		logger.info("runCollectionName: {}", runCollectionName);
		return runCollectionName;
	}
}