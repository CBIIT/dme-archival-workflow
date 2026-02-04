package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.File;
import java.io.IOException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;

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
 * Machida Lab Custom DME Path and Meta-data Processor Implementation
 *
 */
@Service("dtb")
public class DTBPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
		implements DmeSyncPathMetadataProcessor {

	@Autowired
	private DmeMetadataBuilder dmeMetadataBuilder;

	@Value("${dmesync.additional.metadata.excel:}")
	private String metadataFile;

	@Value("${dmesync.source.base.dir}")
	private String syncBaseDir;

	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {
		logger.info("[PathMetadataTask] DTB getArchivePath called");

		String sourcePath = object.getOriginalFilePath();
		String fileName = Paths.get(sourcePath).toFile().getName();
		String archivePath = null;
		Path fullPath = Paths.get(sourcePath);
		String piCollectionName = getPiCollectionName();
		String projectcollectionName = getProjectcollectionName(fullPath);

		if (StringUtils.equalsIgnoreCase(projectcollectionName, "NGS")) {
			String runId = getCollectionNameFromParent(fullPath, "NGS"); // CS039095
			String subdir = getCollectionNameFromParent(fullPath, runId); // e.g., Analysis_combined, Raw_pod5,
																			// Fastq_combined

			if ("Analysis_Combined".equalsIgnoreCase(subdir)) {
				archivePath = destinationBaseDir + "/PI_" + piCollectionName + "/Project_" + projectcollectionName
						+ "/Run_" + runId + "/" + subdir + "/" + fileName;
			} else if ("Fastq_Combined".equalsIgnoreCase(subdir)) {
				archivePath = destinationBaseDir + "/PI_" + piCollectionName + "/Project_" + projectcollectionName
						+ "/Run_" + runId + "/" + subdir + "/" + fileName;
			} else if ("Raw_Pod5".equalsIgnoreCase(subdir)) {
				String podType = getCollectionNameFromParent(fullPath, "Raw_pod5");
				if (StringUtils.containsIgnoreCase(podType, "CV")) {
					archivePath = destinationBaseDir + "/PI_" + piCollectionName + "/Project_" + projectcollectionName
							+ "/Run_" + runId + "/" + subdir + "/Control_Version_" + podType + "/" + fileName;
				} else if (StringUtils.containsIgnoreCase(podType, "BSC")) {
					archivePath = destinationBaseDir + "/PI_" + piCollectionName + "/Project_" + projectcollectionName
							+ "/Run_" + runId + "/" + subdir + "/BaseCalling_Model_" + podType + "/" + fileName;
				}
			}
		} else if (StringUtils.equalsIgnoreCase(projectcollectionName, "CryoEM")) {
			String runId = getCollectionNameFromParent(fullPath, "CryoEM"); // 20250409
			String subdir = getCollectionNameFromParent(fullPath, runId); // movies or process

			if ("Movies".equalsIgnoreCase(subdir)) {
				archivePath = destinationBaseDir + "/PI_" + piCollectionName + "/Project_" + projectcollectionName
						+ "/Run_" + runId + "/" + subdir + "/" + fileName;
			} else if ("process".equalsIgnoreCase(subdir)) {
				archivePath = destinationBaseDir + "/PI_" + piCollectionName + "/Project_" + projectcollectionName
						+ "/Run_" + runId + "/" + subdir + "/" + fileName;
			}
		}

		else if (StringUtils.equalsIgnoreCase(projectcollectionName, "Xtal")) {
			String runDate = getCollectionNameFromParent(fullPath, "Xtal");
			String subdir = getCollectionNameFromParent(fullPath, runDate);
			archivePath = destinationBaseDir + "/PI_" + piCollectionName + "/Project_" + projectcollectionName + "/Run_"
					+ runDate + "/" + subdir + "/" + fileName;

		}

		if (archivePath == null) {
			throw new DmeSyncMappingException("Unrecognized source path for MachidaLab mapping: " + sourcePath);
		}

		// replace spaces with underscore
		archivePath = archivePath.replace(" ", "_");

		logger.info("Archive path for {} : {}", object.getOriginalFilePath(), archivePath);

		return archivePath;
	}

	@Override
	public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {

		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();
		try {

			// find the metadataFilePathKey to use as key value for the metadata from file.
			String sourcePath = object.getOriginalFilePath();
			Path fullPath = Paths.get(sourcePath);
			String piCollectionName = getPiCollectionName();
			String projectCollectionName = getProjectcollectionName(fullPath);
			String metadataFilePathKey = getPathForMetadata(fullPath);

			logger.info("metadataFileKey {} ", metadataFilePathKey);
			metadataFile = resolveMetadataFile(syncBaseDir,metadataFile );
			// load the user metadata from the externally placed excel
			metadataMap = dmeMetadataBuilder.getMetadataMap(metadataFile, "Path");

			// Add to HpcBulkMetadataEntries for path attributes
			HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

			// Add path metadata entries for "Lab_XXX" collection
			// Example row: collectionType - DataOwner_Lab, collectionName - Javed Khan
			// (supplied)
			// key = data_owner_name, value = ? (supplied)
			// key = data_owner_affiliation, value = ? (supplied)
			// key = data_owner_email, value = ? (supplied) , data_owner designee,
			// affiliation, email
			// data_generator, affiliation, email

			String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName.replace(" ", "_");
			HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "DataOwner_Lab"));
			pathEntriesPI.setPath(piCollectionPath);
			pathEntriesPI.getPathMetadataEntries()
					.add(createPathEntry("data_owner", getAttrValueFromMetadataMap(metadataFilePathKey, "data_owner")));
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_email",
					getAttrValueFromMetadataMap(metadataFilePathKey, "data_owner_email")));
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_affiliation",
					getAttrValueFromMetadataMap(metadataFilePathKey, "data_owner_affiliation")));

			pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_generator",
					getAttrValueFromMetadataMap(metadataFilePathKey, "data_generator")));
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_generator_affiliation",
					getAttrValueFromMetadataMap(metadataFilePathKey, "data_generator_affiliation")));
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_generator_email",
					getAttrValueFromMetadataMap(metadataFilePathKey, "data_generator_email")));

			if (getAttrValueFromMetadataMap(metadataFilePathKey, "data_owner_designee") != null) {
				pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_designee",
						getAttrValueFromMetadataMap(metadataFilePathKey, "data_owner_designee")));
				pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_designee_email",
						getAttrValueFromMetadataMap(metadataFilePathKey, "data_owner_designee_email")));
				pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_designee_affiliation",
						getAttrValueFromMetadataMap(metadataFilePathKey, "data_owner_designee_affiliation")));
			}

			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPI);

			// Add path metadata entries for "Project_XXX" collection
			// Example row: collectionType - Project, collectionName - supplied

			String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;
			HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
			pathEntriesProject.getPathMetadataEntries().add(
					createPathEntry("project_poc", getAttrValueFromMetadataMap(metadataFilePathKey, "project_poc")));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc_affiliation",
					getAttrValueFromMetadataMap(metadataFilePathKey, "project_poc_affiliation")));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc_email",
					getAttrValueFromMetadataMap(metadataFilePathKey, "project_poc_email")));
			if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "key_collaborator")))
				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("key_collaborator",
						getAttrValueFromMetadataMap(metadataFilePathKey, "key_collaborator")));
			if (StringUtils
					.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "key_collaborator_affiliation")))
				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("key_collaborator_affiliation",
						getAttrValueFromMetadataMap(metadataFilePathKey, "key_collaborator_affiliation")));
			if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "key_collaborator_email")))
				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("key_collaborator_email",
						getAttrValueFromMetadataMap(metadataFilePathKey, "key_collaborator_email")));

			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_title", projectCollectionName));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_id", projectCollectionName));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_start_date",
					getAttrValueFromMetadataMap(metadataFilePathKey, "project_start_date"), "MM/dd/yy"));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_description",
					getAttrValueFromMetadataMap(metadataFilePathKey, "project_description")));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("platform_name",
					getAttrValueFromMetadataMap(metadataFilePathKey, "platform_name")));
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("organism", getAttrValueFromMetadataMap(metadataFilePathKey, "organism")));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("study_disease",
					getAttrValueFromMetadataMap(metadataFilePathKey, "study_disease")));
			pathEntriesProject.getPathMetadataEntries().add(
					createPathEntry("is_cell_line", getAttrValueFromMetadataMap(metadataFilePathKey, "is_cell_line")));

			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("data_generating_facility",
					getAttrValueFromMetadataMap(metadataFilePathKey, "data_generating_facility")));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_status", "Active"));

			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Controlled Access"));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("retention_years", "7"));

			// Optional Values
			if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "project_completed_date")))
				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_completed_date",
						getAttrValueFromMetadataMap(metadataFilePathKey, "project_completed_date"), "MM/dd/yy"));
			if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "pubmed_id")))
				pathEntriesProject.getPathMetadataEntries().add(
						createPathEntry("pubmed_id", getAttrValueFromMetadataMap(metadataFilePathKey, "pubmed_id")));
			if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "Collaborators")))
				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("Collaborators",
						getAttrValueFromMetadataMap(metadataFilePathKey, "Collaborators")));

			pathEntriesProject.setPath(projectCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);

			// Add path metadata entries for "Run_XXX" collection
			if (StringUtils.equalsIgnoreCase(projectCollectionName, "NGS")) {
				String runId = getCollectionNameFromParent(fullPath, "NGS"); // CS039095
				String subdir = getCollectionNameFromParent(fullPath, runId); // e.g., Analysis_combined, Raw_pod5,
				String runNamePath = projectCollectionPath + "/Run_" + runId.replace(" ", "_");
				HpcBulkMetadataEntry pathEntriesRunName = new HpcBulkMetadataEntry();
				pathEntriesRunName.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Run"));
				pathEntriesRunName.getPathMetadataEntries().add(createPathEntry("run_id", runId));
				pathEntriesRunName.getPathMetadataEntries()
						.add(createPathEntry("run_date", getAttrValueFromMetadataMap(metadataFilePathKey, "run_date")));
				pathEntriesRunName.getPathMetadataEntries().add(createPathEntry("instrument_name",
						getAttrValueFromMetadataMap(metadataFilePathKey, "instrument_name")));
				pathEntriesRunName.getPathMetadataEntries().add(createPathEntry("library_strategy",
						getAttrValueFromMetadataMap(metadataFilePathKey, "library_strategy")));// Fastq_combined
				pathEntriesRunName.getPathMetadataEntries().add(createPathEntry("analyte_type",
						getAttrValueFromMetadataMap(metadataFilePathKey, "analyte_type")));
				pathEntriesRunName.getPathMetadataEntries()
						.add(createPathEntry("tissue", getAttrValueFromMetadataMap(metadataFilePathKey, "tissue")));
				pathEntriesRunName.getPathMetadataEntries().add(createPathEntry("tissue_type",
						getAttrValueFromMetadataMap(metadataFilePathKey, "tissue_type")));
				pathEntriesRunName.setPath(runNamePath);
				hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesRunName);

				// Add path metadata entries for "Raw_data" collection
				if ("Raw_Pod5".equalsIgnoreCase(subdir)) {
					String podType = getCollectionNameFromParent(fullPath, "Raw_pod5");
					String podPath = runNamePath + "/" + subdir.replace(" ", "_");
					HpcBulkMetadataEntry pathEntriesPod = new HpcBulkMetadataEntry();
					HpcBulkMetadataEntry pathEntriesPod5 = new HpcBulkMetadataEntry();
					pathEntriesPod.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Raw_Data"));
					pathEntriesPod.setPath(podPath);
					hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPod);

					if (StringUtils.containsIgnoreCase(podType, "CV")) {
						String pod5Path = podPath + "/Control_Version_" + podType.replace(" ", "_");
						pathEntriesPod5.getPathMetadataEntries()
								.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Folder"));
						pathEntriesPod5.setPath(pod5Path);
					} else if (StringUtils.containsIgnoreCase(podType, "BSC")) {
						String pod5Path = podPath + "/BaseCalling_Model_" + podType.replace(" ", "_");
						pathEntriesPod5.getPathMetadataEntries()
								.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Folder"));
						pathEntriesPod5.setPath(pod5Path);
					}
					hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPod5);

				}
				// Add path metadata entries for "Analysis" collection
				else if ("Analysis_Combined".equalsIgnoreCase(subdir)) {
					String analysisPath = runNamePath + "/" + subdir.replace(" ", "_");
					HpcBulkMetadataEntry pathEntriesAnalysis = new HpcBulkMetadataEntry();
					pathEntriesAnalysis.getPathMetadataEntries()
							.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Analysis"));
					pathEntriesAnalysis.setPath(analysisPath);
					hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesAnalysis);

				} else if ("Fastq_Combined".equalsIgnoreCase(subdir)) {
					String fastqPath = runNamePath + "/" + subdir.replace(" ", "_");
					HpcBulkMetadataEntry pathEntriesFastq = new HpcBulkMetadataEntry();
					pathEntriesFastq.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "FASTQ"));
					pathEntriesFastq.setPath(fastqPath);
					hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesFastq);
				}
				// Add path metadata entries for "Run_XXX" collection for Project CryoEM and
				// Xtal
			} else if (StringUtils.equalsIgnoreCase(projectCollectionName, "CryoEM")
					|| StringUtils.equalsIgnoreCase(projectCollectionName, "Xtal")) {
				String runId = getCollectionNameFromParent(fullPath, projectCollectionName); // 20250409
				String runNamePath = projectCollectionPath + "/Run_" + runId.replace(" ", "_");
				HpcBulkMetadataEntry pathEntriesRunName = new HpcBulkMetadataEntry();
				pathEntriesRunName.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Run"));
				pathEntriesRunName.getPathMetadataEntries().add(createPathEntry("run_id", runId));
				pathEntriesRunName.getPathMetadataEntries()
						.add(createPathEntry("run_name", getAttrValueFromMetadataMap(metadataFilePathKey, "run_name")));
				pathEntriesRunName.getPathMetadataEntries()
						.add(createPathEntry("run_date", getAttrValueFromMetadataMap(metadataFilePathKey, "run_date")));
				pathEntriesRunName.getPathMetadataEntries().add(createPathEntry("instrument_name",
						getAttrValueFromMetadataMap(metadataFilePathKey, "instrument_name")));

				if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "library_strategy")))
					pathEntriesRunName.getPathMetadataEntries().add(createPathEntry("library_strategy",
							getAttrValueFromMetadataMap(metadataFilePathKey, "library_strategy")));

				if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "analyte_type")))
					pathEntriesRunName.getPathMetadataEntries().add(createPathEntry("analyte_type",
							getAttrValueFromMetadataMap(metadataFilePathKey, "analyte_type")));

				if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "tissue")))
					pathEntriesRunName.getPathMetadataEntries()
							.add(createPathEntry("tissue", getAttrValueFromMetadataMap(metadataFilePathKey, "tissue")));

				if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "tissue_type")))
					pathEntriesRunName.getPathMetadataEntries().add(createPathEntry("tissue_type",
							getAttrValueFromMetadataMap(metadataFilePathKey, "tissue_type")));

				pathEntriesRunName.setPath(runNamePath);
				hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesRunName);

				String runDate = getCollectionNameFromParent(fullPath, projectCollectionName);
				String subdir = getCollectionNameFromParent(fullPath, runDate);
				String rawDataPath = runNamePath + "/" + subdir.replace(" ", "_");
				HpcBulkMetadataEntry pathEntriesRawData = new HpcBulkMetadataEntry();
				pathEntriesRawData.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Raw_Data"));
				pathEntriesRawData.setPath(rawDataPath);
				hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesRawData);
			}

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

		}
		logger.info("DTB Machida lab custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}",
				object.getId());
		return dataObjectRegistrationRequestDTO;
	}

	/**
	 * Utility to extract the directory name that follows directly after the given
	 * parent folder. E.g., for path /a/b/c/d.txt and parentName = "b", returns "c".
	 */
	private String getCollectionNameFromParent(Path fullPath, String parentName) {
		int count = fullPath.getNameCount();
		logger.info("Full File Path = {}", fullPath);
		for (int i = 0; i < count - 1; i++) {
			if (fullPath.getName(i).toString().equals(parentName) && (i + 1 < count)) {
				return fullPath.getName(i + 1).toString();
			}
		}
		return null;
	}

	private Path getCollectionPathFromParent(Path fullFilePath, String parentName) {
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

	private String getProjectcollectionName(Path fullPath) {
		String projectCollectionName = null;
		projectCollectionName = getCollectionNameFromParent(fullPath, "Machida_lab");
		logger.info("projectCollectionName: {}", projectCollectionName);
		return projectCollectionName;

	}

	public String getPathForMetadata(Path fullPath) {
		// Path key is Run level /data/Machida_lab/CryoEM/202504
		String metadataKeypath = getCollectionPathFromParent(fullPath,
				getCollectionNameFromParent(fullPath, "Machida_lab")).toString();

		// Normalize known roots to /data
		int idx = metadataKeypath.indexOf("/Machida_lab/");
		if (idx >= 0) {
			return "/data" + metadataKeypath.substring(idx);
		}
		return metadataKeypath;
	}

	private String getPiCollectionName() {
		return "Yuichi_Machida";
	}

	private String resolveMetadataFile(String directory, String propertyPrefix)
			throws DmeSyncMappingException, IOException {

		File dir = new File(directory);
		File[] files = dir.listFiles((d, name) -> name.startsWith(propertyPrefix) && name.endsWith(".xlsx"));
		if (files != null && files.length > 0) {
			// Sort by last modified time, descending
	        Arrays.sort(files, Comparator.comparing(File::lastModified).reversed());
	        return files[0].getAbsolutePath(); // Most recently modified file
		}

		return null; // or throw
	}
}
