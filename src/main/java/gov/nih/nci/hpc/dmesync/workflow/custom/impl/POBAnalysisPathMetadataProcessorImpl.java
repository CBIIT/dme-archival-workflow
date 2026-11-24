package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;


import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.domain.DocConfig.SourceConfig;
import gov.nih.nci.hpc.dmesync.domain.DocConfig.SourceRule;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.util.DmeMetadataBuilder;
import gov.nih.nci.hpc.dmesync.util.WorkflowConstants;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncPathMetadataProcessor;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntries;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;

/**
 * Custom DME Path and Meta-data Processor Implementation
 * POB_CCDI_Analysis_Archive konerum3
 */

@Service("pob-tegs")
public class POBAnalysisPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
		implements DmeSyncPathMetadataProcessor {

	public static final String CCRCCDI_ANALYSIS = "CCRCCDI_ANALYSIS";
	public static final String SHERNDATA_ANALYSIS = "ShernData_Analysis";

	@Autowired
	private DmeMetadataBuilder dmeMetadataBuilder;

	@Override
	public String getArchivePath(StatusInfo object, DocConfig config)
			throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {
		logger.info("[PathMetadataTask] POB Analysis getArchivePath called");

		SourceConfig sourceConfig = config.getSourceConfig();
		SourceRule sourceRule = config.getSourceRule();
		// load the user metadata from the externally placed excel
		metadataMap = dmeMetadataBuilder.getMetadataMap(sourceRule.metadataFile, "path");
        // load the archive metadata entries from the DMEModel
		metaDataEntries = dmeMetadataBuilder.getDMEMetadataModel(sourceConfig.destinationBaseDir, config);
		String originalFilePath = object.getOriginalFilePath();
		String fileName = object.getSourceFileName();
		String archivePath = null;

		Path fullPath = Paths.get(originalFilePath);
		String pobAnalysisType = getPOBAnalysisType(fullPath);
		
		if (pobAnalysisType == null) {
			String msg = messageService.get("VALIDATION_001");
			logger.error(msg + "Couldn't extract the POB Analysis type for the source Path " + originalFilePath);
			throw new DmeSyncMappingException(msg);
		}
		String metadataFilePathKey = getPathForMetadata(fullPath, pobAnalysisType);
		String piCollectionName = getPiCollectionName(metadataFilePathKey);
		String projectcollectionName = getProjectcollectionName(fullPath, metadataFilePathKey);
		String subcollectionType = getSubCollectionName(fullPath, pobAnalysisType);

		if (projectcollectionName != null && subcollectionType != null) {
			archivePath = sourceConfig.destinationBaseDir + "/PI_" + piCollectionName + "/Project_"
					+ projectcollectionName + "/" + subcollectionType + "/" + fileName;
		}

		if (archivePath == null) {
			String msg = messageService.get("VALIDATION_001");
			logger.error("Couldn't extract the DME Path for the source Path " + originalFilePath + " " + msg);
			throw new DmeSyncMappingException(msg);
		}

		// replace spaces with underscore
		archivePath = archivePath.replace(" ", "_");

		logger.info("Archive path for {} : {}", object.getOriginalFilePath(), archivePath);

		return archivePath;
	}

	@Override
	public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object, DocConfig config)
			throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {

		SourceConfig sourceConfig = config.getSourceConfig();
		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();
		try {

			// find the metadataFilePathKey to use as key value for the metadata from file.
			String originalFilePath = object.getOriginalFilePath();
			Path fullPath = Paths.get(originalFilePath);
			String pobAnalysisType = getPOBAnalysisType(fullPath);
			String metadataFilePathKey = getPathForMetadata(fullPath, pobAnalysisType);
			String piCollectionName = getPiCollectionName(metadataFilePathKey);
			String projectCollectionName = getProjectcollectionName(fullPath, metadataFilePathKey);
			String fileName = object.getSourceFileName();

			// Add to HpcBulkMetadataEntries for path attributes
			HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

			// Add path metadata entries for "PI_XXX" collection
			// Example row: collectionType - DataOwner_Lab, collectionName -
			// (supplied)
			// key = data_owner_name, value = ? (supplied)
			// key = data_owner_affiliation, value = ? (supplied)
			// key = data_owner_email, value = ? (supplied) , data_owner designee,
			// affiliation, email
			// data_generator, affiliation, email
			// Add path metadata entries for "DataOwner_Lab" collection
			String piCollectionPath = sourceConfig.destinationBaseDir + "/PI_" + piCollectionName;
			HpcBulkMetadataEntry pathEntriesPI = buildPathEntries(piCollectionPath.replace(" ", "_"), "DataOwner_Lab",
					metadataFilePathKey, metaDataEntries);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPI);

			// Add path metadata entries for "Project" collection
			String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;
			HpcBulkMetadataEntry pathEntriesProject = buildPathEntries(projectCollectionPath.replace(" ", "_"),
					"Project", metadataFilePathKey, metaDataEntries);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);

			String subcollectionType = getSubCollectionName(fullPath, pobAnalysisType);

			String subcollectionPath = projectCollectionPath + "/" + subcollectionType;
			HpcBulkMetadataEntry pathEntriesSubCollection = new HpcBulkMetadataEntry();

			if (StringUtils.equalsIgnoreCase("rawdata", subcollectionType)) {
				pathEntriesSubCollection.getPathMetadataEntries()
						.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Raw_Data"));
			} else {
				pathEntriesSubCollection.getPathMetadataEntries()
						.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Analysis"));
			}
			pathEntriesSubCollection.getPathMetadataEntries().add(createPathEntry("dataset_description",
					getAttrValueFromMetadataMap(metadataFilePathKey, "dataset_description")));

			pathEntriesSubCollection.setPath(subcollectionPath.replace(" ", "_"));
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesSubCollection);

			// Set it to dataObjectRegistrationRequestDTO
			dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
			dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
			dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);

			// Add object metadata
			dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("object_name", fileName));
			dataObjectRegistrationRequestDTO.getMetadataEntries()
					.add(createPathEntry("source_path", object.getOriginalFilePath()));
			dataObjectRegistrationRequestDTO.getMetadataEntries()
					.add(createPathEntry("file_type", getFileType(fileName)));

		} finally {

		}
		logger.info("POB Analysis lab custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}",
				object.getId());
		return dataObjectRegistrationRequestDTO;
	}


	private String getProjectcollectionName(Path fullPath, String metadataFilePathKey) {
		String projectCollectionName = null;
		projectCollectionName = getAttrValueFromMetadataMap(metadataFilePathKey, "project_id");
		logger.info("projectCollectionName: {}", projectCollectionName);
		return projectCollectionName;

	}

	private String getSubCollectionName(Path fullPath, String pobAnalysisType) {
	    String subCollectionName = CCRCCDI_ANALYSIS.equalsIgnoreCase(pobAnalysisType)
	            ? getAdjacentPathElement(fullPath, "CCRCCDI", true)
	            : getAdjacentPathElement(fullPath, "DME_archive", false);
	    logger.info("subCollectionName: {}", subCollectionName);
	    return capitalizeFirst(subCollectionName);
	}

	// There are two types of data sources , each goes to the different archive .
	// however the hierarchy is same
	private String getPOBAnalysisType(Path fullPath) {
	    String pobAnalysisType = "CCRCCDI".equalsIgnoreCase(getAdjacentPathElement(fullPath, "data", true))
	            ? CCRCCDI_ANALYSIS : "ShernData".equalsIgnoreCase(getAdjacentPathElement(fullPath, "data", true)) ? SHERNDATA_ANALYSIS: null;
	    logger.info("Pob Analysis type: {}", pobAnalysisType);
	    return pobAnalysisType;
	}
	
	/**
	 * Returns the path element adjacent to the first occurrence of {@code anchor}.
	 * <ul>
	 *   <li>{@code after=true}: the element that follows it, e.g. "rawdata" for "CCRCCDI" in /data/CCRCCDI/rawdata/x.txt.</li>
	 *   <li>{@code after=false}: the element that precedes it, e.g. "analysis" for "DME_archive" in /data/ShernData/analysis/DME_archive/x.txt.</li>
	 * </ul>
	 * The path is made absolute and normalized before matching. Returns null if the anchor is absent or has no
	 * neighbour in the requested direction (for example, the anchor is the last element and {@code after=true}).
	 */
	private String getAdjacentPathElement(Path path, String anchor, boolean after) {
	    Path p = path.toAbsolutePath().normalize();
	    for (int i = 0; i < p.getNameCount(); i++) {
	        if (StringUtils.equals(p.getName(i).toString(), anchor)) {
	            int neighbourIndex = after ? i + 1 : i - 1;
	            return (neighbourIndex >= 0 && neighbourIndex < p.getNameCount()) ? p.getName(neighbourIndex).toString() : null;
	        }
	    }
	    return null;
	}

	public String getPathForMetadata(Path fullPath, String pobAnalysisType) {
		// Path key is parent level eg. /data/CCRCCDI/analysis/ccrtegs1/DME_archive
		String metadataKeypath = fullPath.getParent().toString();
		int idx = 0;
		if (StringUtils.equalsIgnoreCase(pobAnalysisType, CCRCCDI_ANALYSIS))
			idx = metadataKeypath.indexOf(File.separator + "CCRCCDI" + File.separator);
		else
			idx = metadataKeypath.indexOf(File.separator + "ShernData" + File.separator);
		// Normalize known roots to /data
		if (idx >= 0) {
			return "/data" + metadataKeypath.substring(idx);
		}
		return metadataKeypath;
	}

	private String getPiCollectionName(String metadataFilePathKey) throws DmeSyncMappingException {
	    String piName = getAttrValueFromMetadataMap(metadataFilePathKey, "data_owner");
	    if (StringUtils.isBlank(piName)) {
	        throw new DmeSyncMappingException("Data Owner field is missing for " + metadataFilePathKey);
	    }
	    String[] parts = piName.trim().split("\\s*,\\s*", 2);
	    if (parts.length != 2 || StringUtils.isBlank(parts[0]) || StringUtils.isBlank(parts[1])) {
	        throw new DmeSyncMappingException("Expected 'Lastname, Firstname' but found: " + piName);
	    }
	    String formatted = parts[1].trim() + "_" + parts[0].trim();
	    logger.info("piCollectionNameFormatted: {}", formatted);
	    return formatted;
	}

	static String capitalizeFirst(String text) {
	    if (text == null || text.isEmpty()) {
	        return text;
	    }
	    return Character.toUpperCase(text.charAt(0)) + text.substring(1);
	}

	private String getFileType(String fileName) {
		if (fileName.endsWith(WorkflowConstants.tarContentsFileEndswith) || fileName.endsWith(WorkflowConstants.tarExcludedContentsFileEndswith)) {
			return "tarContentsFile";
		}
		if (fileName.endsWith(".tar")) {
			return "tar";
		}
		return "file";
	}

}