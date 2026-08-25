package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

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
 * Default CRTP CMDL Path and Meta-data Processor Implementation
 * 
 * @author konerum3
 *
 */
@Service("crtp-cmdl")
public class CRTPCmdlPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
		implements DmeSyncPathMetadataProcessor {

	@Value("${dmesync.doc.name}")
	private String doc;

	@Value("${dmesync.source.base.dir}")
	protected String sourceBaseDir;

	@Value("${dmesync.additional.metadata.excel:}")
	private String metadataFile;

	@Autowired
	private DmeMetadataBuilder dmeMetadataBuilder;

	// DOC CRTP CMDL logic for DME path construction and meta data creation

	@Override
	public String getArchivePath(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {

		logger.info("[PathMetadataTask] DOC CRTP CMDL getArchivePath called");

		// load the user metadata from the externally placed excel
		metadataMap = dmeMetadataBuilder.getMetadataMap(metadataFile, "project");

		metaDataEntries = dmeMetadataBuilder.getDMEMetadataModel(destinationBaseDir);

		String metadataFileKey = getMetadataFileKey(object);

		String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
		String collectionName = getCollectionName(object);
		String archivePath = null;
		if (collectionName != null) {
			if (StringUtils.equalsIgnoreCase("hla_custom_refs", collectionName)) {
				archivePath = destinationBaseDir + "/PI_" + getPICollectionName(object) + "/Project_"
						+ getProjectCollectionName(object, metadataFileKey) + "/Reference/" + fileName;
			} else if (StringUtils.equalsIgnoreCase("PGXI", collectionName)) {
				archivePath = destinationBaseDir + "/PI_" + getPICollectionName(object) + "/Project_"
						+ getProjectCollectionName(object, metadataFileKey) + "/PGXI/";
				String folderName = getCollectionNameFromParent(object.getOriginalFilePath(), collectionName);
				if (folderName != null && StringUtils.equalsIgnoreCase("Shared", folderName)) {
					archivePath += "Shared/" + fileName;
				} else {
					archivePath += fileName;
				}

			} else if (StringUtils.equalsIgnoreCase("Validation", collectionName)) {
				archivePath = destinationBaseDir + "/PI_" + getPICollectionName(object) + "/Project_"
						+ getProjectCollectionName(object, metadataFileKey) + "/Validation/" + fileName;
			} else if (StringUtils.equalsIgnoreCase("Pre-Validation Analysis", collectionName)) {
				String batchName = getCollectionNameFromParent(object.getOriginalFilePath(), collectionName);
				String parentName = getCollectionNameFromParent(object.getOriginalFilePath(), batchName);
				if (StringUtils.equalsIgnoreCase("cram", parentName)) {
					archivePath = destinationBaseDir + "/PI_" + getPICollectionName(object) + "/Project_"
							+ getProjectCollectionName(object, metadataFileKey) + "/Pre_Validation_Analysis/"
							+ batchName + "/" + parentName + "/" + fileName;
				} else {
					archivePath = destinationBaseDir + "/PI_" + getPICollectionName(object) + "/Project_"
							+ getProjectCollectionName(object, metadataFileKey) + "/Pre_Validation_Analysis/"
							+ batchName + "/" + fileName;
				}
			}
		}

		if (archivePath == null) {
			String msg = messageService.get("VALIDATION_001");
			logger.error(
					"Couldn't extract the DME Path for the source Path " + object.getOriginalFilePath() + " " + msg);
			throw new DmeSyncMappingException(msg);
		}
		// replace spaces with underscore
		archivePath = archivePath.replace(" ", "_");

		logger.info("[PathMetadataTask] ArchivePath {} ", archivePath);
		return archivePath;
	}

	@Override
	public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException {

		logger.info("[PathMetadataTask] DOC CRTP CMDL getMetaDataJson called");

		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();

		// Add to HpcBulkMetadataEntries for path attributes
		HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();
		String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
		String metadataFileKey = getMetadataFileKey(object);
		// Add path metadata entries for "DataOwner_Lab" collection
		String piCollectionName = getPICollectionName(object);
		String projectCollectionName = getProjectCollectionName(object, metadataFileKey);
		String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName.replace(" ", "_");
		HpcBulkMetadataEntry pathEntriesPI = buildPathEntries(piCollectionPath, "DataOwner_Lab", metadataFileKey,
				metaDataEntries);
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPI);

		// Add path metadata entries for "Project" collection
		String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName.replace(" ", "_");
		HpcBulkMetadataEntry pathEntriesProject = buildPathEntries(projectCollectionPath, "Project", metadataFileKey,
				metaDataEntries);
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);

		// Add path metadata entries for folder
		String collectionName = getCollectionName(object);
		String collectionPath = projectCollectionPath + "/";
		HpcBulkMetadataEntry pathEntriesCollection = new HpcBulkMetadataEntry();
		if (collectionName != null) {

			if (StringUtils.equalsIgnoreCase("hla_custom_refs", collectionName)) {
				collectionPath = projectCollectionPath + "/Reference";
				pathEntriesCollection.getPathMetadataEntries()
						.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Reference"));
			} else if (StringUtils.equalsIgnoreCase("PGXI", collectionName)) {
				collectionPath = projectCollectionPath + "/PGXI";
				pathEntriesCollection.getPathMetadataEntries()
						.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Processed_Data"));
			} else if (StringUtils.equalsIgnoreCase("Validation", collectionName)) {
				collectionPath = projectCollectionPath + "/Validation";
				pathEntriesCollection.getPathMetadataEntries()
						.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Results"));
			} else if (StringUtils.equalsIgnoreCase("Pre-Validation Analysis", collectionName)) {
				collectionPath = projectCollectionPath + "/Pre_Validation Analysis";
				pathEntriesCollection.getPathMetadataEntries()
						.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Analysis"));
			}

			pathEntriesCollection.setPath(collectionPath.replace(" ", "_"));
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesCollection);

			// Add path metadata entries for PGXI Shared folder if it exists
			if (StringUtils.equalsIgnoreCase("PGXI", collectionName)) {
				String folderName = getCollectionNameFromParent(object.getOriginalFilePath(), collectionName);
				if (folderName != null && StringUtils.equalsIgnoreCase("Shared", folderName)) {
					HpcBulkMetadataEntry pathEntriesPgxi = new HpcBulkMetadataEntry();
					String pgxiCollectionPath = collectionPath + "/Shared";
					pathEntriesPgxi.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Folder"));
					pathEntriesPgxi.setPath(pgxiCollectionPath.replace(" ", "_"));
					hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPgxi);
				}
			}

			// Add path metadata entries for Pre-Validation Analysis Batch and CRAM folders if they exist
			else if (StringUtils.equalsIgnoreCase("Pre-Validation Analysis", collectionName)) {
				String batchName = getCollectionNameFromParent(object.getOriginalFilePath(), collectionName);
				String parentName = getCollectionNameFromParent(object.getOriginalFilePath(), batchName);
				HpcBulkMetadataEntry pathEntriesParent = new HpcBulkMetadataEntry();
				String batchCollectionPath = collectionPath + "/" + batchName;
				pathEntriesParent.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Batch"));
				pathEntriesParent.getPathMetadataEntries().add(createPathEntry("batch_id", batchName));
				pathEntriesParent.setPath(batchCollectionPath.replace(" ", "_"));
				hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesParent);
				if (StringUtils.equalsIgnoreCase("cram", parentName)) {
					HpcBulkMetadataEntry pathEntriesCram = new HpcBulkMetadataEntry();
					String cramCollectionPath = batchCollectionPath + "/" + parentName;
					pathEntriesCram.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "CRAM"));
					pathEntriesCram.setPath(cramCollectionPath.replace(" ", "_"));
					hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesCram);
				}
			}
		}

		// Set it to dataObjectRegistrationRequestDTO
		dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
		dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);

		// Add object metadata

		dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("object_name", fileName));
		dataObjectRegistrationRequestDTO.getMetadataEntries()
				.add(createPathEntry("source_path", object.getOriginalFilePath()));

		return dataObjectRegistrationRequestDTO;
	}

	private String getCollectionNameFromParent(String path, String parentName) {
		Path fullFilePath = Paths.get(path);
		logger.info("Full File Path = {}", fullFilePath);

		while (fullFilePath != null) {
			Path parent = fullFilePath.getParent();
			if (parent == null) {
				return null;
			}
			Path parentFileName = parent.getFileName();
			if (parentFileName != null && parentFileName.toString().equals(parentName)) {
				return fullFilePath.getFileName().toString();
			}
			fullFilePath = parent;
		}
		return null;
	}

	private String getPICollectionName(StatusInfo object) throws DmeSyncMappingException {
		return "Stephen_Hewitt";
	}

	private String getProjectCollectionName(StatusInfo object, String metadataFilePathKey)
			throws DmeSyncMappingException {
		String projectId = getAttrValueWithExactKeyFromMetadataMap(metadataFilePathKey, "project_id");
		if (StringUtils.isBlank(projectId)) {
			String msg = messageService.get("VALIDATION_003",
					metadataFilePathKey,
			        Locale.getDefault()
			    );
			logger.info(msg);
 			throw new DmeSyncMappingException(msg);
 		}
		logger.info("Project Id = {}", projectId);
		return projectId;
	}

	private String getCollectionName(StatusInfo object) throws DmeSyncMappingException {
		String collectionName = getCollectionNameFromParent(object.getOriginalFilePath(), "CMDL_Aldy");
		return collectionName;
	}

	private String getMetadataFileKey(StatusInfo object) {
		String metadataFileKey = getCollectionNameFromParent(object.getOriginalFilePath(), "mnt");
		logger.info("Metadata FileKey = {}", metadataFileKey);
		return metadataFileKey;
	}

}
