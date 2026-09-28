package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.domain.DocConfig.SourceConfig;
import gov.nih.nci.hpc.dmesync.domain.DocConfig.SourceRule;
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


	private static final Pattern PATIENT_ID = Pattern.compile("^(P\\d+)");

	@Autowired
	private DmeMetadataBuilder dmeMetadataBuilder;

	// DOC CRTP CMDL logic for DME path construction and meta data creation

	@Override
	public String getArchivePath(StatusInfo object, DocConfig config)
			throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {

		logger.info("[PathMetadataTask] DOC CRTP CMDL getArchivePath called");
		SourceConfig sourceConfig = config.getSourceConfig();
		SourceRule sourceRule = config.getSourceRule();

		// load the user metadata from the externally placed excel
		metadataMap = dmeMetadataBuilder.getMetadataMap(sourceRule.metadataFile, "project");

		// load the doc metadata  model from the DME 
		metaDataEntries = dmeMetadataBuilder.getDMEMetadataModel(sourceConfig.destinationBaseDir, config);

		String metadataFileKey = getMetadataFileKey(object);

		Path filePath = Paths.get(object.getSourceFilePath());
		String fileName = filePath.toFile().getName();
		String parentName = filePath.getParent().getFileName().toString();
		String patientId = extractPatientId(fileName);

		String collectionName = getCollectionName(object);
		String archivePath = null;
		if (collectionName != null) {

			 if (StringUtils.equalsIgnoreCase("PGXI", collectionName)) {
				archivePath = sourceConfig.destinationBaseDir + "/PI_" + getPICollectionName(object) + "/Project_"
						+ getProjectCollectionName(object, metadataFileKey) + "/Patient_"+ patientId +"/PGXI/";
				String folderName = getCollectionNameFromParent(object.getOriginalFilePath(), collectionName);
				if (folderName != null && StringUtils.equalsIgnoreCase("Shared", folderName)) {
					archivePath += fileName;
				} else {
					archivePath = null;
				}

			} else if (StringUtils.equalsIgnoreCase("Validation", collectionName)) {

				archivePath = sourceConfig.destinationBaseDir + "/PI_" + getPICollectionName(object) + "/Project_"
						+ getProjectCollectionName(object, metadataFileKey) + "/Patient_"+ patientId + "/Validation/"  + fileName;
			} else if (StringUtils.equalsIgnoreCase("Pre-Validation Analysis", collectionName)) {
					archivePath = sourceConfig.destinationBaseDir + "/PI_" + getPICollectionName(object) + "/Project_"
							+ getProjectCollectionName(object, metadataFileKey) +  "/Patient_"+ patientId + "/Pre_Validation_Analysis/"
						    + fileName;
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
	public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object, DocConfig config)
			throws DmeSyncMappingException, DmeSyncWorkflowException {

		logger.info("[PathMetadataTask] DOC CRTP CMDL getMetaDataJson called");
		SourceConfig sourceConfig = config.getSourceConfig();

		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();

		// Add to HpcBulkMetadataEntries for path attributes
		HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();
		Path filePath = Paths.get(object.getSourceFilePath());
		String fileName = filePath.toFile().getName();
		String parentName = filePath.getParent().getFileName().toString();
		String patientId = extractPatientId(fileName);
		String metadataFileKey = getMetadataFileKey(object);
		// Add path metadata entries for "DataOwner_Lab" collection
		String piCollectionName = getPICollectionName(object);
		String projectCollectionName = getProjectCollectionName(object, metadataFileKey);
		String piCollectionPath = sourceConfig.destinationBaseDir + "/PI_" + piCollectionName.replace(" ", "_");
		HpcBulkMetadataEntry pathEntriesPI = buildPathEntries(piCollectionPath, "DataOwner_Lab", metadataFileKey,
				metaDataEntries);
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPI);

		// Add path metadata entries for "Project" collection
		String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName.replace(" ", "_");
		HpcBulkMetadataEntry pathEntriesProject = buildPathEntries(projectCollectionPath, "Project", metadataFileKey,
				metaDataEntries);
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);

		// Add path metadata entries for Patient Collection
		
		String patientCollectionPath = projectCollectionPath + "/Patient_" + patientId;
		HpcBulkMetadataEntry pathEntriesPatient = new HpcBulkMetadataEntry();
		pathEntriesPatient.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Batch"));
		pathEntriesPatient.getPathMetadataEntries().add(createPathEntry("patient_id", patientId));
		pathEntriesPatient.setPath(patientCollectionPath.replace(" ", "_"));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPatient);
		
		// Add path metadata entries for folder
		String collectionName = getCollectionName(object);
		String collectionPath = null ;
		HpcBulkMetadataEntry pathEntriesCollection = new HpcBulkMetadataEntry();
		if (collectionName != null) {

			if (StringUtils.equalsIgnoreCase("PGXI", collectionName)) {
				collectionPath = patientCollectionPath + "/PGXI";
				pathEntriesCollection.getPathMetadataEntries()
						.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Report"));
			} else if (StringUtils.equalsIgnoreCase("Validation", collectionName)) {
				collectionPath = patientCollectionPath + "/Validation";
				pathEntriesCollection.getPathMetadataEntries()
						.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Results"));
			} else if (StringUtils.equalsIgnoreCase("Pre-Validation Analysis", collectionName)) {
				collectionPath = projectCollectionPath + "/Pre_Validation Analysis";
				pathEntriesCollection.getPathMetadataEntries()
						.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Analysis"));
			}

			pathEntriesCollection.setPath(collectionPath.replace(" ", "_"));
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesCollection);

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


    public static String extractPatientId(String filename) {
        Matcher matcher = PATIENT_ID.matcher(filename);
        return matcher.find() ? matcher.group(1) : null;
    }

}
