package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
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
 * Default DCTB BBRB Path and Meta-data Processor Implementation
 * 
 * @author konerum3
 *
 */
@Service("dctb-bbrb")
public class DctbBBRBPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
		implements DmeSyncPathMetadataProcessor {

	@Value("${dmesync.doc.name}")
	private String doc;

	@Value("${dmesync.source.base.dir}")
	protected String sourceBaseDir;

	@Value("${dmesync.additional.metadata.excel:}")
	private String metadataFile;
	
	@Autowired
	private DmeMetadataBuilder dmeMetadataBuilder;

	// DOC DCTB BBRB logic for DME path construction and meta data creation

	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {

		logger.info("[PathMetadataTask] DOC DCTB BBRB getArchivePath called");

		// load the user metadata from the externally placed excel
		metadataMap = dmeMetadataBuilder.getMetadataMap(metadataFile, "project_id");
		
		metaDataEntries = dmeMetadataBuilder.getDMEMetadataModel(destinationBaseDir);
		
		String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
		String archivePath = destinationBaseDir + "/PI_" + getPICollectionName(object) + "/Project_"
				+ getProjectCollectionName(object)  + "/" + getCaseId(object) + "/" + fileName;
		// replace spaces with underscore
		archivePath = archivePath.replace(" ", "_");

		logger.info("[PathMetadataTask] ArchivePath {} " , archivePath);
		return archivePath;
	}

	@Override
	public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException {

		logger.info("[PathMetadataTask] DOC DCTB BBRB getMetaDataJson called");

		
		  
		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();

		// Add to HpcBulkMetadataEntries for path attributes
		HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

		// Add path metadata entries for "DataOwner_Lab" collection
		String piCollectionName = getPICollectionName(object);
		String projectCollectionName = getProjectCollectionName(object);
		String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;
		HpcBulkMetadataEntry pathEntriesPI = buildPathEntries(piCollectionPath, "DataOwner_Lab", projectCollectionName , metaDataEntries);
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPI);

		// Add path metadata entries for "Project" collection
		String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;
		HpcBulkMetadataEntry pathEntriesProject =
									buildPathEntries(projectCollectionPath, "Project", projectCollectionName , metaDataEntries);
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);
		
		// Add path metadata entries for "Case" folder
		String caseId = getCaseId(object);
		String caseCollectionPath = projectCollectionPath  + "/" + caseId;
		HpcBulkMetadataEntry pathEntriesCase = new HpcBulkMetadataEntry();
		pathEntriesCase.setPath(caseCollectionPath);
		pathEntriesCase.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Case"));
		pathEntriesCase.getPathMetadataEntries().add(createPathEntry("case_id", caseId));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesCase);

		// Set it to dataObjectRegistrationRequestDTO
		dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
		dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);

		// Add object metadata
		String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
		dataObjectRegistrationRequestDTO.getMetadataEntries()
				.add(createPathEntry("object_name", fileName));
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
		return "Ping_Guan";
	}

	private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {
		String projectId = "BPV";

		return projectId;
	}

	private String getCaseId(StatusInfo object) throws DmeSyncMappingException {
		String caseId = getCollectionNameFromParent(object.getOriginalFilePath(), getProjectCollectionName(object));
		return caseId;
	}

}
