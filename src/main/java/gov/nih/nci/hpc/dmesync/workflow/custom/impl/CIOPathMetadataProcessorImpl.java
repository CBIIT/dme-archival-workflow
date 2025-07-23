package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.nio.file.Path;
import java.nio.file.Paths;
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
 * Default CCR CIO Path and Meta-data Processor Implementation
 * 
 * @author dinhys
 *
 */
@Service("cio")
public class CIOPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
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

		logger.info("[PathMetadataTask] CIO getArchivePath called");

		String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
		String archivePath = destinationBaseDir + "/PI_" + getPICollectionName(object) + "/Project_"
				+ getProjectCollectionName(object) + "/Case_" + getCaseId(object) + "/" + fileName;
		// replace spaces with underscore
		archivePath = archivePath.replace(" ", "_");

		return archivePath;
	}

	@Override
	public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException {

		logger.info("[PathMetadataTask] CIO getMetaDataJson called");

		// load the user metadata from the externally placed excel
		threadLocalMap.set(loadMetadataFile(metadataFile, "project_id"));
		  
		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();

		// Add to HpcBulkMetadataEntries for path attributes
		HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

		// Add path metadata entries for "DataOwner_Lab" collection
		String piCollectionName = getPICollectionName(object);
		String projectCollectionName = getProjectCollectionName(object);
		String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;
		HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "DataOwner_Lab"));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner", getAttrValueWithKey(projectCollectionName, "data_owner")));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_affiliation", getAttrValueWithKey(projectCollectionName, "data_owner_affiliation")));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_email", getAttrValueWithKey(projectCollectionName, "data_owner_email")));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_generator", getAttrValueWithKey(projectCollectionName, "data_generator")));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_generator_affiliation", getAttrValueWithKey(projectCollectionName, "data_generator_affiliation")));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_generator_email", getAttrValueWithKey(projectCollectionName, "data_generator_email")));
		pathEntriesPI.setPath(piCollectionPath);
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPI);

		// Add path metadata entries for "Project" collection
		String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;
		HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc", getAttrValueWithKey(projectCollectionName, "project_poc")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc_affiliation", getAttrValueWithKey(projectCollectionName, "project_poc_affiliation")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc_email", getAttrValueWithKey(projectCollectionName, "project_poc_email")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_id", projectCollectionName));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_start_date", getAttrValueWithKey(projectCollectionName, "project_start_date")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_title", getAttrValueWithKey(projectCollectionName, "project_title")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_description", getAttrValueWithKey(projectCollectionName, "project_description")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("data_generating_facility", getAttrValueWithKey(projectCollectionName, "data_generating_facility")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("platform_name", getAttrValueWithKey(projectCollectionName, "platform_name")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("organism", getAttrValueWithKey(projectCollectionName, "organism")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("is_cell_line", getAttrValueWithKey(projectCollectionName, "is_cell_line")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Closed Access"));
		pathEntriesProject.setPath(projectCollectionPath);
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);
		
		// Add path metadata entries for "Case" folder
		String caseId = getCaseId(object);
		String caseCollectionPath = projectCollectionPath + "/Case_" + caseId;
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
		return "Jenn_Marte";
	}

	private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {
		String projectId = "caris-nci-7158";

		return projectId;
	}

	private String getCaseId(StatusInfo object) throws DmeSyncMappingException {
		String caseId = getCollectionNameFromParent(object.getOriginalFilePath(), getProjectCollectionName(object));
		return caseId;
	}

}
