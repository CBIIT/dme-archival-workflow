package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncPathMetadataProcessor;
import gov.nih.nci.hpc.domain.datamanagement.HpcPathPermissions;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntries;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.HpcArchiveDirectoryPermissionsRequestDTO;
import gov.nih.nci.hpc.dto.datamanagement.HpcArchivePermissionsRequestDTO;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;

/**
 * DCEG LTG DME Path and Meta-data Processor Implementation
 * 
 * @author dinhys
 *
 */
@Service("dceg-ltg")
public class DCEGLtgPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
		implements DmeSyncPathMetadataProcessor {

	// DCEG LTG Custom logic for DME path construction and meta data creation

	@Value("${dmesync.additional.metadata.excel:}")
	private String metadataFile;

	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

		logger.info("[PathMetadataTask] DCEG LTG getArchivePath called");
		
		// Get all the metadata and projectIds, FlowcellIds from excel sheet

		String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
		String archivePath;
		// load the user metadata from the externally placed excel
		threadLocalMap.set(loadMetadataFile(metadataFile, "Path"));

		if (StringUtils.containsIgnoreCase(object.getSourceFileName(), "Analysis_Data")) {

			archivePath = destinationBaseDir + "/PI_" + getPiCollectionName(object) + "/Project_"
					+ getProjectCollectionName(object) + "/Aligned_Data" + "/" + fileName;

		} else {

			archivePath = destinationBaseDir + "/PI_" + getPiCollectionName(object) + "/Project_"
					+ getProjectCollectionName(object) + "/Flowcell_" + getFlowCellId(object) + "/";
			if (StringUtils.containsIgnoreCase(object.getSourceFilePath(), "Sample")) {
				archivePath = archivePath + "Sample_" + getSampleId(object);
			} else {
				archivePath = archivePath + fileName;
			}

		}

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
			String path = FilenameUtils.separatorsToUnix(object.getOriginalFilePath());

			// Add to HpcBulkMetadataEntries for path attributes
			HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

			// Add path metadata entries for "PI_XXX" collection
			// Example row: collectionType - PI, collectionName - Di Xia (supplied)
			// key = pi_name, value = Di Xia (supplied)
			// key = affiliation, value = ? (supplied)
			// key = poc_name, value = ? (supplied)

			String piCollectionName = getPiCollectionName(object);
			String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName.replace(" ", "_");
			HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "DataOwner_Lab"));
			pathEntriesPI.setPath(piCollectionPath);
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner", "Amundadottir, Laufey "));
			pathEntriesPI.getPathMetadataEntries()
					.add(createPathEntry("data_owner_email", "amundadottirl@mail.nih.gov"));
			pathEntriesPI.getPathMetadataEntries()
					.add(createPathEntry("data_owner_affiliation", "Laboratory of Translational Genomics, DCEG"));
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPI);

			// Add path metadata entries for "Project_XXX" collection
			// Example row: collectionType - Project, collectionName - Hemang_ChIP-seq
			// (derived),
			// key = project_title, value = Hemang_ChIP-seq (derived)
			// key = project_start_date, value = (supplied)
			// key = project_description, value = (supplied)
			// key = organism, value = (supplied)
			// key = is_cell_line, value = (supplied)
			// key = study_disease, value = (supplied)
			// key = project_poc (default: Jason Hoskins)
			// key = project_poc_affiliation (default: DCEG LTG)
			// key = project_poc_email (default: jason.hoskins@nih.gov)
			// key = retention_years (default: 7 years)
			// key = access (default: closed)
			// key = data_generating_facility (?)
			// key = summary_of_samples, value = (supplied)
			// key = organism, value = (supplied)

			String projectCollectionName = getProjectCollectionName(object);
			String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;
			HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_id", projectCollectionName));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_title", projectCollectionName));
			pathEntriesProject.getPathMetadataEntries().add(
					createPathEntry("project_start_date", getAttrValueWithKey(path, "project_start_date"), "MM/dd/yy"));
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("project_description", getAttrValueWithKey(path, "project_description")));
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("organism", getAttrValueWithKey(path, "organism")));
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("is_cell_line", getAttrValueWithKey(path, "is_cell_line")));
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("study_disease", getAttrValueWithKey(path, "study_disease")));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Controlled Access"));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc", "Jason Hoskins"));
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("project_poc_affiliation", "Laboratory of Translational Genomics, DCEG"));
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("project_poc_email", "jason.hoskins@nih.gov"));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("retention_years", "7"));
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("data_generating_facility", "Laboratory of Translational Genomics, DCEG"));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_status", "Completed"));
			// pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_completed_date",
			// new Date()));

			pathEntriesProject.setPath(projectCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);

			if (StringUtils.containsIgnoreCase(object.getSourceFileName(), "Analysis_Data")) {

				String flowcellIds = getFlowCellId(object);
				String mergedDataCollectionPath = projectCollectionPath + "/Aligned_Data";
				HpcBulkMetadataEntry pathEntriesAlignedData = new HpcBulkMetadataEntry();
				pathEntriesAlignedData.setPath(mergedDataCollectionPath);
				pathEntriesAlignedData.getPathMetadataEntries()
						.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Merged_Data"));
				pathEntriesAlignedData.getPathMetadataEntries().add(createPathEntry("flowcell_id", flowcellIds));
				hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesAlignedData);

			} else {
				// Add path metadata entries for "Flowcell" collection
				// Example row: collectionType - Flowcell, FlowcellID - 20191101 (derived)
				String flowcellId = getFlowCellId(object);
				String flowcellCollectionPath = projectCollectionPath + "/Flowcell_" + flowcellId;
				HpcBulkMetadataEntry pathEntriesFlowcell = new HpcBulkMetadataEntry();
				pathEntriesFlowcell.setPath(flowcellCollectionPath);
				pathEntriesFlowcell.getPathMetadataEntries()
						.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Flowcell"));
				pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("flowcell_id", flowcellId));
				hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesFlowcell);

				if (StringUtils.containsIgnoreCase(object.getSourceFilePath(), "Sample")) {
					String sampleId = getSampleId(object);
					String sampleCollectionPath = flowcellCollectionPath + "/" + "Sample_" + sampleId;
					HpcBulkMetadataEntry pathEntriesSampleData = new HpcBulkMetadataEntry();

					pathEntriesSampleData.getPathMetadataEntries()
							.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Sample"));
					pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("sample_id", sampleId));
					pathEntriesSampleData.setPath(sampleCollectionPath);
					hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesSampleData);
				}
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
			threadLocalMap.remove();
		}
		logger.info("DCEG LTG custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
		return dataObjectRegistrationRequestDTO;
	}

	private String getCollectionNameFromParent(StatusInfo object, String parentName) {
		// Example originalFilepath -
		// /mnt/NCEF-CryoEM/RMarmorstein-NCEF-033-007-10031/RMarmorstein-NCEF-033-007-10031-A.tar
		Path fullFilePath = Paths.get(object.getOriginalFilePath());
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

	private String getPiCollectionName(StatusInfo object) {
		String piCollectionName = "Laufey_Amundadottir";
		return piCollectionName;

	}

	private String getProjectCollectionName(StatusInfo object) {
		String projectCollectionName = null;
		String path = FilenameUtils.separatorsToUnix(object.getOriginalFilePath());
		projectCollectionName = getAttrValueWithKey(path, "Project");
		logger.info("Project Collection Name: {}", projectCollectionName);
		return projectCollectionName;
	}

	private String getFlowCellId(StatusInfo object) {
		String flowCellId = null;
		String path = FilenameUtils.separatorsToUnix(object.getOriginalFilePath());
		flowCellId = getAttrValueWithKey(path, "Flowcell");
		logger.info("FlowCell Id: {}", flowCellId);
		return flowCellId;
	}

	private String getSampleId(StatusInfo object) {

		// Get the sampleId from the Path folder Name Sample_43 Sample_44
		String sampleId = null;
		String path = FilenameUtils.separatorsToUnix(object.getOriginalFilePath());
		sampleId = path.substring(path.indexOf("Sample_") + "Sample_".length(),
				path.indexOf(File.separator, path.indexOf("Sample_")));
		logger.info("Sample Id: {}", sampleId);
		return sampleId;
	}

}