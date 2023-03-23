package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

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
 * Default GB MGS Path and Meta-data Processor Implementation
 * 
 * @author dinhys
 *
 */
@Service("gb")
public class GBPathMetadataProcessorImpl extends AbstractPathMetadataProcessor implements DmeSyncPathMetadataProcessor {

	@Value("${dmesync.source.base.dir}")
	protected String sourceBaseDir;

	// DOC GB MGS logic for DME path construction and meta data creation

	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

		logger.info("[PathMetadataTask] GB getArchivePath called");

		String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
		String archivePath;
		archivePath = destinationBaseDir + "/PI_" + getPICollectionName() + "/" + getProjectCollectionName(object) + "/"
				+ "/Flowcell_" + getFlowcell(object) + "/" + fileName;

		// replace spaces with underscore
		archivePath = archivePath.replace(" ", "_");

		return archivePath;
	}

	@Override
	public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException {

		logger.info("[PathMetadataTask] GB getMetaDataJson called");

		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();

		String path = object.getOriginalFilePath();

		// Add to HpcBulkMetadataEntries for path attributes
		HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

		// Add path metadata entries for "DataOwner_Lab" collection

		String piCollectionName = getPICollectionName();
		String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;
		HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "DataOwner_Lab"));
		pathEntriesPI.setPath(piCollectionPath);
		hpcBulkMetadataEntries.getPathsMetadataEntries()
				.add(populateStoredMetadataEntries(pathEntriesPI, "DataOwner_Lab", piCollectionName, "gb"));

		// Add path metadata entries for "Project" collection

		String projectCollectionName = getProjectCollectionName(object);
		String projectCollectionPath = piCollectionPath + "/" + projectCollectionName;
		HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Closed Access"));
		pathEntriesProject.setPath(projectCollectionPath);
		hpcBulkMetadataEntries.getPathsMetadataEntries()
				.add(populateStoredMetadataEntries(pathEntriesProject, "Project", projectCollectionName, "gb"));

		// Add path metadata entries for "Flowcell" collection
		String flowcellId = getFlowcell(object);
		String flowcellCollectionPath = projectCollectionPath + "/Flowcell_" + flowcellId;
		HpcBulkMetadataEntry pathEntriesFlowcell = new HpcBulkMetadataEntry();
		pathEntriesFlowcell.setPath(flowcellCollectionPath);
		pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Flowcell"));
		pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("flowcell_id", flowcellId));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesFlowcell);

		// Set it to dataObjectRegistrationRequestDTO
		dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
		dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
		dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);

		// Add object metadata
		dataObjectRegistrationRequestDTO.getMetadataEntries()
				.add(createPathEntry("object_name", Paths.get(object.getSourceFilePath()).toFile().getName()));
		dataObjectRegistrationRequestDTO.getMetadataEntries()
				.add(createPathEntry("source_path", object.getOriginalFilePath()));

		return dataObjectRegistrationRequestDTO;
	}

	private String getPICollectionName() {
		return "Paul_Meltzer";
	}

	private String getProjectCollectionName(StatusInfo object) {
		String parent = Paths.get(object.getSourceFilePath()).getParent().getFileName().toString();
		String project = null;
		if (parent.equals("bam")) {
			project = "Data_BAM";
		} else if (parent.equals("fastq")) {
			project = "Data_FASTQ";
		} else if (parent.equals("sequence")) {
			project = "Data_BCL";
		}
		return project;
	}

	private String getFlowcell(StatusInfo object) {
		String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
		String extractedFlowcellId = StringUtils.substringAfterLast(StringUtils.substringBefore(fileName, "."), "_");
		return StringUtils.length(extractedFlowcellId) > 7 ? extractedFlowcellId: "Other";
	}
}
