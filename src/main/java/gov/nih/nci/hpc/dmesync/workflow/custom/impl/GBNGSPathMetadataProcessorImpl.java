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
 * Default GB MGS Path and Meta-data Processor Implementation
 * 
 * @author dinhys
 *
 */
@Service("gb-ngs")
public class GBNGSPathMetadataProcessorImpl extends AbstractPathMetadataProcessor implements DmeSyncPathMetadataProcessor {

	@Value("${dmesync.source.base.dir}")
	protected String sourceBaseDir;

	// DOC new GB MGS logic for DME path construction and meta data creation

	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

		logger.info("[PathMetadataTask] GB getArchivePath called");

		String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
		String archivePath;
		archivePath = destinationBaseDir + "/PI_" + getPICollectionName() +"/"+getProjectCollectionName()
				+ "/Flowcell_" + getFlowcell(object) +  "/" + getDataTypeCollectionName(object)+ "/" +  fileName;

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

		String projectCollectionName = getProjectCollectionName(); // Project_NGS
		String projectCollectionPath = piCollectionPath + "/" + projectCollectionName;
		HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Closed Access"));
		pathEntriesProject.setPath(projectCollectionPath);
		hpcBulkMetadataEntries.getPathsMetadataEntries()
				.add(populateStoredMetadataEntries(pathEntriesProject, "Project", getProjectCollectionName(), "gb"));

		// Add path metadata entries for "Flowcell" collection
		String flowcellId = getFlowcell(object); // from folder name
		String flowcellCollectionPath = projectCollectionPath + "/Flowcell_" + flowcellId;
		HpcBulkMetadataEntry pathEntriesFlowcell = new HpcBulkMetadataEntry();
		pathEntriesFlowcell.setPath(flowcellCollectionPath);
		pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Flowcell"));
		pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("flowcell_id", flowcellId));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesFlowcell);

		// Add path metadata entries for "data_type" collection

		String dataTypeCollection = getDataTypeCollectionName(object); // from folder name
		String dataTypeCollectionPath = flowcellCollectionPath +"/"+ dataTypeCollection;
		HpcBulkMetadataEntry pathEntriesdata = new HpcBulkMetadataEntry();
		pathEntriesdata.setPath(dataTypeCollectionPath);
	    pathEntriesdata.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, getDataTypeCollectionType(object)));
		pathEntriesdata.getPathMetadataEntries().add(createPathEntry("data_type",dataTypeCollection ));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesdata);
		

		// Set it to dataObjectRegistrationRequestDTO
		dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
		dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
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

	private String getPICollectionName() {
		return "Paul_Meltzer";
	}

	private String getProjectCollectionName() {
		return "Project_NGS";
	}
	
	private String getDataTypeCollectionName(StatusInfo object) throws DmeSyncMappingException {
		String parent = getdataTypeCollectionName(object);
		String dataType = null;
		if (StringUtils.startsWith(parent, "bam")) {
			dataType = "BAM";
		}else if (StringUtils.startsWith(parent, "fastq")) {
			dataType = "FASTQ";
		}else if (StringUtils.startsWith(parent, "qc")) {
			dataType = "QC";
		}
		return dataType;
	}
	
	private String getDataTypeCollectionType(StatusInfo object) throws DmeSyncMappingException{
		String parent = getdataTypeCollectionName(object);
		String dataType = null;
		if (StringUtils.startsWith(parent, "bam")) {
			dataType = "Processed_Data";
		} else if (StringUtils.startsWith(parent, "fastq")) {
			dataType = "Raw_Data";
		} else if (StringUtils.startsWith(parent, "qc")) {
			dataType = "Analysis_Data";
		}
		return dataType;
	}

	private String getFlowcell(StatusInfo object) throws DmeSyncMappingException {
		String FlowCellId= getFlowcellCollectionName(object);
		return !StringUtils.isBlank(FlowCellId)  ? FlowCellId: "Other";
	}
	
	private String getCollectionNameFromParent(StatusInfo object, String parentName) {
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

	private String getFlowcellCollectionName(StatusInfo object) throws DmeSyncMappingException {
		return getCollectionNameFromParent(object, "data");
	}

	private String getdataTypeCollectionName(StatusInfo object) throws DmeSyncMappingException {
		return getCollectionNameFromParent(object, getFlowcellCollectionName(object));
	}
}
