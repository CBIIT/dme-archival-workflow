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
 * MicrobiomeCore DME Path and Meta-data Processor Implementation
 *
 * @author dinhys
 */
@Service("mgc")
public class MGCPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
		implements DmeSyncPathMetadataProcessor {

	// MicrobiomeCore DME path construction and meta data creation

	@Value("${dmesync.additional.metadata.excel:}")
	private String metadataFile;

	@Value("${dmesync.doc.name}")
	private String doc;

	@Value("${dmesync.source.base.dir}")
	private String sourceDir;

	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

		logger.info("[PathMetadataTask] MicrobiomeCore getArchivePath called");

		// load the user metadata from the externally placed excel
		threadLocalMap.set(loadMetadataFile(metadataFile, "project_id"));

		// Example source path -
		// /data/MicrobiomeCore/runs/M03213_20220712/ASAM06_20220629-357138108/RN205d24B4_S303_L001_R1_001.fastq.gz
		// Machine_YYYYMMDD. The Core's machine ids will either be M03213 or M05276 or
		// NB501046
		// project id (created using PI's initials, whether it is amplicon or shotgun,
		// host, number to avoid repeat) and date time stamp
		// <sample id in metadata>_<sample number in run>_<lane number>_<Read 1 or
		// 2>_001.fastq.gz
		String fileName = Paths.get(object.getSourceFileName()).toFile().getName();
		String archivePath = null;

		archivePath = destinationBaseDir + "/PI_" + getPiCollectionName(object) + "/Project_"
				+ getProjectCollectionName(object) + "/Sample_" + getSampleId(object) + "/" + fileName;

		// replace spaces with underscore
		archivePath = archivePath.replace(" ", "_");

		logger.info("Archive path for {} : {}", object.getOriginalFilePath(), archivePath);

		return archivePath;
	}

	@Override
	public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException {

		// Add to HpcBulkMetadataEntries for path attributes
		HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

		// Add path metadata entries for "PI_XXX" collection
		// Example row: collectionType - PI, collectionName - XXX (derived)
		// key = data_owner, value = (derived)
		// key = data_owner_affiliation, (derived)

		String piCollectionName = getPiCollectionName(object);
		String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;
		String projectCollectionName = getProjectCollectionName(object);
		String projectId = getProjectId(projectCollectionName);
		HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "DataOwner_Lab"));
		pathEntriesPI.setPath(piCollectionPath);
		pathEntriesPI.getPathMetadataEntries()
				.add(createPathEntry("data_owner", getAttrValueWithKey(projectId, "data_owner")));
		pathEntriesPI.getPathMetadataEntries()
				.add(createPathEntry("data_owner_email", getAttrValueWithKey(projectId, "data_owner_email")));
		pathEntriesPI.getPathMetadataEntries().add(
				createPathEntry("data_owner_affiliation", getAttrValueWithKey(projectId, "data_owner_affiliation")));
		hpcBulkMetadataEntries.getPathsMetadataEntries()
				.add(populateStoredMetadataEntries(pathEntriesPI, "DataOwner_Lab", "mgc", "mgc"));

		// Add path metadata entries for "Project_XXX" collection
		// Example row: collectionType - Project, collectionName - HudsonAlpha_3680
		// project_id key = Project Identifier
		// project_poc key = Project POC
		// project_poc_email key = POC email
		// project_start_date key = Start Date
		// project_title key = Project Title
		// project_description key = Project Description
		// organism key = Organism
		// is_cell_line key = ?
		// data_generating_facility key = Summary of samples
		// study_disease key = Study Disease
		// project_status key = Status
		// instrument_id, value = (derived)
		// instrument_name, (derived)

		String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;
		projectCollectionPath = projectCollectionPath.replace(" ", "_");
		HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
		pathEntriesProject.setPath(projectCollectionPath);
		String instrumentId = getInstrumentId(object);
		String instrumentName = getInstrumentName(instrumentId);
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("instrument_id", instrumentId));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("instrument_name", instrumentName));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("run_date", getRunDate(object), "yyyyMMdd"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_id", projectId));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_title", getAttrValueWithKey(projectId, "project_title")));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_description", getAttrValueWithKey(projectId, "project_title")));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_poc", getAttrValueWithKey(projectId, "project_poc")));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_poc_email", getAttrValueWithKey(projectId, "project_poc_email")));
		pathEntriesProject.getPathMetadataEntries().add(
				createPathEntry("project_poc_affiliation", getAttrValueWithKey(projectId, "project_poc_affiliation")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Closed Access"));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_start_date", getProjectStartDate(projectCollectionName), "yyyyMMdd"));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("serial_number", getSerialNumber(projectCollectionName)));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("organism", getAttrValueWithKey(projectId, "organism")));
		if (StringUtils.isNotBlank(getAttrValueWithKey(projectId, "study_disease")))
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("study_disease", getAttrValueWithKey(projectId, "study_disease")));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);

		// Add path metadata entries for "Sample" collection
		// Example row: collectionType - Sample, collectionName - Sample_<SampleId>
		// sample_id, value = RN205d24B4 (derived)
		// library_strategy, Library Strategy
		// analyte_type = Analyte Type
		String fileName = Paths.get(object.getOriginalFilePath()).toFile().getName();
		String sampleId = getSampleId(object);
		String sampleCollectionPath = projectCollectionPath + "/Sample_" + sampleId;
		sampleCollectionPath = sampleCollectionPath.replace(" ", "_");
		HpcBulkMetadataEntry pathEntriesSample = new HpcBulkMetadataEntry();
		pathEntriesSample.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Sample"));
		pathEntriesSample.setPath(sampleCollectionPath);
		pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sample_id", sampleId));
		String libraryStrategy = getLibraryStrategy(projectId, instrumentName);
		pathEntriesSample.getPathMetadataEntries()
				.add(createPathEntry("library_strategy", libraryStrategy));
		if (StringUtils.isNotBlank(getAttrValueWithKey(projectId, "analyte_type")))
			pathEntriesSample.getPathMetadataEntries()
					.add(createPathEntry("analyte_type", getAttrValueWithKey(projectId, "analyte_type")));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesSample);

		// Set it to dataObjectRegistrationRequestDTO
		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();
		dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
		dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
		dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);

		// Add object metadata
		// key = object_name, value = xxx.fastq.gz (derived)
		// key = file_type, value = fastq, bam (derived)
		String fileType = StringUtils.substringBefore(fileName, ".gz");
		fileType = fileType.substring(fileType.lastIndexOf('.') + 1);
		dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("object_name", fileName));
		dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("file_type", fileType));
		dataObjectRegistrationRequestDTO.getMetadataEntries()
				.add(createPathEntry("source_path", object.getOriginalFilePath()));
		logger.info("MicrobiomeCore custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
		return dataObjectRegistrationRequestDTO;
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

	private String getPiCollectionName(StatusInfo object) throws DmeSyncMappingException {
		String piCollectionName = null;
		piCollectionName = getAttrValueWithKey(getProjectId(getProjectCollectionName(object)), "data_owner");
		piCollectionName = piCollectionName.replace(" ", "_");
		logger.info("PI Collection Name: {}", piCollectionName);
		return piCollectionName;
	}

	private String getInstrumentId(StatusInfo object) throws DmeSyncMappingException {
		String instrumentId = null;
		// Example: If originalFilePath is
		// data/MicrobiomeCore/runs/M03213_20220712/ASAM06_20220629-357138108/RN205d24B4_S303_L001_R1_001.fastq.gz
		// then return the machine id
		String parentName = Paths.get(sourceDir).getFileName().toString();
		instrumentId = StringUtils.substringBefore(getCollectionNameFromParent(object, parentName), "_");

		logger.info("instrumentCollectionName: {}", instrumentId);
		return instrumentId;
	}

	private String getRunDate(StatusInfo object) throws DmeSyncMappingException {
		String instrumentId = null;
		// Example: If originalFilePath is
		// data/MicrobiomeCore/runs/M03213_20220712/ASAM06_20220629-357138108/RN205d24B4_S303_L001_R1_001.fastq.gz
		// then return the machine id
		String parentName = Paths.get(sourceDir).getFileName().toString();
		instrumentId = StringUtils.substringAfter(getCollectionNameFromParent(object, parentName), "_");

		logger.info("instrumentCollectionName: {}", instrumentId);
		return instrumentId;
	}

	private String getInstrumentName(String instrumentId) throws DmeSyncMappingException {
		String instrumentName = null;
		if (StringUtils.equals(instrumentId, "M03213") || StringUtils.equals(instrumentId, "M05276")) {
			instrumentName = "Miseq";
		} else if (StringUtils.equals(instrumentId, "NB501046")) {
			instrumentName = "NextSeq500";
		} else if (StringUtils.equals(instrumentId, "VH01251")) {
			instrumentName = "NextSeq2000";
		}
		return instrumentName;
	}

	private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {
		String projectCollectionName = null;
		// Example: If originalFilePath is
		// data/MicrobiomeCore/runs/M03213_20220712/ASAM06_20220629-357138108/RN205d24B4_S303_L001_R1_001.fastq.gz
		// then return the project id ASAM06_20220629-357138108
		String parentName = Paths.get(sourceDir).getFileName().toString();
		projectCollectionName = getCollectionNameFromParent(object, getCollectionNameFromParent(object, parentName));

		logger.info("projectCollectionName: {}", projectCollectionName);
		return projectCollectionName;
	}

	private String getSampleId(StatusInfo object) throws DmeSyncMappingException {
		String fileName = Paths.get(object.getOriginalFilePath()).getFileName().toString();
		String sampleId = null;
		// Example: If originalFilePath is
		// data/MicrobiomeCore/runs/M03213_20220712/ASAM06_20220629-357138108/RN205d24B4_S303_L001_R1_001.fastq.gz
		// then return the sample id RN205d24B4
		sampleId = StringUtils.substringBefore(fileName, "_");

		return sampleId;
	}

	private String getProjectId(String projectCollectionName) throws DmeSyncMappingException {

		return StringUtils.substringBefore(projectCollectionName, "-");
	}

	private String getProjectStartDate(String projectCollectionName) throws DmeSyncMappingException {

		return StringUtils.substringAfter(StringUtils.substringBefore(projectCollectionName, "-"), "_");
	}

	private String getSerialNumber(String projectCollectionName) throws DmeSyncMappingException {

		return StringUtils.substringAfter(projectCollectionName, "-");
	}

	private String getLibraryStrategy(String projectId, String instrumentName) throws DmeSyncMappingException {
		String libraryStrategy = null;
		if (StringUtils.equals(instrumentName, "Miseq")) {
			if (instrumentName.toUpperCase().charAt(2) == 'A')
				libraryStrategy = "Amplicon Sequencing";
			else if (instrumentName.toUpperCase().charAt(2) == 'S')
				libraryStrategy = "Metagenomic-seq";
		}

		if (libraryStrategy == null)
			libraryStrategy = getAttrValueWithKey(projectId, "library_strategy");
		return libraryStrategy;
	}
}
