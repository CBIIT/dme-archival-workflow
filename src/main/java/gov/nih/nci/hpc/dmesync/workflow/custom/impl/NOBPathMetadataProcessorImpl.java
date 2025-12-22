package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import org.apache.commons.io.FilenameUtils;
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
 * NOB DME Path and Meta-data Processor Implementation
 * 
 * @author konerum3
 *
 */
@Service("nob")
public class NOBPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
		implements DmeSyncPathMetadataProcessor {

	// NOB Custom logic for DME path construction and meta data creation

	public static final String PRIMARY_ANALYSIS_INPUT_NAME = "Primary Analysis Input";
	public static final String PRIMARY_ANALYSIS_OUTPUT_NAME = "Primary Analysis Output";
	public static final String FASTQ_QC_NAME = "FASTQ_QC";

	public static final String ANALYSIS = "Analysis";
	public static final String FASTQ = "Fastq";
	// deconv, processed, sample_fused (eg adult2_fused, young1b_fused)
	// s176wt_1, s176wt_2, s177wt_1, s177wt_2, ear_down, ear_up, ear_up_pos50,
	// Interesting488, Interesting640, Interesting790, e26, p8heart, p8heart_1,
	// p8heart_2, 790, adult2, young1b, young2, p10_cochlea1, _cochlea1_b,
	// p9p3_cochlea1
	private final List<String> sampleNames = List.of("s176wt_1", "s176wt_2", "s177wt_1", "s177wt_2", "ear_down",
			"ear_up", "ear_up_pos50", "Interesting488", "Interesting640", "Interesting790", "e26", "p8heart",
			"p8heart_1", "p8heart_2", "790", "adult2", "young1b", "young2", "p10_cochlea1", "p10_cochlea1_b",
			"p9p3_cochlea1", "p1117_wtL", "p1117_wtL_round2", "p175_wtR");
	private final List<String> processedFolders = List.of("deconv", "processed", "sample_fused", "adult2_fused",
			"young1b_fused");
	@Value("${dmesync.additional.metadata.excel:}")
	private String metadataFile;

	@Autowired
	private DmeMetadataBuilder dmeMetadataBuilder;

	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {

		logger.info("[PathMetadataTask] POBCCDIgetArchivePath called");

		Path filePath = Paths.get(object.getSourceFilePath());

		String fileName = filePath.toFile().getName();
	

		// find the metadataFilePathKey to use as key value for the metadata from file.
		String metadataFilePathKey = getPathForMetadata(object);
		String archivePath = null;
		String sampleName = getSampleName(filePath);
		
		logger.info("metadataFileKey {} ", metadataFilePathKey);
		
		// load the user metadata from the externally placed excel
		metadataMap = dmeMetadataBuilder.getMetadataMap(metadataFile, "Folder");

		// subcollectionName can be Deconv or null based on path
		String deconvFolderName = getDeconvFolderName(filePath, sampleName);

		if (deconvFolderName != null && !StringUtils.isBlank(deconvFolderName)) {

			String wavelengthFolder = getWavelengthFolderName(object.getOriginalFilePath(), deconvFolderName);
			String waveLengthFolderType = wavelengthFolder != null ? wavelengthFolder + "/" : "";
			archivePath = destinationBaseDir + "/Lab_" + getPiCollectionName(metadataFilePathKey) + "/Researcher_"
					+ getResearchCollectionName(metadataFilePathKey) + "/Project_"
					+ getProjectCollectionName(metadataFilePathKey) + "/Experiment_"
					+ getExperimentName(metadataFilePathKey) + "/Sample_" + getSampleName(filePath) + "/Deconv_"
					+ deconvFolderName + "/" + waveLengthFolderType + fileName;
		} else {

			archivePath = destinationBaseDir + "/Lab_" + getPiCollectionName(metadataFilePathKey) + "/Researcher_"
					+ getResearchCollectionName(metadataFilePathKey) + "/Project_"
					+ getProjectCollectionName(metadataFilePathKey) + "/Experiment_"
					+ getExperimentName(metadataFilePathKey) + "/Sample_" + getSampleName(filePath) + "/RawData/"
					+ fileName;
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


			Path filePath = Paths.get(object.getSourceFilePath());

			// find the metadataFilePathKey to use as key value for the metadata from file.
			String metadataFilePathKey = getPathForMetadata(object);
			String sampleName = getSampleName(filePath);
			// subcollectionName can be Deconv or null based on path
			String deconvFolderName = getDeconvFolderName(filePath, sampleName);

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

			String labCollectionName = getPiCollectionName(metadataFilePathKey);
			String labCollectionPath = destinationBaseDir + "/Lab_" + labCollectionName.replace(" ", "_");
			HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "PI_Lab"));
			pathEntriesPI.setPath(labCollectionPath);
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner", labCollectionName));
			pathEntriesPI.getPathMetadataEntries().add(
					createPathEntry("data_owner_email", getAttrValueFromMetadataMap(metadataFilePathKey, "data_owner_email")));
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_affiliation",
					getAttrValueFromMetadataMap(metadataFilePathKey, "data_owner_affiliation")));

			if (getAttrValueFromMetadataMap(metadataFilePathKey, "data_owner_designee") != null) {
				pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_designee",
						getAttrValueFromMetadataMap(metadataFilePathKey, "data_owner_designee")));
				pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_designee_email",
						getAttrValueFromMetadataMap(metadataFilePathKey, "data_owner_designee_email")));
				pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_designee_affiliation",
						getAttrValueFromMetadataMap(metadataFilePathKey, "data_owner_designee_affiliation")));
			}

			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPI);

			String researchName = getResearchCollectionName(metadataFilePathKey);
			String researchCollectionPath = labCollectionPath + "/Researcher_" + researchName.replace(" ", "_");
			HpcBulkMetadataEntry pathEntriesResearch = new HpcBulkMetadataEntry();
			pathEntriesResearch.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Researcher"));
			pathEntriesResearch.getPathMetadataEntries()
					.add(createPathEntry("researcher", getAttrValueFromMetadataMap(metadataFilePathKey, "researcher")));
			pathEntriesResearch.getPathMetadataEntries().add(
					createPathEntry("researcher_email", getAttrValueFromMetadataMap(metadataFilePathKey, "researcher_email")));
			pathEntriesResearch.setPath(researchCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesResearch);

			// Add path metadata entries for "Project_XXX" collection
			// Example row: collectionType - Project, collectionName - supplied

			String projectCollectionName = getProjectCollectionName(metadataFilePathKey);
			String projectCollectionPath = researchCollectionPath + "/Project_" + projectCollectionName;
			HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("project_poc", getAttrValueFromMetadataMap(metadataFilePathKey, "project_poc")));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc_affiliation",
					getAttrValueFromMetadataMap(metadataFilePathKey, "project_poc_affiliation")));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc_email",
					getAttrValueFromMetadataMap(metadataFilePathKey, "project_poc_email")));
			if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "key_collaborator")))
				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("key_collaborator",
						getAttrValueFromMetadataMap(metadataFilePathKey, "key_collaborator")));
			if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "key_collaborator_affiliation")))
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
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("platform_name", getAttrValueFromMetadataMap(metadataFilePathKey, "platform_name")));
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("organism", getAttrValueFromMetadataMap(metadataFilePathKey, "organism")));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("study_disease",
					getAttrValueFromMetadataMap(metadataFilePathKey, "study_disease")));

			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("data_generating_facility", "NOB"));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_status", "Active"));

			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Controlled Access"));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("retention_years", "7"));

			// Optional Values
			if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "project_completed_date")))
				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_completed_date",
						getAttrValueFromMetadataMap(metadataFilePathKey, "project_completed_date"), "MM/dd/yy"));
			if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "pubmed_id")))
				pathEntriesProject.getPathMetadataEntries()
						.add(createPathEntry("pubmed_id", getAttrValueFromMetadataMap(metadataFilePathKey, "pubmed_id")));
			if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "Collaborators")))
				pathEntriesProject.getPathMetadataEntries().add(
						createPathEntry("Collaborators", getAttrValueFromMetadataMap(metadataFilePathKey, "Collaborators")));

			pathEntriesProject.setPath(projectCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);

			String expermientName = getExperimentName(metadataFilePathKey);
			String expermientNamePath = projectCollectionPath + "/Experiment_" + expermientName.replace(" ", "_");
			HpcBulkMetadataEntry pathEntriesExpermientName = new HpcBulkMetadataEntry();
			pathEntriesExpermientName.getPathMetadataEntries()
					.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Experiment"));
			pathEntriesExpermientName.getPathMetadataEntries()
					.add(createPathEntry("experiment_id", getAttrValueFromMetadataMap(metadataFilePathKey, "experiment_id")));
			pathEntriesExpermientName.getPathMetadataEntries().add(
					createPathEntry("experiment_name", getAttrValueFromMetadataMap(metadataFilePathKey, "experiment_name")));
			pathEntriesExpermientName.getPathMetadataEntries().add(
					createPathEntry("experiment_date", getAttrValueFromMetadataMap(metadataFilePathKey, "experiment_date")));

			if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "experiment_type")))

				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("experiment_type",
						getAttrValueFromMetadataMap(metadataFilePathKey, "experiment_type")));
			if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "number_of_samples")))
				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("number_of_samples",
						getAttrValueFromMetadataMap(metadataFilePathKey, "number_of_samples")));
			if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "cell_line")))
				pathEntriesProject.getPathMetadataEntries()
						.add(createPathEntry("cell_line", getAttrValueFromMetadataMap(metadataFilePathKey, "cell_line")));
			if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "instrument_id")))
				pathEntriesProject.getPathMetadataEntries().add(
						createPathEntry("instrument_id", getAttrValueFromMetadataMap(metadataFilePathKey, "instrument_id")));
			if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "instrument_name")))
				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("instrument_name",
						getAttrValueFromMetadataMap(metadataFilePathKey, "instrument_name")));

			pathEntriesExpermientName.setPath(expermientNamePath);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesExpermientName);

			// String sampleName= getSampleName(filePath);
			String sampleCollectionPath = expermientNamePath + "/Sample_" + sampleName.replace(" ", "_");
			HpcBulkMetadataEntry pathEntriesSample = new HpcBulkMetadataEntry();
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Sample"));
			pathEntriesSample.getPathMetadataEntries()
					.add(createPathEntry("sample_id", sampleName));
			pathEntriesSample.getPathMetadataEntries()
					.add(createPathEntry("sample_name", sampleName));
			pathEntriesSample.setPath(sampleCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesSample);

			if (deconvFolderName != null && !StringUtils.isBlank(deconvFolderName)) {
				String deconvCollectionPath = sampleCollectionPath + "/Deconv_" + deconvFolderName;
				HpcBulkMetadataEntry pathEntriesDeconv = new HpcBulkMetadataEntry();
				pathEntriesDeconv.getPathMetadataEntries()
						.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Processed_Data"));
				pathEntriesDeconv.setPath(deconvCollectionPath);
				hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesDeconv);
				String wavelengthFolder = getWavelengthFolderName(object.getOriginalFilePath(), deconvFolderName);

				if (wavelengthFolder != null) {

					String wavelengthCollectionPath = deconvCollectionPath + "/" + wavelengthFolder;
					HpcBulkMetadataEntry pathEntriesWavelength = new HpcBulkMetadataEntry();
					pathEntriesWavelength.getPathMetadataEntries()
							.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Wavelength"));
					;
					pathEntriesWavelength.setPath(wavelengthCollectionPath);
					hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesWavelength);
				}
			} else {

				String fastqCollectionPath = sampleCollectionPath + "/RawData";
				HpcBulkMetadataEntry pathEntriesFastq = new HpcBulkMetadataEntry();
				pathEntriesFastq.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Raw_Data"));
				pathEntriesFastq.setPath(fastqCollectionPath);
				hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesFastq);

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
		logger.info("POB CCDI custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
		return dataObjectRegistrationRequestDTO;
	}

	private String getPiCollectionName(String metadataFilePathKey) {
		String piCollectionName = null;
		// Retrieve the PI collection name from the Excel spreadsheet
		piCollectionName = getAttrValueFromMetadataMap(metadataFilePathKey, "data_owner");
		logger.info("PI Collection Name: {}", piCollectionName);
		return piCollectionName;

	}

	private String getResearchCollectionName(String metadataFilePathKey) {
		String researchCollectionName = null;
		// Retrieve the PI collection name from the Excel spreadsheet
		researchCollectionName = getAttrValueFromMetadataMap(metadataFilePathKey, "researcher");
		logger.info("Research Collection Name: {}", researchCollectionName);
		return researchCollectionName;

	}

	private String getProjectCollectionName(String metadataFilePathKey) {
		String projectCollectionName = null;
		projectCollectionName = getAttrValueFromMetadataMap(metadataFilePathKey, "project_id");
		logger.info("projectCollectionName: {}", projectCollectionName);
		return projectCollectionName;
	}

	private String getSampleName(Path path) {
		Optional<String> sampleCollectionName = null;
		String pathStr = normalize(path);
		sampleCollectionName = sampleNames.stream().filter(sample -> pathStr.contains("/" + sample + "/")).findFirst();
		logger.info("sampleCollectionName: {}", sampleCollectionName.get());
		return sampleCollectionName.get();
	}

	private String normalize(Path path) {
		return path.toAbsolutePath().toString().replace("\\", "/") + "/";
	}

	private String getExperimentName(String metadataFilePathKey) {
		String expermientCollectionName = null;
		expermientCollectionName = getAttrValueFromMetadataMap(metadataFilePathKey, "experiment_name");
		logger.info("expermientCollectionName: {}", expermientCollectionName);
		return expermientCollectionName;
	}

	private String getDeconvFolderName(Path path, String sampleName) {
		String deconvFolderName = null;
		String pathStr = normalize(path);
		Optional<String> parentCollectionType = processedFolders.stream()
				.filter(type -> pathStr.contains("/" + type + "/")).findFirst();
		if (parentCollectionType.isPresent()) {
			deconvFolderName = getCollectionNameFromParent(path.toAbsolutePath().toString(), sampleName);
		}
		logger.info("deconvFolderName {}", deconvFolderName);
		return deconvFolderName;
	}

	private String getWavelengthFolderName(String path, String deconvFolderName) {
		String wavelengthFolderName = null;
		wavelengthFolderName = getCollectionNameFromParent(path, deconvFolderName);
		logger.info("wavelengthFolderName: {}", wavelengthFolderName);
		return wavelengthFolderName;

	}

	public String getPathForMetadata(StatusInfo object) {

		return getCollectionNameFromParent(object.getOriginalFilePath(), "nob-data");

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

}
