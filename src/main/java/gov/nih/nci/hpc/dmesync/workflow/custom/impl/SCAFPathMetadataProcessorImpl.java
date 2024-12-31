package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;

import javax.annotation.PostConstruct;
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
 * SCAF DME Path and Meta-data Processor Implementation
 *
 * @author konerum3
 */
@Service("scaf")
public class SCAFPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
		implements DmeSyncPathMetadataProcessor {

	// SCAF DME path construction and meta data creation

	public static final String PRIMARY_ANALYSIS_OUTPUT_NAME = "Primary_Analysis_Output";
	public static final String FASTQ_QC_NAME = "FASTQ_QC";

	public static final String ANALYSIS = "Analysis";
	public static final String FASTQ = "FASTQ";

	@Value("${dmesync.additional.metadata.excel:}")
	private String metadataFile;

	@Value("${dmesync.doc.name}")
	private String doc;

	@Value("${dmesync.source.base.dir}")
	private String sourceDir;

	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

		logger.info("[PathMetadataTask] SCAF getArchivePath called");

		if (StringUtils.equalsIgnoreCase(getFileType(object), "tar")
				|| StringUtils.contains(object.getOriginalFilePath(), "summary_metrics.xlsx")) {

			threadLocalMap.set(loadMetadataFile(metadataFile, "project"));

			String fileName = Paths.get(object.getSourceFileName()).toFile().getName();
			String archivePath = null;
			String sampleSubFolder = getSampleSubFolder(object);
			String path = getProjectPathName(object);
			if (StringUtils.isBlank(sampleSubFolder)) {
				archivePath = destinationBaseDir + "/" + getPiCollectionName(object, path) + "_lab" + "/"
						+ getProjectCollectionName(object, path) + "/Analysis" + "/" + fileName;

			} else {

				archivePath = destinationBaseDir + "/" + getPiCollectionName(object, path) + "_lab" + "/"
						+ getProjectCollectionName(object, path) + "/" + getSCAFNumber(object) + "/" + sampleSubFolder
						+ "/" + getTarFileName(object, sampleSubFolder);

			}
			// replace spaces with underscore
			archivePath = archivePath.replace(" ", "_");

			logger.info("Archive path for {} : {}", object.getOriginalFilePath(), archivePath);

			return archivePath;
		} else {
			logger.info("Skipping the file since the file type is not tar {}", object.getOrginalFileName());
			throw new DmeSyncMappingException(
					"Skipping the file since the file type is not tar" + object.getOrginalFileName());
		}

	}

	@Override
	public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException {

		// Add to HpcBulkMetadataEntries for path attributes
		HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();
		String sampleSubFolder = getSampleSubFolder(object);
		String fileName = Paths.get(object.getSourceFileName()).toFile().getName();

		// Add path metadata entries for "PI_XXX" collection
		String metadataFileKey = getProjectPathName(object);
		String piCollectionName = getPiCollectionName(object, metadataFileKey);
		String piCollectionPath = destinationBaseDir + "/" + piCollectionName + "_lab";
		HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "PI_Lab"));
		piCollectionPath = piCollectionPath.replace(" ", "_");
		pathEntriesPI.setPath(piCollectionPath);
		pathEntriesPI.getPathMetadataEntries()
				.add(createPathEntry("data_owner", getAttrValueWithExactKey(metadataFileKey, "data_owner")));
		pathEntriesPI.getPathMetadataEntries()
				.add(createPathEntry("data_owner_email", getAttrValueWithKey(metadataFileKey, "data_owner_email")));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_affiliation",
				getAttrValueWithKey(metadataFileKey, "data_owner_affiliation")));

		// data_generator, affiliation, email: SCAF, CRTP, CCR-SCAF@nih.gov (workflow
		// will add this metadata)
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_generator", "SCAF"));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_generator_email", "CCR-SCAF@nih.gov"));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_generator_affiliation", "CRTP"));

		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPI);

		// Add path metadata entries for "Project" collection
		// Example row: collectionType - Project, collectionName - CS027118
		String projectCollectionName = getProjectCollectionName(object, metadataFileKey);
		String projectCollectionPath = piCollectionPath + "/" + projectCollectionName;
		HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_id", projectCollectionName));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Controlled Access"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("retention_years ", "7"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_status", "Active"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_title", projectCollectionName));

		// TODO: update logic for project_start_date
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_start_date", new Date().toString(), "MM/dd/yy"));

		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_poc", getAttrValueWithKey(metadataFileKey, "project_poc_id")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc_affiliation",
				getAttrValueWithKey(metadataFileKey, "project_poc_affiliation")));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_poc_email", getAttrValueWithKey(metadataFileKey, "project_poc_email")));

		// optional data
		if (StringUtils.isNotBlank(getAttrValueWithKey(metadataFileKey, "project_description")))
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_description",
					getAttrValueWithKey(metadataFileKey, "project_description")));

		if (StringUtils.isNotBlank(getAttrValueWithKey(metadataFileKey, "key_collaborator")))
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("key_collaborator", getAttrValueWithKey(metadataFileKey, "key_collaborator")));
		if (StringUtils.isNotBlank(getAttrValueWithKey(metadataFileKey, "key_collaborator_affiliation")))
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("key_collaborator_affiliation",
					getAttrWithKey(metadataFileKey, "key_collaborator_affiliation")));
		if (StringUtils.isNotBlank(getAttrValueWithKey(metadataFileKey, "key_collaborator_email")))
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("key_collaborator_email",
					getAttrValueWithKey(metadataFileKey, "key_collaborator_email")));

		// Optional Values
		if (StringUtils.isNotBlank(getAttrValueWithKey(metadataFileKey, "project_completed_date")))
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_completed_date",
					getAttrValueWithKey(metadataFileKey, "project_completed_date")));
		if (StringUtils.isNotBlank(getAttrValueWithKey(metadataFileKey, "pubmed_id")))
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("pubmed_id", getAttrValueWithKey(metadataFileKey, "pubmed_id")));
		if (StringUtils.isNotBlank(getAttrValueWithKey(metadataFileKey, "public_data_accession_id")))
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("public_data_accession_id",
					getAttrValueWithKey(metadataFileKey, "public_data_accession_id")));
		if (StringUtils.isNotBlank(getAttrValueWithKey(metadataFileKey, "Collaborators")))
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("Collaborators", getAttrValueWithKey(metadataFileKey, "Collaborators")));
		projectCollectionPath = projectCollectionPath.replace(" ", "_");
		pathEntriesProject.setPath(projectCollectionPath);
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);

		if (sampleSubFolder != null) {

			// Add path metadata entries for "Sample" collection
			// Example row: collectionType - Sample
			String sampleCollectionName = getSCAFNumber(object);
			String sampleCollectionPath = projectCollectionPath + "/" + sampleCollectionName;
			HpcBulkMetadataEntry pathEntriesSample = new HpcBulkMetadataEntry();
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Sample"));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sample_id", sampleCollectionName));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sample_name", sampleCollectionName));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("curation_status", "False"));

			if (StringUtils.isNotBlank(getAttrValueWithKey(metadataFileKey, "library_strategy")))
				pathEntriesSample.getPathMetadataEntries().add(
						createPathEntry("library_strategy", getAttrValueWithKey(metadataFileKey, "library_strategy")));

			if (StringUtils.isNotBlank(getAttrValueWithKey(metadataFileKey, "study_disease")))
				pathEntriesSample.getPathMetadataEntries()
						.add(createPathEntry("study_disease", getAttrValueWithKey(metadataFileKey, "study_disease")));

			if (StringUtils.isNotBlank(getAttrValueWithKey(metadataFileKey, "tissue_type")))
				pathEntriesSample.getPathMetadataEntries()
						.add(createPathEntry("tissue_type", getAttrValueWithKey(metadataFileKey, "tissue_type")));

			if (StringUtils.isNotBlank(getAttrValueWithKey(metadataFileKey, "age")))
				pathEntriesSample.getPathMetadataEntries()
						.add(createPathEntry("age", getAttrValueWithKey(metadataFileKey, "age")));

			if (StringUtils.isNotBlank(getAttrValueWithKey(metadataFileKey, "organism_strain")))
				pathEntriesSample.getPathMetadataEntries().add(
						createPathEntry("organism_strain", getAttrValueWithKey(metadataFileKey, "organism_strain")));
			if (StringUtils.isNotBlank(getAttrValueWithKey(metadataFileKey, "gender")))
				pathEntriesSample.getPathMetadataEntries()
						.add(createPathEntry("gender", getAttrValueWithKey(metadataFileKey, "gender")));
			pathEntriesSample.setPath(sampleCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesSample);

			String sampleSubCollectionPath = sampleCollectionPath + "/" + sampleSubFolder;
			HpcBulkMetadataEntry pathEntriesSubSample = new HpcBulkMetadataEntry();
			pathEntriesSubSample.getPathMetadataEntries()
					.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, sampleSubFolder));
			pathEntriesSubSample.setPath(sampleSubCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesSubSample);
		} else {

			String AnalyisCollectionPath = projectCollectionPath + "/" + "Analysis";
			HpcBulkMetadataEntry pathEntriesAnalysis = new HpcBulkMetadataEntry();
			pathEntriesAnalysis.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Analysis"));
			pathEntriesAnalysis.setPath(AnalyisCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesAnalysis);

		}

		// Set it to dataObjectRegistrationRequestDTO
		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();
		dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
		dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
		dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);
		// Add object metadata
		// (derived)
		dataObjectRegistrationRequestDTO.getMetadataEntries()
				.add(createPathEntry("source_path", object.getOriginalFilePath()));
		dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("file_type", getFileType(object)));

		if (sampleSubFolder != null) {
			dataObjectRegistrationRequestDTO.getMetadataEntries()
					.add(createPathEntry("object_name", getTarFileName(object, sampleSubFolder)));
			// TODO: update logic for platform_name

			dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("platform_name",  getAttrValueWithKey(metadataFileKey, "platform_name")));
			if (StringUtils.isNotBlank(getAttrValueWithKey(metadataFileKey, "analyte_type")))
				dataObjectRegistrationRequestDTO.getMetadataEntries()
						.add(createPathEntry("analyte_type", getAttrValueWithKey(metadataFileKey, "analyte_type")));
			if (StringUtils.isNotBlank(getAttrValueWithKey(metadataFileKey, "chemistry")))
				dataObjectRegistrationRequestDTO.getMetadataEntries()
						.add(createPathEntry("chemistry", getAttrValueWithKey(metadataFileKey, "chemistry")));
		} else {
			dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("object_name", fileName));

		}
		logger.info("Metadata custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
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

	private String getPiCollectionName(StatusInfo object, String path) throws DmeSyncMappingException {
		String piCollectionName = null;
		piCollectionName = getAttrValueWithKey(path, "data_owner_full_name");

		logger.info("Lab Collection Name: {}", piCollectionName);
		return piCollectionName;
	}

	private String getProjectCollectionName(StatusInfo object, String metadataFileKey) throws DmeSyncMappingException {
		// Example: If originalFilePath is
		/// mnt/scaf-ccr-a-data/CS029391_Staudt_Shaffer/02_PrimaryAnalysisOutput
		// then return CS029391_Staudt_Shaffer
		return getAttrValueWithKey(metadataFileKey, "project_id");
	}

	private String getProjectPathName(StatusInfo object) throws DmeSyncMappingException {
		// Example: If originalFilePath is
		/// mnt/scaf-ccr-a-data/CS029391_Staudt_Shaffer/02_PrimaryAnalysisOutput
		// then return CS029391_Staudt_Shaffer
		return getCollectionNameFromParent(object, "scaf-ccr-a-data");
	}

	private String getSampleSubFolder(StatusInfo object) {
		String sampleSubFolder = null;
		if (StringUtils.containsIgnoreCase(object.getOriginalFilePath(), "01_DemultiplexedFastqs")) {
			sampleSubFolder = FASTQ;
			logger.info("sampleSubFolder: {}", sampleSubFolder);
		} else if ((StringUtils.containsIgnoreCase(object.getOriginalFilePath(), "00_FullCellrangerOutputs"))) {
			sampleSubFolder = PRIMARY_ANALYSIS_OUTPUT_NAME;
			logger.info("sampleSubFolder: {}", sampleSubFolder);

		}
		return sampleSubFolder;

	}

	private String getTarFileName(StatusInfo object, String sampleSubFolder) throws DmeSyncMappingException {
		String tarFileName = null;
		String scafNumber = getSCAFNumber(object);

		if (StringUtils.equals(FASTQ, sampleSubFolder)) {
			tarFileName = scafNumber + "_FQ_" + getFlowcellId(object) + "_" + getChemistry(object) + "."
					+ getFileType(object);
		} else if (StringUtils.equals(PRIMARY_ANALYSIS_OUTPUT_NAME, sampleSubFolder))
			// TODO: Add Chemistry function
			tarFileName = scafNumber + "_PA_" + getChemistryforPAO(object) + "." + getFileType(object);
		logger.info("tarFileName: {}", tarFileName);
		return tarFileName;
	}

	private String getSCAFNumber(StatusInfo object) throws DmeSyncMappingException {

		// filename without the extension is the SCAF Number
		String fileName = Paths.get(object.getSourceFileName()).toFile().getName();
		String sampleName = fileName.replaceAll("\\.tar$", "");

		logger.info("Sample Name", sampleName);
		if (sampleName != null && sampleName.startsWith("SCAF")) {
			return sampleName;
		} else {
			logger.info("Different Structure: The name of fastq file or cellranger output file (sample name) doesn't start with SCAF: {}",
					object.getSourceFilePath());
			throw new DmeSyncMappingException(
					"Different Structure:The name of fastq file or cellranger output (sample name) file doesn't start with SCAF: "
							+ object.getSourceFilePath());
		}
	}

	private String getFlowcellId(StatusInfo object) throws DmeSyncMappingException {
		String flowcellId = null;
		// Example: If originalFilePath is
		// /mnt/scaf-ccr-a-data/CS029391_Staudt_Shaffer/01_DemultiplexedFastqs/Seq1_AAAKYHJM5_VDJ
		// then the flowcell Id will be AAAKYHJM5

		flowcellId = getFlowcellName(object);
		flowcellId = StringUtils.substringBeforeLast(StringUtils.substring(flowcellId, flowcellId.indexOf('_') + 1),
				"_");
		logger.info("flowcellId: {}", flowcellId);
		return flowcellId;
	}

	private String getChemistry(StatusInfo object) throws DmeSyncMappingException {
		String chemistry = null;
		// Example: If originalFilePath is
		// /mnt/scaf-ccr-a-data/CS029391_Staudt_Shaffer/01_DemultiplexedFastqs/Seq1_AAAKYHJM5_VDJ
		// then the chemistry will be VDJ

		String flowcellName = getFlowcellName(object);
		chemistry = StringUtils.substringAfterLast(StringUtils.substring(flowcellName, flowcellName.indexOf('_') + 1),
				"_");
		logger.info("Chemistry: {}", chemistry);
		return chemistry;
	}

	private String getChemistryforPAO(StatusInfo object) throws DmeSyncMappingException {
		Path path = Paths.get(object.getOriginalFilePath());
		String chemistry = path.getParent().getFileName().toString();
		if (chemistry != null && !chemistry.startsWith("00_FullCellrangerOutputs")) {
			logger.info("chemistry: {}", chemistry);
			return chemistry;
		} else {
			logger.info("The chemistry metadata attribute is not able to derive from the parent folder: {}", object.getOriginalFilePath() );
			throw new DmeSyncMappingException(
					"Different Structure:The chemistry metadata attribute couldn't be able derive from the parent folder path: " + object.getOriginalFilePath());
		}
	}

	private String getAttrWithKey(String key, String attrKey) {
		if (StringUtils.isEmpty(key)) {
			logger.error("Excel mapping not found for {}", key);
			return null;
		}
		return (metadataMap.get(key) == null ? null : metadataMap.get(key).get(attrKey));
	}

	private String getFileType(StatusInfo object) {
		String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
		return fileName.substring(fileName.indexOf('.') + 1);
	}

	private String getFlowcellName(StatusInfo object) throws DmeSyncMappingException {

		String flowcellName = getCollectionNameFromParent(object, "01_DemultiplexedFastqs");
		if (flowcellName != null && flowcellName.startsWith("Seq")) {
			logger.info("flowcellId: {}", flowcellName);
			return flowcellName;
		} else {
			logger.info("Different Structure:The flowcell Id couldn't be able to derive from the path: {}", object.getOriginalFilePath());
			throw new DmeSyncMappingException(
					"Different Structure: The flowcell Id couldn't be able to derive from the path" + object.getOriginalFilePath());
		}
	}

}
