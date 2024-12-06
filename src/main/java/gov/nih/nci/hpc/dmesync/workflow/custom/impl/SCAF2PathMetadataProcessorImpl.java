package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.nio.file.Path;
import java.nio.file.Paths;
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
@Service("scaf2")
public class SCAF2PathMetadataProcessorImpl extends AbstractPathMetadataProcessor
		implements DmeSyncPathMetadataProcessor {

	// SCAF DME path construction and meta data creation

	public static final String PRIMARY_ANALYSIS_OUTPUT_NAME = "Primary Analysis Output";
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

		// Example source path -
		// /SCAF/CS027118/02_PrimaryAnalysisOutput/00_FullCellrangerOutputs/GEX/SCAF1536_CD45TumCor/outs
		String fileName = Paths.get(object.getSourceFileName()).toFile().getName();
		String archivePath = null;
		String sampleName = getSampleName(object);
		String sampleSubFolder = getSampleSubFolder(object);

		if (StringUtils.isBlank(sampleSubFolder)) {
			archivePath = destinationBaseDir + "/PI_" + getPiCollectionName(object) + "/Project_"
					+ getProjectCollectionName(object) + "/Analysis" + fileName;

		} else {
			archivePath = destinationBaseDir + "/PI_" + getPiCollectionName(object) + "/Project_"
					+ getProjectCollectionName(object) + "/Sample_" + getSampleName(object) + "/" + sampleSubFolder
					+ getTarFileName(object, sampleSubFolder);

		}
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
		String sampleSubFolder = getSampleSubFolder(object);
		String tarFileName = getTarFileName(object, sampleSubFolder);
		// String fileName = getSampleName(object) + "_" +
		// Paths.get(object.getSourceFileName()).toFile().getName();
		String scafId = getSCAFNumber(object);

		// Add path metadata entries for "PI_XXX" collection
		// Example row: collectionType - PI, collectionName - XXX (derived)
		// key = data_owner, value = Tim Greten (supplied)
		// key = data_owner_affiliation, value = Thoracic and GI Malignancies Branch
		// (supplied)
		String piCollectionName = getPiCollectionName(object);
		String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;
		HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "PI_Lab"));
		pathEntriesPI.setPath(piCollectionPath);
		hpcBulkMetadataEntries.getPathsMetadataEntries()
				.add(populateStoredMetadataEntries(pathEntriesPI, "PI_Lab", piCollectionName, "scaf"));

		// Add path metadata entries for "Project" collection
		// Example row: collectionType - Project, collectionName - CS027118
		String projectCollectionName = getProjectCollectionName(object);
		String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;
		HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_id", projectCollectionName));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Controlled Access"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("retention_years ", "7"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_status", "Active"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_title", projectCollectionName));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_start_date", getAttrWithKey(scafId, "project_start_date"), "MM/dd/yy"));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_description", getAttrWithKey(scafId, "project_description")));

		// TODO: Project POC details
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc", " "));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc_affiliation", " "));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc_email", " "));

		// optional data

		if (StringUtils.isNotBlank(getAttrWithKey(scafId, "key_collaborator")))
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("key_collaborator", getAttrWithKey(scafId, "key_collaborator")));
		if (StringUtils.isNotBlank(getAttrWithKey(scafId, "key_collaborator_affiliation")))
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("key_collaborator_affiliation",
					getAttrWithKey(scafId, "key_collaborator_affiliation")));
		if (StringUtils.isNotBlank(getAttrWithKey(scafId, "key_collaborator_email")))
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("key_collaborator_email", getAttrWithKey(scafId, "key_collaborator_email")));

		// Optional Values
		if (StringUtils.isNotBlank(getAttrWithKey(scafId, "project_completed_date")))
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("project_completed_date", getAttrWithKey(scafId, "project_completed_date")));
		if (StringUtils.isNotBlank(getAttrWithKey(scafId, "pubmed_id")))
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("pubmed_id", getAttrWithKey(scafId, "pubmed_id")));
		if (StringUtils.isNotBlank(getAttrWithKey(scafId, "Collaborators")))
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("Collaborators", getAttrWithKey(scafId, "Collaborators")));
		pathEntriesProject.setPath(projectCollectionPath);


		if (sampleSubFolder!=null) {
			// Add path metadata entries for "sample" collection
			// Example row: collectionType - Patient
			String patientCollectionName = getPatientCollectionName(object);
			String patientCollectionPath = projectCollectionPath + "/Patient_" + patientCollectionName;
			HpcBulkMetadataEntry pathEntriesPatient = new HpcBulkMetadataEntry();
			pathEntriesPatient.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Patient"));
			pathEntriesPatient.getPathMetadataEntries().add(createPathEntry("patient_id", patientCollectionName));
			pathEntriesPatient.getPathMetadataEntries().add(createPathEntry("age", getAttrWithKey(scafId, "age")));
			pathEntriesPatient.getPathMetadataEntries()
					.add(createPathEntry("gender", getAttrWithKey(scafId, "gender")));
			pathEntriesPatient.getPathMetadataEntries().add(createPathEntry("race", getAttrWithKey(scafId, "race")));
			pathEntriesPatient.getPathMetadataEntries()
					.add(createPathEntry("disease_type", getAttrWithKey(scafId, "disease_type")));
			pathEntriesPatient.getPathMetadataEntries()
					.add(createPathEntry("primary_site", getAttrWithKey(scafId, "primary_site")));
			pathEntriesPatient.setPath(patientCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPatient);

			// Add path metadata entries for "Sample" collection
			// Example row: collectionType - Sample
			String sampleCollectionName = getSampleName(object);
			String sampleCollectionPath = patientCollectionPath + "/Sample_" + sampleCollectionName;
			HpcBulkMetadataEntry pathEntriesSample = new HpcBulkMetadataEntry();
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Sample"));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sample_name", sampleCollectionName));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("scaf_number", scafId));
			pathEntriesSample.getPathMetadataEntries()
					.add(createPathEntry("sample_id", getAttrWithKey(scafId, "New sample ID")));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("library_strategy", "SingleCellRNA-Seq"));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("analyte_type", "RNA"));
			pathEntriesSample.getPathMetadataEntries()
					.add(createPathEntry("tissue", getAttrWithKey(scafId, "primary_site")));
			pathEntriesSample.getPathMetadataEntries()
					.add(createPathEntry("tissue_type", getAttrWithKey(scafId, "Location")));
			pathEntriesSample.setPath(sampleCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesSample);
		}

		// Set it to dataObjectRegistrationRequestDTO
		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();
		dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
		dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
		dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);

		// Add object metadata
		// key = object_name, value = SCAF1535_CD45TumRim_outs.tar
		// (derived)
		dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("object_name", tarFileName));
		dataObjectRegistrationRequestDTO.getMetadataEntries()
				.add(createPathEntry("source_path", object.getOriginalFilePath()));
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

	private String getPiCollectionName(StatusInfo object) throws DmeSyncMappingException {
		String piCollectionName = null;
		piCollectionName = getCollectionMappingValue("SCAF", "PI_Lab", "scaf");

		logger.info("PI Collection Name: {}", piCollectionName);
		return piCollectionName;
	}

	private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {
		// Example: If originalFilePath is
		/// mnt/scaf-ccr-a-data/CS029391_Staudt_Shaffer/02_PrimaryAnalysisOutput
		// then return CS029391_Staudt_Shaffer
		return getCollectionNameFromParent(object, "scaf-ccr-a-data");
	}

	private String getSampleName(StatusInfo object) throws DmeSyncMappingException {
		String path = Paths.get(object.getOriginalFilePath()).toString();
		String sampleName = null;
		// Example: If originalFilePath is
		// /SCAF/CS027118/02_PrimaryAnalysisOutput/00_FullCellrangerOutputs/GEX/SCAF1536_CD45TumCor/outs
		// then return SCAF1536_CD45TumCor
		if (path.contains("GEX")) {
			sampleName = getCollectionNameFromParent(object, "GEX");
		} else {
			sampleName = getCollectionNameFromParent(object, "TCR");
		}
		return sampleName;
	}

	private boolean isAggregatedDatasets(StatusInfo object) throws DmeSyncMappingException {
		return StringUtils.equals(getSampleName(object), "AggregatedDatasets");
	}

	private String getSampleSubFolder(StatusInfo object) {
		String sampleSubFolder = null;
		if (StringUtils.containsIgnoreCase("01_DemultiplexedFastqs", object.getOriginalFilePath())) {
			sampleSubFolder = FASTQ;
			logger.info("sampleSubFolder: {}", sampleSubFolder);
		} else if ((StringUtils.containsIgnoreCase("00_FullCellrangerOutputs", object.getOriginalFilePath()))) {

			sampleSubFolder = PRIMARY_ANALYSIS_OUTPUT_NAME;
			logger.info("sampleSubFolder: {}", sampleSubFolder);

		}
		return sampleSubFolder;

	}

	private String getTarFileName(StatusInfo object, String sampleSubFolder) throws DmeSyncMappingException {
		String tarFileName = null;
		String scafNumber = getSCAFNumber(object);
		if (StringUtils.equals(FASTQ, sampleSubFolder)) {
			tarFileName = scafNumber + "_ FQ_ " + getFlowcellId(object) + "_" + getChemistry(object);
		} else if (StringUtils.equals(PRIMARY_ANALYSIS_OUTPUT_NAME, sampleSubFolder))
			// TODO: Add Chemistry function
			tarFileName = scafNumber + "_ PA_ " + "GEX";
		logger.info("tarFileName: {}", tarFileName);
		return tarFileName;
	}

	private String getSCAFNumber(StatusInfo object) throws DmeSyncMappingException {
		String sampleName = getSampleName(object);
		// Example: If sample name is SCAF1536_CD45TumCor or SCAF1536t_CD45TumCor
		// then return SCAF1536
		String scafNumber = StringUtils.substringBefore(sampleName, "t_");
		return StringUtils.substringBefore(scafNumber, "_");
	}

	private String getFlowcellId(StatusInfo object) {
		String flowcellId = null;
		// Example: If originalFilePath is
		// /mnt/scaf-ccr-a-data/CS029391_Staudt_Shaffer/01_DemultiplexedFastqs/Seq1_AAAKYHJM5_VDJ
		// then the flowcell Id will be AAAKYHJM5

		flowcellId = getCollectionNameFromParent(object, "ClassifierReports");
		flowcellId = StringUtils.substringBeforeLast(StringUtils.substring(flowcellId, flowcellId.indexOf('_') + 1),
				"_");
		logger.info("flowcellId: {}", flowcellId);
		return flowcellId;
	}

	private String getChemistry(StatusInfo object) {
		String chemistry = null;
		// Example: If originalFilePath is
		// /mnt/scaf-ccr-a-data/CS029391_Staudt_Shaffer/01_DemultiplexedFastqs/Seq1_AAAKYHJM5_VDJ
		// then the chemistry will be VDJ

		chemistry = getCollectionNameFromParent(object, "ClassifierReports");
		chemistry = StringUtils.substringAfterLast(StringUtils.substring(chemistry, chemistry.indexOf('_') + 1), "_");
		logger.info("flowcellId: {}", chemistry);
		return chemistry;
	}

	private String getPatientCollectionName(StatusInfo object) throws DmeSyncMappingException {
		String scafId = getSCAFNumber(object);
		return getAttrWithKey(scafId, "Patient ID");
	}

	private String getAttrWithKey(String key, String attrKey) {
		if (StringUtils.isEmpty(key)) {
			logger.error("Excel mapping not found for {}", key);
			return null;
		}
		return (metadataMap.get(key) == null ? null : metadataMap.get(key).get(attrKey));
	}

	
}
