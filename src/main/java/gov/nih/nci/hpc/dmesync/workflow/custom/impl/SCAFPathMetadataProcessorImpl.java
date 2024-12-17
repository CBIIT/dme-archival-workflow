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
@Service("scaf")
public class SCAFPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
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

		threadLocalMap.set(loadMetadataFile(metadataFile, "Path"));
		
		String fileName = Paths.get(object.getSourceFileName()).toFile().getName();
		String archivePath = null;
		String sampleSubFolder = getSampleSubFolder(object);
        String path=null;
		if (StringUtils.isBlank(sampleSubFolder)) {
			archivePath = destinationBaseDir  + "/"+ getPiCollectionName(object,path) +"_lab" + "/"
					+ getProjectCollectionName(object) + "/Analysis" + "/"+ fileName;

		} else {
			archivePath = destinationBaseDir + "/" + getPiCollectionName(object,path) + "_lab" + "/"
					+ getProjectCollectionName(object) + "/" + getSCAFNumber(object) + "/" + sampleSubFolder + "/"
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
		String fileName = Paths.get(object.getSourceFileName()).toFile().getName();

		// Add path metadata entries for "PI_XXX" collection
		String metadataFileKey = "Sample";
		String piCollectionName = getPiCollectionName(object,metadataFileKey);
		String piCollectionPath = destinationBaseDir + "/" + piCollectionName + "_lab";
		HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "PI_Lab"));
		pathEntriesPI.setPath(piCollectionPath);
		pathEntriesPI.getPathMetadataEntries()
				.add(createPathEntry("data_owner", getAttrValueWithExactKey(metadataFileKey, "data_owner")));
		pathEntriesPI.getPathMetadataEntries()
				.add(createPathEntry("data_owner_email", getAttrValueWithKey(metadataFileKey, "data_owner_email")));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_affiliation",
				getAttrValueWithKey(metadataFileKey, "data_owner_affiliation")));
		
		//data_generator, affiliation, email: SCAF, CRTP, CCR-SCAF@nih.gov (workflow will add this metadata)		
		pathEntriesPI.getPathMetadataEntries()
				.add(createPathEntry("data_generator", "SCAF"));
		pathEntriesPI.getPathMetadataEntries().add(
				createPathEntry("data_generator_email","CCR-SCAF@nih.gov"));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_generator_affiliation",
				"CRTP"));

		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPI);

		// Add path metadata entries for "Project" collection
		// Example row: collectionType - Project, collectionName - CS027118
		String projectCollectionName = getProjectCollectionName(object);
		String projectCollectionPath = piCollectionPath + "/" + projectCollectionName;
		HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_id", projectCollectionName));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Controlled Access"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("retention_years ", "7"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_status", "Active"));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_title", projectCollectionName));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_start_date",
				getAttrWithKey(metadataFileKey, "project_start_date"), "MM/dd/yy"));

		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_poc", getAttrWithKey(metadataFileKey, "project_poc")));
		pathEntriesProject.getPathMetadataEntries().add(
				createPathEntry("project_poc_affiliation", getAttrWithKey(metadataFileKey, "project_poc_affiliation")));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_poc_email", getAttrWithKey(metadataFileKey, "project_poc_email")));

		// optional data
		if (StringUtils.isNotBlank(getAttrWithKey(metadataFileKey, "project_description")))
			pathEntriesProject.getPathMetadataEntries().add(
					createPathEntry("project_description", getAttrWithKey(metadataFileKey, "project_description")));

		if (StringUtils.isNotBlank(getAttrWithKey(metadataFileKey, "key_collaborator")))
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("key_collaborator", getAttrWithKey(metadataFileKey, "key_collaborator")));
		if (StringUtils.isNotBlank(getAttrWithKey(metadataFileKey, "key_collaborator_affiliation")))
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("key_collaborator_affiliation",
					getAttrWithKey(metadataFileKey, "key_collaborator_affiliation")));
		if (StringUtils.isNotBlank(getAttrWithKey(metadataFileKey, "key_collaborator_email")))
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("key_collaborator_email",
					getAttrWithKey(metadataFileKey, "key_collaborator_email")));

		// Optional Values
		if (StringUtils.isNotBlank(getAttrWithKey(metadataFileKey, "project_completed_date")))
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_completed_date",
					getAttrWithKey(metadataFileKey, "project_completed_date")));
		if (StringUtils.isNotBlank(getAttrWithKey(metadataFileKey, "pubmed_id")))
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("pubmed_id", getAttrWithKey(metadataFileKey, "pubmed_id")));
		if (StringUtils.isNotBlank(getAttrWithKey(metadataFileKey, "public_data_accession_id")))
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("public_data_accession_id",
					getAttrWithKey(metadataFileKey, "public_data_accession_id")));
		if (StringUtils.isNotBlank(getAttrWithKey(metadataFileKey, "Collaborators")))
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("Collaborators", getAttrWithKey(metadataFileKey, "Collaborators")));
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
			pathEntriesSample.getPathMetadataEntries()
					.add(createPathEntry("sample_name", sampleCollectionName));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("curation_status", "False"));

			if (StringUtils.isNotBlank(getAttrWithKey(metadataFileKey, "library_strategy")))
				pathEntriesSample.getPathMetadataEntries()
						.add(createPathEntry("library_strategy", getAttrWithKey(metadataFileKey, "library_strategy")));

			if (StringUtils.isNotBlank(getAttrWithKey(metadataFileKey, "study_disease")))
				pathEntriesSample.getPathMetadataEntries()
						.add(createPathEntry("study_disease", getAttrWithKey(metadataFileKey, "study_disease")));

			if (StringUtils.isNotBlank(getAttrWithKey(metadataFileKey, "tissue_type")))
				pathEntriesSample.getPathMetadataEntries()
						.add(createPathEntry("tissue_type", getAttrWithKey(metadataFileKey, "tissue_type")));

			if (StringUtils.isNotBlank(getAttrWithKey(metadataFileKey, "age")))
				pathEntriesSample.getPathMetadataEntries()
						.add(createPathEntry("age", getAttrWithKey(metadataFileKey, "age")));

			if (StringUtils.isNotBlank(getAttrWithKey(metadataFileKey, "organism_strain")))
				pathEntriesSample.getPathMetadataEntries()
						.add(createPathEntry("organism_strain", getAttrWithKey(metadataFileKey, "organism_strain")));
			if (StringUtils.isNotBlank(getAttrWithKey(metadataFileKey, "gender")))
				pathEntriesSample.getPathMetadataEntries()
						.add(createPathEntry("gender", getAttrWithKey(metadataFileKey, "gender")));
			pathEntriesSample.setPath(sampleCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesSample);
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
			dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("object_name", getTarFileName(object, sampleSubFolder)));
			dataObjectRegistrationRequestDTO.getMetadataEntries()
					.add(createPathEntry("platform_name", getAttrWithKey(metadataFileKey, "platform_name")));
			if (StringUtils.isNotBlank(getAttrWithKey(metadataFileKey, "analyte_type")))
				dataObjectRegistrationRequestDTO.getMetadataEntries()
						.add(createPathEntry("analyte_type", getAttrWithKey(metadataFileKey, "analyte_type")));
			if (StringUtils.isNotBlank(getAttrWithKey(metadataFileKey, "chemistry")))
				dataObjectRegistrationRequestDTO.getMetadataEntries()
						.add(createPathEntry("chemistry", getAttrWithKey(metadataFileKey, "chemistry")));
		}else {
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
		piCollectionName = getAttrValueWithKey(path, "data_owner");
        
		logger.info("Lab Collection Name: {}", piCollectionName);
		return piCollectionName;
	}

	private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {
		// Example: If originalFilePath is
		/// mnt/scaf-ccr-a-data/CS029391_Staudt_Shaffer/02_PrimaryAnalysisOutput
		// then return CS029391_Staudt_Shaffer
		return getCollectionNameFromParent(object, "scaf-ccr-a-data");
	}

	private String getSampleName(StatusInfo object) throws DmeSyncMappingException {
		Path path = Paths.get(object.getOriginalFilePath());
		String sampleName=path.getFileName().toString().replaceAll("\\.tar$", "");
		
		logger.info("Sample Name",sampleName);
		return sampleName;

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
			tarFileName = scafNumber + "_ FQ_ " + getFlowcellId(object) + "_" + getChemistry(object)+"." + getFileType(object);
		} else if (StringUtils.equals(PRIMARY_ANALYSIS_OUTPUT_NAME, sampleSubFolder))
			// TODO: Add Chemistry function
			tarFileName = scafNumber + "_ PA_ " + getChemistry(object)+"." + getFileType(object);
		logger.info("tarFileName: {}", tarFileName);
		return tarFileName;
	}

	private String getSCAFNumber(StatusInfo object) throws DmeSyncMappingException {
		
		// filename without the extension is the SCAF Number
		   String fileName = Paths.get(object.getSourceFileName()).toFile().getName();
			String sampleName=fileName.replaceAll("\\.tar$", "");
			
			logger.info("Sample Name",sampleName);
			return sampleName;

		
	}

	private String getFlowcellId(StatusInfo object) {
		String flowcellId = null;
		// Example: If originalFilePath is
		// /mnt/scaf-ccr-a-data/CS029391_Staudt_Shaffer/01_DemultiplexedFastqs/Seq1_AAAKYHJM5_VDJ
		// then the flowcell Id will be AAAKYHJM5

		flowcellId = getCollectionNameFromParent(object, "01_DemultiplexedFastqs");
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

		chemistry = getCollectionNameFromParent(object, "01_DemultiplexedFastqs");
		chemistry = StringUtils.substringAfterLast(StringUtils.substring(chemistry, chemistry.indexOf('_') + 1), "_");
		logger.info("Chemistry: {}", chemistry);
		return chemistry;
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
	
}
