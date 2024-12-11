package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import gov.nih.nci.hpc.dmesync.util.ExcelUtil;

/**
 * SCAF DME Path and Meta-data Processor Implementation
 *
 * @author konerum3
 */
@Service("scaf")
public class SCAFPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
		implements DmeSyncPathMetadataProcessor {

	// SCAF DME path construction and meta data creation

	public static final String PRIMARY_ANALYSIS_OUTPUT_NAME = "PrimaryAnalysisOutput";
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
		String sampleSubFolder = getSampleSubFolder(object);
		if (StringUtils.isBlank(sampleSubFolder)) {
			retieveAnalysisMetadataFilePath(object.getOriginalFilePath());
			String piCollectionName = getPiCollectionName(object);
			archivePath = destinationBaseDir + "/PI_" + piCollectionName + "/Project_"
					+ getProjectCollectionName(object) + "/Analysis" + fileName;

		} else {
			retrieveMetadataFromFile(object.getOriginalFilePath());
			archivePath = destinationBaseDir + "/PI_" + getPiCollectionName(object) + "/Project_"
					+ getProjectCollectionName(object) + "/Sample_" + getSampleName(object) + "/" + sampleSubFolder +"/"
					+ fileName;

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
		// Example row: collectionType - PI, collectionName - XXX (derived)
		// key = data_owner, value = Tim Greten (supplied)
		// key = data_owner_affiliation, value = Thoracic and GI Malignancies Branch
		// (supplied)
		String piCollectionName = getPiCollectionName(object);
		String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;
		String metadataFileKey = "Sample";
		HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "PI_Lab"));
		pathEntriesPI.setPath(piCollectionPath);
		pathEntriesPI.getPathMetadataEntries()
				.add(createPathEntry("data_owner", getAttrValueWithExactKey(metadataFileKey, "data_owner")));
		pathEntriesPI.getPathMetadataEntries()
				.add(createPathEntry("data_owner_email", getAttrValueWithKey(metadataFileKey, "data_owner_email")));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_affiliation",
				getAttrValueWithKey(metadataFileKey, "data_owner_affiliation")));
		pathEntriesPI.getPathMetadataEntries()
				.add(createPathEntry("data_generator", getAttrValueWithKey(metadataFileKey, "data_generator")));
		pathEntriesPI.getPathMetadataEntries().add(
				createPathEntry("data_generator_email", getAttrValueWithKey(metadataFileKey, "data_generator_email")));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_generator_affiliation",
				getAttrValueWithKey(metadataFileKey, "data_generator_affiliation")));

		pathEntriesPI.getPathMetadataEntries()
				.add(createPathEntry("data_owner_designee", getAttrValueWithKey(metadataFileKey, "data_owner")));
		pathEntriesPI.getPathMetadataEntries().add(
				createPathEntry("data_owner_designee_email", getAttrValueWithKey(metadataFileKey, "data_owner_email")));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_designee_affiliation",
				getAttrValueWithKey(metadataFileKey, "data_owner_affiliation ")));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPI);

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
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_title", getAttrValueWithKey(metadataFileKey, "project_title")));
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
			String sampleCollectionName = getSampleName(object);
			String sampleCollectionPath = projectCollectionPath + "/Sample_" + sampleCollectionName;
			HpcBulkMetadataEntry pathEntriesSample = new HpcBulkMetadataEntry();
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Sample"));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sample_id", sampleCollectionName));
			pathEntriesSample.getPathMetadataEntries()
					.add(createPathEntry("sample_name", getAttrWithKey(metadataFileKey, "sample_name")));
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
		dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("object_name", fileName));
		dataObjectRegistrationRequestDTO.getMetadataEntries()
				.add(createPathEntry("source_path", object.getOriginalFilePath()));
		dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("file_type", getFileType(object)));

		if (sampleSubFolder != null) {
			dataObjectRegistrationRequestDTO.getMetadataEntries()
					.add(createPathEntry("platform_name", getAttrWithKey(metadataFileKey, "platform_name")));
			if (StringUtils.isNotBlank(getAttrWithKey(metadataFileKey, "analyte_type")))
				dataObjectRegistrationRequestDTO.getMetadataEntries()
						.add(createPathEntry("analyte_type", getAttrWithKey(metadataFileKey, "analyte_type")));
			if (StringUtils.isNotBlank(getAttrWithKey(metadataFileKey, "chemistry")))
				dataObjectRegistrationRequestDTO.getMetadataEntries()
						.add(createPathEntry("chemistry", getAttrWithKey(metadataFileKey, "chemistry")));
		}
		logger.info("Metadata custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
		return dataObjectRegistrationRequestDTO;
	}

	private String getCollectionNameFromParent(String originalFilePath, String parentName) {
		Path fullFilePath = Paths.get(originalFilePath);
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
		// Example: If originalFilePath is
		/// /CCR_SCAF_Archive/Jay_Berzofsky_lab/CS036014_CS037666_Sui/Analysis/01_SummaryHTMLs.tar
		// then return Jay_Berzofsky_lab
		piCollectionName = getCollectionNameFromParent(getAttrValueWithKey("scaf", "object_name"),
				"CCR_SCAF_Archive");

		logger.info("PI Collection Name for : {}", piCollectionName);
		return piCollectionName;
	}

	private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {
		String projectCollectionName = null;
		projectCollectionName = getAttrValueWithKey("Sample", "project_id");
		logger.info("Project Collection Name  : {}", projectCollectionName);
		return projectCollectionName;
	}

	private String getProjectCollectionNameForAnalysis(StatusInfo object, String picollectionName)
			throws DmeSyncMappingException {
		// Example: If originalFilePath is
		//// CCR_SCAF_Archive/Jay_Berzofsky_lab/CS036014_CS037666_Sui/Analysis/01_SummaryHTMLs.tar
		// then return CS036014_CS037666_Sui
		return getCollectionNameFromParent(object.getOriginalFilePath(), picollectionName);
	}

	private String getSampleName(StatusInfo object) throws DmeSyncMappingException {

		String sampleCollectionName = null;
		sampleCollectionName = getAttrValueWithKey("Sample", "sample_id");
		logger.info("Sample Collection Name  : {}", sampleCollectionName);
		return sampleCollectionName;
	}

	private String getSampleSubFolder(StatusInfo object) {
		// TODO: Change the logic to Parent name
		String sampleSubFolder = null;
		if (StringUtils.containsIgnoreCase(object.getOriginalFilePath(), FASTQ)) {
			sampleSubFolder = FASTQ;
			logger.info("sampleSubFolder: {}", sampleSubFolder);
		} else if ((StringUtils.containsIgnoreCase(object.getOriginalFilePath(), PRIMARY_ANALYSIS_OUTPUT_NAME))) {

			sampleSubFolder = PRIMARY_ANALYSIS_OUTPUT_NAME;
			logger.info("sampleSubFolder: {}", sampleSubFolder);

		}
		return sampleSubFolder;

	}

	private String getAttrWithKey(String key, String attrKey) {
		if (StringUtils.isEmpty(key)) {
			logger.error("Excel mapping not found for {}", key);
			return null;
		}
		return (metadataMap.get(key) == null ? null : metadataMap.get(key).get(attrKey));
	}

	// For Anlaysis Folder, retrieve the filePath of any of the 
	public void retieveAnalysisMetadataFilePath(String filePath) throws DmeSyncMappingException {
		 
		Map<String, Map<String, String>> metdataMap = new HashMap<>();
		Path parentDir = Paths.get(filePath).getParent();
		Path parentParentDir=parentDir.getParent();

	        if (parentDir == null || !Files.exists(parentDir)) {
				throw new DmeSyncMappingException("Metadata json file not found for " + filePath);
	        }

	        // List all .metadata.json files in the parent directory, excluding "Analysis.metadata.json"
	        File[] metadataFiles = parentParentDir.toFile().listFiles((dir, name) ->
	                name.endsWith(".metadata.json") && !name.equals("Analysis.metadata.json"));

	        if (metadataFiles == null || metadataFiles.length == 0) {
	        	logger.error("Metadata json file not found for {}", filePath);
				throw new DmeSyncMappingException("Metadata json file not found for " + filePath);
			}

			metadataMap = loadAttributesJsonMetadataFile(metadataFiles[0].getAbsolutePath(), "scaf", metdataMap);
			String metadataFileName = Paths.get(filePath).getFileName().toString() + ".metadata.json";
	        File analyisMetadataFile = new File(parentDir.toFile(), metadataFileName);
			metadataMap = loadAttributesJsonMetadataFile(analyisMetadataFile.getAbsolutePath(), "scaf", metdataMap);
			threadLocalMap.set(metadataMap);


	}

	// Start from the file path and retrieve all the metadata for the folder and its
	// parents, only if metadata file name matches folder name
	public void retrieveMetadataFromFile(String filePath) throws DmeSyncMappingException {
		Path path = Paths.get(filePath);

		Map<String, Map<String, String>> metdataMap = new HashMap<>();
		// Traverse the directory structure upwards from the file
		while (path != null && Files.exists(path)) {
			Path parentPath = path.getParent();
			// Get the folder name and construct the metadata file name
			String folderName = path.getFileName().toString();
			String metadataFileName = folderName + ".metadata.json";

			// Look for metadata file that matches the folder name
			// Correctly resolve the path for the metadata file by checking the parent
			// directory
			Path metadataFilePath = parentPath.resolve(metadataFileName);

			// Look for metadata file in the current folder that matches the folder name
			File metadataFile = metadataFilePath.toFile();

			if (metadataFile.exists()) {
				metadataMap = loadAttributesJsonMetadataFile(metadataFile.getPath(), "scaf", metdataMap);

			} else {
				threadLocalMap.set(metadataMap);
				logger.info("No matching metadata file found for folder seeting the threadlocalMap: {}", folderName);
				break;
			}
			// Move to the parent directory for the next iteration
			path = path.getParent();
		}

	}

	private String getFileType(StatusInfo object) {
		String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
		return fileName.substring(fileName.indexOf('.') + 1);
	}

}
