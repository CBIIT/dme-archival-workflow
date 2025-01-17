package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.io.File;

import javax.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.apache.poi.xwpf.usermodel.XWPFRun;
import org.apache.poi.xwpf.usermodel.XWPFTable;
import org.apache.poi.xwpf.usermodel.XWPFTableCell;
import org.apache.poi.xwpf.usermodel.XWPFTableRow;
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

	public static final String REPORT_DATE = "Report Date";
	public static final String POC = "Project Point of Contact";
	public static final String PROJECT_ID = "NAS Project ID";
	public static final String PI = "Principal Investigator";

	@Value("${dmesync.additional.metadata.excel:}")
	private String metadataFile;

	@Value("${dmesync.sample.report:}")
	private String sampleFile;

	private String finalReportPath;

	@Value("${dmesync.doc.name}")
	private String doc;

	@Value("${dmesync.source.base.dir}")
	private String sourceDir;

	// Instance variable to hold the extracted fields
	private Map<String, String> medatadaMapFromReport;

	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException, IOException {

		logger.info("[PathMetadataTask] SCAF getArchivePath called");

		if (StringUtils.equalsIgnoreCase(getFileType(object), "tar")
				|| StringUtils.contains(object.getOriginalFilePath(), "summary_metrics.xlsx")) {

			threadLocalMap.set(loadMetadataFile(metadataFile, "Project"));
			constructFinalReportPath(object);
			extractMetadataFromFinalReport(finalReportPath.toString());
			String path = getProjectPathName(object);

			String fileName = Paths.get(object.getSourceFileName()).toFile().getName();
			String archivePath = null;
			String sampleCollectionType = getSampleCollectionType(object);
			if (StringUtils.isBlank(sampleCollectionType)) {
				archivePath = destinationBaseDir + "/" + getPiCollectionName(object, path) + "_lab" + "/"
						+ getProjectCollectionName(object, path) + "/Analysis" + "/" + fileName;

			} else {

				archivePath = destinationBaseDir + "/" + getPiCollectionName(object, path) + "_lab" + "/"
						+ getProjectCollectionName(object, path) + "/" + getSCAFNumber(object) + "/"
						+ sampleCollectionType + "/" + getTarFileName(object, sampleCollectionType);

			}
			// replace spaces with underscore
			archivePath = archivePath.replace(" ", "_");

			logger.info("Archive path for {} : {}", object.getOriginalFilePath(), archivePath);

			return archivePath;
		} else {
			logger.info("Error: Unsupported file type. Only '.tar' files are supported {}",
					object.getOrginalFileName());
			throw new DmeSyncMappingException(
					"Error: Unsupported file type. Only '.tar' files are supported:" + object.getOrginalFileName());
		}

	}

	@Override
	public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {

		// Add to HpcBulkMetadataEntries for path attributes
		HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();
		String sampleCollectionType = getSampleCollectionType(object);
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
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_status", "Completed"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_title", projectCollectionName));

		// TODO: project_start_date is extrated from the finalReport file in OtherData
		// folder.
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_start_date", medatadaMapFromReport.get(REPORT_DATE)));

		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_poc", getProjectPOC(object, metadataFileKey)));
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

		if (sampleCollectionType != null) {

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

			String sampleSubCollectionPath = sampleCollectionPath + "/" + sampleCollectionType;
			HpcBulkMetadataEntry pathEntriesSubSample = new HpcBulkMetadataEntry();
			pathEntriesSubSample.getPathMetadataEntries()
					.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, sampleCollectionType));
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

		if (sampleCollectionType != null) {
			// The plaform name metadata is extracted from the csv file provided from the
			// user.
			// samplecollectioname=SCAF45676_PRODCUT_1 where key in csv file is SCAF045676
			String sampleCollectionName = getSCAFNumber(object);
			String firstWord = sampleCollectionName.replaceAll("\\d.*", "");
			String numberPart = sampleCollectionName.replaceAll("\\D", "");
			String samplekey = firstWord + "0" + numberPart;

			logger.info("sample key to get the platform name {} ", samplekey);

			dataObjectRegistrationRequestDTO.getMetadataEntries()
					.add(createPathEntry("object_name", getTarFileName(object, sampleCollectionType)));

			threadLocalMap.set(loadCsvMetadataFile(sampleFile, "SCAF_Number"));

			dataObjectRegistrationRequestDTO.getMetadataEntries()
					.add(createPathEntry("platform_name", getAttrValueWithKey(samplekey, "Platform")));
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
		if (piCollectionName == null) {
			piCollectionName = medatadaMapFromReport.get(PI);
		}

		logger.info("Lab Collection Name: {}", piCollectionName);

		return piCollectionName;
	}

	private String getProjectCollectionName(StatusInfo object, String metadataFileKey) throws DmeSyncMappingException {
		// Example: If originalFilePath is
		/// mnt/scaf-ccr-a-data/CS029391_Staudt_Shaffer/02_PrimaryAnalysisOutput
		// then return CS029391_Shaffer
		String projectName = getAttrValueWithKey(metadataFileKey, "project_id");
		if (projectName == null) {
			throw new DmeSyncMappingException("Excel mapping not found for " + metadataFileKey);
		}
		return projectName;
	}

	private String getProjectPOC(StatusInfo object, String metadataFileKey) throws DmeSyncMappingException {
		// Example: If originalFilePath is
		/// mnt/scaf-ccr-a-data/CS029391_Staudt_Shaffer/02_PrimaryAnalysisOutput
		// then return CS029391_Shaffer
		String poc = medatadaMapFromReport.get(POC);
		if (poc != null) {
			String[] pocs = poc.trim().split("\\s*,\\s*");
			if (pocs.length > 1) {
				logger.info("Invalid project pocs : The project has more than one pocs {}", poc);
				throw new DmeSyncMappingException("Invalid project pocs : The project has more than one pocs  " + poc);
			}
		}
		String pocfromSpreadhseet = getAttrValueWithKey(metadataFileKey, "project_poc_id");
		if (pocfromSpreadhseet == null)
			return poc;
		return pocfromSpreadhseet;
	}

	private String getProjectPathName(StatusInfo object) throws DmeSyncMappingException {
		// Example: If originalFilePath is
		/// mnt/scaf-ccr-a-data/CS029391_Staudt_Shaffer/02_PrimaryAnalysisOutput
		// then return CS029391_Staudt_Shaffer
		String projectpathName = getCollectionNameFromParent(object, "scaf-ccr-a-data");

		if (projectpathName != null) {
			String[] projectKeywords = projectpathName.trim().split("_");
			if (projectKeywords.length != 3) {
				logger.info("Invalid project name : The project {} name has more/less than 3 words: {}",
						projectpathName, projectKeywords.length);
				throw new DmeSyncMappingException("Invalid project name : The project name " + projectpathName
						+ " has more/less than 3 words: " + object.getSourceFilePath());
			} else {
				return projectpathName;
			}

		}
		return null;
	}

	private String getSampleCollectionType(StatusInfo object) {
		String sampleCollectionType = null;
		if (StringUtils.containsIgnoreCase(object.getOriginalFilePath(), "01_DemultiplexedFastqs")) {
			sampleCollectionType = FASTQ;
			logger.info("sampleCollectionType : {}", sampleCollectionType);
		} else if ((StringUtils.containsIgnoreCase(object.getOriginalFilePath(), "00_FullCellrangerOutputs"))) {
			sampleCollectionType = PRIMARY_ANALYSIS_OUTPUT_NAME;
			logger.info("sampleCollectionType : {}", sampleCollectionType);

		}
		return sampleCollectionType;

	}

	private String getTarFileName(StatusInfo object, String sampleCollectionType) throws DmeSyncMappingException {
		String tarFileName = null;
		String scafNumber = getSCAFNumber(object);

		if (StringUtils.equals(FASTQ, sampleCollectionType)) {
			tarFileName = scafNumber + "_FQ_" + getFlowcellId(object) + "_" + getChemistry(object) + "."
					+ getFileType(object);
		} else if (StringUtils.equals(PRIMARY_ANALYSIS_OUTPUT_NAME, sampleCollectionType))
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
			logger.info(
					"Invalid folder structure: The name of fastq file or cellranger output file (sample name) doesn't start with SCAF: {}",
					object.getSourceFilePath());
			throw new DmeSyncMappingException(
					"Invalid folder structure:The name of fastq file or cellranger output (sample name) file doesn't start with SCAF: "
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
			String[] chemistryKeywords = chemistry.trim().split("_");
			if (chemistryKeywords.length > 2) {
				logger.info("Invalid chemistry name for full ranger output tars : {}", chemistry);
				throw new DmeSyncMappingException("Invalid chemistry name for full ranger output tars :" + chemistry);
			} else {
				if (chemistryKeywords.length == 1) {
					logger.info("chemistry: {}", chemistry);
					return chemistry;
				}
				if (chemistryKeywords.length == 2) {
					if (StringUtils.startsWithIgnoreCase(chemistryKeywords[0], "Seq")) {
						logger.info("chemistry pathname {} derived chemistry: {}", chemistry, chemistryKeywords[1]);
						return chemistryKeywords[1];
					} else {
						logger.info("Invalid chemistry name for full ranger output tars : {}", chemistry);
						throw new DmeSyncMappingException(
								"Invalid chemistry name for full ranger output tars :" + chemistry);
					}
				}

			}

		} else {
			logger.info(
					"Invalid folder structure:The chemistry metadata attribute is not able to derive from the parent folder: {}",
					object.getOriginalFilePath());
			throw new DmeSyncMappingException(
					"Invalid folder structure:The chemistry metadata attribute couldn't be able derive from the parent folder path: "
							+ object.getOriginalFilePath());
		}
		return null;
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
			logger.info("flowcellName: {}", flowcellName);
			return flowcellName;
		} else {
			logger.info("Invalid folder structure:The flowcell Id couldn't be able to derive from the path: {}",
					object.getOriginalFilePath());
			throw new DmeSyncMappingException(
					"Invalid folder structure: The flowcell Id couldn't be able to derive from the path"
							+ object.getOriginalFilePath());
		}
	}

	private void extractMetadataFromFinalReport(String filePath) throws IOException, DmeSyncMappingException {
		FileInputStream fis = new FileInputStream(new File(filePath));

		// Map to store extracted fields and their values
		medatadaMapFromReport = new HashMap<>();
		try (XWPFDocument document = new XWPFDocument(fis)) {
			List<XWPFParagraph> paragraphs = document.getParagraphs();

			// Define the keys to extract
			String[] keys = { PI, POC, PROJECT_ID, REPORT_DATE };
			int i = 0;
			// Iterate through the paragraphs in the document
			for (XWPFParagraph paragraph : document.getParagraphs()) {
				List<XWPFRun> runs = paragraph.getRuns();
				// for (XWPFRun run : runs) {
				String text = paragraph.getText();
				if (text != null) {
					// Check for each key and extract its corresponding value
					for (String key : keys) {
						if (StringUtils.containsIgnoreCase(text, "Project Summary") || i == 4)
							break;
						if (StringUtils.containsIgnoreCase(text, key)) {
							int index = text.indexOf(key + ":") + (key + ":").length();
							String value = text.substring(index).trim();
							medatadaMapFromReport.put(key, value);// Save the extracted value
						}
					}
					// }
				}
			}
			logger.info("metadata retrieved from the FinalReport {} values {}", filePath, medatadaMapFromReport);
		} catch (Exception e) {
			logger.info("Error while retrieving metadata from the FinalReport file{}", filePath);
			throw new DmeSyncMappingException("Error while retrieving metadata in the FinalReport file " + filePath);
		}
	}

	private Map<String, String> extractTableData(String filePath) throws IOException, DmeSyncMappingException {
		FileInputStream fis = new FileInputStream(new File(filePath));
		try (XWPFDocument document = new XWPFDocument(fis)) {
			// Initialize a map to store key-value pairs
			Map<String, String> data = new LinkedHashMap<>();

			// Iterate through all tables in the document
			for (XWPFTable table : document.getTables()) {
				// Find the table with the "sample_data_table" section, if applicable
				// Assuming you want to find a specific section, you can identify it by checking
				// headers, for example
				if (isTableSampleDataTable(table)) {
					// Process each row in the table
					for (XWPFTableRow row : table.getRows()) {
						// Get the first and third cells (assuming the table structure is consistent)
						if (row.getTableCells().size() >= 3) {
							String key = row.getTableCells().get(0).getText().trim();
							String value = row.getTableCells().get(2).getText().trim();

							// Add the key-value pair to the map
							if (!key.isEmpty() && !value.isEmpty()) {
								data.put(key, value);
							}
						}
					}
				}
			}

			return data;
		}
	}

	private boolean isTableSampleDataTable(XWPFTable table) throws DmeSyncMappingException {
		boolean chemistryFound = false;
		// Check if the table has at least one row
		if (table.getRows().size() > 0) {
			// Get the first row of the table
			XWPFTableRow firstRow = table.getRow(0);

			// Check all cells in the first row for the word "Chemistry"
			for (XWPFTableCell cell : firstRow.getTableCells()) {
				String cellText = cell.getText(); // Case-insensitive check
				if (StringUtils.containsIgnoreCase(cellText, "Chemistry")) {
					chemistryFound = true; // Return true if "Chemistry" is found
				}
			}
		}

		if (chemistryFound)
			return chemistryFound;
		else
			logger.info("Error while retrieving Platform name from the final report{}");
		throw new DmeSyncMappingException("Error while retrieving Platform name from the final report");
	}

	private void constructFinalReportPath(StatusInfo object) throws DmeSyncMappingException, IOException {

		// Combine the parent folder path with the 'other_data' folder and the file name
		finalReportPath = null;
		Path parentFolderPath = null;
		logger.info("Starting constructing the FinalReport file path to extract the metadata");

		// Get the parent folder path /mnt/scaf-ccr-a-data/CS029391_Staudt_Shaffer/
		Path fullFilePath = Paths.get(object.getOriginalFilePath());
		int count = fullFilePath.getNameCount();
		for (int i = 0; i <= count; i++) {
			if (fullFilePath.getParent().getFileName().toString().equals("scaf-ccr-a-data")) {
				parentFolderPath = fullFilePath;
				break;
			}
			fullFilePath = fullFilePath.getParent();
		}

		// Report file path :
		// /mnt/scaf-ccr-a-data/CS029391_Staudt_Shaffer/Other_Data/*FinalReport.docx
		if (parentFolderPath != null) {
			logger.info("Parent folder path to get the FinalReport file = {}", parentFolderPath);

			// Pattern for directories that start with 'other' (case-insensitive)
			String pattern = "Other*"; 

			// Case-insensitive match using DirectoryStream
			try (DirectoryStream<Path> stream = Files.newDirectoryStream(parentFolderPath, pattern)) {
				for (Path otherDataFolderPath : stream) {
					// Check if it's a directory and the name starts with "other" (case-insensitive)
					if (Files.isDirectory(otherDataFolderPath)
							&& otherDataFolderPath.getFileName().toString().toLowerCase().startsWith("other")) {
						try {

							// List files in the 'other_data' folder and filter by the pattern
							finalReportPath = Files.list(otherDataFolderPath)
									.filter(file -> file.getFileName().toString().endsWith("FinalReport.docx")
											|| file.getFileName().toString().endsWith("Report.docx"))
									.map(Path::toString).findFirst().orElse("");
							logger.info("Retrieving the data from the FinalReport file = {}", finalReportPath);
							if (finalReportPath.isBlank()) {
								logger.info("Couldn't find the FinalReport file for the project: {}",
										otherDataFolderPath);
								throw new DmeSyncMappingException(
										"Couldn't find the FinalReport file for the project: " + otherDataFolderPath);

							}

						} catch (IOException e) {
							logger.info("Couldn't find the FinalReport file for the project in Other* folder: {}",
									otherDataFolderPath);
							throw new DmeSyncMappingException(
									"Couldn't find the FinalReport file for the project: " + otherDataFolderPath);
						}
					} 
				}

			} catch (IOException e) {
				logger.info("Couldn't find the FinalReport file for the project: {}", parentFolderPath);
				throw new DmeSyncMappingException(
						"Couldn't find the FinalReport file for the project: " + parentFolderPath);
			}
		}
		if (finalReportPath==null) {
			logger.info("Couldn't find the FinalReport file for the project:");
			throw new DmeSyncMappingException(
					"Couldn't find the FinalReport file for the project in other* folder"  );

		}
	}

}
