package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.dme_ccdidataSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncPathMetadataProcessor;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntries;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;

/**
 * POB CCDI DME Path and Meta-data Processor Implementation
 * 
 * @author konerum3
 *
 */
@Service("pob")
public class POBCCDIPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
		implements DmeSyncPathMetadataProcessor {

	// POB CCDI Custom logic for DME path construction and meta data creation

	public static final String PRIMARY_ANALYSIS_INPUT_NAME = "Primary Analysis Input";
	public static final String PRIMARY_ANALYSIS_OUTPUT_NAME = "Primary Analysis Output";
	public static final String FASTQ_QC_NAME = "FASTQ_QC";

	public static final String ANALYSIS = "Analysis";
	public static final String FASTQ = "Fastq";

	@Value("${dmesync.additional.metadata.excel:}")
	private String metadataFile;

	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

		logger.info("[PathMetadataTask] POBCCDIgetArchivePath called");

		String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();

		// load the user metadata from the externally placed excel
		threadLocalMap.set(loadMetadataFile(metadataFile, "path"));

		// find the metadataFilePathKey to use as key value for the metadata from file.
		String metadataFilePathKey = getPathForMetadata(object);
		String archivePath = null;

		// subcollectionName can be fastq or analysis based on path
		String subCollectionName = getSubCollectionName(object);

		if (StringUtils.equalsIgnoreCase("Fastq", subCollectionName)) {

			if (isRawDataFile(object)) {
				// if 00_RawSequencingData/file.txt directly upload under the fastg
				// Lab_X/project_X/Fastq/
				archivePath = destinationBaseDir + "/Lab_" + getPiCollectionName(object, metadataFilePathKey)
						+ "/Project_" + getProjectCollectionName(object, metadataFilePathKey) + "/FASTQ/" + fileName;
			} else {
				// Lab_X/project_X/Fastq/seq_x/tarfiles:
				archivePath = destinationBaseDir + "/Lab_" + getPiCollectionName(object, metadataFilePathKey)
						+ "/Project_" + getProjectCollectionName(object, metadataFilePathKey) + "/FASTQ/"
						+ getSeqCollectionName(object) + "/" + fileName;
			}
		} else {
			if (isMetricsFile(object)) {
				// if Extracted data then directly upload under analysis folder:
				// Lab_X/project_X/Analysis/file
				archivePath = destinationBaseDir + "/Lab_" + getPiCollectionName(object, metadataFilePathKey)
						+ "/Project_" + getProjectCollectionName(object, metadataFilePathKey) + "" + "/Analysis/"
						+ fileName;
			} else {
				String geonomeType = getGenomeCollectionName(object);

				if (isFullRangerOutput(object)) {
					// Lab_X/project_X/Analysis/PrimaryAnalysisOutput/<Genome>/00_FullCellrangerOutputs/tarfile
					archivePath = destinationBaseDir + "/Lab_" + getPiCollectionName(object, metadataFilePathKey)
							+ "/Project_" + getProjectCollectionName(object, metadataFilePathKey) + "" + "/Analysis/"
							+ getAnalysisCollectionName(object) + "/"
							+ (geonomeType != null ? geonomeType + "/" + "FullCellrangerOutputs/"
									: "FullCellrangerOutputs/")
							+ fileName;

				} else {
					// Lab_X/project_X/Analysis/<Analysis sub folder>/tarfile
					archivePath = destinationBaseDir + "/Lab_" + getPiCollectionName(object, metadataFilePathKey)
							+ "/Project_" + getProjectCollectionName(object, metadataFilePathKey) + "" + "/Analysis/"
							+ getAnalysisCollectionName(object) + "/" + (geonomeType != null ? geonomeType + "/" : "")
							+ fileName;
				}
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

			// find the metadataFilePathKey to use as key value for the metadata from file.
			String metadataFilePathKey = getPathForMetadata(object);
			// subcollectionName can be fastq or analysis based on path
			String subCollectionName = getSubCollectionName(object);

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

			String labCollectionName = getPiCollectionName(object, metadataFilePathKey);
			String labCollectionPath = destinationBaseDir + "/Lab_" + labCollectionName.replace(" ", "_");
			HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "DataOwner_Lab"));
			pathEntriesPI.setPath(labCollectionPath);
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner", labCollectionName));
			pathEntriesPI.getPathMetadataEntries().add(
					createPathEntry("data_owner_email", getAttrValueWithKey(metadataFilePathKey, "data_owner_email")));
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_affiliation",
					getAttrValueWithKey(metadataFilePathKey, "data_owner_affiliation")));
			pathEntriesPI.getPathMetadataEntries()
					.add(createPathEntry("data_generator", getAttrValueWithKey(metadataFilePathKey, "data_generator")));
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_generator_email",
					getAttrValueWithKey(metadataFilePathKey, "data_generator_email")));
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_generator_affiliation",
					getAttrValueWithKey(metadataFilePathKey, "data_generator_affiliation")));

			if (getAttrValueWithKey(metadataFilePathKey, "data_owner_designee") != null) {
				pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_designee",
						getAttrValueWithKey(metadataFilePathKey, "data_owner_designee")));
				pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_designee_email",
						getAttrValueWithKey(metadataFilePathKey, "data_owner_designee_email")));
				pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_designee_affiliation",
						getAttrValueWithKey(metadataFilePathKey, "data_owner_designee_affiliation ")));
			}

			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPI);

			// Add path metadata entries for "Project_XXX" collection
			// Example row: collectionType - Project, collectionName - supplied

			String projectCollectionName = getProjectCollectionName(object, metadataFilePathKey);
			String projectCollectionPath = labCollectionPath + "/Project_" + projectCollectionName;
			HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("project_poc", getAttrValueWithKey(metadataFilePathKey, "project_poc")));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc_affiliation",
					getAttrValueWithKey(metadataFilePathKey, "project_poc_affiliation")));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc_email",
					getAttrValueWithKey(metadataFilePathKey, "project_poc_email")));
			if (StringUtils.isNotBlank(getAttrValueWithKey(metadataFilePathKey, "key_collaborator")))
				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("key_collaborator",
						getAttrValueWithKey(metadataFilePathKey, "key_collaborator")));
			if (StringUtils.isNotBlank(getAttrValueWithKey(metadataFilePathKey, "key_collaborator_affiliation")))
				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("key_collaborator_affiliation",
						getAttrValueWithKey(metadataFilePathKey, "key_collaborator_affiliation")));
			if (StringUtils.isNotBlank(getAttrValueWithKey(metadataFilePathKey, "key_collaborator_email")))
				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("key_collaborator_email",
						getAttrValueWithKey(metadataFilePathKey, "key_collaborator_email")));

			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_title", projectCollectionName));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_id", projectCollectionName));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_start_date",
					getAttrValueWithKey(metadataFilePathKey, "project_start_date"), "MM/dd/yy"));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_description",
					getAttrValueWithKey(metadataFilePathKey, "project_description")));
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("platform_name", getAttrValueWithKey(metadataFilePathKey, "platform_name")));
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("organism", getAttrValueWithKey(metadataFilePathKey, "organism")));
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("is_cell_line", getAttrValueWithKey(metadataFilePathKey, "is_cell_line")));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("study_disease",
					getAttrValueWithKey(metadataFilePathKey, "study_disease (project or sample level if multiple)")));

			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("is_pdx", getAttrValueWithKey(metadataFilePathKey, "is_pdx")));

			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("data_generating_facility", "Pediatric Oncology Branch"));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_status", "Active"));

			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Controlled Access"));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("retention_years", "7"));

			// Optional Values
			if (StringUtils.isNotBlank(getAttrValueWithKey(metadataFilePathKey, "project_completed_date")))
				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_completed_date",
						getAttrValueWithKey(metadataFilePathKey, "project_completed_date")));
			if (StringUtils.isNotBlank(getAttrValueWithKey(metadataFilePathKey, "pubmed_id")))
				pathEntriesProject.getPathMetadataEntries()
						.add(createPathEntry("pubmed_id", getAttrValueWithKey(metadataFilePathKey, "pubmed_id")));
			if (StringUtils.isNotBlank(getAttrValueWithKey(metadataFilePathKey, "Collaborators")))
				pathEntriesProject.getPathMetadataEntries().add(
						createPathEntry("Collaborators", getAttrValueWithKey(metadataFilePathKey, "Collaborators")));

			pathEntriesProject.setPath(projectCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);

			if (StringUtils.equalsIgnoreCase(subCollectionName, "Fastq")) {
				// library_layout, library_source
				String fastqCollectionPath = projectCollectionPath + "/FASTQ";
				HpcBulkMetadataEntry pathEntriesFastq = new HpcBulkMetadataEntry();
				pathEntriesFastq.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Raw_Data"));
				pathEntriesFastq.getPathMetadataEntries().add(
						createPathEntry("library_layout", getAttrValueWithKey(metadataFilePathKey, "library_layout")));
				pathEntriesFastq.getPathMetadataEntries().add(
						createPathEntry("library_source", getAttrValueWithKey(metadataFilePathKey, "library_source")));
				pathEntriesFastq.setPath(fastqCollectionPath);
				hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesFastq);

				// library_strategy,library_selection, analyte_type
				// optional: tissue (or cell_line_name),tissue_type (or cell_line_type)
				if (!isRawDataFile(object)) {

					String seqCollectionPath = fastqCollectionPath + "/" + getSeqCollectionName(object);
					HpcBulkMetadataEntry pathEntriesSeq = new HpcBulkMetadataEntry();
					pathEntriesSeq.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Seq_Run"));
					pathEntriesSeq.getPathMetadataEntries().add(createPathEntry("library_strategy",
							getAttrValueWithKey(metadataFilePathKey, "library_strategy")));
					pathEntriesSeq.getPathMetadataEntries().add(createPathEntry("library_selection",
							getAttrValueWithKey(metadataFilePathKey, "library_selection")));
					pathEntriesSeq.getPathMetadataEntries().add(
							createPathEntry("analyte_type", getAttrValueWithKey(metadataFilePathKey, "data_type")));
					pathEntriesSeq.getPathMetadataEntries()
							.add(createPathEntry("tissue", getAttrValueWithKey(metadataFilePathKey, "tissue")));
					pathEntriesSeq.getPathMetadataEntries().add(
							createPathEntry("tissue_type", getAttrValueWithKey(metadataFilePathKey, "tissue_type")));
					pathEntriesSeq.getPathMetadataEntries().add(createPathEntry("instrument_model",
							getAttrValueWithKey(metadataFilePathKey, "Instrument model")));
					pathEntriesSeq.setPath(seqCollectionPath);
					hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesSeq);
				}
			} else {

				String analysisCollectionPath = projectCollectionPath + "/Analysis";
				HpcBulkMetadataEntry pathEntriesAnalysis = new HpcBulkMetadataEntry();
				pathEntriesAnalysis.getPathMetadataEntries()
						.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Analysis"));
				pathEntriesAnalysis.setPath(analysisCollectionPath);
				hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesAnalysis);

				if (!isMetricsFile(object)) {

					// Metadata for Analysis SubCollection Type
					String analysisCollectionName = getAnalysisCollectionName(object);
					String analysisSubCollectionPath = analysisCollectionPath;
					HpcBulkMetadataEntry pathEntriesAnalysisSub = new HpcBulkMetadataEntry();
					if (StringUtils.equalsIgnoreCase(analysisCollectionName, PRIMARY_ANALYSIS_INPUT_NAME)) {
						analysisSubCollectionPath = analysisSubCollectionPath + "/Primary_Analysis_Input";
						pathEntriesAnalysisSub.getPathMetadataEntries()
								.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Primary_Analysis_Input"));
						pathEntriesAnalysisSub.setPath(analysisSubCollectionPath);
						hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesAnalysisSub);

					} else if (StringUtils.equalsIgnoreCase(analysisCollectionName, FASTQ_QC_NAME)) {
						analysisSubCollectionPath = analysisSubCollectionPath + "/FASTQ_QC";
						pathEntriesAnalysisSub.getPathMetadataEntries()
								.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "QC"));
						pathEntriesAnalysisSub.setPath(analysisSubCollectionPath);
						hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesAnalysisSub);

					} else if (StringUtils.equalsIgnoreCase(analysisCollectionName, PRIMARY_ANALYSIS_OUTPUT_NAME)) {
						analysisSubCollectionPath = analysisSubCollectionPath + "/Primary_Analysis_Output";
						pathEntriesAnalysisSub.getPathMetadataEntries()
								.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Primary_Analysis_Output"));

						String geonomeType = getGenomeCollectionName(object);

						pathEntriesAnalysisSub.setPath(analysisSubCollectionPath);
						hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesAnalysisSub);

						// metadata for mapped_geonome if present
						String geonomeCollectionPath = analysisSubCollectionPath;
						if (geonomeType != null && geonomeType.startsWith("GRCh")) {
							geonomeCollectionPath = geonomeCollectionPath + "/" + geonomeType;
							HpcBulkMetadataEntry pathEntriesGeonome = new HpcBulkMetadataEntry();
							pathEntriesGeonome.getPathMetadataEntries()
									.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Mapped_Genome"));
							pathEntriesGeonome.getPathMetadataEntries().add(createPathEntry("mapped_genome_name",
									getAttrValueWithKey(metadataFilePathKey, "mapped_genome")));

							pathEntriesGeonome.setPath(geonomeCollectionPath);
							hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesGeonome);
						}

						if (isFullRangerOutput(object)) {
							String fullRangerSubCollectionPath = geonomeCollectionPath + "/FullCellrangerOutputs";
							HpcBulkMetadataEntry pathEntriesFullRanger = new HpcBulkMetadataEntry();
							pathEntriesFullRanger.getPathMetadataEntries()
									.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Folder"));
							pathEntriesFullRanger.setPath(fullRangerSubCollectionPath);
							hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesFullRanger);
						}

					}

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
		logger.info("POB CCDI custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
		return dataObjectRegistrationRequestDTO;
	}

	private String getPiCollectionName(StatusInfo object, String metadataFilePathKey) {
		String piCollectionName = null;
		// Retrieve the PI collection name from the Excel spreadsheet
		piCollectionName = getAttrValueWithKey(metadataFilePathKey, "DataOwner_Lab");
		logger.info("PI Collection Name: {}", piCollectionName);
		return piCollectionName;

	}

	private String getProjectCollectionName(StatusInfo object, String metadataFilePathKey) {
		String projectCollectionName = null;
		projectCollectionName = getAttrValueWithKey(metadataFilePathKey, "project_title");
		logger.info("projectCollectionName: {}", projectCollectionName);
		return projectCollectionName;
	}

	private String getSeqCollectionName(StatusInfo object) {

		String seqCollectionName = Paths.get(object.getSourceFilePath()).getParent().getFileName().toString();
		logger.info("seqCollectionName: {}", seqCollectionName);
		return seqCollectionName;
	}

	private String getGenomeCollectionName(StatusInfo object) {
		
		if(isGeonomeFolder(object)) {
		String parentName = getGenomeCollectionPath(object);

		if (parentName != null) {
			String genomeFolder = Paths.get(parentName).getFileName().toString();
			if (genomeFolder!=null && genomeFolder.startsWith("GRCh")) {
				logger.info("genomeCollectionName: {}", parentName);
				return genomeFolder;
			}
		}
		}
		return null;
	}

	private String getGenomeCollectionPath(StatusInfo object) {
		return getPathFromParent(object, "02_PrimaryAnalysisOutput");

	}

	private String getSubCollectionName(StatusInfo object) {
		String parentCollectionType = null;
		// Example: If originalFilePath is
		// /data/CCRCCDI/dme_ccdidata/CS035485_Shern_Zhang_3PGEX/01_DemultiplexedFastqs/Seq1_GEX/tarfile
		// then the subCollectionName will be Fastq and all other path will comes under
		// analysis
		String path = object.getOriginalFilePath();
		if (StringUtils.containsIgnoreCase(path, "01_DemultiplexedFastqs/Seq")
				|| StringUtils.containsIgnoreCase(path, "00_RawSequencingData")) {
			parentCollectionType = FASTQ;
		} else {
			parentCollectionType = ANALYSIS;
		}
		logger.info("parentCollectionName: {}", parentCollectionType);
		return parentCollectionType;
	}

	private String getAnalysisCollectionName(StatusInfo object) {
		String anlaysisCollectionName = null;
		//  the Analysis collection name will be Primary Analysis Input, Primary Analysis Output, Fastq_qc
		String path = object.getOriginalFilePath();
		if (StringUtils.containsIgnoreCase(path, "fastqc")) {
			anlaysisCollectionName = FASTQ_QC_NAME;
		} else if (StringUtils.containsIgnoreCase(path, "PrimaryAnalysisInputFiles")) {
			anlaysisCollectionName = PRIMARY_ANALYSIS_INPUT_NAME;
		} else if (StringUtils.containsIgnoreCase(path, "PrimaryAnalysisOutput")) {
			anlaysisCollectionName = PRIMARY_ANALYSIS_OUTPUT_NAME;
		}
		logger.info("anlaysisCollectionName: {}", anlaysisCollectionName);
		return anlaysisCollectionName;
	}

	/*
	 * metadata spreadsheet has mapping only for fastq paths and geoneme path. For
	 * Analysis we get the project file substring and retrieve one of the rows for
	 * that project in Fastq paths --- 
	 * Folder : Path 
	 * Fastq: Original File Path ,
	 * Analysis: Parent_file_path from parent keyword "dme_ccdidata" (project path)
	 * Geonome: Original File Path
	 * raw data & metric files : parent file path which
	 * is project path
	 */
	public String getPathForMetadata(StatusInfo object) {

		Path fullPath = Paths.get(object.getOriginalFilePath());
		String parentPath = null;

		if (StringUtils.equals(getSubCollectionName(object), FASTQ)) {

			if (isRawDataFile(object)) {
				parentPath = getPathFromParent(object, "dme_ccdidata");
				logger.info("Path key to search metadata file for rawData file {}", parentPath);
			} else {
				// path is equal to parent file path which is
				// /data/CCRCCDI/dme_ccdidata/CS035485_Shern_Zhang_3PGEX/01_DemultiplexedFastqs/Seq1_GEX/
				parentPath = fullPath.getParent().toString();
				logger.info("Path key to search metadata file for fastq folder {}", parentPath);

			}
		} else {
			// for Analysis, path is substring of path which is project folder path
			// /data/CCRCCDI/dme_ccdidata/CS035485_Shern_Zhang_3PGEX
			parentPath = getPathFromParent(object, "dme_ccdidata");
			logger.info("Path key to search metadata file for Extracted data files {}", parentPath);

			if (!isMetricsFile(object)) {

				String analysisSubType = getAnalysisCollectionName(object);

				if (StringUtils.equalsIgnoreCase(FASTQ_QC_NAME, analysisSubType)) {
					// For fastqc remove the fastqc value at the end of path.
					parentPath = StringUtils.replace(parentPath, "/fastqc", "");
					logger.info("Path key to search metadata file for fastqc folder {}", parentPath);

				} else if (StringUtils.equalsIgnoreCase(PRIMARY_ANALYSIS_OUTPUT_NAME, analysisSubType)) {

					String geonomeType = getGenomeCollectionName(object);
					if (geonomeType != null) {
						// For geonome path is parent path
						/// data/CCRCCDI/dme_ccdidata/CS035485_Shern_Zhang_3PGEX/02_PrimaryAnalysisOutput/GRCh37/
						parentPath = getGenomeCollectionPath(object);
						logger.info("Path key to search metadata file for geonome folder {}", parentPath);

					}
				}
			}

		}
		String metadataFilePathKey = null;

		// The data is in helix and below logic discards the mnt path /vf/users in
		// originalfilePath.
		if (parentPath != null) {
			int startIndex = parentPath.indexOf("/CCRCCDI");
			String fileSubStringPath = null;
			if (startIndex != -1) {
				// Extract the substring from the starting index
				fileSubStringPath = parentPath.substring(startIndex);
			}
			// String fileSubStringPath = StringUtils.substringAfter(matchPath, "/data");
			logger.info(" Path key to check {} , fullPath{}", metadataFilePathKey, fileSubStringPath, fullPath);
			for (String key : threadLocalMap.get().keySet()) {
				if (key.contains(fileSubStringPath)) {
					metadataFilePathKey = key;
					logger.info("Full File Path in Excel = {}, matchPath {} , fullPath{}", metadataFilePathKey,
							fileSubStringPath, fullPath);
					break;
				}
			}
		}
		return metadataFilePathKey;
	}

	private String getPathFromParent(StatusInfo object, String parentName) {
		/*
		 * Example originalFilepath -
		 * /data/CCRCCDI/dme_ccdidata/CS035485_Shern_Zhang_3PGEX/PrimaryAnalysisInputFiles /
		 * tarfile parent name: dme_ccdidata then parent path:
		 * /data/CCRCCDI/dme_ccdidata/CS035485_Shern_Zhang_3PGEX
		 */
		Path fullFilePath = Paths.get(object.getOriginalFilePath());
		logger.info("Full File Path = {}", fullFilePath);
		int count = fullFilePath.getNameCount();
		for (int i = 0; i <= count; i++) {
			if (fullFilePath.getParent().getFileName().toString().equals(parentName)) {
				return fullFilePath.toString();
			}
			fullFilePath = fullFilePath.getParent();
		}
		return null;
	}

	private boolean isMetricsFile(StatusInfo object) {
		boolean isMetricsFile = StringUtils.equalsIgnoreCase(
				(Paths.get(object.getSourceFilePath()).getParent().getFileName().toString()), "ExtractedData");
		return isMetricsFile;

	}

	private boolean isRawDataFile(StatusInfo object) {
		boolean isRawDataFile = StringUtils.equalsIgnoreCase(
				(Paths.get(object.getSourceFilePath()).getParent().getFileName().toString()), "00_RawSequencingData");
		return isRawDataFile;

	}

	private boolean isFullRangerOutput(StatusInfo object) {
		String parentPath=Paths.get(object.getSourceFilePath()).getParent().getFileName().toString();
		boolean isFullRangerOutput = (StringUtils.equalsIgnoreCase(parentPath,
				"00_FullCellrangerOutputs") || StringUtils.equalsIgnoreCase(parentPath,
						"00_FullspacerangerOutputs")) ;
		return isFullRangerOutput;

	}
	
	private boolean isGeonomeFolder(StatusInfo object) {
		boolean isFullRangerOutput = StringUtils.containsIgnoreCase(object.getOriginalFilePath(),"02_PrimaryAnalysisOutput/GRCh");
				
		return isFullRangerOutput;

	}

}
