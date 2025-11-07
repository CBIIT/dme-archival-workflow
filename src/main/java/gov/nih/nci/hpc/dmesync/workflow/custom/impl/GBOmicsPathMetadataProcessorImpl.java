package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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

/**
 * Default GB OMICS Path and Meta-data Processor Implementation
 * 
 * @author dinhys
 *
 */
@Service("gb-omics")
public class GBOmicsPathMetadataProcessorImpl extends AbstractPathMetadataProcessor implements DmeSyncPathMetadataProcessor {

	@Value("${dmesync.doc.name}")
	private String doc;
	
	@Value("${dmesync.source.base.dir}")
	protected String sourceBaseDir;

	@Value("${dmesync.work.base.dir}")
	private String workDir;
	  
	@Value("${dmesync.create.collection.softlink:false}")
	private boolean createCollectionSoftlink;
	
	@Value("${dmesync.additional.metadata.excel:}")
	private String metadataFile;
	
	@Value("${dmesync.db.access:local}")
	  protected String access;
	
	Map<String, Map<String, String>> multiplexedMetadataMap = null;
	
	// DOC GB OMICS logic for DME path construction and meta data creation

	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

		logger.info("[PathMetadataTask] GB OMICS getArchivePath called");

		String fileName = Paths.get(createCollectionSoftlink ? object.getOriginalFilePath() :  object.getSourceFilePath()).toFile().getName();
		String archivePath = "";
		if(isDATA() || isONT()) {
			String sampleId = "";
			try {
				sampleId = getSample(object);
				String flowcellId = getFlowcell(object);
				String key = isONT() ? flowcellId : sampleId;
				
				String archiveStatus = getAttrValueWithExactKey(key, "Archive");
				boolean archiveReady = archiveStatus != null && archiveStatus.trim().matches("(?i)^(archive|archived|yes|y|true|1)\\s*\\.?$");
				
				// Archive column in the master file, that indicates whether a run is ready to be archived in DME.
				if(archiveReady) {  
				//If it is ONT, all files other than fastq or bam will be placed under the Flowcell folder.
				if(isONT() && !isFastqOrBam(object.getOriginalFilePath())){
					archivePath = destinationBaseDir + "/Lab_" + getPICollectionName(object) + "/DATA" + "/Year_" + getYear(object)
					+ "/Flowcell_" + getFlowcell(object) 
					+ "/" + fileName;
				} else {
					//If it is Illumina or ONT but single sample flowcells, files will be placed under the Sample folder.
					if (!isONT() || !isMultiplexed(object.getOrginalFileName())) {
						archivePath = destinationBaseDir + "/Lab_" + getPICollectionName(object) + "/DATA" + "/Year_" + getYear(object)
						+ "/Flowcell_" + getFlowcell(object) 
						+ (!sampleId.startsWith("Sample_") ? "/Sample_": "/") + sampleId + "/" + fileName;
					} else {
						sampleId = getMultiplexedSample(object);
						// For ONT Multiplexed flow cell, if the sample metadata is found, place it under the Sample folder.
						// Otherwise, place it under the Flowcell folder.
						String parentFolderName = Paths.get(object.getOriginalFilePath()).getParent().getFileName().toString();
						if(sampleId != null) {
							archivePath = destinationBaseDir + "/Lab_" + getPICollectionName(object) + "/DATA" + "/Year_" + getYear(object)
								+ "/Flowcell_" + getFlowcell(object) 
								+ (!sampleId.startsWith("Sample_") ? "/Sample_": "/") + sampleId + "/" + parentFolderName + "_" + fileName;
						} else {
							archivePath = destinationBaseDir + "/Lab_" + getPICollectionName(object) + "/DATA" + "/Year_" + getYear(object)
							+ "/Flowcell_" + getFlowcell(object) 
							+ "/" + parentFolderName + "_" + fileName;
						}
					}
				}
			} else {
				// This Archive column is not Yes in the master file, so dataset is not ready to upload, log the path and complete the workflow
				logger.info("No need to upload file : {} Archive column is {} ", object.getOriginalFilePath(), archiveStatus);
				// update the current status info row as completed so this workflow is complete and next task won't be processed.
				object.setRunId(object.getRunId() + "_IGNORED");
				object.setEndWorkflow(true);
				object.setError("No need to upload, Archive column is set to "+ archiveStatus);
				object = dmeSyncWorkflowService.getService(access).saveStatusInfo(object);
				return "";	
			}
			} catch (DmeSyncMappingException e) {
				if(StringUtils.isEmpty(sampleId)) {
					//This path might not be in the master file. If it is not, we want to ignore this path for now.
					try {
						searchPathInMasterFile(object.getOriginalFilePath());
					} catch (DmeSyncMappingException me) {
						//This path is not in the master file, so log the path and complete the workflow
						logger.info("No need to upload file : {}", object.getOriginalFilePath());
						// update the current status info row as completed so this workflow is completed and next task won't be processed.
						object.setRunId(object.getRunId() + "_IGNORED");
						object.setEndWorkflow(true);
						object.setError("No need to upload");
						object = dmeSyncWorkflowService.getService(access).saveStatusInfo(object);
						return "";
					}
					// If path is found, this means that the sample metadata is not available, so raise exception
					throw e;
				}
			}
		} else {
			String projectCollectionName = getProjectCollectionName(object);
			String piCollectionName = getPICollectionName(object);
			piCollectionName = piCollectionName.replace(" ", "_");
			archivePath = destinationBaseDir + "/Lab_" + piCollectionName + "/Project_" + projectCollectionName
					+ (createCollectionSoftlink ? "/Source_Data" + "/Flowcell_" + getFlowcellIdForProject(object)
							: (isRawData(object.getOriginalFilePath()) ? "/Source_Data" : "/Analysis") + "/"
									+ fileName);
			
			Path collectionLinkFilePath = Paths.get(workDir, "collection_link", getPIFolder(object), projectCollectionName, "collection_link.txt");
			if(!Files.exists(collectionLinkFilePath)) {
				String flowcellPaths = getAttrValueWithExactKey(projectCollectionName, "flowcell_path");
				if(StringUtils.isNotBlank(flowcellPaths)) {
					BufferedWriter writer = null;
					try {
						Files.createDirectories(collectionLinkFilePath.getParent());
						writer = new BufferedWriter(new FileWriter(collectionLinkFilePath.toString()));
			            for (String item : flowcellPaths.split(",")) {
			                writer.write(item);
			                writer.newLine();
			            }
			        } catch (IOException e) {
			        	logger.error("Error writing to collection link file: {}", e.getMessage(), e);
			        } finally {
			            // Ensure the writer is closed, even if an exception occurred
			            if (writer != null) {
			                try {
			                    writer.close();
			                } catch (IOException e) {
			                	logger.error("Error closing BufferedWriter: {}", e.getMessage(), e);
			                }
			            }
			        }
				}
			}
		}

		// replace spaces with underscore
		archivePath = archivePath.replace(" ", "_");

		return archivePath;
	}

	@Override
	public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException {

		logger.info("[PathMetadataTask] GB OMICS getMetaDataJson called");

		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();

		//This is the case where this file is not listed in the master file path.
		if(StringUtils.isBlank(object.getFullDestinationPath())) {
			return dataObjectRegistrationRequestDTO;
		}
		
		// Add to HpcBulkMetadataEntries for path attributes
		HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

		// Add path metadata entries for "DataOwner_Lab" collection

		String piCollectionName;
		String piCollectionPath;

		if(isDATA() || isONT()) {
			piCollectionName = getPICollectionName(object);
			piCollectionPath = destinationBaseDir + "/Lab_" + piCollectionName;
			HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "DataOwner_Lab"));
			pathEntriesPI.setPath(piCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries()
					.add(populateStoredMetadataEntries(pathEntriesPI, "DataOwner_Lab", piCollectionName, "gb-omics"));
			
			// Add path metadata entries for "DATA" folder
			String dataCollectionPath = piCollectionPath + "/DATA";
			HpcBulkMetadataEntry pathEntriesDATA = new HpcBulkMetadataEntry();
			pathEntriesDATA.setPath(dataCollectionPath);
			pathEntriesDATA.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Raw_Data"));
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesDATA);
			
			// Add path metadata entries for "Date" folder
			String sampleId = getSample(object);
			String flowcellId = getFlowcell(object);
			String key = isONT() ? flowcellId : sampleId;
			String dateCollectionPath = dataCollectionPath + "/Year_" + getYear(object);
			HpcBulkMetadataEntry pathEntriesDate = new HpcBulkMetadataEntry();
			pathEntriesDate.setPath(dateCollectionPath);
			pathEntriesDate.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Date"));
			pathEntriesDate.getPathMetadataEntries().add(createPathEntry("year", getYear(object)));
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesDate);
			
			// Add path metadata entries for "Flowcell" collection
			String flowcellCollectionPath = dateCollectionPath + "/Flowcell_" + flowcellId;
			HpcBulkMetadataEntry pathEntriesFlowcell = new HpcBulkMetadataEntry();
			pathEntriesFlowcell.setPath(flowcellCollectionPath);
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Flowcell"));
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("flowcell_id", flowcellId));
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("data_generating_facility", getAttrValueWithExactKey(key, "Data generating facility")));
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("platform_name", getAttrValueWithExactKey(key, "Platform")));
			if(StringUtils.isNotEmpty(getAttrValueWithExactKey(key, "Enrichment step")))
				pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("enrichment_step", getAttrValueWithExactKey(key, "Enrichment step")));
			if(StringUtils.isNotEmpty(getAttrValueWithExactKey(key, "SampleRef")))
				pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("reference_genome", getAttrValueWithExactKey(key, "SampleRef")));
			if(StringUtils.isNotEmpty(getAttrValueWithExactKey(key, "Run Start Date")))
				pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("sequenced_date", getAttrValueWithExactKey(key, "Run Start Date")));
			if(StringUtils.isNotEmpty(getAttrValueWithExactKey(key, "Type of sequencing")))
				pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("sequencing_application_type", getAttrValueWithExactKey(key, "Type of sequencing")));
			pathEntriesFlowcell.getPathMetadataEntries().add(createPathEntry("run_date", getRunDate(key)));
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesFlowcell);

			// Check whether this is a multiplexed ONT flowcell with sample entry in the master file
			String multiplexedSampleId = null;
			if(isONT() && isMultiplexed(object.getOrginalFileName())) {
				multiplexedSampleId = getMultiplexedSample(object);
				key = multiplexedSampleId != null ? flowcellId + "_" + multiplexedSampleId : flowcellId;
				sampleId = multiplexedSampleId != null ? multiplexedSampleId : sampleId;
			}
			
			// Add path metadata entries for "Sample" collection
			//If it is Illumina or ONT fast or bam with single sample flowcells
			if (!isONT() || (!isMultiplexed(object.getOrginalFileName()) && isFastqOrBam(object.getOriginalFilePath()))) {
				String sampleCollectionPath = flowcellCollectionPath + (!sampleId.startsWith("Sample_") ? "/Sample_": "/") + sampleId;
				HpcBulkMetadataEntry pathEntriesSample = new HpcBulkMetadataEntry();
				pathEntriesSample.setPath(sampleCollectionPath);
				pathEntriesSample.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Sample"));
				pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sample_name", sampleId));
				pathEntriesSample.getPathMetadataEntries().add(createPathEntry("patient_id", getAttrValueWithExactKey(key, "Patient ID")));
				pathEntriesSample.getPathMetadataEntries().add(createPathEntry("library_strategy", getAttrValueWithExactKey(key, "Library strategy")));
				pathEntriesSample.getPathMetadataEntries().add(createPathEntry("analyte_type", getAttrValueWithExactKey(key, "Analyte Type")));
				pathEntriesSample.getPathMetadataEntries().add(createPathEntry("organism", getAttrValueWithExactKey(key, "Species")));
				pathEntriesSample.getPathMetadataEntries().add(createPathEntry("is_cell_line", getAttrValueWithExactKey(key, "Is cell line")));
				pathEntriesSample.getPathMetadataEntries().add(createPathEntry("study_disease", getAttrValueWithExactKey(key, "Diagnosis")));
				if(StringUtils.isNotEmpty(getAttrValueWithExactKey(key, "Library ID")))
					pathEntriesSample.getPathMetadataEntries().add(createPathEntry("library_id", getAttrValueWithExactKey(key, "Library ID")));
				if(StringUtils.isNotEmpty(getAttrValueWithExactKey(key, "Case Name")))
					pathEntriesSample.getPathMetadataEntries().add(createPathEntry("case_name", getAttrValueWithExactKey(key, "Case Name")));
				if(StringUtils.isNotEmpty(getAttrValueWithExactKey(key, "Anatomy/Cell Type")))
					pathEntriesSample.getPathMetadataEntries().add(createPathEntry("tissue", getAttrValueWithExactKey(key, "Anatomy/Cell Type")));
				if(StringUtils.isNotEmpty(getAttrValueWithExactKey(key, "Type")))
					pathEntriesSample.getPathMetadataEntries().add(createPathEntry("tissue_type", getAttrValueWithExactKey(key, "Type")));
				
				hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesSample);
			} else if (isMultiplexed(object.getOrginalFileName()) && isFastqOrBam(object.getOriginalFilePath()) && multiplexedSampleId != null) {
				//ONT fast or bam with multiplexed ONT flowcell that has a sample entry in the master file
				String sampleCollectionPath = flowcellCollectionPath + (!multiplexedSampleId.startsWith("Sample_") ? "/Sample_": "/") + multiplexedSampleId;
				HpcBulkMetadataEntry pathEntriesSample = new HpcBulkMetadataEntry();
				pathEntriesSample.setPath(sampleCollectionPath);
				pathEntriesSample.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Sample"));
				pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sample_name", multiplexedSampleId));
				pathEntriesSample.getPathMetadataEntries().add(createPathEntry("patient_id", getAttrValueWithMultiplexedKey(key, "Patient ID")));
				pathEntriesSample.getPathMetadataEntries().add(createPathEntry("library_strategy", getAttrValueWithMultiplexedKey(key, "Library strategy")));
				pathEntriesSample.getPathMetadataEntries().add(createPathEntry("analyte_type", getAttrValueWithMultiplexedKey(key, "Analyte Type")));
				pathEntriesSample.getPathMetadataEntries().add(createPathEntry("organism", getAttrValueWithMultiplexedKey(key, "Species")));
				pathEntriesSample.getPathMetadataEntries().add(createPathEntry("is_cell_line", getAttrValueWithMultiplexedKey(key, "Is cell line")));
				pathEntriesSample.getPathMetadataEntries().add(createPathEntry("study_disease", getAttrValueWithMultiplexedKey(key, "Diagnosis")));
				if(StringUtils.isNotEmpty(getAttrValueWithMultiplexedKey(key, "Library ID")))
					pathEntriesSample.getPathMetadataEntries().add(createPathEntry("library_id", getAttrValueWithMultiplexedKey(key, "Library ID")));
				if(StringUtils.isNotEmpty(getAttrValueWithMultiplexedKey(key, "Case Name")))
					pathEntriesSample.getPathMetadataEntries().add(createPathEntry("case_name", getAttrValueWithMultiplexedKey(key, "Case Name")));
				if(StringUtils.isNotEmpty(getAttrValueWithMultiplexedKey(key, "Anatomy/Cell Type")))
					pathEntriesSample.getPathMetadataEntries().add(createPathEntry("tissue", getAttrValueWithMultiplexedKey(key, "Anatomy/Cell Type")));
				if(StringUtils.isNotEmpty(getAttrValueWithMultiplexedKey(key, "Type")))
					pathEntriesSample.getPathMetadataEntries().add(createPathEntry("tissue_type", getAttrValueWithMultiplexedKey(key, "Type")));
				
				hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesSample);
			}
			
		} else {
			
			piCollectionName = getPICollectionName(object);
			piCollectionPath = destinationBaseDir + "/Lab_" + piCollectionName;
			HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "DataOwner_Lab"));
			pathEntriesPI.setPath(piCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries()
					.add(populateStoredMetadataEntries(pathEntriesPI, "DataOwner_Lab", piCollectionName, "gb-omics"));
			
			String projectCollectionName = getProjectCollectionName(object);
			
			// Add path metadata entries for "Project" collection
			String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;
			HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
			//pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Closed Access"));
			pathEntriesProject.setPath(projectCollectionPath);
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_id", projectCollectionName));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_title", getAttrValueWithExactKey(projectCollectionName, "project_title")));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_description", getAttrValueWithExactKey(projectCollectionName, "project_description")));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc", getAttrValueWithExactKey(projectCollectionName, "project_poc")));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc_email", getAttrValueWithExactKey(projectCollectionName, "project_poc_email")));
			if(StringUtils.isNotEmpty(getAttrValueWithExactKey(projectCollectionName, "project_poc_affiliation")))
				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc_affiliation", getAttrValueWithExactKey(projectCollectionName, "project_poc_affiliation")));
			else
				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc_affiliation", "CCRGB"));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Closed Access"));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_start_date", getAttrValueWithExactKey(projectCollectionName, "project_start_date")));
			if(StringUtils.isNotEmpty(getAttrValueWithExactKey(projectCollectionName, "project_end_date")))
				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_completed_date", getAttrValueWithExactKey(projectCollectionName, "project_completed_date")));
			if(StringUtils.isNotEmpty(getAttrValueWithExactKey(projectCollectionName, "key_collaborator")))
				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("key_collaborator", getAttrValueWithExactKey(projectCollectionName, "key_collaborator")));
			if(StringUtils.isNotEmpty(getAttrValueWithExactKey(projectCollectionName, "key_collaborator_email")))
				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("key_collaborator_email", getAttrValueWithExactKey(projectCollectionName, "key_collaborator_email")));
			if(StringUtils.isNotEmpty(getAttrValueWithExactKey(projectCollectionName, "key_collaborator_affiliation")))
				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("key_collaborator_affiliation", getAttrValueWithExactKey(projectCollectionName, "key_collaborator_affiliation")));
			else if (StringUtils.isNotEmpty(getAttrValueWithExactKey(projectCollectionName, "key_collaborator")))
				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("key_collaborator_affiliation", "CCRGB"));
			if(StringUtils.isNotEmpty(getAttrValueWithExactKey(projectCollectionName, "pubmed_id")))
				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("pubmed_id", getAttrValueWithExactKey(projectCollectionName, "pubmed_id")));
			if(StringUtils.isNotEmpty(getAttrValueWithExactKey(projectCollectionName, "public_data_accession_id")))
				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("public_data_accession_id", getAttrValueWithExactKey(projectCollectionName, "public_data_accession_id")));
			
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);
			
			// Add path metadata entries for "Source_Data" or "Analysis" folder
			String analysisCollectionPath = projectCollectionPath + (createCollectionSoftlink || isRawData(object.getOriginalFilePath()) ? "/Source_Data" : "/Analysis");
			HpcBulkMetadataEntry pathEntriesAnalysis = new HpcBulkMetadataEntry();
			pathEntriesAnalysis.setPath(analysisCollectionPath);
			pathEntriesAnalysis.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, (createCollectionSoftlink || isRawData(object.getOriginalFilePath()) ? "Source_Data" : "Analysis")));
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesAnalysis);
			
			if(createCollectionSoftlink) {
				// Add path metadata entries for "Flowcell_Link" collection
				String flowcellId = getFlowcellIdForProject(object);
				dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Flowcell_Link"));
				dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("flowcell_id", flowcellId));
				dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);
				return dataObjectRegistrationRequestDTO;
			}
	    }

		// Set it to dataObjectRegistrationRequestDTO
		dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
		dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);

		// Add object metadata
		String fileName = Paths.get(createCollectionSoftlink ? object.getOriginalFilePath() : object.getSourceFilePath()).toFile().getName();
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
	
	private String getPIFolder(StatusInfo object) throws DmeSyncMappingException {
		//For /data/khanlab3/gb_omics/DATA/caplen, it will return caplen.
		//For /data/khanlab3/gb_omics/projects/caplen, it will return caplen.
		String piFolder = null;
		if(isONT()) {
			String key = getFlowcell(object);
			piFolder = getAttrValueWithExactKey(key, "PI");
		}
		else if(createCollectionSoftlink)
			piFolder = getCollectionNameFromParent(object.getSourceFilePath(), StringUtils.substringAfterLast(sourceBaseDir, File.separator));
		else
			piFolder = getCollectionNameFromParent(object.getOriginalFilePath(), StringUtils.substringAfterLast(sourceBaseDir, File.separator));
		return piFolder;
	}
	
	private String getPICollectionName(StatusInfo object) throws DmeSyncMappingException {
		//For /data/khanlab3/gb_omics/DATA/caplen, it will get mapped collection name for caplen.
		//For /data/khanlab3/gb_omics/projects/caplen, it will get mapped collection name for caplen.
		String piFolder = getPIFolder(object);
		return getCollectionMappingValue(piFolder.toLowerCase(), "DataOwner_Lab", "gb-omics");
	}

	private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {
		String projectId = null;
		if(createCollectionSoftlink)
			projectId = getCollectionNameFromParent(object.getSourceFilePath(), getPIFolder(object));
		else
			projectId = getCollectionNameFromParent(object.getOriginalFilePath(), getPIFolder(object));
		return projectId.toUpperCase().replace('_', '-');
	}

	private String getFlowcell(StatusInfo object) throws DmeSyncMappingException {
		String flowcellId = "";
		if(!isONT()) {
			flowcellId = getAttrValueWithExactKey(getSample(object), "FCID");
		}
		else {
			flowcellId = getCollectionNameFromParent(object.getOriginalFilePath(), StringUtils.substringAfterLast(sourceBaseDir, File.separator));
		}
		return flowcellId;
	}
	
	private String getYear(StatusInfo object) throws DmeSyncMappingException {
		String key = isONT() ? getFlowcell(object) : getSample(object);
		String runDate = getAttrValueWithExactKey(key, "Run Start Date");
		String year = "";
		if(StringUtils.isNotBlank(runDate) && StringUtils.contains(runDate, "/")) {
			year = StringUtils.substringAfterLast(runDate, "/");
			if(year.length() == 2)
				year = "20" + year;
		} else if (StringUtils.isNotBlank(runDate)) {
			year = "20" + StringUtils.substring(runDate, 0, 2);
		}
		return year;
	}
	
	private String getRunDate(String key) {
		String runDate = getAttrValueWithExactKey(key, "Run Start Date");
		String date = "";
		DateFormat outputFormatter = new SimpleDateFormat("yyyy-MM-dd");
		if(StringUtils.isNotBlank(runDate) && StringUtils.contains(runDate, "/")) {
			SimpleDateFormat inputFormatter = new SimpleDateFormat("MM/dd/yy");
			try {
				date = outputFormatter.format(inputFormatter.parse(runDate));
			} catch (ParseException e) {
				logger.error("Can't parse run_date: ", runDate);
			}
		} else if (StringUtils.isNotBlank(runDate)) {
			SimpleDateFormat inputFormatter = new SimpleDateFormat("yyMMdd");
			try {
				date = outputFormatter.format(inputFormatter.parse(runDate));
			} catch (ParseException e) {
				logger.error("Can't parse run_date: ", runDate);
			}
		}
		return date;
	}
	
	private String getFlowcellIdForProject(StatusInfo object) {
		String flowcellId = "";
		if(object.getOriginalFilePath().contains("Flowcell_")) {
			String pathStartingFlowcellId = StringUtils.substringAfter(object.getOriginalFilePath(), "Flowcell_");
			return StringUtils.substringBefore(pathStartingFlowcellId, "/");
		}
		return flowcellId;
	}
	
	private String getSample(StatusInfo object) throws DmeSyncMappingException {
		String sampleId = "";
		if(!isONT()) {
			String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
			sampleId = getAttrKeyFromKeyInSearchString(fileName);
		} else {
			sampleId = getAttrValueWithExactKey(getFlowcell(object), "Sample name");
		}
		return sampleId;
	}
	
	/**
	 * Gets the Sample name of a multiplexed ONT flowcell based on flowcell and folder name
	 * @param object Status info object
	 * @return Sample name, null if not found
	 * @throws DmeSyncMappingException
	 */
	private String getMultiplexedSample(StatusInfo object) throws DmeSyncMappingException {
		String flowcellId = getFlowcell(object);
		return getAttrValueWithTwoKeys(flowcellId, object.getOrginalFileName(), "Sample name");
	}
	
	public String getAttrValueWithExactKey(String key, String attrKey) {
		if(StringUtils.isEmpty(key)) {
	      logger.error("Excel mapping not found for {}", key);
	      return null;
	    }
	    return (metadataMap.get(key) == null? null : metadataMap.get(key).get(attrKey));
	}
	
	/**
	 * Gets the attribute value of a attrKey specified using key1 and key2
	 * @param key1
	 * @param key2
	 * @param attrKey
	 * @return attribute value, null if not found
	 */
	public String getAttrValueWithTwoKeys(String key1, String key2, String attrKey) {
		if(StringUtils.isEmpty(key1) || StringUtils.isEmpty(key2)) {
	      logger.error("Excel mapping not found for {}, {}", key1, key2);
	      return null;
	    }
		String key = getMultiplexedKeyFromTwoKeys(key1, key2);
	    return (multiplexedMetadataMap.get(key) == null? null : multiplexedMetadataMap.get(key).get(attrKey));
	}
	
	/**
	 * Gets the ONT multiplexed attribute value of attrKey specified using the key
	 * @param key
	 * @param attrKey
	 * @return attrValue
	 */
	public String getAttrValueWithMultiplexedKey(String key, String attrKey) {
		if(StringUtils.isEmpty(key)) {
	      logger.error("Excel mapping not found for {}", key);
	      return null;
	    }
		return (multiplexedMetadataMap.get(key) == null? null : multiplexedMetadataMap.get(key).get(attrKey));
	}
	
	/**
	 * Gets the multiplexed key that contains both two keys specified
	 * @param key1
	 * @param key2
	 * @return Multiplexed Key
	 */
	private String getMultiplexedKeyFromTwoKeys(String key1,  String key2) {
	    String key = null;
	    for (Map.Entry<String, Map<String, String>> entry : multiplexedMetadataMap.entrySet()) {
	    	// Iterate through the keys to find a key that contains both key1 and key2
	        if(StringUtils.contains(entry.getKey(), key1) && StringUtils.contains(entry.getKey(), key2)) {
	          key = entry.getKey();
	        }
	    }
	    return key;
    }
	
	private String getAttrKeyFromKeyInSearchString(String searchString) throws DmeSyncMappingException {
	    String key = "";
	    for (Map.Entry<String, Map<String, String>> entry : metadataMap.entrySet()) {
	    	// Iterate through the keys to find a partial string match with the highest char matches.
	        if(StringUtils.contains(searchString, entry.getKey())) {
	          //Partial key match.
	          if(key.length() < entry.getKey().length())
	        	  key = entry.getKey();
	        }
	    }
	    if(StringUtils.isEmpty(key)) {
	      logger.error("Excel mapping not found for search string {}", searchString);
	      throw new DmeSyncMappingException("Excel mapping not found for " + searchString);
	    }
	    return key;
    }
	
	private String searchPathInMasterFile(String searchString) throws DmeSyncMappingException {
	    String key = null;
	    for (Map.Entry<String, Map<String, String>> entry : metadataMap.entrySet()) {
	        if(StringUtils.contains(searchString, getAttrValueWithExactKey(entry.getKey(), "PI") + File.separator + getAttrValueWithExactKey(entry.getKey(), "Path"))) {
	          //Partial key match.
	          key = entry.getKey();
	          break;
	        }
	    }
	    if(StringUtils.isEmpty(key)) {
	      logger.error("Excel mapping not found for search string {}", searchString);
	      throw new DmeSyncMappingException("Excel mapping not found for " + searchString);
	    }
	    return key;
    }
	
	private boolean isDATA() {
	    return (StringUtils.contains(sourceBaseDir, "DATA")? true : false);
	}
	
	private boolean isONT() {
		return (StringUtils.contains(sourceBaseDir, "GB_OMICS_ONT")? true : false);
	}
	
	private boolean isFastqOrBam(String originalFilePath) {
		return StringUtils.contains(originalFilePath, "fastq_") || StringUtils.contains(originalFilePath, "bam_");
	}
	
	private boolean isRawData(String originalFilePath) {
		return StringUtils.contains(originalFilePath, "raw");
	}
	
	private boolean isMultiplexed(String originalFileName) {
		return !(originalFileName.equals("fastq_pass")
				|| originalFileName.equals("fastq_fail")
				|| originalFileName.equals("bam_pass")
				|| originalFileName.equals("bam_fail"));
	}
	
	@PostConstruct
	  private void init() throws IOException {
		if("gb-omics".equalsIgnoreCase(doc)) {
		    try {
		    	// load the user metadata from the externally placed excel
				if(StringUtils.isNotEmpty(metadataFile) && !isONT() && isDATA())
					metadataMap = loadMetadataFile(metadataFile, "Sample name");
				else if (StringUtils.isNotEmpty(metadataFile) && isONT()) {
					metadataMap = loadMetadataFile(metadataFile, "FCID");
					// Loading the metadata using <Flowcell>_<Sample> key for multiplexed ONT flowcells
					multiplexedMetadataMap = loadMetadataFile(metadataFile, "FCID", "Sample name");
				}
				else {
					// This is project metadata
					metadataMap = loadMetadataFile(metadataFile, "project_id");
				}
		    } catch (DmeSyncMappingException e) {
		        logger.error(
		            "Failed to initialize metadata  path metadata processor", e);
		    }
		}
	  }
	
}
