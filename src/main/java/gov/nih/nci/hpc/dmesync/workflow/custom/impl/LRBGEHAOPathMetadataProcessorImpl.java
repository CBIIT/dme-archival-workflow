package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;

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
 * LCBG HAO Custom DME Path and Meta-data Processor Implementation
 * konerum3
 *
 */
@Service("hao")
public class LRBGEHAOPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
		implements DmeSyncPathMetadataProcessor {

	@Autowired
	private DmeMetadataBuilder dmeMetadataBuilder;

	@Value("${dmesync.additional.metadata.excel:}")
	private String metadataFile;

	@Value("${dmesync.source.base.dir}")
	private String syncBaseDir;

	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {
		logger.info("[PathMetadataTask] HAO getArchivePath called");

		// load the user metadata from the externally placed excel
		metadataMap = dmeMetadataBuilder.getMetadataMap(metadataFile, "Path");

		String sourcePath = object.getOriginalFilePath();
		String fileName = object.getSourceFileName();
		String archivePath = null;
		Path fullPath = Paths.get(sourcePath);
		String piCollectionName = getPiCollectionName();
		String metadataFilePathKey = getPathForMetadata(fullPath);
		String projectcollectionName = getProjectcollectionName(fullPath,metadataFilePathKey);
		String outcollectionName= getOutCollectionName(fullPath);
		String subcollectionType = getCollectionNameFromParent(fullPath, outcollectionName);
		
		if(outcollectionName!=null && subcollectionType!=null) {

		if (StringUtils.equalsIgnoreCase(subcollectionType, "Flowcell")) {
			String sampleId = getCollectionNameFromParent(fullPath, subcollectionType); // e.g., sampleId
																			
			if(sampleId !=null) {
			
				archivePath = destinationBaseDir + "/PI_" + piCollectionName + "/Project_" + projectcollectionName 
						+ "/" + outcollectionName + "/Raw_Data" + "/" + sampleId + "/" + fileName;
			
			}
		} else if (StringUtils.equalsIgnoreCase(subcollectionType, "Analysis")) {
			
			archivePath = destinationBaseDir + "/PI_" + piCollectionName + "/Project_" + projectcollectionName
					+ "/" + outcollectionName + "/Analysis" +  "/" + fileName;
		
			}
		  else if (StringUtils.equalsIgnoreCase(subcollectionType, "QC")) {
				
				archivePath = destinationBaseDir + "/PI_" + piCollectionName + "/Project_" + projectcollectionName
						+ "/" + outcollectionName + "/QC" +  "/" + fileName;
			
		  }
	   }
		
		//if there is readme.txt add readme file under out directory collection
	   if (archivePath == null && StringUtils.equalsIgnoreCase(fileName, "readme.txt")) {
			if (outcollectionName != null) {
				archivePath = destinationBaseDir + "/PI_" + piCollectionName + "/Project_" + projectcollectionName
						+ "/" + outcollectionName + "/" + fileName;
			}
		}

		if (archivePath == null) {
			String msg = messageService.get("VALIDATION_001");
			logger.error( "Couldn't extract the DME Path for the source Path " + sourcePath + " " + msg);
			throw new DmeSyncMappingException(msg);
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

			// find the metadataFilePathKey to use as key value for the metadata from file.
			String sourcePath = object.getOriginalFilePath();
			Path fullPath = Paths.get(sourcePath);
			String piCollectionName = getPiCollectionName();
			String metadataFilePathKey = getPathForMetadata(fullPath);
			String projectCollectionName = getProjectcollectionName(fullPath, metadataFilePathKey);
			
			
						
			// Add to HpcBulkMetadataEntries for path attributes
			HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

			// Add path metadata entries for "PI_XXX" collection
			// Example row: collectionType - DataOwner_Lab, collectionName - 
			// (supplied)
			// key = data_owner_name, value = ? (supplied)
			// key = data_owner_affiliation, value = ? (supplied)
			// key = data_owner_email, value = ? (supplied) , data_owner designee,
			// affiliation, email
			// data_generator, affiliation, email

			String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName.replace(" ", "_");
			HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "DataOwner_Lab"));
			pathEntriesPI.setPath(piCollectionPath);
			pathEntriesPI.getPathMetadataEntries()
					.add(createPathEntry("data_owner", getAttrValueFromMetadataMap(metadataFilePathKey, "data_owner")));
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_email",
					getAttrValueFromMetadataMap(metadataFilePathKey, "data_owner_email")));
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_affiliation",
					getAttrValueFromMetadataMap(metadataFilePathKey, "data_owner_affiliation")));

			pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_generator",
					getAttrValueFromMetadataMap(metadataFilePathKey, "data_generator")));
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_generator_affiliation",
					getAttrValueFromMetadataMap(metadataFilePathKey, "data_generator_affiliation")));
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_generator_email",
					getAttrValueFromMetadataMap(metadataFilePathKey, "data_generator_email")));

			if (getAttrValueFromMetadataMap(metadataFilePathKey, "data_owner_designee") != null) {
				pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_designee",
						getAttrValueFromMetadataMap(metadataFilePathKey, "data_owner_designee")));
				pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_designee_email",
						getAttrValueFromMetadataMap(metadataFilePathKey, "data_owner_designee_email")));
				pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_designee_affiliation",
						getAttrValueFromMetadataMap(metadataFilePathKey, "data_owner_designee_affiliation")));
			}

			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPI);

			// Add path metadata entries for "Project_XXX" collection
			// Example row: collectionType - Project, collectionName - supplied

			String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;
			HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
			pathEntriesProject.getPathMetadataEntries().add(
					createPathEntry("project_poc", getAttrValueFromMetadataMap(metadataFilePathKey, "project_poc")));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc_affiliation",
					getAttrValueFromMetadataMap(metadataFilePathKey, "project_poc_affiliation")));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc_email",
					getAttrValueFromMetadataMap(metadataFilePathKey, "project_poc_email")));
			if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "key_collaborator")))
				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("key_collaborator",
						getAttrValueFromMetadataMap(metadataFilePathKey, "key_collaborator")));
			if (StringUtils
					.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "key_collaborator_affiliation")))
				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("key_collaborator_affiliation",
						getAttrValueFromMetadataMap(metadataFilePathKey, "key_collaborator_affiliation")));
			if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "key_collaborator_email")))
				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("key_collaborator_email",
						getAttrValueFromMetadataMap(metadataFilePathKey, "key_collaborator_email")));

			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_title", getAttrValueFromMetadataMap(metadataFilePathKey, "project_title")));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_id", projectCollectionName));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_start_date",
					getAttrValueFromMetadataMap(metadataFilePathKey, "project_start_date"), "MM/dd/yy"));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_description",
					getAttrValueFromMetadataMap(metadataFilePathKey, "project_description")));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("platform_name",
					getAttrValueFromMetadataMap(metadataFilePathKey, "platform_name")));
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("organism", getAttrValueFromMetadataMap(metadataFilePathKey, "organism")));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("study_disease",
					getAttrValueFromMetadataMap(metadataFilePathKey, "study_disease")));
			pathEntriesProject.getPathMetadataEntries().add(
					createPathEntry("is_cell_line", getAttrValueFromMetadataMap(metadataFilePathKey, "is_cell_line")));

			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("data_generating_facility",
					getAttrValueFromMetadataMap(metadataFilePathKey, "data_generating_facility")));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_status", "Active"));

			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Controlled Access"));
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("retention_years", "7"));

			// Optional Values
			if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "project_completed_date")))
				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_completed_date",
						getAttrValueFromMetadataMap(metadataFilePathKey, "project_completed_date"), "MM/dd/yy"));
			if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "pubmed_id")))
				pathEntriesProject.getPathMetadataEntries().add(
						createPathEntry("pubmed_id", getAttrValueFromMetadataMap(metadataFilePathKey, "pubmed_id")));
			if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "Collaborators")))
				pathEntriesProject.getPathMetadataEntries().add(createPathEntry("Collaborators",
						getAttrValueFromMetadataMap(metadataFilePathKey, "Collaborators")));

			pathEntriesProject.setPath(projectCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);

			String outcollectionName= getOutCollectionName(fullPath);
			
			String outFolderPath = projectCollectionPath + "/"+ outcollectionName;
			HpcBulkMetadataEntry pathEntriesOutFolder = new HpcBulkMetadataEntry();
			pathEntriesOutFolder.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Run"));
			pathEntriesOutFolder.getPathMetadataEntries().add(createPathEntry("run_id",
					outcollectionName));
			pathEntriesOutFolder.getPathMetadataEntries().add(createPathEntry("run_date",outcollectionName.replaceAll("OUT_", ""), "yymmdd"));
			pathEntriesOutFolder.setPath(outFolderPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesOutFolder);

			
			String subcollectionType = getCollectionNameFromParent(fullPath, outcollectionName);
			
            if(subcollectionType != null) {
			// Add path metadata entries for "Sub folder" collection
			if (StringUtils.equalsIgnoreCase(subcollectionType, "Flowcell")) {
				String sampleId = getCollectionNameFromParent(fullPath, subcollectionType); // e.g., sampleId
																				
				String subFolderPath = outFolderPath + "/Raw_Data";
				HpcBulkMetadataEntry pathEntriesSubFolder = new HpcBulkMetadataEntry();
				pathEntriesSubFolder.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Raw_Data"));
				pathEntriesSubFolder.getPathMetadataEntries().add(createPathEntry("library_strategy",
						getAttrValueFromMetadataMap(metadataFilePathKey, "library_strategy")));// Fastq_combined
				pathEntriesSubFolder.getPathMetadataEntries().add(createPathEntry("analyte_type",
						getAttrValueFromMetadataMap(metadataFilePathKey, "analyte_type")));
				pathEntriesSubFolder.getPathMetadataEntries()
						.add(createPathEntry("tissue", getAttrValueFromMetadataMap(metadataFilePathKey, "tissue")));
				pathEntriesSubFolder.setPath(subFolderPath);
				hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesSubFolder);

				// Add path metadata entries for "Sample" collection
					
					String samplePath = subFolderPath + "/" + sampleId;
					HpcBulkMetadataEntry pathEntriessample = new HpcBulkMetadataEntry();
					pathEntriessample.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Sample"));
					pathEntriessample.setPath(samplePath);
					pathEntriessample.getPathMetadataEntries().add(createPathEntry("sample_id",
							sampleId.replaceAll("Sample_", "")));
					if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "tissue_type")))
					pathEntriessample.getPathMetadataEntries().add(createPathEntry("tissue_type",
							getAttrValueFromMetadataMap(metadataFilePathKey, "tissue_type")));
					if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "age")))
						pathEntriessample.getPathMetadataEntries()
								.add(createPathEntry("age", getAttrValueFromMetadataMap(metadataFilePathKey,"age")));

					if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "organism_strain")))
						pathEntriessample.getPathMetadataEntries().add(
								createPathEntry("organism_strain", getAttrValueFromMetadataMap(metadataFilePathKey, "organism_strain")));
					if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(metadataFilePathKey, "sex")))
						pathEntriessample.getPathMetadataEntries()
								.add(createPathEntry("sex", getAttrValueFromMetadataMap(metadataFilePathKey, "sex")));

					hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriessample);

					

				}
				// Add path metadata entries for "Analysis" collection
				else if (StringUtils.equalsIgnoreCase(subcollectionType, "Analysis")) {
					String analysisPath = outFolderPath + "/Analysis";
					HpcBulkMetadataEntry pathEntriesAnalysis = new HpcBulkMetadataEntry();
					pathEntriesAnalysis.getPathMetadataEntries()
							.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Analysis"));
					pathEntriesAnalysis.setPath(analysisPath);
					hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesAnalysis);

				}  
			   // Add path metadata entries for "QC" collection
				else if (StringUtils.equalsIgnoreCase(subcollectionType, "QC")) {
					String analysisPath = outFolderPath + "/QC";
					HpcBulkMetadataEntry pathEntriesAnalysis = new HpcBulkMetadataEntry();
					pathEntriesAnalysis.getPathMetadataEntries()
							.add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "QC"));
					pathEntriesAnalysis.setPath(analysisPath);
					hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesAnalysis);

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

		}
		logger.info("HAO lab custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}",
				object.getId());
		return dataObjectRegistrationRequestDTO;
	}

	/**
	 * Utility to extract the directory name that follows directly after the given
	 * parent folder. E.g., for path /a/b/c/d.txt and parentName = "b", returns "c".
	 */
	private String getCollectionNameFromParent(Path fullFilePath, String parentName) {
	
		logger.info("Full File Path = {}", fullFilePath);
		int count = fullFilePath.getNameCount();
		for (int i = 0; i <= count; i++) {
			if (fullFilePath.getParent()!=null && Files.isDirectory(fullFilePath.getParent()) 
					&& StringUtils.equals(fullFilePath.getParent().getFileName().toString(),parentName)) {
				return Files.isDirectory(fullFilePath)? fullFilePath.getFileName().toString():null;
			}
			fullFilePath = fullFilePath.getParent();
		}
		return null;
	}

	private Path getCollectionPathFromParent(Path fullFilePath, String parentName) {
		logger.info("Full File Path = {}", fullFilePath);
		int count = fullFilePath.getNameCount();
		for (int i = 0; i <= count; i++) {
			if (fullFilePath.getParent().getFileName().toString().equals(parentName)) {
				return fullFilePath;
			}
			fullFilePath = fullFilePath.getParent();
		}
		return null;
	}

	private String getProjectcollectionName(Path fullPath, String metadataFilePathKey) {
		String projectCollectionName = null;
		projectCollectionName = getAttrValueFromMetadataMap(metadataFilePathKey, "project_id");
		logger.info("projectCollectionName: {}", projectCollectionName);
		return projectCollectionName;

	}
	
	private String getOutCollectionName(Path fullPath) {
		String OutCollectionName = null;
		OutCollectionName = getCollectionNameFromParent(fullPath, "OUT");
		logger.info("outCollectionName: {}", OutCollectionName);
		return OutCollectionName;
	}

	public String getPathForMetadata(Path fullPath) {
		// Path key is Run level /data/Machida_lab/CryoEM/202504
		String metadataKeypath = getCollectionPathFromParent(fullPath,"OUT").toString();

		// Normalize known roots to /data
		int idx = metadataKeypath.indexOf("/Hager/");
		if (idx >= 0) {
			return "/data" + metadataKeypath.substring(idx);
		}
		return metadataKeypath;
	}

	private String getPiCollectionName() {
		return "Gordon_Hager";
	}

	private String resolveMetadataFile(String directory, String propertyPrefix) throws DmeSyncMappingException {
		
			File dir = new File(directory);
			File[] files = dir.listFiles((d, name) -> name.startsWith(propertyPrefix) && name.endsWith(".xlsx"));
			if (files != null && files.length > 0) {
				// Sort by last modified time, descending
				Arrays.sort(files, Comparator.comparing(File::lastModified).reversed());
				return files[0].getAbsolutePath(); // Most recently modified file
			}else {
				logger.error("Metadata excel file not found for {}", directory + "with pattern" + propertyPrefix);
				throw new DmeSyncMappingException(
						"Metadata excel file not found for " + directory + "with pattern" + propertyPrefix);
		      }
	}
	
}
