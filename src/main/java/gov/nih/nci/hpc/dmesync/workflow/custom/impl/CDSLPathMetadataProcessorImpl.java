package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;


import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.util.ExcelUtil;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncPathMetadataProcessor;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntries;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;

/**
 *
 * @author Yuri Dinh
 */

@Service("cdsl")
public class CDSLPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
		implements DmeSyncPathMetadataProcessor {

	@Value("${dmesync.doc.name}")
	private String doc;

	@Value("${dmesync.source.base.dir}")
	private String sourceDir;
	
	@Value("${dmesync.work.base.dir}")
	private String workDir;

	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

		logger.info("CDSL custom DmeSyncPathMetadataProcessor start process for object {}", object.getId());

		String fileName = Paths.get(object.getSourceFileName()).toFile().getName();
		String filePath = object.getOriginalFilePath();
		String archivePath;

		logger.info("File Path " + filePath);
		// Build path based on Analysis or Raw_Data
		if (object.getSourceFilePath().contains("fast5")) {

			archivePath = destinationBaseDir + "/PI_" + getPiCollectionName(filePath, "DataOwner") + "/"
					+ getFolderNameFromPathContaining(filePath, "Project") + "/"
					+ getFolderNameFromPathContaining(filePath, "Sample") + "/"
					+ getFolderNameFromPathContaining(filePath, "Run") + "/" + "fast5/" + fileName;
		} else {
			archivePath = destinationBaseDir + "/PI_" + getPiCollectionName(filePath, "DataOwner") + "/"
					+ getFolderNameFromPathContaining(filePath, "Project") + "/"
					+ getFolderNameFromPathContaining(filePath, "Sample") + "/"
					+ getFolderNameFromPathContaining(filePath, "Run") + "/" + "Sequencing_Reports/" + fileName;
		}

		// replace spaces with underscore
		archivePath = archivePath.replace(" ", "_");

		return archivePath;
	}

	@Override
	public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException , IOException {

		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();

		// Add to HpcBulkMetadataEntries for path attributes
		HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

		String filePath = object.getOriginalFilePath();

		// Construct location of user metadata csv file
		String ownerMetadataCsvFile = getPathToFolderContaining(object.getOriginalFilePath(), "DataOwner").getParent()
				.toString() + "/owner_metadata.csv";
		String projectMetadataCsvFile = getPathToFolderContaining(object.getOriginalFilePath(), "Project").getParent()
				.toString() + "/project_metadata.csv";
		String sampleMetadataCsvFile = getPathToFolderContaining(object.getOriginalFilePath(), "Sample").getParent()
				.toString() + "/sample_metadata.csv";

		// Construct location of excel file in work dir
	    Path baseDirPath;
	    Path workDirPath;
	    /*Path ownerMetadataExcelFile;
	    Path projectMetadataExcelFile;
	    Path sampleMetadataExcelFile;
	    
		try {
			baseDirPath = Paths.get(sourceDir).toRealPath();
			workDirPath = Paths.get(workDir).toRealPath();

		    Path sourceDirPath = Paths.get(object.getOriginalFilePath());
		    Path relativePath = baseDirPath.relativize(sourceDirPath);
		    String workDir = workDirPath.toString() + File.separatorChar + relativePath.toString();
			ownerMetadataExcelFile = Paths.get(getPathToFolderContaining(workDir, "DataOwner").getParent()
					.toString() + "/owner_metadata.xls");
			projectMetadataExcelFile = Paths.get(getPathToFolderContaining(workDir, "Project").getParent()
					.toString() + "/project_metadata.xls");
			sampleMetadataExcelFile = Paths.get(getPathToFolderContaining(workDir, "Sample").getParent()
					.toString() + "/sample_metadata.xls");
	
		    Files.createDirectories(ownerMetadataExcelFile.getParent());
		    Files.createDirectories(projectMetadataExcelFile.getParent());
		    Files.createDirectories(sampleMetadataExcelFile.getParent());
		    
		} catch (IOException e) {
			throw new DmeSyncMappingException("Source directory or work directory not defined.");
		}
		
		createExcelFile(ownerMetadataCsvFile, ownerMetadataExcelFile.toString());
		createExcelFile(projectMetadataCsvFile, projectMetadataExcelFile.toString());
		createExcelFile(sampleMetadataCsvFile, sampleMetadataExcelFile.toString()); */

		// Add path metadata entries for DataOwner_Lab collection
		String piCollectionPath = destinationBaseDir + "/PI_" + getPiCollectionName(filePath, "DataOwner");
		piCollectionPath = piCollectionPath.replace(" ", "_");
		HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "DataOwner_Lab"));
		pathEntriesPI.setPath(piCollectionPath);
		threadLocalMap.set(loadCsvMetadataFile(ownerMetadataCsvFile.toString(), "data_owner"));
		String dataOwnerKey = "KolmogorovLab2";
		pathEntriesPI.getPathMetadataEntries()
				.add(createPathEntry("data_owner", getPiCollectionName(filePath, "DataOwner")));
		pathEntriesPI.getPathMetadataEntries()
				.add(createPathEntry("data_owner_email", getAttrValueWithKey(dataOwnerKey, "data_owner_email")));
		pathEntriesPI.getPathMetadataEntries().add(
				createPathEntry("data_owner_affiliation", getAttrValueWithKey(dataOwnerKey, "data_owner_affiliation")));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPI);

		// Add path metadata entries for Project collection
		String projectCollectionPath = piCollectionPath + "/" + getFolderNameFromPathContaining(filePath, "Project");
		HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
		pathEntriesProject.setPath(projectCollectionPath);
		threadLocalMap.set(loadCsvMetadataFile(projectMetadataCsvFile.toString(), "project_id"));

		String projectKey = getFolderNameFromPathContaining(filePath, "Project");
		String dateEntry = getAttrValueWithKey(projectKey, "project_start_date");
		String[] dateArray = dateEntry.split("/");
		String createDate = dateArray[2] + "-" + dateArray[1] + "-" + dateArray[0];
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_id", projectKey));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_title", getAttrValueWithKey(projectKey, "project_title")));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_description", getAttrValueWithKey(projectKey, "project_description")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_start_date", createDate));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("organism", getAttrValueWithKey(projectKey, "organism")));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("is_cell_line", getAttrValueWithKey(projectKey, "is_cell_line")));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("study_disease", getAttrValueWithKey(projectKey, "study_disease")));
		// Set defaults
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_poc", "Mikhail Kolmogorov"));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_poc_affiliation", "CDSL/CCR/NCI"));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_poc_email", "mikhail.kolmogorov@nih.gov"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("data_generating_facility", "NIH CARD"));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);

		// Add path metadata entries for Sample collection section
		String sampleCollectionPath = projectCollectionPath + "/" + getFolderNameFromPathContaining(filePath, "Sample");
		HpcBulkMetadataEntry pathEntriesSample = new HpcBulkMetadataEntry();
		pathEntriesSample.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Sample"));
		pathEntriesSample.setPath(sampleCollectionPath);
		threadLocalMap.set(loadCsvMetadataFile(sampleMetadataCsvFile.toString(), "sample_id"));
		String sampleKey = getFolderNameFromPathContaining(filePath, "Sample");
		String partKey[] = sampleKey.split("_");
		sampleKey = partKey[1];

		pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sample_id", sampleKey));
		pathEntriesSample.getPathMetadataEntries()
				.add(createPathEntry("library_strategy", getAttrValueWithKey(sampleKey, "library_strategy")));
		pathEntriesSample.getPathMetadataEntries()
				.add(createPathEntry("analyte_type", getAttrValueWithKey(sampleKey, "analyte_type")));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesSample);

		// Add path metadata entries for Run collection section
		String runCollectionPath = sampleCollectionPath + "/" + getFolderNameFromPathContaining(filePath, "Run");
		HpcBulkMetadataEntry pathEntriesRun = new HpcBulkMetadataEntry();
		pathEntriesRun.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Run"));
		pathEntriesRun.setPath(runCollectionPath);
		pathEntriesRun.getPathMetadataEntries()
				.add(createPathEntry("run_id", getFolderNameFromPathContaining(filePath, "Run")));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesRun);

		if (object.getSourceFilePath().contains("fast5")) {
			String rawDataCollectionPath = runCollectionPath + "/" + "fast5";
			HpcBulkMetadataEntry pathEntriesRawData = new HpcBulkMetadataEntry();

			pathEntriesRawData.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Raw_Data"));
			pathEntriesRawData.setPath(rawDataCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesRawData);
		} else {
			String analysisCollectionPath = runCollectionPath + "/" + "Sequencing_Reports";
			HpcBulkMetadataEntry pathEntriesAnalysis = new HpcBulkMetadataEntry();
			pathEntriesAnalysis.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Analysis"));

			pathEntriesAnalysis.setPath(analysisCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesAnalysis);
		}

		// Set it to dataObjectRegistrationRequestDTO
		dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
		dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
		dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);

		// Add object metadata
		String fileName = Paths.get(object.getOriginalFilePath()).toFile().getName();
		dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("object_name", fileName));
		dataObjectRegistrationRequestDTO.getMetadataEntries()
				.add(createPathEntry("source_path", object.getOriginalFilePath()));

		logger.info("Completed CDSL custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());

		return dataObjectRegistrationRequestDTO;

	}

	private String getPiCollectionName(String filePath, String match) {

		String folderName = getFolderNameFromPathContaining(filePath, match);
		String[] token = folderName.split("_");
		String piCollectionName = "Mikhail" + " " + token[2];
		logger.info("getPiCollectionName {}" + piCollectionName);

		return piCollectionName;
	}

	public String getAttrValueWithKey(String rowKey, String attrKey) {
		String key = null;
		if (threadLocalMap.get() == null)
			return null;
		for (String partialKey : threadLocalMap.get().keySet()) {
			if (StringUtils.contains(rowKey, partialKey)) {
				key = partialKey;
				break;
			}
		}
		if (StringUtils.isEmpty(key)) {
			logger.error("Excel mapping not found for {}", rowKey);
			return null;
		}

		return (threadLocalMap.get().get(key) == null ? null : threadLocalMap.get().get(key).get(attrKey));
	}

	public String getAttrValueWithExactKey(String key, String attrKey) {
		if (StringUtils.isEmpty(key)) {
			logger.error("Excel mapping not found for {}", key);
			return null;
		}
		return (threadLocalMap.get().get(key) == null ? null : threadLocalMap.get().get(key).get(attrKey));
	}

	private String getFolderNameFromPathContaining(String filePath, String token) {

		String folderName = "";
		Path fullPath = Paths.get(filePath);

		while (fullPath.getParent() != null) {
			if (fullPath.getParent().getFileName().toString().contains(token))
				return fullPath.getParent().getFileName().toString();
			fullPath = fullPath.getParent();
		}
		return folderName;

	}

	private Path getPathToFolderContaining(String filePath, String token) {

		Path fullPath = Paths.get(filePath);

		while (fullPath.getParent() != null) {
			if (fullPath.getParent().getFileName().toString().contains(token))
				return fullPath.getParent();
			fullPath = fullPath.getParent();
		}
		return fullPath;
	}

	private void createExcelFile(String originalCsvPath, String destinationExcelPath) throws DmeSyncMappingException {

		Path metadataFilePath = Paths.get(destinationExcelPath);

		try {
			if (!Files.exists(metadataFilePath))
				ExcelUtil.convertTextToExcel(new File(originalCsvPath), new File(destinationExcelPath), ",");

		} catch (IOException e) {
			throw new DmeSyncMappingException(
					"Can't convert CSV file :  " + originalCsvPath + " to excel " + destinationExcelPath, e);
		}

	}

}
