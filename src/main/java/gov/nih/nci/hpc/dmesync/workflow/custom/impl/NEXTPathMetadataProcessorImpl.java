package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

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
 * NExT DME Path and Meta-data Processor Implementation
 *
 * @author dinhys
 */
@Service("next")
public class NEXTPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
		implements DmeSyncPathMetadataProcessor {

	// NExT Custom logic for DME path construction and meta data creation

	@Value("${dmesync.source.base.dir}")
	private String sourceBaseDir;

	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

		logger.info("[PathMetadataTask] NExT getArchivePath called");
		// Example source path:
		// nanoimaging-data/797/2022/wbg2g22jun22a/frames/rawdata/references
		// Example dest path:
		// PI_James_Doroshow/Project_NOX/Year_2022/Session_wbg2g22jun22a/rawdata
		// Example dest path:
		// PI_James_Doroshow/Project_NOX/Year_2022/Session_wbg2g22jun22a/rawdata/references

		Path sourceDirPath = Paths.get(object.getOriginalFilePath());

		String archivePath = destinationBaseDir + "/PI_" + getPiCollectionName() + "/Project_"
				+ getProjectCollectionName(object) + "/Year_" + getYearCollectionName(object) + "/Session_"
				+ getSessionId(object)
				+ (isReferenceFile(object.getOriginalFilePath()) ? "/rawdata/references" : "/rawdata") + "/"
				+ sourceDirPath.getFileName().toString();

		// replace spaces with underscore
		archivePath = archivePath.replace(" ", "_");

		logger.info("Archive path for {} : {}", object.getOriginalFilePath(), archivePath);

		return archivePath;
	}

	private boolean isReferenceFile(String originalFilePath) {
		return originalFilePath.contains("references");
	}

	@Override
	public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException {

		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();

		// Add to HpcBulkMetadataEntries for path attributes
		HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

		// Add path metadata entries for "PI_XXX" collection
		// Example row: collectionType - PI, collectionName (derived)
		// key = data_owner, value = (derived)

		String piCollectionName = getPiCollectionName();
		String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;
		HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "DataOwner_Lab"));
		pathEntriesPI.setPath(piCollectionPath);
		hpcBulkMetadataEntries.getPathsMetadataEntries()
				.add(populateStoredMetadataEntries(pathEntriesPI, "DataOwner_Lab", piCollectionName, "next"));

		// Add path metadata entries for "Project_XXX" collection
		// Example row: collectionType - Project, collectionName (derived)
		// key = project_id, project_title, project_description, project_start_date,
		// value = (supplied)
		// key = project_poc, project_poc_email, project_poc_affiliation value =
		// (supplied)
		// key = organism, study_disease, data_generating_facility value = (supplied)
		// key = access, value = "Controlled Access" (default)
		// key = project_status, value = Active (default)
		// key = retention_years, value = 7 (default)

		String projectId = getProjectId(object);
		String projectCollectionName = getProjectCollectionName(object);
		String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;
		HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_id", projectId));
		pathEntriesProject.setPath(projectCollectionPath);
		hpcBulkMetadataEntries.getPathsMetadataEntries()
				.add(populateStoredMetadataEntries(pathEntriesProject, "Project", projectCollectionName, "next"));

		// Add path metadata entries for "Year_XXX" collection
		// Example row: collectionType - Year, collectionName (derived)

		String yearCollectionName = getYearCollectionName(object);
		String yearCollectionPath = projectCollectionPath + "/Year_" + yearCollectionName;
		HpcBulkMetadataEntry pathEntriesYear = new HpcBulkMetadataEntry();
		pathEntriesYear.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Year"));
		pathEntriesYear.setPath(yearCollectionPath);
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesYear);

		// Add path metadata entries for "Session_XXX" collection
		// Example row: collectionType - Session, collectionName (derived)
		// key = session_id, value = wbg2g22jun22a (derived)
		// key = microscope_name, 2g = "2nd glacios", 1k = "1st krios" (derived)
		// key = microscope_location, value = wbg (derived)
		// key = session_date, value = 2022-06-22 (derived)
		String sessionId = getSessionId(object);
		String sessionCollectionPath = yearCollectionPath + "/Session_" + sessionId;
		HpcBulkMetadataEntry pathEntriesSession = new HpcBulkMetadataEntry();
		pathEntriesSession.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Run"));
		pathEntriesSession.getPathMetadataEntries().add(createPathEntry("session_id", sessionId));
		pathEntriesSession.getPathMetadataEntries()
				.add(createPathEntry("microscope_name", getMicroscopeName(sessionId)));
		pathEntriesSession.getPathMetadataEntries()
				.add(createPathEntry("microscope_location", getMicroscopeLocation(sessionId)));
		pathEntriesSession.getPathMetadataEntries().add(createPathEntry("session_date", getSessionDate(sessionId)));
		pathEntriesSession.setPath(sessionCollectionPath);
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesSession);

		String rawDataCollectionPath = sessionCollectionPath + "/rawdata";
		HpcBulkMetadataEntry pathEntriesRawData = new HpcBulkMetadataEntry();
		pathEntriesRawData.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Raw_Data"));
		pathEntriesRawData.setPath(rawDataCollectionPath);
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesRawData);
		
		// Set it to dataObjectRegistrationRequestDTO
		dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
		dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);

		// Add object metadata
		dataObjectRegistrationRequestDTO.getMetadataEntries()
				.add(createPathEntry("object_name", Paths.get(object.getSourceFilePath()).toFile().getName()));
		dataObjectRegistrationRequestDTO.getMetadataEntries()
				.add(createPathEntry("source_path", object.getOriginalFilePath()));

		logger.info("NExT custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
		return dataObjectRegistrationRequestDTO;
	}

	private String getSessionDate(String sessionId) throws DmeSyncMappingException {
		// Example: For sessionId wbg2g22jun22a, session_date is 2022-06-22
		String year = StringUtils.substring(sessionId, 5, 7);
		String day = StringUtils.substring(sessionId, sessionId.length() - 3, sessionId.length() - 1);
		String mon = StringUtils.substringBeforeLast(sessionId, day);
		mon = StringUtils.substringAfterLast(mon, year);
		Date date;
		try {
			date = new SimpleDateFormat("MMM", Locale.ENGLISH).parse(mon);
		} catch (ParseException e) {
			throw new DmeSyncMappingException(e);
		}
		SimpleDateFormat sd1 = new SimpleDateFormat("MM");
		String month = sd1.format(date);
		return "20" + year + "-" + month + "-" + day;
	}

	private String getMicroscopeLocation(String sessionId) {
		// Example: For sessionId wbg2g22jun22a, microscope_location is wbg
		String microscopeLocation = StringUtils.substring(sessionId, 0, 3);
		logger.info("microscopeLocation: {}", microscopeLocation);
		return microscopeLocation;
	}

	private String getMicroscopeName(String sessionId) {
		// Example: For sessionId wbg2g22jun22a, microscope_name is 2g = "2nd glacios",
		// for 1k = "1st krios"
		String microscopeName = StringUtils.substring(sessionId, 3, 5);
		logger.info("microscopeName: {}", microscopeName);
		if (StringUtils.equalsIgnoreCase("2g", microscopeName)) {
			microscopeName = "2nd glacios";
		} else if (StringUtils.equalsIgnoreCase("1k", microscopeName)) {
			microscopeName = "1st krios";
		} else {
			microscopeName = "";
		}
		return microscopeName;
	}

	private String getCollectionNameFromParent(StatusInfo object, String parentName) {
		Path fullFilePath = Paths.get(object.getOriginalFilePath());
		int count = fullFilePath.getNameCount();
		for (int i = 0; i <= count; i++) {
			if (fullFilePath.getParent().getFileName().toString().equals(parentName)) {
				return fullFilePath.getFileName().toString();
			}
			fullFilePath = fullFilePath.getParent();
		}
		return null;
	}

	private String getPiCollectionName() {
		return "James_Doroshow";
	}

	private String getProjectId(StatusInfo object) {
		Path fullFilePath = Paths.get(object.getOriginalFilePath());
		String projectId = fullFilePath.getName(0).toString();
		logger.info("projectId: {}", projectId);
		return projectId;
	}

	private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {
		String projectCollectionName = getCollectionMappingValue(getProjectId(object), "Project", "next");
		logger.info("projectCollectionName: {}", projectCollectionName);
		return projectCollectionName;
	}

	private String getSessionId(StatusInfo object) {
		String sessionId = getCollectionNameFromParent(object, getYearCollectionName(object));
		logger.info("sessionId: {}", sessionId);
		return sessionId;
	}

	private String getYearCollectionName(StatusInfo object) {
		String year = getCollectionNameFromParent(object, getProjectId(object));
		logger.info("year: {}", year);
		return year;
	}
}