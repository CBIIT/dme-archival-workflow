package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.nio.file.Paths;
import org.springframework.stereotype.Service;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncPathMetadataProcessor;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntries;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;

/**
 * PCL DME Path and Meta-data Processor Implementation
 * 
 * @author dinhys
 *
 */
@Service("pcl")
public class PCLPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
		implements DmeSyncPathMetadataProcessor {

	// PCL Custom logic for DME path construction and meta data creation
	public static final int FOLDER_FIELD_PI = 0;
	public static final int FOLDER_FIELD_YEAR = 1;
	public static final int FOLDER_FIELD_MMDD = 2;
	public static final int FOLDER_FIELD_POSTDOC = 3;
	public static final int FOLDER_FIELD_DESCRIPTOR = 4;
	public static final int FOLDER_FIELD_STAFF = 5;

	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

		logger.info("[PathMetadataTask] PCL getArchivePath called");

		// extract the detail from the Path
		// PIFirst-PILast_Year_MM-DD_PostDocFirst-PostDocLast_Descriptor_StaffFirst-StaffLast
		// PiFirst-PILast_Year_MM-DD_PostdocFirst-PostdocLast_Description_PCLFirst-PCLLast

		// Example path - /mnt/lpat/mass_spec/Active/FY2021/Maura
		// O'Neill/Kyung-Lee/Kyung-Lee_2021_03-17_Klara-Pongorne-Kirsch_Plk1_Maura-O'Neill

		String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();

		String archivePath = destinationBaseDir + "/DataOwner_" + getPiCollectionName(fileName) + "/POC_"
				+ getPOCCollectionName(fileName) + "/Staff_" + getStaffCollectionName(fileName) + "/Year_"
				+ getYearCollectionName(fileName) + "/Project_" + getProjectCollectionName(fileName) + "/" + fileName;

		// replace spaces with underscore
		archivePath = archivePath.replace(" ", "_");
		// remove single quotes
		archivePath = archivePath.replace("'", "");

		logger.info("Archive path for {} : {}", object.getOriginalFilePath(), archivePath);

		return archivePath;
	}

	@Override
	public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException {

		String folderName = Paths.get(object.getOriginalFilePath()).toFile().getName();
		String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
		String[] piName = getFieldFromFolderName(folderName, FOLDER_FIELD_PI).split("-");
		String piFirstName = piName[0];
		String piLastName = piName[1];
		String[] postDocName = getFieldFromFolderName(folderName, FOLDER_FIELD_POSTDOC).split("-");
		String postDocFirstName = postDocName[0];
		String postDocLastName = postDocName[1];
		String[] staffName = getFieldFromFolderName(folderName, FOLDER_FIELD_STAFF).split("-");
		String staffFirstName = staffName[0];
		String staffLastName = staffName[1];
		String year = getFieldFromFolderName(folderName, FOLDER_FIELD_YEAR);
		String monthDay = getFieldFromFolderName(folderName, FOLDER_FIELD_MMDD);
		String descriptor = getFieldFromFolderName(folderName, FOLDER_FIELD_DESCRIPTOR);

		// Add to HpcBulkMetadataEntries for path attributes
		HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

		// Add path metadata entries for "DataOwner_XXX" collection
		// Example row: collectionType - DataOwner_Lab, collectionName - Kyung_Lee
		// key = data_owner, value = Lee, Kyung
		// key = data_generator, value = Thorkell Andresson
		// key = data_generator_email, value = andressont@mail.nih.gov
		// key = data_generator_affiliation, value = CCR PCL
		HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
		String piCollectionName = getPiCollectionName(folderName);
		String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;

		pathEntriesPI.setPath(piCollectionPath);
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("collection_type", "DataOwner_Lab"));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner", piLastName + ", " + piFirstName));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_designee", ""));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_designee_email", ""));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_designee_affiliation", "CCR PCL"));
		hpcBulkMetadataEntries.getPathsMetadataEntries()
				.add(populateStoredMetadataEntries(pathEntriesPI, "DataOwner_Lab", piCollectionName, "pcl"));

		// Add path metadata entries for "POC_XXX" collection

		HpcBulkMetadataEntry pathEntriesPOC = new HpcBulkMetadataEntry();
		String pocCollectionName = getPOCCollectionName(folderName);
		String pocCollectionPath = piCollectionPath + "/POC_" + pocCollectionName;
		pathEntriesPOC.setPath(pocCollectionPath);
		pathEntriesPOC.getPathMetadataEntries().add(createPathEntry("collection_type", "Researcher"));
		pathEntriesPOC.getPathMetadataEntries()
				.add(createPathEntry("project_poc", postDocLastName + ", " + postDocFirstName));
		pathEntriesPOC.getPathMetadataEntries()
				.add(createPathEntry("project_poc_affiliation", postDocLastName + ", " + postDocFirstName));
		pathEntriesPOC.getPathMetadataEntries()
				.add(createPathEntry("project_poc_email", postDocLastName + ", " + postDocFirstName));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPOC);

		// Add path metadata entries for "Staff_XXX" collection

		HpcBulkMetadataEntry pathEntriesStaff = new HpcBulkMetadataEntry();
		String staffCollectionName = getStaffCollectionName(folderName);
		String staffCollectionPath = pocCollectionPath + "/Staff_" + staffCollectionName;
		pathEntriesStaff.setPath(staffCollectionPath);
		pathEntriesStaff.getPathMetadataEntries()
				.add(createPathEntry("staff_name", staffLastName + ", " + staffFirstName));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesStaff);

		// Add path metadata entries for "Year_XXX" collection

		HpcBulkMetadataEntry pathEntriesYear = new HpcBulkMetadataEntry();
		String yearCollectionName = getYearCollectionName(folderName);
		String yearCollectionPath = staffCollectionPath + "/Year_" + yearCollectionName;
		pathEntriesYear.setPath(yearCollectionPath);
		pathEntriesYear.getPathMetadataEntries().add(createPathEntry("year", year));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesYear);

		// Add path metadata entries for "Project_XXX" collection
		HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
		String projectCollectionName = getProjectCollectionName(folderName);
		String projectCollectionPath = yearCollectionPath + "/Project_" + projectCollectionName;
		pathEntriesProject.setPath(projectCollectionPath);
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("collection_type", "Project"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Closed Access"));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("data_generating_facility", "Protein Characterization Laboratory"));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_id",
						getFieldFromFolderName(folderName, FOLDER_FIELD_PI) + "_" + year + "_"
								+ getFieldFromFolderName(folderName, FOLDER_FIELD_POSTDOC) + "_"
								+ getFieldFromFolderName(folderName, FOLDER_FIELD_STAFF)));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_title", descriptor));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_start_date", year + "-" + monthDay));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_description", getAttrValueWithKey(folderName, "project_description")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("organism", getAttrValueWithKey(folderName, "organism")));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("study_disease", getAttrValueWithKey(folderName, "study_disease")));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("platform_name", getAttrValueWithKey(folderName, "platform_name")));

		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);

		// Set it to dataObjectRegistrationRequestDTO
		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();
		dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
		dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
		dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);

		// Add object metadata
		dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("object_name", fileName));
		dataObjectRegistrationRequestDTO.getMetadataEntries()
				.add(createPathEntry("source_path", object.getOriginalFilePath()));

		logger.info("PCL custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
		return dataObjectRegistrationRequestDTO;
	}

	private String getFieldFromFolderName(String folderName, int field) {
		String[] fields = folderName.split("_");
		if (fields.length > field)
			return fields[field];
		return null;
	}

	private String getPiCollectionName(String fileName) throws DmeSyncMappingException {
		String piName = getFieldFromFolderName(fileName, FOLDER_FIELD_PI);
		piName = piName.replace("-", "_");
		return piName;
	}

	private String getProjectCollectionName(String fileName) throws DmeSyncMappingException {
		return getFieldFromFolderName(fileName, FOLDER_FIELD_DESCRIPTOR) + "_";
	}

	private String getYearCollectionName(String fileName) {
		return getFieldFromFolderName(fileName, FOLDER_FIELD_YEAR) + "-"
				+ getFieldFromFolderName(fileName, FOLDER_FIELD_MMDD);
	}

	private String getStaffCollectionName(String fileName) {
		String staffName = getFieldFromFolderName(fileName, FOLDER_FIELD_STAFF);
		staffName = staffName.replace("-", "_");
		return staffName;
	}

	private String getPOCCollectionName(String fileName) {
		String pocName = getFieldFromFolderName(fileName, FOLDER_FIELD_POSTDOC);
		pocName = pocName.replace("-", "_");
		return pocName;
	}

}
