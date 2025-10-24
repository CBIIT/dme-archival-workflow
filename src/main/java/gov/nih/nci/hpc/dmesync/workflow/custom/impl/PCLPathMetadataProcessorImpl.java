package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.annotation.PostConstruct;

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
	
	
	@Value("${dmesync.doc.name}")
	private String doc;
	

	@Value("${dmesync.additional.metadata.excel:}")
	private String metadataFile;
	
	@Autowired
	private DmeMetadataBuilder dmeMetadataBuilder;

	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

		logger.info("[PathMetadataTask] PCL getArchivePath called");

		// extract the detail from the Path
		// PIFirst-PILast_Year_MM-DD_PostDocFirst-PostDocLast_Descriptor_StaffFirst-StaffLast
		// PiFirst-PILast_Year_MM-DD_PostdocFirst-PostdocLast_Description_PCLFirst-PCLLast

		// Example path - /mnt/lpat/mass_spec/Active/FY2021/Maura
		// O'Neill/Kyung-Lee/Kyung-Lee_2021_03-17_Klara-Pongorne-Kirsch_Plk1_Maura-O'Neill
		

		Path filePath = Paths.get(object.getSourceFilePath());
		String fileName = filePath.toFile().getName().replace("\"", "");

		String parentFolderName = filePath.getParent().getFileName().toString().replace("\"", "");
	

		String archivePath = destinationBaseDir + "/PI_" + getPiCollectionName(parentFolderName) + "/POC_"
				+ getPOCCollectionName(parentFolderName) + "/Staff_" + getStaffCollectionName(parentFolderName) + "/Year_"
				+ getYearCollectionName(parentFolderName, true) + "/Project_" + getProjectCollectionName(parentFolderName) + "/" + fileName;

		// replace spaces with underscore , singleQuotes
		archivePath = replaceCharacters(archivePath);


		logger.info("Archive path for {} : {}", object.getOriginalFilePath(), archivePath);

		return archivePath;
	}

	@Override
	public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {
		
		

		Path filePath = Paths.get(object.getSourceFilePath());
		String fileName = filePath.toFile().getName();
		String folderName = filePath.getParent().getFileName().toString().replace("\"", "");
		String[] staffName = getFieldFromFolderName(folderName, FOLDER_FIELD_STAFF).split("-");
		String staffFirstName = staffName[0];
		String staffLastName = staffName[1];
		String year = getFieldFromFolderName(folderName, FOLDER_FIELD_YEAR);
		String monthDay = getFieldFromFolderName(folderName, FOLDER_FIELD_MMDD);
		String descriptor = getFieldFromFolderName(folderName, FOLDER_FIELD_DESCRIPTOR);
		
		// load the user metadata from the externally placed excel
		 metadataMap = dmeMetadataBuilder.getMetadataMap(metadataFile, "Folder");

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
		//String metadataFileKey= fileName.replace(".tar", "");

		pathEntriesPI.setPath(piCollectionPath);
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("collection_type", "PI_Lab"));
		String dataOwnerFullName=getAttrValueFromMetadataMap(folderName, "data_owner");
		String[] dataOwnerFullNameParts = dataOwnerFullName.split(" ");
		String formattedPIName=null;
        if (dataOwnerFullNameParts.length == 2) {
            String pIFirstName = dataOwnerFullNameParts[0];
            String pIlastName = dataOwnerFullNameParts[1];
             formattedPIName = pIlastName + "," + pIFirstName;
        }else {
        	formattedPIName=dataOwnerFullName;
        }
		pathEntriesPI.getPathMetadataEntries()
				.add(createPathEntry("data_owner", formattedPIName));
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_affiliation",
				getAttrValueFromMetadataMap(folderName, "data_owner_affiliation")));
		pathEntriesPI.getPathMetadataEntries().add(
				createPathEntry("data_owner_email", getAttrValueFromMetadataMap(folderName, "data_owner_email")));
		
		if (getAttrValueFromMetadataMap(folderName, "data_owner_designee") != null) {
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_designee",
					getAttrValueFromMetadataMap(folderName, "data_owner_designee")));
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_designee_email",
					getAttrValueFromMetadataMap(folderName, "data_owner_designee_email")));
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner_designee_affiliation",
					getAttrValueFromMetadataMap(folderName, "data_owner_designee_affiliation ")));
		}
		hpcBulkMetadataEntries.getPathsMetadataEntries()
				.add(pathEntriesPI);
		

		// Add path metadata entries for "POC_XXX" collection

		HpcBulkMetadataEntry pathEntriesPOC = new HpcBulkMetadataEntry();
		String pocCollectionName = getPOCCollectionName(folderName);
		String pocCollectionPath = piCollectionPath + "/POC_" + pocCollectionName;
		pathEntriesPOC.setPath(pocCollectionPath);
		pathEntriesPOC.getPathMetadataEntries().add(createPathEntry("collection_type", "Researcher"));
		pathEntriesPOC.getPathMetadataEntries()
				.add(createPathEntry("project_poc", getAttrValueFromMetadataMap(folderName, "project_poc")));
		pathEntriesPOC.getPathMetadataEntries()
				.add(createPathEntry("project_poc_affiliation",getAttrValueFromMetadataMap(folderName, "project_poc_affiliation")));
		pathEntriesPOC.getPathMetadataEntries()
				.add(createPathEntry("project_poc_email", getAttrValueFromMetadataMap(folderName, "project_poc_email")));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPOC);

		// Add path metadata entries for "Staff_XXX" collection

		HpcBulkMetadataEntry pathEntriesStaff = new HpcBulkMetadataEntry();
		String staffCollectionName = getStaffCollectionName(folderName);
		String staffCollectionPath = pocCollectionPath + "/Staff_" + staffCollectionName;
		pathEntriesStaff.getPathMetadataEntries().add(createPathEntry("collection_type", "Staff"));
		pathEntriesStaff.setPath(staffCollectionPath);
		pathEntriesStaff.getPathMetadataEntries()
				.add(createPathEntry("staff_name", staffLastName + ", " + staffFirstName));
		if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(folderName, "staff_email")))
			pathEntriesStaff.getPathMetadataEntries().add(createPathEntry("staff_email",
					getAttrValueFromMetadataMap(folderName, "staff_email")));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesStaff);
		

		// Add path metadata entries for "Year_XXX" collection

		HpcBulkMetadataEntry pathEntriesYear = new HpcBulkMetadataEntry();
		String yearCollectionName = getYearCollectionName(folderName,true);
		String yearCollectionPath = staffCollectionPath + "/Year_" + yearCollectionName;
		pathEntriesYear.setPath(yearCollectionPath);
		pathEntriesYear.getPathMetadataEntries().add(createPathEntry("collection_type", "Date"));
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
				.add(createPathEntry("project_description", getAttrValueFromMetadataMap(folderName, "project_description")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("organism", getAttrValueFromMetadataMap(folderName,  "organism")));
		if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(folderName, "study_disease")))
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("study_disease", getAttrValueFromMetadataMap(folderName, "study_disease")));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("platform_name", getAttrValueFromMetadataMap(folderName,  "platform_name")));
		if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(folderName, "project_completed_date")))
			pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_completed_date",
					getAttrValueFromMetadataMap(folderName, "project_completed_date")));
		if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(folderName, "pubmed_id")))
			pathEntriesProject.getPathMetadataEntries()
					.add(createPathEntry("pubmed_id", getAttrValueFromMetadataMap(folderName, "pubmed_id")));
		if (StringUtils.isNotBlank(getAttrValueFromMetadataMap(folderName, "Collaborators")))
			pathEntriesProject.getPathMetadataEntries().add(
					createPathEntry("Collaborators", getAttrValueFromMetadataMap(folderName, "Collaborators")));


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

	private String getPiCollectionName(String parentFolderName) throws DmeSyncMappingException {
		String piName = getFieldFromFolderName(parentFolderName, FOLDER_FIELD_PI);
		piName = replaceCharacters(piName);
		return piName;
	}

	private String getProjectCollectionName(String parentFolderName) throws DmeSyncMappingException {
		String projectName=getFieldFromFolderName(parentFolderName, FOLDER_FIELD_DESCRIPTOR) ;
		  projectName=replaceCharacters(projectName);
		return projectName;
	}

	private String getYearCollectionName(String parentFolderName , boolean onlyYear) {
		if(onlyYear) {
			return getFieldFromFolderName(parentFolderName, FOLDER_FIELD_YEAR);
		}else {
		return getFieldFromFolderName(parentFolderName, FOLDER_FIELD_YEAR) + "-"
				+ getFieldFromFolderName(parentFolderName, FOLDER_FIELD_MMDD);
		}
	}

	private String getStaffCollectionName(String parentFolderName) {
		String staffName = getFieldFromFolderName(parentFolderName, FOLDER_FIELD_STAFF);
		staffName = replaceCharacters(staffName);
		return staffName;
	}

	private String getPOCCollectionName(String parentFolderName) {
		String pocName = getFieldFromFolderName(parentFolderName, FOLDER_FIELD_POSTDOC);
		pocName = replaceCharacters(pocName);
		return pocName;
	}
	

	
	private String replaceCharacters(String collectionName) {
		// replace spaces with underscore
		collectionName = collectionName.replace(" ", "_");
		// remove single quotes
		collectionName = collectionName.replace("'", "_");
		collectionName = collectionName.replace("-", "_");
		return collectionName;
	}

}