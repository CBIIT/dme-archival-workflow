package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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
 * DCTD OCCPR PCL DME Path and Meta-data Processor Implementation
 *
 * @author dinhys
 */
@Service("dctd-pcl")
public class DctdPclPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
		implements DmeSyncPathMetadataProcessor {

	// MicrobiomeCore DME path construction and meta data creation

	@Value("${dmesync.additional.metadata.excel:}")
	private String metadataFile;

	@Value("${dmesync.doc.name}")
	private String doc;

	@Value("${dmesync.source.base.dir}")
	private String sourceDir;

	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

		logger.info("[PathMetadataTask] DCTD OCCPR PCL getArchivePath called");

		// load the user metadata from the externally placed excel
		threadLocalMap.set(loadMetadataFile(metadataFile, "folder"));

		// Example source path -
		// /mnt/OCCPR_ACL_Archive/0_Data_archive/DCTD_PCL_Tara_Hiltke/TargetedProteomics_Chelsea Boo \
		//    /PCLT001_Altis_MRM_Benchmark/Data_Raw/20230425_400ngFFPE_0amolH_1fmolL_a.raw
		// Example destination path -
		// /DCTD_OCCPR_PCL_Archive/PI_Tara_Hiltke/POC_Chelsea_Boo/Project_PCLT001 \
		//    /Experiment_MRM/Instrument_Altis/Data_Raw/20230425_400ngFFPE_0amolH_1fmolL_a.raw
		// OR
		// /DCTD_OCCPR_PCL_Archive/DCTD_PCL_Tara_Hiltke/TargetedProteomics_Chelsea_Boo/PCLT001_Altis_MRM_Benchmark \
		//     /Experiment_MRM/Instrument_Altis/Data_Raw/20230425_400ngFFPE_0amolH_1fmolL_a.raw
		String fileName = Paths.get(object.getSourceFileName()).toFile().getName();
		String archivePath = null;

		archivePath = destinationBaseDir + "/PI_" + getPiCollectionName(object) + "/" + getPOCCollectionName(object) + "/Project_"
				+ getProjectCollectionName(object) + "/Experiment_" + getExperimentType(object) + "/Instrument_" + getInstrumentType(object) 
				+ (object.getOriginalFilePath().contains("Raw")? "/Data_Raw" : "/Data_Processed") + "/" + fileName;

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

		// Add path metadata entries for "PI_XXX" collection
		// Example row: collectionType - DataOwner_Lab, collectionName - XXX (user metadata)
		// key = data_owner, value = (user metadata)
		// key = data_owner_affiliation, (user metadata)
		// key = data_owner_email, value = (user metadata)
		// key = data_owner_designee, (user metadata)
		// key = data_owner_designee_email, (user metadata)

		String piCollectionName = getPiCollectionName(object);
		String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;
		piCollectionPath = piCollectionPath.replace(" ", "_");
		String projectCollectionName = getProjectCollectionName(object);
		String projectId = getProjectId(object);
		HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "DataOwner_Lab"));
		pathEntriesPI.setPath(piCollectionPath);
		pathEntriesPI.getPathMetadataEntries()
				.add(createPathEntry("data_owner", piCollectionName));
		pathEntriesPI.getPathMetadataEntries()
				.add(createPathEntry("data_owner_email", getAttrValueWithKey(projectCollectionName, "data_owner_email")));
		pathEntriesPI.getPathMetadataEntries().add(
				createPathEntry("data_owner_affiliation", getAttrValueWithKey(projectCollectionName, "data_owner_affiliation")));
		pathEntriesPI.getPathMetadataEntries().add(
				createPathEntry("data_owner_designee", getAttrValueWithKey(projectCollectionName, "data_owner_designee")));
		pathEntriesPI.getPathMetadataEntries().add(
				createPathEntry("data_owner_designee_email", getAttrValueWithKey(projectCollectionName, "data_owner_designee_email")));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPI);

		// Add path metadata entries for "POC_XXX" collection
		// Example row: collectionType - POC, collectionName - XXX (user metadata)
		// key = researcher, value = (user metadata)
		// key = researcher_email, value = (user metadata)
		String pocCollectionName = getPOCCollectionName(object);
		String pocCollectionPath = piCollectionPath + "/" + pocCollectionName;
		pocCollectionPath = pocCollectionPath.replace(" ", "_");
		HpcBulkMetadataEntry pathEntriesPOC = new HpcBulkMetadataEntry();
		pathEntriesPOC.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "POC"));
		pathEntriesPOC.setPath(pocCollectionPath);
		pathEntriesPOC.getPathMetadataEntries()
				.add(createPathEntry("researcher", getResearcher(object)));
		pathEntriesPOC.getPathMetadataEntries()
				.add(createPathEntry("researcher_email", getAttrValueWithKey(projectCollectionName, "researcher_email")));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPOC);

		// Add path metadata entries for "Project_XXX" collection
		// Example row: collectionType - Project, collectionName - based on projectId
		// project_poc, value = (user metadata)
		// project_poc_affiliation, value = (user metadata)
		// project_poc_email, value = (user metadata)
		// project_start_date, value = (user metadata)
		// project_id, value = (user metadata)
		// project_title, value = (user metadata)
		// project_description, value = (user metadata)

		String projectCollectionPath = pocCollectionPath + "/Project_" + projectCollectionName;
		projectCollectionPath = projectCollectionPath.replace(" ", "_");
		HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
		pathEntriesProject.setPath(projectCollectionPath);
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_id", projectId));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_title", getAttrValueWithKey(projectCollectionName, "project_title")));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_description", getAttrValueWithKey(projectCollectionName, "project_description")));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_poc", getAttrValueWithKey(projectCollectionName, "project_poc")));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_poc_email", getAttrValueWithKey(projectCollectionName, "project_poc_email")));
		pathEntriesProject.getPathMetadataEntries().add(
				createPathEntry("project_poc_affiliation", getAttrValueWithKey(projectCollectionName, "project_poc_affiliation")));
		pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_start_date", getProjectDate(object, "project_start_date")));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Closed Access"));
		if (StringUtils.isNotBlank(getAttrValueWithKey(projectCollectionName, "project_completed_date")))
			pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_completed_date", getProjectDate(object, "project_completed_date")));
		if (StringUtils.isNotBlank(getAttrValueWithKey(projectCollectionName, "project_status")))
			pathEntriesProject.getPathMetadataEntries()
				.add(createPathEntry("project_status", getAttrValueWithKey(projectCollectionName, "project_status")));
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);

		// Add path metadata entries for "Experiment" collection
		// Example row: collectionType - Experiment, collectionName - Experiment_<experiment_type>
		// experiment_id, value = (user metadata)
		// experiment_type, value = (user metadata)
		// experiment_date, value = (user metadata)
		String experimentType = getExperimentType(object);
		String experimentCollectionPath = projectCollectionPath + "/Experiment_" + experimentType;
		experimentCollectionPath = experimentCollectionPath.replace(" ", "_");
		HpcBulkMetadataEntry pathEntriesExperiment = new HpcBulkMetadataEntry();
		pathEntriesExperiment.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Experiment"));
		pathEntriesExperiment.getPathMetadataEntries().add(createPathEntry("experiment_type", experimentType));
		if (StringUtils.isNotBlank(getAttrValueWithKey(projectCollectionName, "experiment_id")))
			pathEntriesExperiment.getPathMetadataEntries()
					.add(createPathEntry("experiment_id", getAttrValueWithKey(projectCollectionName, "experiment_id")));
		if (StringUtils.isNotBlank(getAttrValueWithKey(projectCollectionName, "experiment_date")))
			pathEntriesExperiment.getPathMetadataEntries()
					.add(createPathEntry("experiment_date", getAttrValueWithKey(projectCollectionName, "experiment_date")));
		pathEntriesExperiment.setPath(experimentCollectionPath);
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesExperiment);

		// Add path metadata entries for "Instrument" collection
		// Example row: collectionType - Instrument, collectionName - Instrument_<instrument_type>
		// instrument_id, value = (user metadata)
		// instrument_type, value = (user metadata)
		String instrumentType = getInstrumentType(object);
		String instrumentCollectionPath = experimentCollectionPath + "/Instrument_" + instrumentType;
		instrumentCollectionPath = instrumentCollectionPath.replace(" ", "_");
		HpcBulkMetadataEntry pathEntriesInstrument = new HpcBulkMetadataEntry();
		pathEntriesInstrument.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Instrument"));
		pathEntriesInstrument.getPathMetadataEntries().add(createPathEntry("instrument_type", instrumentType));
		pathEntriesInstrument.getPathMetadataEntries().add(createPathEntry("instrument_id", getInstrumentId(instrumentType)));
		pathEntriesInstrument.setPath(instrumentCollectionPath);
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesInstrument);
		
		// Add collection type "Raw_Data" or "Processed_Data"
		String dataPath = instrumentCollectionPath + (object.getOriginalFilePath().contains("Raw")? "/Data_Raw" : "/Data_Processed");
		HpcBulkMetadataEntry pathEntriesData = new HpcBulkMetadataEntry();
		pathEntriesData.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, (object.getOriginalFilePath().contains("Raw")? "Raw_Data" : "Processed_Data")));
		pathEntriesData.setPath(dataPath);
		hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesData);
		
		// Set it to dataObjectRegistrationRequestDTO
		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();
		dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
		dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
		dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);

		// Add object metadata
		// key = object_name (derived)
		// key = file_type (derived)
		String fileName = Paths.get(object.getOriginalFilePath()).toFile().getName();
		String fileType = StringUtils.substringBefore(fileName, ".gz");
		fileType = fileType.substring(fileType.lastIndexOf('.') + 1);
		dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("object_name", fileName));
		dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("file_type", fileType));
		dataObjectRegistrationRequestDTO.getMetadataEntries()
				.add(createPathEntry("source_path", object.getOriginalFilePath()));
		logger.info("DCTD OCCPR PCL custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
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

	private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {
		String projectCollectionName = null;
		// Example: If originalFilePath is
		// /mnt/OCCPR_ACL_Archive/0_Data_archive/DCTD_PCL_Tara_Hiltke/TargetedProteomics_Chelsea Boo
		// then return the project folder TargetedProteomics_Chelsea Boo
		projectCollectionName = getCollectionNameFromParent(object, getCollectionNameFromParent(object, "DCTD_PCL_Tara_Hiltke"));
		projectCollectionName = projectCollectionName.replace(" ", "_");
		logger.info("projectCollectionName: {}", projectCollectionName);
		return projectCollectionName;
	}

	private String getPiCollectionName(StatusInfo object) throws DmeSyncMappingException {
		String piCollectionName = null;
		piCollectionName = getAttrValueWithKey(getProjectCollectionName(object), "data_owner");
		logger.info("PI Collection Name: {}", piCollectionName);
		if(StringUtils.isEmpty(piCollectionName.substring(piCollectionName.length()-1)));
			piCollectionName = piCollectionName.substring(0, piCollectionName.length()-1);
		return piCollectionName;
	}
	
	private String getPOCCollectionName(StatusInfo object) throws DmeSyncMappingException {
		String pocCollectionName = null;
		pocCollectionName = getCollectionNameFromParent(object, "DCTD_PCL_Tara_Hiltke");
		logger.info("PI Collection Name: {}", pocCollectionName);
		return pocCollectionName;
	}
	
	private String getResearcher(StatusInfo object) throws DmeSyncMappingException {
		String researcher = null;
		researcher = getAttrValueWithKey(getProjectCollectionName(object), "researcher");
		logger.info("Researcher Name: {}", researcher);
		return researcher;
	}

	private String getProjectId(StatusInfo object) throws DmeSyncMappingException {
		String projectId = null;
		projectId = getAttrValueWithKey(getProjectCollectionName(object), "project_id");
		logger.info("Project ID: {}", projectId);
		return projectId;
	}

	private String getExperimentType(StatusInfo object) throws DmeSyncMappingException {
		String experimentType = null;
		experimentType = getAttrValueWithKey(getProjectCollectionName(object), "experiment_type");
		logger.info("Experiment Type: {}", experimentType);
		return experimentType;
	}
	
	private String getProjectDate(StatusInfo object, String attributeName) throws DmeSyncMappingException {
		String projectStartDate = null;
		projectStartDate = getAttrValueWithKey(getProjectCollectionName(object), attributeName);
		if(projectStartDate.contains("/")) {
			  DateFormat outputFormatter = new SimpleDateFormat("yyyy-MM-dd");
			  SimpleDateFormat inputFormatter = new SimpleDateFormat("MM/dd/yy");
			  Date date = null;
			  try {
					date = inputFormatter.parse(projectStartDate);
			  } catch (ParseException e) {
					throw new DmeSyncMappingException(e);
			  }
			  projectStartDate = outputFormatter.format(date);
		  }
		logger.info("Project Start Date: {}", projectStartDate);
		return projectStartDate;
	}
	
	private String getInstrumentType(StatusInfo object) throws DmeSyncMappingException {
		String instrumentType = null;
		instrumentType = getAttrValueWithKey(getProjectCollectionName(object), "instrument_type");
		if (StringUtils.containsIgnoreCase(instrumentType, "Altis")) {
			instrumentType = "Altis";
		} else if (StringUtils.containsIgnoreCase(instrumentType, "Sciex")) {
			instrumentType = "Sciex";
		} else if (StringUtils.containsIgnoreCase(instrumentType, "Eclipse")) {
			instrumentType = "Eclipse";
		}
		return instrumentType;
	}
	
	private String getInstrumentId(String instrumentType) throws DmeSyncMappingException {
		String instrumentId = null;
		if (StringUtils.equals(instrumentType, "Altis")) {
			instrumentId = "TSQ-A-10869/C141935";
		} else if (StringUtils.equals(instrumentType, "Sciex")) {
			instrumentId = "FA222292203/C142401";
		} else if (StringUtils.equals(instrumentType, "Eclipse")) {
			instrumentId = "FSN40342/C141597";
		}
		return instrumentId;
	}

}
