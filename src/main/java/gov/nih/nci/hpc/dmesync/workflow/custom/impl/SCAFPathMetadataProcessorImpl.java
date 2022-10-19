package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.nio.file.Path;
import java.nio.file.Paths;
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
 * @author dinhys
 */
@Service("scaf")
public class SCAFPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
		implements DmeSyncPathMetadataProcessor {

	// SCAF DME path construction and meta data creation

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
		String sampleName = getSampleName(object);

		archivePath = destinationBaseDir 
				+ "/PI_" + getPiCollectionName(object) 
				+ "/Project_"
				+ getProjectCollectionName(object) 
				+ (isAggregatedDatasets(object) ? "" : "/Patient_" 
				+ getPatientCollectionName(object)
				+ "/Sample_" 
				+ getSampleName(object))
				+ "/"
				+ getSampleName(object) + "_" + fileName;

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
		String fileName = getSampleName(object) + "_" + Paths.get(object.getSourceFileName()).toFile().getName();

		// Add path metadata entries for "PI_XXX" collection
		// Example row: collectionType - PI, collectionName - XXX (derived)
		// key = data_owner, value = Tim Greten (supplied)
		// key = data_owner_affiliation, value = Thoracic and GI Malignancies Branch (supplied)
		String piCollectionName = getPiCollectionName(object);
		String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;
		HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
		pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "PI_Lab"));
		pathEntriesPI.setPath(piCollectionPath);
		hpcBulkMetadataEntries.getPathsMetadataEntries()
				.add(populateStoredMetadataEntries(pathEntriesPI, "PI_Lab", piCollectionName, "scaf"));

		// Add path metadata entries for "Project" collection
		// Example row: collectionType - Project, collectionName - CS027118
		String projectCollectionName = getProjectCollectionName(object);
		String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;
		HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_id", projectCollectionName));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Controlled Access"));
		pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_status", "Active"));
		pathEntriesProject.setPath(projectCollectionPath);
		hpcBulkMetadataEntries.getPathsMetadataEntries()
				.add(populateStoredMetadataEntries(pathEntriesProject, "Project", projectCollectionName, "scaf"));

		if(!isAggregatedDatasets(object)) {
			// Add path metadata entries for "Patient" collection
			// Example row: collectionType - Patient
			String patientCollectionName = getPatientCollectionName(object);
			String patientCollectionPath = projectCollectionPath + "/Patient_" + patientCollectionName;
			String scafId = getSCAFNumber(object);
			HpcBulkMetadataEntry pathEntriesPatient = new HpcBulkMetadataEntry();
			pathEntriesPatient.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Patient"));
			pathEntriesPatient.getPathMetadataEntries().add(createPathEntry("patient_id", patientCollectionName));
			pathEntriesPatient.getPathMetadataEntries().add(createPathEntry("age", getAttrWithKey(scafId, "age")));
			pathEntriesPatient.getPathMetadataEntries().add(createPathEntry("gender", getAttrWithKey(scafId, "gender")));
			pathEntriesPatient.getPathMetadataEntries().add(createPathEntry("race", getAttrWithKey(scafId, "race")));
			pathEntriesPatient.getPathMetadataEntries().add(createPathEntry("disease_type", getAttrWithKey(scafId, "disease_type")));
			pathEntriesPatient.getPathMetadataEntries().add(createPathEntry("primary_site", getAttrWithKey(scafId, "primary_site")));
			pathEntriesPatient.setPath(patientCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPatient);
						
			// Add path metadata entries for "Sample" collection
			// Example row: collectionType - Sample
			String sampleCollectionName = getSampleName(object);
			String sampleCollectionPath = patientCollectionPath + "/Sample_" + sampleCollectionName;
			HpcBulkMetadataEntry pathEntriesSample = new HpcBulkMetadataEntry();
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Sample"));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sample_name", sampleCollectionName));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("scaf_number", scafId));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sample_id", getAttrWithKey(scafId, "New sample ID")));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("library_strategy", "SingleCellRNA-Seq"));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("analyte_type", "RNA"));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("tissue", getAttrWithKey(scafId, "primary_site")));
			pathEntriesSample.getPathMetadataEntries().add(createPathEntry("tissue_type", getAttrWithKey(scafId, "Location")));
			pathEntriesSample.setPath(sampleCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesSample);
		}
		
		// Set it to dataObjectRegistrationRequestDTO
		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();
		dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
		dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
		dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);

		// Add object metadata
		// key = object_name, value = SCAF1535_CD45TumRim_outs.tar
		// (derived)
		dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("object_name", fileName));
		dataObjectRegistrationRequestDTO.getMetadataEntries()
				.add(createPathEntry("source_path", object.getOriginalFilePath()));
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

	private String getPiCollectionName(StatusInfo object) throws DmeSyncMappingException {
		String piCollectionName = null;
		piCollectionName = getCollectionMappingValue("SCAF", "PI_Lab", "scaf");

		logger.info("PI Collection Name: {}", piCollectionName);
		return piCollectionName;
	}

	private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {
		// Example: If originalFilePath is
		// /SCAF/CS027118/02_PrimaryAnalysisOutput/00_FullCellrangerOutputs/GEX/SCAF1536_CD45TumCor/outs
		// then return CS027118
		return getCollectionNameFromParent(object, "SCAF");
	}

	private String getSampleName(StatusInfo object) throws DmeSyncMappingException {
		String path = Paths.get(object.getOriginalFilePath()).toString();
		String sampleName = null;
		// Example: If originalFilePath is
		// /SCAF/CS027118/02_PrimaryAnalysisOutput/00_FullCellrangerOutputs/GEX/SCAF1536_CD45TumCor/outs
		// then return SCAF1536_CD45TumCor
		if (path.contains("GEX")) {
			sampleName = getCollectionNameFromParent(object, "GEX");
		} else {
			sampleName = getCollectionNameFromParent(object, "TCR");
		}
		return sampleName;
	}

	private boolean isAggregatedDatasets(StatusInfo object) throws DmeSyncMappingException {
		return StringUtils.equals(getSampleName(object), "AggregatedDatasets");
	}
	
	private String getSCAFNumber(StatusInfo object) throws DmeSyncMappingException {
		String sampleName = getSampleName(object);
		// Example: If sample name is SCAF1536_CD45TumCor or SCAF1536t_CD45TumCor
		// then return SCAF1536
		String scafNumber = StringUtils.substringBefore(sampleName, "t_");
		return StringUtils.substringBefore(scafNumber, "_");
	}

	private String getPatientCollectionName(StatusInfo object) throws DmeSyncMappingException {
		String scafId = getSCAFNumber(object);
		return getAttrWithKey(scafId, "Patient ID");
	}

	private String getAttrWithKey(String key, String attrKey) {
		if (StringUtils.isEmpty(key)) {
			logger.error("Excel mapping not found for {}", key);
			return null;
		}
		return (metadataMap.get(key) == null ? null : metadataMap.get(key).get(attrKey));
	}

	@PostConstruct
	private void init() {
		if ("scaf".equalsIgnoreCase(doc)) {
			try {
				metadataMap = ExcelUtil.parseBulkMetadataEntries(metadataFile, "SCAF_ID");
			} catch (DmeSyncMappingException e) {
				logger.error("Failed to initialize metadata  path metadata processor", e);
			}
		}
	}
}
