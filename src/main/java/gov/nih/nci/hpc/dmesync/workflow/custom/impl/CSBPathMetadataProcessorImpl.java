package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

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
 * CSB DME Path and Meta-data Processor Implementation
 *
 * @author dinhys
 */
@Service("csb")
public class CSBPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
		implements DmeSyncPathMetadataProcessor {

	// CSB Custom logic for DME path construction and meta data creation

	@Value("${dmesync.additional.metadata.excel:}")
	private String mappingFile;
	
	@Value("${dmesync.source.base.dir}")
	private String sourceBaseDir;
	
	@Override
	public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

		logger.info("[PathMetadataTask] CSB getArchivePath called");

	    Path sourceDirPath = Paths.get(object.getOriginalFilePath());
        String dataSet = getCollectionNameFromParent(object, "CSB-CryoEM-raw");
	    if(dataSet.equals("arctica") || dataSet.equals("krios")) {
	    	dataSet = getCollectionNameFromParent(object, getCollectionNameFromParent(object,
					getCollectionNameFromParent(object, dataSet)));
	    }
        Path checkExistFilePath = Paths.get(StringUtils.substringBefore(sourceDirPath.toString(), dataSet) + dataSet);
		// load the metadata from the json file
		String metadataFile;
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(checkExistFilePath,
				path -> path.getFileName().toString().equals("archive.json"))) {
			Iterator<Path> it = stream.iterator();
			if (it.hasNext()) {
				metadataFile = it.next().toString();
			} else {
				logger.error("Metadata json file not found for {}", object.getOriginalFilePath());
				throw new DmeSyncMappingException("Metadata json file not found for " + object.getOriginalFilePath());
			}
		} catch (IOException e) {
			logger.error("Metadata json file not found for {}", object.getOriginalFilePath());
			throw new DmeSyncMappingException("Metadata json file not found for " + object.getOriginalFilePath());
		}

		threadLocalMap.set(loadJsonMetadataFile(metadataFile, "dataset"));

		String archivePath = destinationBaseDir + "/PI_" + getPiCollectionName() + "/Instrument_"
				+ getInstrumentCollectionName() + "/Date_" + getDateCollectionName() + "/Dataset_" + getDatasetName()
				+ StringUtils.substringAfter(sourceDirPath.toString(), dataSet)+ "/"+object.getSourceFileName();

		// replace spaces with underscore
		archivePath = archivePath.replace(" ", "_");
		archivePath = archivePath.replace("\\", "/");

		logger.info("Archive path for {} : {}", object.getOriginalFilePath(), archivePath);

		return archivePath;
	}

	@Override
	public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException {

		HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();
		try {

			// Add to HpcBulkMetadataEntries for path attributes
			HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

			// Add path metadata entries for "PI_XXX" collection
			// Example row: collectionType - PI, collectionName (derived)
			// key = data_owner, value = (derived)

			String piCollectionName = getPiCollectionName();
			String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;
			HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "DataOwner_Lab"));
			pathEntriesPI.getPathMetadataEntries().add(createPathEntry("data_owner", getAttrValueWithKey("dataset", "ownername")));
			pathEntriesPI.setPath(piCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesPI);

			// Add path metadata entries for "Instrument_XXX" collection
			// Example row: collectionType - Instrument, collectionName (derived)
			// key = microscope_name, value = (supplied)
			// key = microscope_serial, value = (supplied)
			// key = access, value = "Closed Access" (default)
			// key = project_status, value = Active (default)
			// key = retention_years, value = 7 (default)

			String instrumentCollectionName = getInstrumentCollectionName();
			String instrumentCollectionPath = piCollectionPath + "/Instrument_" + instrumentCollectionName;
			HpcBulkMetadataEntry pathEntriesInstrument = new HpcBulkMetadataEntry();
			pathEntriesInstrument.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Instrument"));
			pathEntriesInstrument.getPathMetadataEntries().add(createPathEntry("microscope_name", getAttrValueWithKey("dataset", "datasource")));
			pathEntriesInstrument.getPathMetadataEntries().add(createPathEntry("microscope_serial", getAttrValueWithKey("dataset", "datasourceserial")));
			pathEntriesInstrument.setPath(instrumentCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesInstrument);

			// Add path metadata entries for "Date_(Year_Month)" collection
			// Example row: collectionType - Date, collectionName (derived)
			// key = year_month, value = (supplied)

			String dateCollectionName = getDateCollectionName();
			String dateCollectionPath = instrumentCollectionPath + "/Date_" + dateCollectionName;
			HpcBulkMetadataEntry pathEntriesDate = new HpcBulkMetadataEntry();
			pathEntriesDate.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Date"));
			pathEntriesDate.getPathMetadataEntries().add(createPathEntry("year_month", dateCollectionName));
			pathEntriesDate.setPath(dateCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesDate);

			// Add path metadata entries for "Dataset_XXX" collection
			// Example row: collectionType - Dataset, collectionName (derived)
			// key = dataset_id, value = 2021_5_4_154630_Krios1_123456789 (supplied)
			// key = dataset_location, value = /data/location/dataset_X (supplied)
			// key = dataset_name, value = dataset_X (supplied)
			// key = dataset_owner, value = (supplied)
			// key = pi_collaborator, value = (optionally supplied)
			// key = scientist, value = (optionally supplied)
			String datasetCollectionName = getDatasetName();
			String datasetCollectionPath = dateCollectionPath + "/Dataset_" + datasetCollectionName;
			HpcBulkMetadataEntry pathEntriesDataset = new HpcBulkMetadataEntry();
			pathEntriesDataset.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Dataset"));
			pathEntriesDataset.getPathMetadataEntries()
					.add(createPathEntry("dataset_id", getAttrValueWithKey("dataset", "datasetid")));
			pathEntriesDataset.getPathMetadataEntries()
					.add(createPathEntry("dataset_location", getAttrValueWithKey("dataset", "datasetlocation")));
			pathEntriesDataset.getPathMetadataEntries()
					.add(createPathEntry("dataset_name", getAttrValueWithKey("dataset", "datasetname")));
			pathEntriesDataset.getPathMetadataEntries()
					.add(createPathEntry("dataset_owner", getAttrValueWithKey("dataset", "ownername")));
			if(StringUtils.isNotBlank(getAttrValueWithKey("dataset", "collaborator")))
				pathEntriesDataset.getPathMetadataEntries()
					.add(createPathEntry("pi_collaborator", getAttrValueWithKey("dataset", "collaborator")));
			if(StringUtils.isNotBlank(getAttrValueWithKey("dataset", "scientist")))
				pathEntriesDataset.getPathMetadataEntries()
					.add(createPathEntry("scientist", getAttrValueWithKey("dataset", "scientist")));
			pathEntriesDataset.setPath(datasetCollectionPath);
			hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesDataset);

			// Set it to dataObjectRegistrationRequestDTO
			dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
			dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
			dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);

			// Add object metadata
			dataObjectRegistrationRequestDTO.getMetadataEntries()
					.add(createPathEntry("object_name", Paths.get(object.getSourceFilePath()).toFile().getName()));
			dataObjectRegistrationRequestDTO.getMetadataEntries()
					.add(createPathEntry("source_path", object.getSourceFilePath()));

		} finally {
			threadLocalMap.remove();
		}
		logger.info("CSB custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
		return dataObjectRegistrationRequestDTO;
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
		String ownername = getAttrValueWithKey("dataset", "ownername");
		String ownerLastName = StringUtils.substringBefore(ownername, ",");
		String ownerFirstName = StringUtils.trim(StringUtils.substringAfter(ownername, ","));
		String piCollectionName = null;
		if(StringUtils.isNotBlank(ownerFirstName))
			piCollectionName = ownerFirstName + " " + ownerLastName;
		else
			piCollectionName = ownerLastName;
		return piCollectionName.replace(" ", "_");
	}

	// TODO: Revert these changes once testing is done
	/*private String getInstrumentCollectionName() {
		String instrumentCollectionName = getAttrValueWithKey("dataset", "datasource");
		instrumentCollectionName = instrumentCollectionName.replace(" ", "_");
		return instrumentCollectionName;
	}*/
	
	private String getInstrumentCollectionName() {
	String instrumentCollectionName = "Talos_Arctica_K1";
	instrumentCollectionName = instrumentCollectionName.replace(" ", "_");
	return instrumentCollectionName;
    }
	
	
	private String getDatasetName() {
		String datasetName = getAttrValueWithKey("dataset", "datasetname");
		logger.info("datasetName: {}", datasetName);
		return datasetName;
	}
	
	private String getDateCollectionName() {
		String datasetName = getAttrValueWithKey("dataset", "datasetname");
		String year = StringUtils.substring(datasetName, 0, 4);
		String month = StringUtils.substring(datasetName, 4, 6);
		logger.info("dateCollectionName: {}", year + "_" + month);
		return year + "_" + month;
	}
}