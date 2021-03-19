package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.nio.file.Path;
import java.nio.file.Paths;

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
 * UOB DME Path and Meta-data Processor Implementation
 * 
 * @author dinhys
 *
 */
@Service("uob")
public class UOBPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
    implements DmeSyncPathMetadataProcessor {

  //UOB Custom logic for DME path construction and meta data creation

@Value("${dmesync.additional.metadata.excel:}")
  private String metadataFile;
	
  @Override
  public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

    logger.info("[PathMetadataTask] UOBgetArchivePath called");

    // load the user metadata from the externally placed excel
    threadLocalMap.set(loadMetadataFile(metadataFile, "File name ID"));

    //extract the user name from the source Path
    //Example source path - /data/UOB_genomics/rawdata/ccbr769_RNAseq/mRNA/1030-Linehan-15/1030-Linehan-15.R1.fastq.gz
    String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
        
    //Get the PI collection value from the CollectionNameMetadata
    //Example row - mapKey - UOB_genomics, collectionType - PI, mapValue - Mary Carrington
    //Extract the Project Name from the excel

    //Extract SampleId from excel UOK208
    String archivePath =
        destinationBaseDir
            + "/PI_"
            + getPiCollectionName()
            + "/Project_"
            + getProjectName(object)
            + "/Sample_"
            + getSampleId(object)
            + "/"
            + fileName;
    
    //replace spaces with underscore
    archivePath = archivePath.replace(" ", "_");
    
    logger.info("Archive path for {} : {}", object.getOriginalFilePath(), archivePath);

    return archivePath;
  }
  

  @Override
  public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object) throws DmeSyncMappingException, DmeSyncWorkflowException {

	  
      HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();
	  try {
		  String path = object.getOriginalFilePath();
	      

		  //Add to HpcBulkMetadataEntries for path attributes
		  HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();
		  
		    
	      //Add path metadata entries for "PI_XXX" collection
		  //Example row: collectionType - PI, collectionName - UOB_genomics (derived)
		  //key = pi_name, value = Mary Carrington (supplied)
		  //key = affiliation, value = ? (supplied)
	     
	      String piCollectionName = getPiCollectionName();
	      String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;
	      HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
	      pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "PI_Lab"));
	      pathEntriesPI.setPath(piCollectionPath);
	      hpcBulkMetadataEntries.getPathsMetadataEntries().add(populateStoredMetadataEntries(pathEntriesPI, "PI_Lab", piCollectionName));
	      
	      //Add path metadata entries for "Project_XXX" collection
		  //Example row: collectionType - Project, collectionName - RNAseq_40_cell_lines (extracted), 
		  //key = project_title, value = RNAseq 40 cell lines (extracted)
	      //key = access, value = "Closed Access" (constant)
	      //key = method, value = "CryoEM" (constant)
		  //key = start_date, value = (extracted)
		  //key = description, value = (extracted)
	      //key = origin, value = (extracted)
	      //key = summary_of_datasets, value = (extracted)
	      //key = organism, value = (extracted)
	     
	      String projectCollectionName = getProjectName(object);
	      projectCollectionName = projectCollectionName.replace(" ", "_");
	      String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;
	      HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("project_title", getProjectName(object)));
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Closed Access"));
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("method", "Placeholder for method"));
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("start_date", "Placeholder for start_date"));
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("description", "Placeholder for description"));
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("origin", getAttrValueWithKey(path, "Tissue_of_origin")));
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("summary_of_datasets", "Placeholder for summary_of_datasets"));
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("organism", "Placeholder for organism"));
	      pathEntriesProject.setPath(projectCollectionPath);
	      hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesProject);
	 
	      
	      //Add path metadata entries for "Sample_XXX" collection
	      //Example row: collectionType - Sample, collectionName - UOK208 (extracted)
	      String sampleId = getSampleId(object);
	      String sampleCollectionPath = projectCollectionPath + "/Sample_" + sampleId;
	      HpcBulkMetadataEntry pathEntriesSample = new HpcBulkMetadataEntry();
	      pathEntriesSample.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Sample"));
	      pathEntriesSample.getPathMetadataEntries().add(createPathEntry("sample_id", sampleId));
	      pathEntriesSample.getPathMetadataEntries().add(createPathEntry("gender", getAttrValueWithKey(path, "Gender")));
	      pathEntriesSample.getPathMetadataEntries().add(createPathEntry("age_at_procurement", getAttrValueWithKey(path, "Age_at_procurement")));
	      pathEntriesSample.getPathMetadataEntries().add(createPathEntry("race", getAttrValueWithKey(path, "Race")));
	      pathEntriesSample.getPathMetadataEntries().add(createPathEntry("histology", getAttrValueWithKey(path, "Histology")));
	      pathEntriesSample.getPathMetadataEntries().add(createPathEntry("diagnosis", getAttrValueWithKey(path, "Diagnosis")));
	      pathEntriesSample.getPathMetadataEntries().add(createPathEntry("family_id", getAttrValueWithKey(path, "Family_ID")));
	      pathEntriesSample.getPathMetadataEntries().add(createPathEntry("specimen_type", getAttrValueWithKey(path, "Specimen_Type")));
	      pathEntriesSample.getPathMetadataEntries().add(createPathEntry("tissue_of_origin", getAttrValueWithKey(path, "Tissue_of_origin")));
	      pathEntriesSample.getPathMetadataEntries().add(createPathEntry("familial_sporadic", getAttrValueWithKey(path, "Familial_Sporadic")));
	      pathEntriesSample.setPath(sampleCollectionPath);
	      hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesSample);
			     
	      
	      //Set it to dataObjectRegistrationRequestDTO
	      dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
	      dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
	      dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);
	
	      //Add object metadata
	      dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("object_name", Paths.get(object.getSourceFilePath()).toFile().getName()));
	      dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("source_path", object.getOriginalFilePath()));
	      
  } finally {
	threadLocalMap.remove();
  }
      logger.info(
        "UOB custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
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

  private String getPiCollectionName() throws DmeSyncMappingException {
	  String piCollectionName = null;
	  //Example: If originalFilePath is /data/UOB_genomics/rawdata/ccbr769_RNAseq/mRNA/1030-Linehan-15/1030-Linehan-15.R1.fastq.gz
	  //then the piName will be UOB_genomics
	  String piDirName = "UOB_genomics";
	  logger.info("PI Directory Name: {}", piDirName);
	
	  piCollectionName = getCollectionMappingValue(piDirName, "PI_Lab");
	
	  logger.info("PI Collection Name: {}", piCollectionName);
	  return piCollectionName;
	  
  }
  
  
  private String getProjectName(StatusInfo object) {
	  String projectName = null;
	  //Example: If originalFilePath is /data/UOB_genomics/rawdata/ccbr769_RNAseq/mRNA/1030-Linehan-15/1030-Linehan-15.R1.fastq.gz
	  //then the projectDirName will be RNAseq 40 cell lines from spreadsheet
	  String fileNameId = Paths.get(object.getSourceFilePath()).getParent().getFileName().toString();
	  logger.info("File name ID: {}", fileNameId);
	  projectName = getAttrValueWithKey(fileNameId, "Project_Name");
	  logger.info("projectName: {}", projectName);
    return projectName;
  }
  
  
  private String getSampleId(StatusInfo object) {
	  String sampleId = null;
	  //Example: If sourceFilePath is /data/UOB_genomics/rawdata/ccbr769_RNAseq/mRNA/1030-Linehan-15/1030-Linehan-15.R1.fastq.gz
	  //then the fileNameId will be 1030-Linehan-15, sampleId extracted from mapping file UOK208
	  String fileNameId = Paths.get(object.getSourceFilePath()).getParent().getFileName().toString();
	  logger.info("File name ID: {}", fileNameId);
	  sampleId = getAttrValueWithKey(fileNameId, "Sample ID");
	  
	  logger.info("sampleId: {}", sampleId);
    return sampleId;
  }
  

}
