package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncPathMetadataProcessor;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntries;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;

/**
 * CMB DME Path and Meta-data Processor Implementation
 * 
 * @author dinhys
 *
 */
@Service("biobank")
public class BiobankPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
    implements DmeSyncPathMetadataProcessor {

  //CMB Custom logic for DME path construction and meta data creation
	
  @Override
  public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

    logger.info("[PathMetadataTask] Biobank getArchivePath called");
      
    //Get the PI collection name from the PI column from metadata file using path
    String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
   
    // extract the derived metadata from the excel that exists in the folder (assuming one excel)
    String metadataFile;
    try {
	  List<Path> mappingFiles = Files.list(Paths.get(object.getSourceFilePath()).getParent())
	    .filter(s -> s.toString().endsWith(".xlsx"))
	    .sorted(Collections.reverseOrder())
	    .collect(Collectors.toList());
    
      Iterator<Path> it = mappingFiles.iterator();
      if(it.hasNext()) {
        metadataFile = it.next().toString();
      } else {
        logger.error("Metadata excel file not found for {}", object.getOriginalFilePath());
        throw new DmeSyncMappingException("Metadata excel file not found for " + object.getOriginalFilePath());
      }
    } catch (IOException e) {
      logger.error("Metadata excel file not found for {}", object.getOriginalFilePath());
      throw new DmeSyncMappingException("Metadata excel file not found for " + object.getOriginalFilePath());
    }
    
    // load the id mapping file from the latest excel mapping file
    threadLocalMap.set(loadMetadataFile(metadataFile, "rave_spec_id"));
  
    String subfolderCollectionName = getSubCollectionName(object.getSourceFilePath());
    if(subfolderCollectionName.equals("Genomic_Reports") || subfolderCollectionName.equals("Molecular_Data")) {
  	  String raveSpecId = StringUtils.substring(fileName, 0, StringUtils.indexOf(fileName, "_"));
  	  String pubSpecId = getAttrValueWithExactKey(raveSpecId, "pub_spec_id");
  	  fileName = fileName.replace(raveSpecId, pubSpecId);
    }
    
    String archivePath =
        destinationBaseDir
            + "/PI_"
            + getPiCollectionName()
            + "/Project_"
            + getProjectCollectionName()
            + "/"
            + subfolderCollectionName
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

		  //Add to HpcBulkMetadataEntries for path attributes
		  HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();
		  
	      //Add path metadata entries for "PI_Helen_Moore" collection
		  //Example row: collectionType - PI_Lab, collectionName - Helen_Moore (supplied)
		  //key = pi_name, value = ? (supplied)
		  //key = affiliation, value = ? (supplied)
	     
	      String piCollectionName = getPiCollectionName();
	      String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName.replace(" ", "_");
	      HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
	      pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "PI_Lab"));
	      pathEntriesPI.setPath(piCollectionPath);
	      hpcBulkMetadataEntries.getPathsMetadataEntries().add(populateStoredMetadataEntries(pathEntriesPI, "PI_Lab", piCollectionName));
	      
	      //Add path metadata entries for "Project_XXX" collection
		  //Example row: collectionType - Project, collectionName - CMB (supplied)
		  //key = project_title, value = ? (supplied)
	      //key = access, value = "Controlled Access" (constant)
	      //key = project_status, value = ? (supplied)
		  //key = start_date, value = (supplied)
		  //key = summary_of_samples, value = (supplied)
	      //key = origin, value = (supplied)
	      //key = description, value = (supplied)
	      //key = method, value = (supplied)
	     
	      String projectCollectionName = getProjectCollectionName();
	      String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;
	      HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
	      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Controlled Access"));
	      pathEntriesProject.setPath(projectCollectionPath);
	      hpcBulkMetadataEntries.getPathsMetadataEntries().add(populateStoredMetadataEntries(pathEntriesProject, "Project", projectCollectionName));
	 
	      
	      //Add path metadata entries for "Genomic_Reports" or "Molecular_Data" collection
	      //Example row: collectionType - Report or Molecular, collectionName - Genomic_Reports (derived)
		  //key = description, value = (supplied)
	      String subfolderCollectionName = getSubCollectionName(object.getSourceFilePath());
	      String subfolderCollectionPath = projectCollectionPath + "/" + subfolderCollectionName;
	      if(!StringUtils.contains(subfolderCollectionName, "Clinical")) {
		      String subfolderCollectionType = StringUtils.contains(subfolderCollectionName, "Molecular") ? "Molecular" : "Report";
		      HpcBulkMetadataEntry pathEntriesSubFolder = new HpcBulkMetadataEntry();
		      pathEntriesSubFolder.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, subfolderCollectionType));
		      pathEntriesSubFolder.setPath(subfolderCollectionPath);
		      hpcBulkMetadataEntries.getPathsMetadataEntries().add(populateStoredMetadataEntries(pathEntriesSubFolder, subfolderCollectionType, subfolderCollectionName));
	      } else {
	    	  Path subfolderPath = Paths.get(subfolderCollectionPath);
		      HpcBulkMetadataEntry pathEntriesRun = new HpcBulkMetadataEntry();
		      pathEntriesRun.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Run"));
		      pathEntriesRun.getPathMetadataEntries().add(createPathEntry("run_date", subfolderPath.getFileName().toString()));
		      pathEntriesRun.setPath(subfolderCollectionPath.replace("\\", "/"));
		      
		      HpcBulkMetadataEntry pathEntriesDatabase = new HpcBulkMetadataEntry();
		      pathEntriesDatabase.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Database"));
		      pathEntriesDatabase.setPath(subfolderPath.getParent().toString().replace("\\", "/"));
		      
		      HpcBulkMetadataEntry pathEntriesClinical = new HpcBulkMetadataEntry();
		      pathEntriesClinical.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Clinical"));
		      pathEntriesClinical.setPath(subfolderPath.getParent().getParent().toString().replace("\\", "/"));
		      hpcBulkMetadataEntries.getPathsMetadataEntries().add(populateStoredMetadataEntries(pathEntriesClinical, "Clinical", subfolderPath.getParent().getParent().getFileName().toString()));
		      hpcBulkMetadataEntries.getPathsMetadataEntries().add(populateStoredMetadataEntries(pathEntriesDatabase, "Database", subfolderPath.getParent().getFileName().toString()));
		      hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesRun);
		      
	      }
	      
	      //Set it to dataObjectRegistrationRequestDTO
	      dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
	      dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
	      dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);
	
	      //Add object metadata
	      String fileName = Paths.get(object.getSourceFilePath()).toFile().getName();
	      if(subfolderCollectionName.equals("Genomic_Reports") || subfolderCollectionName.equals("Molecular_Data")) {
	    	  String raveSpecId = StringUtils.substring(fileName, 0, StringUtils.indexOf(fileName, "_"));
	    	  String pubId = getAttrValueWithExactKey(raveSpecId, "pub_id");
	    	  String pubSpecId = getAttrValueWithExactKey(raveSpecId, "pub_spec_id");
		      dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("patient_id", pubId));
		      dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("report_id", pubSpecId + "_" + getReportDate(fileName)));
	      } else {
	    	  dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("object_name", fileName));
	      }
  } finally {
	threadLocalMap.remove();
  }
      logger.info(
        "Biobank custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
      return dataObjectRegistrationRequestDTO;
  }


  private String getPiCollectionName() {
	  return "Helen_Moore";
  }
  
  
  private String getProjectCollectionName() {
	  return "CMB";
  }
	  
  private String getSubCollectionName(String path) {
	  if(StringUtils.endsWith(path, "vcf"))
		  return "Molecular_Data";
	  if(StringUtils.endsWith(path, "pdf"))
		  return "Genomic_Reports";
	  //entity_ids.20210516.xlsx
	  return "Clinical_Data/RAVE/" + getRunDate(Paths.get(path).getFileName().toString());
  }

  private String getRunDate(String fileName) {
    return StringUtils.substring(fileName, fileName.indexOf('.') + 1, fileName.lastIndexOf('.'));
  }
  
  private String getReportDate(String fileName) {
    return StringUtils.substring(fileName, fileName.indexOf("Non-Filtered") + 13, fileName.indexOf("Non-Filtered") + 23);
  }
}
