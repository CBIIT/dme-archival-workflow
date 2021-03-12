package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.StringTokenizer;

import org.springframework.beans.factory.annotation.Autowired;
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
 * DCEG CGR DME Path and Meta-data Processor Implementation
 * 
 * @author dinhys
 *
 */
@Service("dceg")
public class DCEGPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
    implements DmeSyncPathMetadataProcessor {

  //DCEG CGR Custom logic for DME path construction and meta data creation

  @Value("${dmesync.additional.metadata.excel:}")
  private String metadataFile;
	
  @Value("${dmesync.source.base.dir}")
  protected String sourceBaseDir;

  @Autowired private DCEGCollectionTypeMapper mapper;
  
  @Override
  public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

    logger.info("[PathMetadataTask] DCEG CGR getArchivePath called");
      
    // For dceg, take all folders name under sequencing as collections.
	Path relativePath = Paths.get(object.getOriginalFilePath()
				.substring(object.getOriginalFilePath().toLowerCase().indexOf("sequencing") + "sequencing/".length()));
	String relativePathStr = relativePath.toString().replace("\\", "/");
    String platformName = getPlatformName(object);
   
    String archivePath;
    if(!relativePathStr.contains("/")) {
      // File belongs directly under the destination dir
      archivePath = destinationBaseDir + "/" + object.getSourceFileName();
    }
    else if (relativePath.toString().endsWith(object.getSourceFileName())) {
      //Case when the file name is not altered.
      archivePath = destinationBaseDir + "/" + relativePathStr;
    } else {
      //Case when the filename in the destination changes.
      archivePath =
          destinationBaseDir
              + "/"
              + relativePath.getParent().toString().replace("\\", "/")
              + "/"
              + object.getSourceFileName().replace("\\", "/");
    }
    
    //replace spaces with underscore
    archivePath = archivePath.replaceAll(" ", "_");
    
    if(platformName.equalsIgnoreCase("PacBio")) {
    	//Repeat run folder name for both project and run collection
    	StringTokenizer tokenizer = new StringTokenizer(archivePath, "/");
    	String runFolderName = null;
    	for (int i = 0; i < 4; i++)
    		runFolderName = tokenizer.nextToken();
    	archivePath = archivePath.replace(runFolderName, runFolderName + "/" + runFolderName);
    } else if (platformName.startsWith("Illumina")) {
    	archivePath = archivePath.replace("Illumina/MiSeq", platformName);
    	archivePath = archivePath.replace("Illumina/HiSeq", platformName);
    }
    
    logger.info("Archive path for {} : {}", object.getOriginalFilePath(), archivePath);

    return archivePath;
  }
  

  private String getPlatformName(StatusInfo object) {
	  String platformName = getCollectionNameFromParent(object, "sequencing");
	  if (platformName.equals("Illumina")) {
	    String platformType = getCollectionNameFromParent(object, "Illumina");
	    platformName = platformName + "_" + platformType;
	  }
	  return platformName;
  }


@Override
  public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object) throws DmeSyncMappingException, DmeSyncWorkflowException {


      HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO = new HpcDataObjectRegistrationRequestDTO();

	  //Add to HpcBulkMetadataEntries for path attributes
	  HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();
	  
	    
      //Add path metadata entries for different platforms
	  String platformName = getPlatformName(object);
      List<String> collectionTypesList = null;
      if(platformName.equalsIgnoreCase("PacBio")) {
    	  collectionTypesList = mapper.getPacBioCollectonTypes();
      } else if (platformName.equalsIgnoreCase("Illumina_MiSeq")) { 
    	  collectionTypesList = mapper.getIlluminaMiSeqProjectCollectionTypes();
      } else if (platformName.equalsIgnoreCase("Illumina_HiSeq")) { 
    	  collectionTypesList = mapper.getIlluminaHiSeqProjectCollectionTypes();
      }
      
      //Tokenize DME path starting from platform
      StringTokenizer tokenizer = new StringTokenizer(object.getFullDestinationPath().substring(destinationBaseDir.length()+1), "/");
	  StringBuilder collectionPathBuilder = new StringBuilder();
	  collectionPathBuilder.append(destinationBaseDir);
	  boolean skipped = false;
	  int skipCount = 0;
	  String collectionName = null;
      for (String collectionType: collectionTypesList) {
    	  if(!skipped)
    	  	collectionName = tokenizer.nextToken();
    	  //Check if this is the last token which is the file name
    	  if(!tokenizer.hasMoreTokens())
    		  break;
    	  if(platformName.equals("Illumina_HiSeq") && collectionType.equals("Manifest") && !collectionName.toLowerCase().contains("manifest")) {
    		  skipped = true;
    		  skipCount++;
    		  continue;
    	  }
    	  skipped = false;
    	  collectionPathBuilder.append("/");
    	  collectionPathBuilder.append(collectionName);
          HpcBulkMetadataEntry pathEntry = new HpcBulkMetadataEntry();
          pathEntry.getPathMetadataEntries().add(createPathEntry("collection_type", collectionType));
          pathEntry.setPath(collectionPathBuilder.toString());
          hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntry);
      }
      if(collectionTypesList.size() - skipCount == hpcBulkMetadataEntries.getPathsMetadataEntries().size())
    	  tokenizer.nextToken();
      if(tokenizer.hasMoreTokens()) {
    	  throw new DmeSyncMappingException("There are extra sub-folders under path: " + object.getSourceFilePath());
      }
      
      //Set it to dataObjectRegistrationRequestDTO
      dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
      dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(hpcBulkMetadataEntries);

      //Add object metadata
      dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("object_name", Paths.get(object.getSourceFilePath()).toFile().getName()));
      dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("source_path", object.getOriginalFilePath()));

	  
      logger.info(
        "DCEG custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
      return dataObjectRegistrationRequestDTO;
  }

  private String getCollectionNameFromParent(StatusInfo object, String parentName) {
	  Path fullFilePath = Paths.get(object.getOriginalFilePath());
	  logger.info("Full File Path = " + fullFilePath);
	  int count = fullFilePath.getNameCount();
	  for (int i = 0; i <= count; i++) {
	    if (fullFilePath.getParent().getFileName().toString().equalsIgnoreCase(parentName)) {
	      return fullFilePath.getFileName().toString();
	    }
	    fullFilePath = fullFilePath.getParent();
	  }
    return null;
  }
}
