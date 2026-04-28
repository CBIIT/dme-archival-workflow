package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import javax.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import gov.nih.nci.hpc.dmesync.DmeSyncPathMetadataProcessorFactory;
import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import gov.nih.nci.hpc.dmesync.domain.MetadataInfo;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncPathMetadataProcessor;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncTask;
import gov.nih.nci.hpc.dmesync.workflow.MessageService;
import gov.nih.nci.hpc.domain.error.HpcDomainValidationResult;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntries;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntry;
import gov.nih.nci.hpc.domain.metadata.HpcMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.HpcArchivePermissionsRequestDTO;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;

/**
 * DME Sync Path and Meta-data Task Implementation
 * 
 * @author dinhys
 */
@Component
public class DmeSyncMetadataTaskImpl extends AbstractDmeSyncTask implements DmeSyncTask {

  @Autowired private DmeSyncPathMetadataProcessorFactory metadataProcessorFactory;
  @Autowired private MessageService messageService;

  @PostConstruct
  public boolean init() {
    super.setTaskName("PathMetadataTask");
    super.setCheckTaskForCompletion(false);
    return true;
  }
  
  @Override
  public StatusInfo process(StatusInfo object, DocConfig config)
      throws DmeSyncMappingException, DmeSyncWorkflowException {

	  DocConfig.UploadConfig upload = config.getUploadConfig();
	  DocConfig.PreprocessingRule preRule = config.getPreprocessingRule();
	  
    try {
      DmeSyncPathMetadataProcessor metadataTask = metadataProcessorFactory.getService(config.getDocName());
      String archivePath = metadataTask.getArchivePath(object);
      if(upload.moveProcessedFiles)
    	  object.setMoveDataObjectOrignalPath(object.getFullDestinationPath());
      object.setFullDestinationPath(archivePath);
      //Save Archive Path in DB
      saveArchivePath(object, archivePath);

      HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO =
          metadataTask.getMetaDataJson(object);
      object.setDataObjectRegistrationRequestDTO(dataObjectRegistrationRequestDTO);

      //If automated metadata extraction is turned on, extractMetadata
      String fileType = object.getOriginalFilePath().substring(object.getOriginalFilePath().lastIndexOf('.') + 1);
      boolean extractMetadataFromFile = preRule.extractMetadata;
      if(StringUtils.isNotBlank(fileType) && StringUtils.isNotBlank(preRule.extractMetadataExt)) {
        List<String> extractMetadataFileTypeList = Arrays.asList(preRule.extractMetadataExt.toLowerCase().split("\\s*,\\s*"));
        if(!extractMetadataFileTypeList.contains(fileType.toLowerCase()))
            extractMetadataFromFile=false;
      }
      if(extractMetadataFromFile) {
        //Extract metadata from file
        List<HpcMetadataEntry> extractedMetadataEntries = metadataTask.extractMetadataFromFile(new File(object.getOriginalFilePath()));
        dataObjectRegistrationRequestDTO.getExtractedMetadataEntries().addAll(extractedMetadataEntries);
      }
      
      HpcBulkMetadataEntries  pathMetadataEntries = null;
      if (dataObjectRegistrationRequestDTO != null) {
    	  pathMetadataEntries = dataObjectRegistrationRequestDTO.getParentCollectionsBulkMetadataEntries() ;
      }
      
      if (pathMetadataEntries!=null && pathMetadataEntries.getPathsMetadataEntries()!= null) {
        HpcDomainValidationResult validationResult = isValidMetadataEntries(pathMetadataEntries.getPathsMetadataEntries(),false);
      
        if (!validationResult.getValid()) {
			if (StringUtils.isEmpty(validationResult.getMessage())) {
				logger.error("[{}] Validation Error while creating path and metadata:  {} ", super.getTaskName(), messageService.get("INVALID_METADATA_MSG"));
				throw new DmeSyncMappingException(messageService.get("INVALID_METADATA_MSG"));
			} else {
				logger.error("[{}] Validation Error while creating path and metadata:  {} ", super.getTaskName(), validationResult.getMessage());
				throw new DmeSyncMappingException(validationResult.getMessage());
			}
		} 
      }
      
      //Save Metadata Info in DB
      saveMetaDataInfo(object, dataObjectRegistrationRequestDTO);

      //If file system upload, get the archive permission
      if (upload.fileSystemUpload) {
        HpcArchivePermissionsRequestDTO archivePermissionsRequestDTO =
              metadataTask.getArchivePermission(object);
        object.setArchivePermissionsRequestDTO(archivePermissionsRequestDTO);
      }
          
    } catch (DmeSyncMappingException e) {
      throw e;
    } catch (Exception e) {
      logger.error("[{}] Error while creating path and metadata ", super.getTaskName(), e);
      throw new DmeSyncMappingException("Error while creating path and metadata", e);
    }
    
    return object;
  }
  
  public StatusInfo saveArchivePath(StatusInfo object, String archivePath) {
    object.setFullDestinationPath(archivePath);
    dmeSyncWorkflowService.getService(access).saveStatusInfo(object);
    return object;
  }
  
  public void saveMetaDataInfo(StatusInfo object, HpcDataObjectRegistrationRequestDTO requestDto) {
    //Save Metadata entries
    for(HpcMetadataEntry entry: requestDto.getMetadataEntries()) {
      MetadataInfo metadataInfo = new MetadataInfo();
      metadataInfo.setObjectId(object.getId());
      metadataInfo.setMetaDataKey(entry.getAttribute());
      metadataInfo.setMetaDataValue(entry.getValue());
      dmeSyncWorkflowService.getService(access).saveMetadataInfo(metadataInfo);
    }
    //Save Extracted Metadata entries
    for(HpcMetadataEntry entry: requestDto.getExtractedMetadataEntries()) {
      MetadataInfo metadataInfo = new MetadataInfo();
      metadataInfo.setObjectId(object.getId());
      metadataInfo.setMetaDataKey(entry.getAttribute());
      metadataInfo.setMetaDataValue(entry.getValue());
      dmeSyncWorkflowService.getService(access).saveMetadataInfo(metadataInfo);
    }
    //Save parent metadata entries
    HpcBulkMetadataEntries entries = requestDto.getParentCollectionsBulkMetadataEntries();
    if(entries != null) {
      for(HpcBulkMetadataEntry bulkEntry: entries.getPathsMetadataEntries()) {
        for(HpcMetadataEntry entry: bulkEntry.getPathMetadataEntries()) {
          MetadataInfo metadataInfo = new MetadataInfo();
          metadataInfo.setObjectId(object.getId());
          metadataInfo.setMetaDataKey(entry.getAttribute()); // Might need to append bulkEntry.getPath()
          metadataInfo.setMetaDataValue(entry.getValue());
          dmeSyncWorkflowService.getService(access).saveMetadataInfo(metadataInfo);
        }
      }
    }
  }
  
  /**
	 * Validate metadata entry collection.
	 *
	 * @param list Metadata entry collection.
	 * @param editMetadata    true if the metadata is being edited. This is to
	 *                        enable delete.
	 * @return true if valid, false otherwise.
	 * @throws DmeSyncMappingException 
	 */
	private HpcDomainValidationResult isValidMetadataEntries(List<HpcBulkMetadataEntry> bulkMetadataEntries,
			boolean editMetadata) throws DmeSyncMappingException {
		HpcDomainValidationResult validationResult = new HpcDomainValidationResult();
		validationResult.setValid(true);
		
		if (bulkMetadataEntries == null) {
 			validationResult.setValid(false);
 			validationResult.setMessage(messageService.get("EMPTY_COLLECTION_MSG"));
 			return validationResult;
 		}

		for (HpcBulkMetadataEntry bulkMetadataEntry : bulkMetadataEntries) {
			
			if (bulkMetadataEntry == null) {
 				validationResult.setValid(false);
 				validationResult.setMessage(messageService.get("EMPTY_BULK_METADATA_MSG"));
 				return validationResult;
 			}

			List<HpcMetadataEntry> list = bulkMetadataEntry.getPathMetadataEntries();
			if (list == null) {
				validationResult.setValid(false);
				validationResult.setMessage(messageService.get("EMPTY_PATH_METADATA_MSG"));
				return validationResult;
			}
			for (int i = 0; i < list.size(); i++) {
				HpcMetadataEntry metadataEntry = list.get(i);
				
				if (metadataEntry == null) {
 					validationResult.setValid(false);
 					validationResult.setMessage(messageService.get("NULL_METADATA_ENTRY_MSG"));
 					return validationResult;
 				}
				
				if (StringUtils.isEmpty(metadataEntry.getAttribute())) {
					validationResult.setValid(false);
					validationResult.setMessage(messageService.get("EMPTY_METADATA_MSG"));
					return validationResult;

				} else {
					if (editMetadata == false && StringUtils.isEmpty(metadataEntry.getValue())) {
						if (validationResult.getValid()) {
							validationResult.setMessage(
									"The following entries cannot be empty: " + metadataEntry.getAttribute());
						} else {
							validationResult
									.setMessage(validationResult.getMessage() + ", " + metadataEntry.getAttribute());
						}
						validationResult.setValid(false);
					} else {
						list.get(i).setAttribute(list.get(i).getAttribute().trim());
						list.get(i).setValue(StringUtils.isEmpty(metadataEntry.getValue()) ? metadataEntry.getValue()
								: list.get(i).getValue().trim());
					}
				}
			}
		}
		return validationResult;

	}
  
  
}
