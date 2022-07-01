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
import gov.nih.nci.hpc.dmesync.domain.MetadataInfo;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncPathMetadataProcessor;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncTask;
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

  @Value("${dmesync.doc.name:default}")
  private String doc;

  @Value("${dmesync.extract.metadata:false}")
  private boolean extractMetadata;
  
  @Value("${dmesync.extract.metadata.ext:}")
  private String extractMetadatafileTypes;
  
  @Value("${dmesync.filesystem.upload:false}")
  private boolean fileSystemUpload;
  
  @Value("${dmesync.move.processed.files:false}")
  private boolean moveProcessedFiles;
  
  @PostConstruct
  public boolean init() {
    super.setTaskName("PathMetadataTask");
    super.setCheckTaskForCompletion(false);
    return true;
  }
  
  @Override
  public StatusInfo process(StatusInfo object)
      throws DmeSyncMappingException, DmeSyncWorkflowException {

    try {
      DmeSyncPathMetadataProcessor metadataTask = metadataProcessorFactory.getService(doc);
      String archivePath = metadataTask.getArchivePath(object);
      if(moveProcessedFiles)
    	  object.setMoveDataObjectOrignalPath(object.getFullDestinationPath());
      object.setFullDestinationPath(archivePath);
      //Save Archive Path in DB
      saveArchivePath(object, archivePath);

      HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO =
          metadataTask.getMetaDataJson(object);
      object.setDataObjectRegistrationRequestDTO(dataObjectRegistrationRequestDTO);

      //If automated metadata extraction is turned on, extractMetadata
      String fileType = object.getOriginalFilePath().substring(object.getOriginalFilePath().lastIndexOf('.') + 1);
      boolean extractMetadataFromFile = extractMetadata;
      if(StringUtils.isNotBlank(fileType) && StringUtils.isNotBlank(extractMetadatafileTypes)) {
        List<String> extractMetadataFileTypeList = Arrays.asList(extractMetadatafileTypes.toLowerCase().split("\\s*,\\s*"));
        if(!extractMetadataFileTypeList.contains(fileType.toLowerCase()))
            extractMetadataFromFile=false;
      }
      if(extractMetadataFromFile) {
        //Extract metadata from file
        List<HpcMetadataEntry> extractedMetadataEntries = metadataTask.extractMetadataFromFile(new File(object.getOriginalFilePath()));
        dataObjectRegistrationRequestDTO.getExtractedMetadataEntries().addAll(extractedMetadataEntries);
      }
      //Save Metadata Info in DB
      saveMetaDataInfo(object, dataObjectRegistrationRequestDTO);

      //If file system upload, get the archive permission
      if (fileSystemUpload) {
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
    dmeSyncWorkflowService.saveStatusInfo(object);
    return object;
  }
  
  public void saveMetaDataInfo(StatusInfo object, HpcDataObjectRegistrationRequestDTO requestDto) {
    //Save Metadata entries
    for(HpcMetadataEntry entry: requestDto.getMetadataEntries()) {
      MetadataInfo metadataInfo = new MetadataInfo();
      metadataInfo.setObjectId(object.getId());
      metadataInfo.setMetaDataKey(entry.getAttribute());
      metadataInfo.setMetaDataValue(entry.getValue());
      dmeSyncWorkflowService.saveMetadataInfo(metadataInfo);
    }
    //Save Extracted Metadata entries
    for(HpcMetadataEntry entry: requestDto.getExtractedMetadataEntries()) {
      MetadataInfo metadataInfo = new MetadataInfo();
      metadataInfo.setObjectId(object.getId());
      metadataInfo.setMetaDataKey(entry.getAttribute());
      metadataInfo.setMetaDataValue(entry.getValue());
      dmeSyncWorkflowService.saveMetadataInfo(metadataInfo);
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
          dmeSyncWorkflowService.saveMetadataInfo(metadataInfo);
        }
      }
    }
  }
  
}
