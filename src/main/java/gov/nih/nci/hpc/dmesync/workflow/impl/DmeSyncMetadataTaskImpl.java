package gov.nih.nci.hpc.dmesync.workflow.impl;

import javax.annotation.PostConstruct;
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
      object.setFullDestinationPath(archivePath);
      //Save Archive Path in DB
      saveArchivePath(object, archivePath);

      HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO =
          metadataTask.getMetaDataJson(object);
      object.setDataObjectRegistrationRequestDTO(dataObjectRegistrationRequestDTO);

      //Save Metadata Info in DB
      saveMetaDataInfo(object, dataObjectRegistrationRequestDTO);

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
