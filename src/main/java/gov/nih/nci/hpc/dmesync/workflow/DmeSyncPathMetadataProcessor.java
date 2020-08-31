package gov.nih.nci.hpc.dmesync.workflow;

import java.io.File;
import java.io.IOException;
import java.util.List;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.domain.metadata.HpcMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;

/**
 * DME Sync DME Path and Meta-data Processor Interface
 *
 * @author dinhys
 */
public interface DmeSyncPathMetadataProcessor {

  /**
   * Get the DME Archival path
   *
   * @param object StatusInfo object
   * @return archival path
   * @throws DmeSyncMappingException on mapping error
   * @throws IOException on IO Error
   */
  String getArchivePath(StatusInfo object) throws DmeSyncMappingException, IOException;

  /**
   * Gets the collection and data object meta-data
   *
   * @param object StatusInfo object
   * @return HpcDataObjectRegistrationRequestDTO
   * @throws DmeSyncMappingException on mapping error
   * @throws DmeSyncWorkflowException on system error
   * @throws IOException on IO Error
   */
  HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
      throws DmeSyncMappingException, DmeSyncWorkflowException, IOException;

  /**
   * @param dataObjectFile File to extract metadata from
   * @return list of HpcMetadataEntry
   * @throws DmeSyncMappingException on error
   */
  List<HpcMetadataEntry> extractMetadataFromFile(File dataObjectFile)
      throws DmeSyncMappingException;
}
