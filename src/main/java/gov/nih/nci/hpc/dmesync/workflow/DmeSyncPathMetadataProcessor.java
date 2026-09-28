package gov.nih.nci.hpc.dmesync.workflow;

import java.io.File;
import java.io.IOException;
import java.util.List;

import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.domain.metadata.HpcMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.HpcArchivePermissionsRequestDTO;
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
 * @throws DmeSyncWorkflowException 
   */
  String getArchivePath(StatusInfo object, DocConfig config) throws DmeSyncMappingException, IOException, DmeSyncWorkflowException;

  /**
   * Gets the collection and data object meta-data
   *
   * @param object StatusInfo object
   * @return HpcDataObjectRegistrationRequestDTO
   * @throws DmeSyncMappingException on mapping error
   * @throws DmeSyncWorkflowException on system error
   * @throws IOException on IO Error
   */
  HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object, DocConfig config)
      throws DmeSyncMappingException, DmeSyncWorkflowException, IOException;

  /**
   * @param dataObjectFile File to extract metadata from
   * @return list of HpcMetadataEntry
   * @throws DmeSyncMappingException on error
   */
  List<HpcMetadataEntry> extractMetadataFromFile(File dataObjectFile)
      throws DmeSyncMappingException;
  
  /**
   * Gets the archive permission object
   * @param object
   * @return
   * @throws DmeSyncMappingException
   * @throws DmeSyncWorkflowException
   * @throws IOException
   */
  default HpcArchivePermissionsRequestDTO getArchivePermission(StatusInfo object)
	      throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {
	    return null;
  }
  
  /**
   * Determines whether tarring should proceed for upload.
   * Default implementation returns true.
   * @param object
   * @return boolean true if metadata available or false 
   * @throws DmeSyncMappingException
   * @throws DmeSyncWorkflowException
   * 
   */
	default boolean isMetadataAvailable(StatusInfo object, DocConfig config)
			throws DmeSyncMappingException, DmeSyncWorkflowException {
		return true;
	}
}
