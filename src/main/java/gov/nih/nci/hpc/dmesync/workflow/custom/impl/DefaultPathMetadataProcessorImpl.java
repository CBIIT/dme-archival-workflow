package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncPathMetadataProcessor;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;

/**
 * Default DME Path and Meta-data Processor Implementation
 * 
 * @author dinhys
 *
 */
@Service("default")
public class DefaultPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
    implements DmeSyncPathMetadataProcessor {

  @Value("${dmesync.source.base.dir}")
  protected String sourceBaseDir;

  //DOC Default logic for DME path construction and meta data creation

  @Override
  public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {
    
    logger.info("[PathMetadataTask] Default getArchivePath called");

    // For now, all files goes directly to base destination dir, under the folders from source path.
    Path baseDirPath;
    try {
      baseDirPath = Paths.get(sourceBaseDir).toRealPath();
    } catch (IOException e) {
      throw new DmeSyncMappingException("source.base.dir does not exist: " + sourceBaseDir, e);
    }
    Path sourceDirPath = Paths.get(object.getOriginalFilePath());
    Path relativePath = baseDirPath.relativize(sourceDirPath);
    String relativePathStr = relativePath.toString().replace("\\", "/");
    
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
    
    return archivePath;
  }

  @Override
  public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
      throws DmeSyncMappingException, DmeSyncWorkflowException {
    
    logger.info("[PathMetadataTask] Default getMetaDataJson called");
    
    //Create default object with no metadata
    HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO =
        new HpcDataObjectRegistrationRequestDTO();
    dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
    dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(false);

    //Add object metadata
    dataObjectRegistrationRequestDTO.getMetadataEntries().add(createPathEntry("source_path", object.getOriginalFilePath()));
    
    return dataObjectRegistrationRequestDTO;
  }
}
