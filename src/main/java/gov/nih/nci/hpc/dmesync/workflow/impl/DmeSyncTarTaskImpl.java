package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.util.ExcelUtil;
import gov.nih.nci.hpc.dmesync.util.TarUtil;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncTask;

/**
 * DME Sync Tar Task Implementation
 * 
 * @author dinhys
 */
@Component
public class DmeSyncTarTaskImpl extends AbstractDmeSyncTask implements DmeSyncTask {

  @Value("${dmesync.compress:false}")
  private boolean compress;

  @Value("${dmesync.source.base.dir}")
  private String syncBaseDir;

  @Value("${dmesync.work.base.dir}")
  private String syncWorkDir;

  @Value("${dmesync.dryrun:false}")
  private boolean dryRun;
  
  @Value("${dmesync.tar.exclude.folder:}")
  private String excludeFolder;
  
  @Value("${dmesync.file.tar:false}")
  private boolean tarIndividualFiles;
  
  @Value("${dmesync.tar.filename.excel.exist:false}")
  private boolean tarNameinExcelFile;
  
  @Value("${dmesync.additional.metadata.excel:}")
  private String metadataFile;
  
  @PostConstruct
  public boolean init() {
    super.setTaskName("TarTask");
    if (tarIndividualFiles)
    	super.setCheckTaskForCompletion(false);
    return true;
  }
  
  protected static ThreadLocal<Map<String, Map<String, String>>> threadLocalMap = new ThreadLocal<Map<String, Map<String, String>>>() {
	    @Override
	    protected HashMap<String, Map<String, String>> initialValue() {
	        return new HashMap<>();
	    }
	  };
  
  @Override
  public StatusInfo process(StatusInfo object)
      throws DmeSyncMappingException, DmeSyncWorkflowException {
    
    //Task: Create tar file in work directory for processing
    try {
      object.setTarStartTimestamp(new Date());
      //Construct work dir path
      Path baseDirPath = Paths.get(syncBaseDir).toRealPath();
      Path workDirPath = Paths.get(syncWorkDir).toRealPath();
      Path sourceDirPath = Paths.get(object.getOriginalFilePath());
      Path relativePath = baseDirPath.relativize(sourceDirPath);
      String tarWorkDir = workDirPath.toString() + File.separatorChar + relativePath.toString();
      Path tarWorkDirPath = Paths.get(tarWorkDir);
      Files.createDirectories(tarWorkDirPath);
      
      String tarFileName;
      if (tarNameinExcelFile) {
    	  threadLocalMap.set(loadMetadataFile(metadataFile, "Path"));
  		  String path = FilenameUtils.separatorsToUnix(object.getOriginalFilePath());
    	  tarFileName = getAttrValueWithKey(path,"tar_name");
      }else {
       tarFileName = object.getOrginalFileName() + ".tar";
      }
      String tarFile = tarWorkDir + File.separatorChar + tarFileName;
      tarFile = Paths.get(tarFile).normalize().toString();
      File directory = new File(object.getOriginalFilePath());

      logger.info("[{}] Creating tar file in {}", super.getTaskName(), tarFile);
      
      List<String> excludeFolders =
          excludeFolder == null || excludeFolder.isEmpty()
              ? null
              : new ArrayList<>(Arrays.asList(excludeFolder.split(",")));
      
      //Check directory permission
      if (!Files.isReadable(Paths.get(object.getOriginalFilePath()))) {
    	  throw new Exception("No Read permission to " + object.getOriginalFilePath());
      }
      if (compress) {
        tarFile = tarFile + ".gz";
        tarFileName = tarFileName + ".gz";
        if (!dryRun) {
          TarUtil.targz(tarFile, excludeFolders, directory);
        }
      } else {
        if (!dryRun) {
          TarUtil.tar(tarFile, excludeFolders, directory);
        }
      }

      // Update the record for upload
      File createdTarFile = new File(tarFile);
      object.setFilesize(createdTarFile.length());
      object.setSourceFileName(tarFileName);
      object.setSourceFilePath(tarFile);
      object.setTarEndTimestamp(new Date());
      object = dmeSyncWorkflowService.getService(access).saveStatusInfo(object);

    } catch (Exception e) {
      logger.error("[{}] error {}", super.getTaskName(), e.getMessage(), e);
      throw new DmeSyncWorkflowException("Error occurred during tar. " + e.getMessage(), e);
    }finally {
    	threadLocalMap.remove();
    }

    return object;
  }
  
	public Map<String, Map<String, String>> loadMetadataFile(String metadataFile, String key)
			throws DmeSyncMappingException {
		return ExcelUtil.parseBulkMetadataEntries(metadataFile, key);
	}

	public String getAttrValueWithKey(String rowKey, String attrKey) throws Exception {
		String key = null;
		if (threadLocalMap.get() == null)
			return null;
		for (String partialKey : threadLocalMap.get().keySet()) {
			if (StringUtils.contains(rowKey, partialKey)) {
				key = partialKey;
				break;
			}
		}
		if (StringUtils.isEmpty(key)) {
			logger.error("Excel mapping not found for {}", rowKey);
			throw new Exception("Excel mapping not found for {} " + rowKey);
		}
		return (threadLocalMap.get().get(key) == null ? null : threadLocalMap.get().get(key).get(attrKey));
	}

}
