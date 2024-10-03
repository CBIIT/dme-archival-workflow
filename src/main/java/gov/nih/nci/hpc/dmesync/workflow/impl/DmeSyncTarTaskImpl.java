package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.apache.commons.io.FilenameUtils;
import gov.nih.nci.hpc.dmesync.util.ExcelUtil;

import gov.nih.nci.hpc.dmesync.DmeSyncMailServiceFactory;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.dto.DmeSyncMessageDto;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncStorageException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncVerificationException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.jms.DmeSyncProducer;
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

	@Value("${dmesync.multiple.tars.dir.folders:}")
	private String multpleTarsFolders;

	@Value("${dmesync.multiple.tars.files.count:0}")
	private Integer filesPerTar;

	@Value("${dmesync.verify.prev.upload:none}")
	private String verifyPrevUpload;

	@Value("${dmesync.multiple.tars.files.validation:true}")
	private boolean verifyTarFilesCount;

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

	protected static ThreadLocal<Map<String, Map<String, String>>> threadLocalMap=new ThreadLocal<Map<String,Map<String,String>>>(){@Override protected HashMap<String,Map<String,String>>initialValue(){return new HashMap<>();}};

	@Autowired
	private DmeSyncProducer sender;



	@Override
	public StatusInfo process(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException, DmeSyncStorageException {

		
		if(filesPerTar > 0  && object.getSourceFileName()!=null && StringUtils.contains(object.getSourceFileName(),"movies_TarContentsFile.txt")){
			// Skipping this task for the contents file for multiple Tars processing
			return object;
			
		}else {
			// Task: Create tar file in work directory for processing
		try {
			object.setTarStartTimestamp(new Date());
			// Construct work dir path
			Path baseDirPath = Paths.get(syncBaseDir).toRealPath();
			Path workDirPath = Paths.get(syncWorkDir).toRealPath();
			Path sourceDirPath = Paths.get(object.getOriginalFilePath());
			Path relativePath = baseDirPath.relativize(sourceDirPath);
			String tarWorkDir = workDirPath.toString() + File.separatorChar + relativePath.toString();
			Path tarWorkDirPath = Paths.get(tarWorkDir);
			
			synchronized (this) {
			Files.createDirectories(tarWorkDirPath);
			logger.info("[{}] Creating Tar work space directory {}", super.getTaskName(), tarWorkDirPath);			
			}

			List<String> excludeFolders = excludeFolder == null || excludeFolder.isEmpty() ? null
					: new ArrayList<>(Arrays.asList(excludeFolder.split(",")));
			// if this index range are given for files in status_info object then the tar
			// should be done for files in folders
			if (filesPerTar > 0 && object.getTarIndexStart() != null && object.getTarIndexEnd() != null) {

				createTarForFiles(object, sourceDirPath, tarWorkDir, excludeFolders);
				

			} else {
				object.setTarStartTimestamp(new Date());
				String tarFileName;
				if (tarNameinExcelFile) {
					threadLocalMap.set(loadMetadataFile(metadataFile, "Path"));
					String path = FilenameUtils.separatorsToUnix(object.getOriginalFilePath() + "/");
					tarFileName = getAttrValueWithKey(path, "tar_name");
				} else {
					tarFileName = object.getOrginalFileName() + ".tar";
				}
				String tarFile = tarWorkDir + File.separatorChar + tarFileName;
				tarFile = Paths.get(tarFile).normalize().toString();
				File directory = new File(object.getOriginalFilePath());

				logger.info("[{}] Creating tar file in {}", super.getTaskName(), tarFile);


				// Check directory permission
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
			}
		} catch (Exception e) {
			logger.error("[{}] error {}", super.getTaskName(), e.getMessage(), e);
			throw new DmeSyncStorageException("Error occurred during tar. " + e.getMessage(), e);
		} finally {
			threadLocalMap.remove();
		}
		}
		return object;
	}

	private StatusInfo createTarForFiles(StatusInfo object, Path sourceDirPath, String tarWorkDir,
			List<String> excludeFolders) throws Exception {
		
		try{

		File directory = new File(object.getOriginalFilePath());
		File[] files = directory.listFiles();
		String tarFileName = object.getSourceFileName();
		String tarFile = tarWorkDir + File.separatorChar + tarFileName;
		tarFile = Paths.get(tarFile).normalize().toString();

		// sorting the files based on the lastModified in asc, so every rerun we get
		// them in same order.  
		Arrays.sort(files, Comparator.comparing(File::lastModified));
		List<File> fileList = new ArrayList<>(Arrays.asList(files));

		int start = object.getTarIndexStart().intValue();
		int end = object.getTarIndexEnd().intValue();
		int totalFiles = end - start;

		List<File> subList = fileList.subList(start, end);
 
		File tarWorkDirectory= new File(tarWorkDir);

		logger.info("[{}] Creating tar file in {}", super.getTaskName(), tarFile);
		
		if (!tarWorkDirectory.exists()) {			
			logger.info("[{}] Tar work space directory doesn't exists {}", super.getTaskName(), tarWorkDirectory);
			
		}
		try {
	        File tfile = new File(tarFile);
	        if(!tfile.exists()) {
				logger.info("[{}] Tar file doesn't exists {}", super.getTaskName(), tarWorkDirectory);
	        }

		}catch(Exception e) {
			logger.error("[{}] error {}", super.getTaskName(), e.getMessage(), e);
			throw new DmeSyncStorageException("Error occurred creating the tar. " + e.getMessage(), e);
		}
		
		// tarFile 
		File[] filesArray = new File[subList.size()];
		subList.toArray(filesArray);
		if (compress) {
			tarFile = tarFile + ".gz";
			tarFileName = tarFileName + ".gz";
			if (!dryRun) {
				TarUtil.targz(tarFile, excludeFolders, filesArray);
			}
		} else {
			if (!dryRun) {
				TarUtil.tar(tarFile, excludeFolders, filesArray);
			}
		}
		
		File createdTarFile = new File(tarFile);
		int tarContentsCount=TarUtil.countFilesinTar(createdTarFile.getAbsolutePath());
		
		if (totalFiles != tarContentsCount) {
			String msg = "Files in the tar " + tarContentsCount + " doesn't matched with files in the original path"+ totalFiles;
			logger.error("[{}] {}", super.getTaskName(), msg);
			throw new DmeSyncVerificationException(msg);
		}
		// Update the record for upload
		object.setFilesize(createdTarFile.length());
		object.setSourceFileName(tarFileName);
		object.setSourceFilePath(tarFile);
		object.setTarEndTimestamp(new Date());
		object.setTarContentsCount(tarContentsCount);
		object = dmeSyncWorkflowService.getService(access).saveStatusInfo(object);
	}catch(Exception e)
	{
		logger.error("[{}] error {}", super.getTaskName(), e.getMessage(), e);
		throw new DmeSyncStorageException("Error occurred during tar. " + e.getMessage(), e);
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
