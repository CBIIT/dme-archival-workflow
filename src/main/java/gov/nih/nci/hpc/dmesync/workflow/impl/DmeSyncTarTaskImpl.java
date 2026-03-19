package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.io.File;

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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.apache.commons.io.FilenameUtils;
import gov.nih.nci.hpc.dmesync.util.ExcelUtil;
import gov.nih.nci.hpc.dmesync.DmeSyncPathMetadataProcessorFactory;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncStorageException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncVerificationException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.jms.DmeSyncProducer;
import gov.nih.nci.hpc.dmesync.util.TarUtil;
import gov.nih.nci.hpc.dmesync.util.WorkflowConstants;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncPathMetadataProcessor;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncTask;

/**
 * DME Sync Tar Task Implementation
 * 
 * @author dinhys
 */
@Component
public class DmeSyncTarTaskImpl extends AbstractDmeSyncTask implements DmeSyncTask {
	
	private static final Pattern GROUPED_SITE_TAR_NAME = Pattern.compile("^(\\d+)_(\\d+)\\.tar$");
	
	@Value("${dmesync.doc.name}")
	private String doc;

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
	
	@Value("${dmesync.tar.contents.file:false}")
	private boolean createTarContentsFile;
	
	@Value("${dmesync.tar.excluded.contents.file:false}")
	private boolean createTarExcludedContentsFile;
	
	@Value("${dmesync.max.recommended.file.size}")
	private String maxRecommendedFileSize;
	
	@Value("${dmesync.tar.ignore.broken.link:false}")
	private boolean ignoreBrokenLinksInTar;
	
	@Value("${dmesync.selective.scan:false}")
    private boolean selectiveScan;
	
	@Value("${dmesync.multiple.tar.exclude.folders.prefix:}")
	private String multipleTarsExcludeFolderPrefixes;
	
	@Value("${dmesync.multiple.tars.batch.folders:false}")
	private boolean muttipleTarBatchFolders;
	
	@Value("${dmesync.multiple.tars.batch.folder.delimiter:_}")
	private String batchFolderDelimiter;

	@Value("${dmesync.multiple.tars.batch.folder.delimiter.level:2}")
	private int batchFolderDelimiterLevel;
	
	@Value("${dmesync.process.multiple.tars:false}")
	private boolean processMultipleTars;

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
	
	@Autowired 
	private DmeSyncPathMetadataProcessorFactory metadataProcessorFactory; 
	
	@Override
	public StatusInfo process(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException, DmeSyncStorageException {


		DmeSyncPathMetadataProcessor metadataTask = metadataProcessorFactory.getService(doc);
		List<String> excludeFolders = excludeFolder == null || excludeFolder.isEmpty() ? null
				: new ArrayList<>(Arrays.asList(excludeFolder.split(",")));
		
		long maxAllowedFileSize = Long.parseLong(maxRecommendedFileSize);


		Path originalFilePath=Paths.get(object.getOriginalFilePath());

        
		
		if(filesPerTar > 0  && object.getSourceFileName()!=null && StringUtils.contains(object.getSourceFileName(),"TarContentsFile.txt")){
			// Skipping this task for the contents file for multiple Tars processing
			return object;
			
		}else if (createTarContentsFile && object.getSourceFileName()!=null && StringUtils.contains(object.getSourceFileName(),"ContentsFile.txt") ){
		   //// Skipping this task for the contents file 
			return object;	
		}else if (selectiveScan && TarUtil.isSelectiveScanFileUpload(originalFilePath)){
			// Skipping this task for the selective scan files
			return object;
		}else if( metadataTask.isMetadataAvailable(object)) {
		// Task: Create tar file in work directory for processing
		try {
		    File Folder = new File(object.getOriginalFilePath());
	        
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


			// if this index range are given for files in status_info object then the tar
			// should be done for files in folders
			if (processMultipleTars && object.getTarIndexStart() != null && object.getTarIndexEnd() != null) {

				object=createTarForFiles(object, sourceDirPath, tarWorkDir, excludeFolders);
				
			} else {
				long folderSize=TarUtil.getDirectorySize(originalFilePath,excludeFolders);
			    // check to validate is the folder to tar is less than maxFilesize
				if (folderSize > maxAllowedFileSize) {
					logger.error("[{}] error :Source folder with size {}  that exceeds the recommended file size of  {}",
							super.getTaskName(),folderSize, maxAllowedFileSize);
					throw new DmeSyncStorageException("Source folder with size " +ExcelUtil.humanReadableByteCount(folderSize,true) + " exceeds the permitted size of "
							+ ExcelUtil.humanReadableByteCount(maxAllowedFileSize, true));
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
						TarUtil.targz(tarFile, excludeFolders, ignoreBrokenLinksInTar, directory);
					}
				} else {
					if (!dryRun) {
						TarUtil.tar(tarFile, excludeFolders, ignoreBrokenLinksInTar, directory);
					}
				}

				// Update the record for upload
				File createdTarFile = new File(tarFile);
				long createdTarFileSize = createdTarFile.length();
				
				if (createdTarFileSize > maxAllowedFileSize) {
					logger.error("[{}] error :Source folder with size {}  that exceeds the recommended file size of  {}",
							super.getTaskName(), createdTarFileSize , maxAllowedFileSize);
		             TarUtil.deleteTarAndParentsIfEmpty(object.getSourceFilePath(), syncWorkDir, doc);
					throw new DmeSyncStorageException("Source folder exceeds the permitted size of "
							+ ExcelUtil.humanReadableByteCount(maxAllowedFileSize, true));
				}
				
				verifyTarSizeAgainstSourceFolder(sourceDirPath.toString(), folderSize,tarFileName, createdTarFileSize);

				object.setFilesize(createdTarFileSize);
				object.setSourceFileName(tarFileName);
				object.setSourceFilePath(tarFile);
				object.setTarEndTimestamp(new Date());
				object = dmeSyncWorkflowService.getService(access).saveStatusInfo(object);

			}
			}
		} catch (Exception e) {
			logger.error("[{}] error {}", super.getTaskName(), e.getMessage(), e);
			throw new DmeSyncStorageException("Error occurred during tar. " + e.getMessage(), e);
		} finally {
			threadLocalMap.remove();
		}
		}else {
			logger.info("No need to upload file : {}", object.getOriginalFilePath());
			object.setRunId(object.getRunId() + WorkflowConstants.IGNORED_RUN_SUFFIX);
			object.setEndWorkflow(true);
			object.setError("No need to upload yet");
			object = dmeSyncWorkflowService.getService(access).saveStatusInfo(object);
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
		long maxFileSize = Long.parseLong(maxRecommendedFileSize);
		int totalFiles=0;

		// sorting the files based on the lastModified in asc, so every rerun we get
		// them in same order.  
		Arrays.sort(files, Comparator.comparing(File::lastModified));
		
		if (files != null && files.length > 0) {
			// Exclude folders listed in multiple tars excludeFolder property from files array
						
				if ( StringUtils.isNotBlank(multipleTarsExcludeFolderPrefixes)) {
					
					 logger.info("{} is excluded for Batch Tar Processing", multipleTarsExcludeFolderPrefixes);
				    files = TarUtil.excludeBatchFoldersByPrefix(files, multipleTarsExcludeFolderPrefixes);
				}
		}
		List<File> fileList = new ArrayList<>(Arrays.asList(files));
		File tarWorkDirectory= new File(tarWorkDir);
        
        File[] filesArray = null;
		// tarFile 
		logger.info("[{}] Creating tar file in {}", super.getTaskName(), tarFile);
		if (!tarWorkDirectory.exists()) {			
			logger.info("[{}] Tar work space directory doesn't exists {}", super.getTaskName(), tarWorkDirectory);
			
		}
        if(muttipleTarBatchFolders) {
        
			// --- NEW: grouped-Batch folders tar by name ---

			if (StringUtils.isBlank(tarFileName)) {
				throw new DmeSyncStorageException("Invalid batch tar name");
			}
			String groupKey = tarFileName.replace(".tar", ""); // e.g. "1_11" from "1_11.tar"
			
			logger.info("[{}] Batch tar request detected: tar={}, delimiter='{}', level={}, groupKey={}",
					super.getTaskName(), tarFileName, batchFolderDelimiter, batchFolderDelimiterLevel, groupKey);

			List<File> matchedFolders = Arrays.stream(files).filter(File::isDirectory)
					.filter(f -> TarUtil.buildBatchGroupKey(f.getName(),batchFolderDelimiter,batchFolderDelimiterLevel).map(groupKey::equals).orElse(false))
					.sorted(Comparator.comparing(File::getName)).collect(Collectors.toList());

			if (matchedFolders.isEmpty()) {
				throw new DmeSyncStorageException("Batch tar " + tarFileName + " matched groupKey=" + groupKey
						+ " but found no folders under " + object.getOriginalFilePath());
			}
			
			 // verification: compare with count persisted by Process task
		    if (object.getTarContentsCount() != null
		            && matchedFolders.size() != object.getTarContentsCount().intValue()) {
		        throw new DmeSyncStorageException("Batch tar membership count mismatch for " + tarFileName
		                + ": DB tar_contents_count=" + object.getTarContentsCount()
		                + ", filesystem matched=" + matchedFolders.size());
		    }

			filesArray = matchedFolders.toArray(new File[0]);
            
		}else {

			int start = object.getTarIndexStart().intValue();
			int end = object.getTarIndexEnd().intValue();
			totalFiles = (end+1) - start;
			List<File> subList = fileList.subList(start, end+1);
	        filesArray = new File[subList.size()];
	        subList.toArray(filesArray);
		}
       
		
		if (compress) {
			tarFile = tarFile + ".gz";
			tarFileName = tarFileName + ".gz";
			if (!dryRun) {
				TarUtil.targz(tarFile, excludeFolders, ignoreBrokenLinksInTar, filesArray);
			}
		} else {
			if (!dryRun) {
				TarUtil.tar(tarFile, excludeFolders, ignoreBrokenLinksInTar, filesArray);
			}
		}
		
		File createdTarFile = new File(tarFile);
		int tarContentsCount=TarUtil.countFilesinTar(createdTarFile.getAbsolutePath());
		
		if (createdTarFile.length() > maxFileSize) {
			logger.error("[{}] error :Batch Tar with size {}  that exceeds the recommended file size of  {}",
					super.getTaskName(), object.getFilesize(), maxFileSize);
             TarUtil.deleteTarAndParentsIfEmpty(object.getSourceFilePath(), syncWorkDir, doc);
			throw new DmeSyncStorageException("Batch Tar exceeds the permitted size of "
					+ ExcelUtil.humanReadableByteCount(maxFileSize, true));
		}

		if(!muttipleTarBatchFolders || !dryRun) {
			if (totalFiles != tarContentsCount) {
				// Tar Verification.
				String msg = "Files in the tar " + tarContentsCount + " doesn't matched with files in the original path"+ totalFiles;
				logger.error("[{}] {}", super.getTaskName(), msg);
				throw new DmeSyncVerificationException(msg);
			}
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
	
	/**
	 * Verify that the generated TAR file size is greater than total size of the source folder.
	 * Note:
	 * - TAR can be slightly larger than source due to TAR headers/block padding.
	 * - TAR should never be smaller than the sum of source file sizes.
	 */
	private void verifyTarSizeAgainstSourceFolder(String sourceDirPath, long sourceFolderSize, String tarFileName,
			long createdTarFileSize) throws DmeSyncStorageException {
		
		if (createdTarFileSize >= sourceFolderSize) {
			logger.info(
					"[{}] TAR size verification successful. tarFile={}, tarSize={}, sourceFolder={}, sourceSize={}",
					super.getTaskName(), tarFileName, createdTarFileSize, sourceDirPath, sourceFolderSize);
			return;
		}

		//If TAR is smaller than source, it is a mismatch (treat as failure)
		
		String msg = String.format(
				"TAR verification failed (size mismatch): The generated TAR is smaller than the source folder total. "
						+ "Source folder: (total size: %s). " + "Generated TAR: %s (size: %s).",
				ExcelUtil.humanReadableByteCount(sourceFolderSize, true), tarFileName,
				ExcelUtil.humanReadableByteCount(createdTarFileSize, true));

		logger.error("[{}] {}", super.getTaskName(), msg);

		// Throwing exception prevents tar task from being marked completed / saved as
		// success
		throw new DmeSyncStorageException(msg);
	}


}
