package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.io.BufferedWriter;
import java.io.File;
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

	@Autowired
	private DmeSyncProducer sender;
	
	@Autowired 
	private DmeSyncMailServiceFactory dmeSyncMailServiceFactory;
	
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

	@Value("${dmesync.cleanup:false}")
	private boolean cleanup;
	
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
	
	protected static ThreadLocal<Map<String, Map<String, String>>> threadLocalMap = new ThreadLocal<Map<String, Map<String, String>>>() {
		@Override
		protected HashMap<String, Map<String, String>> initialValue() {
			return new HashMap<>();
		}
	};

	@Override
	public StatusInfo process(StatusInfo object) throws DmeSyncMappingException, DmeSyncWorkflowException , DmeSyncStorageException {

		// Task: Create tar file in work directory for processing
		try {
			// Construct work dir path
			Path baseDirPath = Paths.get(syncBaseDir).toRealPath();
			Path workDirPath = Paths.get(syncWorkDir).toRealPath();
			Path sourceDirPath = Paths.get(object.getOriginalFilePath());
			Path relativePath = baseDirPath.relativize(sourceDirPath);
			String tarWorkDir = workDirPath.toString() + File.separatorChar + relativePath.toString();
			Path tarWorkDirPath = Paths.get(tarWorkDir);
			Files.createDirectories(tarWorkDirPath);

			// if this property is set perform multiple tars based on the number of files
			// in each tier
			if (filesPerTar > 0 && multpleTarsFolders != null
						&& StringUtils.contains( multpleTarsFolders, object.getOrginalFileName())) {
				
				createMultipleTars(object,sourceDirPath , tarWorkDir);	
				
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

				List<String> excludeFolders = excludeFolder == null || excludeFolder.isEmpty() ? null
						: new ArrayList<>(Arrays.asList(excludeFolder.split(",")));

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
			throw new DmeSyncStorageException("Error occurred during tar. " +
			 e.getMessage(), e);
		}finally {
	    	threadLocalMap.remove();
	    }

		return object;
	}
	
	private StatusInfo createMultipleTars(StatusInfo object, Path sourceDirPath, String tarWorkDir ) throws Exception {
		
		try {
			
			logger.info("[{}] Started multiple tar files creations in {}", super.getTaskName(),object.getOriginalFilePath());							

			String tarFileParentName = sourceDirPath.getParent().getFileName().toString();
			String tarFileNameFormat = tarFileParentName + "_"
					+ object.getOrginalFileName();
			File tarMappingFile = new File(syncWorkDir + "/"+tarFileParentName, (tarFileNameFormat + "_TarContentsFile.txt"));
			BufferedWriter notesWriter = new BufferedWriter(new FileWriter(tarMappingFile));
			File directory = new File(object.getOriginalFilePath());
			File[] files = directory.listFiles();
			// Check directory permission
			if (!Files.isReadable(Paths.get(object.getOriginalFilePath()))) {
				throw new DmeSyncStorageException("No Read permission to " + object.getOriginalFilePath());
			}
			List<String> excludeFolders = excludeFolder == null || excludeFolder.isEmpty() ? null
					: new ArrayList<>(Arrays.asList(excludeFolder.split(",")));
			if (files != null) {
				// sorting the files based on the lastModified in asc, so every rerun we get them in same order.
				Arrays.sort(files, Comparator.comparing(File::lastModified));
				List<File> fileList = new ArrayList<>(Arrays.asList(files));
				int tarLoopCount = (fileList.size() + filesPerTar - 1) / filesPerTar;
				logger.info("[{}] Started creating {} tars for the dataset {} with {} files ", super.getTaskName(),tarLoopCount,tarFileParentName,fileList.size());							

				for (int i = 0; i < tarLoopCount; i++) {
					Date tarStartedTimeStamp= new Date();
					int start = i * filesPerTar;
					int end = Math.min(start + filesPerTar, fileList.size());
					int totalFiles = end-start;
					List<File> subList = fileList.subList(start, end);
					String tarFileName = tarFileNameFormat + "_part_" + (i + 1) + ".tar";
					String tarFile = tarWorkDir + File.separatorChar + tarFileName;
					tarFile = Paths.get(tarFile).normalize().toString();
					/*
					 * Two checks before creation of the tar mainly for rerun when there are issues.
					 * First: check if tarName already got uploaded to DME from statusInfo table
					 * second: If not , check if the tar is available in the temporary work directory. If both the cases are false , then only create the tar.
					 */
					StatusInfo checkForUploadedTar = dmeSyncWorkflowService.getService(access)
							.findFirstStatusInfoByOriginalFilePathAndSourceFileNameAndStatus(
									object.getOriginalFilePath(), tarFileName, "COMPLETED");
                    // check if tar already got uploaded to DME from local DB
					
					if (("local".equals(verifyPrevUpload)) && checkForUploadedTar != null) {
						
						logger.info("[{}] Skipping the Creation of tar file in since this is already got uploaded {} {}",
								super.getTaskName(), tarFile, checkForUploadedTar.getId() , checkForUploadedTar.getStatus());	
						notesWriter.write("Tar File: " + tarFileName + "\n");
						for (File fileName : subList) {
							notesWriter.write(tarFileName+ " " + fileName.getName() + "\n");
						}
						notesWriter.write("\n");
						continue;
						
					}else {
						Path checkForTarinTemp = Paths.get(tarFile);	
						// check if tar already created and placed in temp directory.
						if (("local".equals(verifyPrevUpload))&& Files.exists(checkForTarinTemp) && totalFiles == TarUtil.countFilesinTar(checkForTarinTemp.toString())) {
							logger.info("[{}] Retrieving the tar file from work Directory instead of creating again {} , {}", super.getTaskName(), checkForTarinTemp.toString(), tarFile);		
						}else {
							logger.info("[{}] No  tar file found in work Directory or completed status in the Db row {} , {}", super.getTaskName(), tarFile);		
							logger.info("[{}] Creating tar file in {}", super.getTaskName(), tarFile);							
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
						} 
						// check is there is already record in DB for this runId and sourceFilePath. 
						 // mainly for failed tar reruns 
						StatusInfo recordForTarfile = dmeSyncWorkflowService.getService(access)
								.findTopBySourceFilePathAndRunId(tarFile, object.getRunId());
						if (("local".equals(verifyPrevUpload)) && recordForTarfile != null) {
							
							upsertTask(recordForTarfile.getId());
							DmeSyncMessageDto message = new DmeSyncMessageDto();
							message.setObjectId(recordForTarfile.getId());
							sender.send(message, "inbound.queue");
							logger.info("get queue count"+ sender.getQueueCount("inbound.queue"));
							
						} else {
							// add new row in status info table for created tar.
							StatusInfo statusInfo = insertNewRowforTar(object, tarFileName, tarFile , tarStartedTimeStamp);
							// Mark the tar task as completed for individual Tars.
							upsertTask(statusInfo.getId());
							// Send the objectId to the message queue for processing
							DmeSyncMessageDto message = new DmeSyncMessageDto();
							message.setObjectId(statusInfo.getId());
							sender.send(message, "inbound.queue");
							logger.info("get queue count"+ sender.getQueueCount("inbound.queue"));	
						}
					} 
						
									
					/*
					 * Write tracking information to notes file
					 */
					
					notesWriter.write("Tar File: " + tarFileName + "\n");
					for (File fileName : subList) {
						notesWriter.write(tarFileName+ " " + fileName.getName() + "\n");
					}
					notesWriter.write("\n");
				}
				
				notesWriter.close();
				logger.info("[{}] Ended Multiple tar creations in {}", super.getTaskName());							
				Long totalFilesinTars;
				if("local".equals(verifyPrevUpload)){
					totalFilesinTars=	dmeSyncWorkflowService.getService(access).totalFilesinAllTarsForOriginalFilePath(object.getOriginalFilePath());
				}else {
					totalFilesinTars=	dmeSyncWorkflowService.getService(access).totalFilesinAllTarsForOriginalFilePathAndRunId(object.getOriginalFilePath(),object.getRunId());
				}
				
			   if (verifyTarFilesCount && totalFilesinTars !=null && 
						files.length!=totalFilesinTars) {
					object.setError((" Files in original folder "+ files.length  + " didn't match the files in multiple created tars " + totalFilesinTars));
				    dmeSyncWorkflowService.getService(access).recordError(object);
					throw new DmeSyncMappingException((" Files in original folder "+ files.length  + " didn't match the files in multiple created tars " + totalFilesinTars));
				}
					// update the current statusInfo row with the TarMappingNotesFile
				object.setFilesize(tarMappingFile.length());
				object.setSourceFileName(tarMappingFile.getName());
				object.setSourceFilePath(tarMappingFile.getAbsolutePath());
				object.setTarStartTimestamp(null);
				object = dmeSyncWorkflowService.getService(access).saveStatusInfo(object);				
			}
		} catch (Exception e) {
			logger.error("[{}] error {}", super.getTaskName(), e.getMessage(), e);
			throw new DmeSyncStorageException("Error occurred during tar. " +
			 e.getMessage(), e);
		}
		return object;
	}

	
	private StatusInfo insertNewRowforTar(StatusInfo object, String tarFileName, String tarFile , Date tarStartedTimeStamp) throws IOException {
		
		File createdTarFile = new File(tarFile);

		StatusInfo statusInfo = new StatusInfo();
		statusInfo.setRunId(object.getRunId());
		statusInfo.setOrginalFileName(object.getOrginalFileName());
		statusInfo.setOriginalFilePath(object.getOriginalFilePath());
		statusInfo.setSourceFileName(tarFileName);
		statusInfo.setSourceFilePath(tarFile);
		statusInfo.setFilesize(createdTarFile.length());
		statusInfo.setStartTimestamp(tarStartedTimeStamp);
		statusInfo.setTarStartTimestamp(tarStartedTimeStamp);
		statusInfo.setTarEndTimestamp(new Date());
		statusInfo.setDoc(object.getDoc());
		statusInfo.setTarContentsCount(TarUtil.countFilesinTar(createdTarFile.getAbsolutePath()));
		statusInfo = dmeSyncWorkflowService.getService(access).saveStatusInfo(statusInfo);
		
		return statusInfo;
		
		
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
