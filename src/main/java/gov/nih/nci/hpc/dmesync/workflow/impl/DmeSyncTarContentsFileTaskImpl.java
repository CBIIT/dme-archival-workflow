package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.apache.commons.io.FilenameUtils;
import gov.nih.nci.hpc.dmesync.util.ExcelUtil;
import gov.nih.nci.hpc.dmesync.util.TarContentsFileUtil;
import gov.nih.nci.hpc.dmesync.util.TarTaskContext;

import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.dto.DmeSyncMessageDto;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncStorageException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncVerificationException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.jms.DmeSyncProducer;
import gov.nih.nci.hpc.dmesync.util.TarUtil;
import gov.nih.nci.hpc.dmesync.util.WorkflowConstants;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncTask;

/**
 * DME Sync Tar Contents File Task Implementation
 * 
 * @author konerum3
 */
@Component
public class DmeSyncTarContentsFileTaskImpl extends AbstractDmeSyncTask implements DmeSyncTask {
	
	
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

	@PostConstruct
	public boolean init() {
		super.setTaskName("TarContentsFileTask");
	    super.setCheckTaskForCompletion(false);
		return true;
	}


	@Autowired
	private DmeSyncProducer sender;

	@Override
	public StatusInfo process(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException, DmeSyncStorageException {
				
		
	
      if (object.getSourceFileName()!=null && object.getSourceFileName().endsWith("tar")) {

    	  try {
			// Construct work dir path
			Path baseDirPath = Paths.get(syncBaseDir).toRealPath();
			Path workDirPath = Paths.get(syncWorkDir).toRealPath();
			Path sourceDirPath = Paths.get(object.getOriginalFilePath());
			Path relativePath = baseDirPath.relativize(sourceDirPath);
			String tarWorkDir = workDirPath.toString() + File.separatorChar + relativePath.toString();
			Path tarWorkDirPath = Paths.get(tarWorkDir);
			String 	tarFileName = object.getOrginalFileName() + ".tar";

					File tarMappingFile = new File(tarWorkDir + "/" + tarFileName +WorkflowConstants.tarContentsFileEndswith);
					logger.info("[{}] Creating Tar contents file {}", super.getTaskName(),
							tarMappingFile.getAbsolutePath());
					File tarExcludedFile =null;
					if(createTarExcludedContentsFile) {
						 tarExcludedFile = new File(tarWorkDir + "/" + tarFileName +WorkflowConstants.tarExcludedContentsFileEndswith);
						logger.info("[{}] Creating Tar excluded contents file {}", super.getTaskName(),
								tarExcludedFile.getAbsolutePath());
					}

					createContentsFile(tarMappingFile,sourceDirPath,object,tarExcludedFile);
				} catch (Exception e) {
			logger.error("[{}] error {}", super.getTaskName(), e.getMessage(), e);
			throw new DmeSyncStorageException("Error occurred during tar. " + e.getMessage(), e);
		} 
      }
		return object;
	}

	
	
	// This method validate and sends content file request to JMS if contents file is not uploaded to DME
	private void sendContentsFileRequestToJms(File tarMappingFile, StatusInfo object) throws IOException {

		StatusInfo checkForUploadedContentsFile = dmeSyncWorkflowService.getService(access)
				.findTopStatusInfoByDocAndSourceFilePathAndOriginalFilePath(doc, tarMappingFile.getAbsolutePath() , object.getOriginalFilePath());

		StatusInfo contentsFileRecord = null;

		if ("local".equals(verifyPrevUpload) && checkForUploadedContentsFile != null) {

			if (StringUtils.equalsIgnoreCase("COMPLETED", checkForUploadedContentsFile.getStatus())) {
				// Tar contents file request have already uplaoded in previous run
				logger.info("[{}] Tar Contents file already uploaded to DME  {} ,{} ,{}", super.getTaskName(),
						checkForUploadedContentsFile.getSourceFileName(), checkForUploadedContentsFile.getId(),
						checkForUploadedContentsFile.getStatus());
			} else {
				checkForUploadedContentsFile.setRunId(object.getRunId());
				checkForUploadedContentsFile.setStatus(null);
				checkForUploadedContentsFile.setError("");
				checkForUploadedContentsFile.setRetryCount(0L);
				checkForUploadedContentsFile.setEndWorkflow(false);
				checkForUploadedContentsFile.setFilesize(tarMappingFile.length());
				checkForUploadedContentsFile = dmeSyncWorkflowService.getService(access).saveStatusInfo(checkForUploadedContentsFile);
				contentsFileRecord = checkForUploadedContentsFile;
			}
		} else {
			// If there is no content files record in DB, create a new one.
			logger.info("[{}]Inserting the new contents file upload request {}", super.getTaskName(),
					tarMappingFile.getName());
			// add new row in status info table for uploading tarContentsFile.
			contentsFileRecord = insertNewRowForContentsFile(tarMappingFile, object);
		}

		if (contentsFileRecord != null) {

			upsertTaskByTaskName(contentsFileRecord.getId(), "TarTask");
			// This contentsFileRecord objectId is send to the message queue in the cleanup
			DmeSyncMessageDto message = new DmeSyncMessageDto();
			message.setObjectId(contentsFileRecord.getId());
			sender.send(message, "inbound.queue");
			logger.info("get queue count" + sender.getQueueCount("inbound.queue"));
		}

	}
	
	private void createContentsFile(File tarMappingFile, Path sourceDirPath, StatusInfo object , File tarExcludedFile)
			throws IOException, DmeSyncMappingException {

		BufferedWriter tarContentsFileWriter = new BufferedWriter(new FileWriter(tarMappingFile));

		// Use the file lists already collected by TarTask when available, so the
		// source directory does not have to be walked a second time.
		List<File> includedTarFiles = TarTaskContext.getIncludedFiles();
		List<File> excludedTarFiles = TarTaskContext.getExcludedFiles();

		if (includedTarFiles == null) {
			// Fallback: TarTask context is not available (e.g. standalone run). Walk the
			// source directory as before.
			File[] files = sourceDirPath.toFile().listFiles();
			if (files == null || files.length == 0) {
				return;
			}

			includedTarFiles = new ArrayList<>();
			excludedTarFiles = new ArrayList<>();
			final List<File> includedRef = includedTarFiles;
			final List<File> excludedRef = excludedTarFiles;

			Files.walkFileTree(sourceDirPath, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
					if (excludeFolder != null && !excludeFolder.isEmpty() && excludeFolder.contains(dir.getFileName().toString())) {
						logger.info("{} is excluded for tar", dir.getFileName().toString());
						return FileVisitResult.SKIP_SUBTREE;
					}
					// Record each non-excluded directory (except the root itself) in the manifest.
					if (!dir.equals(sourceDirPath)) {
						includedRef.add(dir.toFile());
					}
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) {
					try {
						if (Files.isSymbolicLink(path)) {
							try {
								Path target = Files.readSymbolicLink(path);
								Path resolved = path.getParent().resolve(target).normalize();

								if (Files.exists(resolved) && Files.isReadable(resolved)) {
									includedRef.add(path.toFile());
								} else {
									logger.error("{} is not supported", path.toString());
									excludedRef.add(path.toFile());
								}
							} catch (IOException e) {
								logger.error("{} is not supported", path.toString());
								excludedRef.add(path.toFile());
							}
						} else if (Files.isReadable(path)) {
							includedRef.add(path.toFile());
						} else {
							logger.error("{} is not readable", path.toString());
							excludedRef.add(path.toFile());
						}
					} catch (Exception e) {
						excludedRef.add(path.toFile());
					}
					return FileVisitResult.CONTINUE;
				}
			});
		}

		try {
			if (!includedTarFiles.isEmpty()) {
				boolean contentsFileCheck = TarContentsFileUtil.writeToTarContentsFile(tarContentsFileWriter,
						object.getOriginalFilePath(), includedTarFiles);
				if (contentsFileCheck) {
					sendContentsFileRequestToJms(tarMappingFile, object);
				}
			}
			if (tarExcludedFile != null && excludedTarFiles != null && !excludedTarFiles.isEmpty()) {
				BufferedWriter excludedFilesContentsFileWriter = new BufferedWriter(new FileWriter(tarExcludedFile));
				boolean excludedContentsFileCheck = TarContentsFileUtil.writeToTarContentsFile(excludedFilesContentsFileWriter,
						object.getOriginalFilePath(), excludedTarFiles);
				if (excludedContentsFileCheck) {
					sendContentsFileRequestToJms(tarExcludedFile, object);
				}
			}
		} finally {
			TarTaskContext.clear();
		}
	}

	private StatusInfo insertNewRowForContentsFile(File tarMappingFile, StatusInfo object) {
		StatusInfo statusInfo = new StatusInfo();
		statusInfo.setRunId(object.getRunId());
		statusInfo.setOrginalFileName(object.getOrginalFileName());
		statusInfo.setOriginalFilePath(object.getOriginalFilePath());
		statusInfo.setSourceFileName(tarMappingFile.getName());
		statusInfo.setStartTimestamp(new Date());
		statusInfo.setDoc(object.getDoc());
		statusInfo.setSourceFilePath(tarMappingFile.getAbsolutePath());
		statusInfo.setFilesize(tarMappingFile.length());

		statusInfo = dmeSyncWorkflowService.getService(access).saveStatusInfo(statusInfo);

		return statusInfo;

	}


}
