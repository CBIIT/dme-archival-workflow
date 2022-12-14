package gov.nih.nci.hpc.dmesync.scheduler;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import gov.nih.nci.hpc.dmesync.util.HpcLocalDirectoryListQuery;
import gov.nih.nci.hpc.dmesync.util.HpcPathAttributes;
import gov.nih.nci.hpc.dmesync.util.TarUtil;
import gov.nih.nci.hpc.dmesync.workflow.impl.DmeSyncDataObjectListQuery;
import gov.nih.nci.hpc.exception.HpcException;
import gov.nih.nci.hpc.dmesync.DmeSyncApplication;
import gov.nih.nci.hpc.dmesync.DmeSyncMailServiceFactory;
import gov.nih.nci.hpc.dmesync.DmeSyncWorkflowServiceFactory;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.dto.DmeSyncMessageDto;
import gov.nih.nci.hpc.dmesync.jms.DmeSyncProducer;

/**
 * DME Sync Scheduler to scan for files to be Archived
 * 
 * @author dinhys
 *
 */
@Component
public class DmeSyncScheduler {

  private static final Logger logger = LoggerFactory.getLogger(DmeSyncScheduler.class);

  private final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
  private final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyyMMddHHmmss");

  @Autowired private DmeSyncProducer sender;
  @Autowired private DmeSyncMailServiceFactory dmeSyncMailServiceFactory;
  @Autowired private DmeSyncWorkflowServiceFactory dmeSyncWorkflowService;
  @Autowired private DmeSyncDataObjectListQuery dmeSyncDataObjectListQuery;

  @Value("${dmesync.db.access:local}")
  private String access;
  
  @Value("${dmesync.doc.name}")
  private String doc;
  
  @Value("${dmesync.source.base.dir}")
  private String syncBaseDir;

  @Value("${dmesync.work.base.dir}")
  private String syncWorkDir;

  @Value("${dmesync.verify.prev.upload:none}")
  private String verifyPrevUpload;

  @Value("${dmesync.exclude.pattern:}")
  private String excludePattern;

  @Value("${dmesync.include.pattern:}")
  private String includePattern;

  @Value("${dmesync.tar:false}")
  private boolean tar;

  @Value("${dmesync.untar:false}")
  private boolean untar;

  @Value("${dmesync.preprocess.depth:0}")
  private String depth;

  @Value("${dmesync.run.once.and.shutdown:false}")
  private boolean shutDownFlag;

  @Value("${dmesync.run.once.run_id:}")
  private String oneTimeRunId;

  @Value("${dmesync.last.modified.days:}")
  private String lastModfiedDays;

  @Value("${dmesync.replace.modified.files:false}")
  private boolean replaceModifiedFiles;
  
  @Value("${dmesync.tar.file.exist:}")
  private String checkExistsFile;
  
  @Value("${dmesync.tar.file.exist.ext:}")
  private String checkExistsFileExt;
  
  @Value("${dmesync.file.exist.under.basedir:false}")
  private boolean checkExistsFileUnderBaseDir;
  
  @Value("${dmesync.file.exist.under.basedir.depth:0}")
  private String checkExistsFileUnderBaseDirDepth;
  
  @Value("${logging.file}")
  private String logFile;

  @Value("${dmesync.create.softlink:false}")
  private boolean createSoftlink;
  
  @Value("${dmesync.move.processed.files:false}")
  private boolean moveProcessedFiles;
  
  @Value("${dmesync.source.softlink.file:}")
  private String sourceSoftlinkFile;
  
  private String runId;

  /**
   * Main scheduler method to crawl the file system and find files to enqueue
   */
  @Scheduled(cron = "${dmesync.cron.expression}")
  public void findFilesToPush() {

	if (moveProcessedFiles) {
		findFilesToMove();
		return;
	}
	
    // Make sure the work dir is different from the source dir so we don't cleanup any original files.
    if ((tar || untar) && (syncWorkDir == null || Paths.get(syncBaseDir).startsWith(syncWorkDir) )){
      logger.error("[Scheduler] work directory must be different from the source dir");
      return;
    }
    
    if ((tar && untar)){
      logger.error("[Scheduler] Both tar and untar cannot be specified at the same time.");
      return;
    }
    
    runId = shutDownFlag ? oneTimeRunId : "Run_" + timestampFormat.format(new Date());

    if (shutDownFlag) {
      //check if the one time run has already occurred
      List<StatusInfo> statusInfo = dmeSyncWorkflowService.getService(access).findStatusInfoByRunIdAndDoc(oneTimeRunId, doc);
      //If it has been called already, return
      if (!CollectionUtils.isEmpty(statusInfo)) {
        return;
      }
    }

    MDC.put("run.id", runId);

    if(createSoftlink)
    	 logger.info(
    		        "[Scheduler] Current time: {} executing Run ID: {} softlink file to read {}",
    		        dateFormat.format(new Date()),
    		        runId,
    		        sourceSoftlinkFile);
    else
	    logger.info(
	        "[Scheduler] Current time: {} executing Run ID: {} base directory to scan {}",
	        dateFormat.format(new Date()),
	        runId,
	        syncBaseDir);

    // The scheduler will scan through the specified directories to find candidate for archival.
    // Find the eligible file/directory,Include/Exclude or existence of a file, modified date etc.
    // If it finds a candidate, it checks the local db to see if it has been completed.
    // If not, then it inserts the data and sends the details to the message queue for processing.

    try {
      List<HpcPathAttributes> paths = null;
      if(createSoftlink) {
    	  paths = queryDataObjects();
      } else {
      // Scan through the specified base directory and find candidates for processing
    	  paths = scanDirectory();
      }
      
      List<HpcPathAttributes> folders = new ArrayList<>();
      List<HpcPathAttributes> files = new ArrayList<>();
      if (paths != null && paths.isEmpty()) {
        logger.info("[Scheduler] No files/folders found for runID: {}", runId);
        MDC.clear();
        return;
      } else {
        for (HpcPathAttributes pathAttr : paths) {
          if (pathAttr.getIsDirectory()) {
            //If depth of -1 is specified, skip if it is not a leaf folder
            if (tar && depth.equals("-1")) {
              try(Stream<Path> stream = Files.list(Paths.get(pathAttr.getAbsolutePath()))) {
               if(stream.anyMatch(x -> x.toFile().isDirectory()))
                  continue;
              }
            }
            folders.add(pathAttr);
          } else {
            files.add(pathAttr);
          }
        }
      }

      // If tar is required, need to manipulate the list.
      if (tar) {
        processFiles(folders);
      } else if (untar) {
        //If untar is required, need to find the files included in tar.
        for (HpcPathAttributes file : files) {
          //List tar files from each folder
          List<HpcPathAttributes> tarFileList = TarUtil.listTar(file.getAbsolutePath());
          processFiles(tarFileList);
        }
      } else {
        // Process the list of files
        processFiles(files);
      }
      logger.info(
          "[Scheduler] Completed file scan at {} for Run ID: {} base directory to scan {}",
          dateFormat.format(new Date()),
          runId,
          syncBaseDir);

    } catch (Exception e) {
      logger.error("[Scheduler] Failed to access files in directory, {}", syncBaseDir, e);
    } finally {
      MDC.clear();
      runId = null;
    }
  }

  /**
   * Temporary scheduler to move SB Single Cell fastq from Sample to Run directory
   */
  private void findFilesToMove() {

  
    runId = shutDownFlag ? oneTimeRunId : "Run_" + timestampFormat.format(new Date());

    if (shutDownFlag) {
      //check if the one time run has already occurred
      List<StatusInfo> statusInfo = dmeSyncWorkflowService.getService(access).findStatusInfoByRunIdAndDoc(oneTimeRunId, doc);
      //If it has been called already, return
      if (!CollectionUtils.isEmpty(statusInfo)) {
        return;
      }
    }

    MDC.put("run.id", runId);

    logger.info(
        "[Scheduler] Current time: {} executing Run ID: {} base directory {}, restarting files from DB",
        dateFormat.format(new Date()),
        runId,
        syncBaseDir);

    try {

      List<StatusInfo> statusInfoList =
                dmeSyncWorkflowService.getService(access).findStatusInfoByDocAndStatus(doc, "COMPLETED");
      for(StatusInfo statusInfo : statusInfoList) {
	      if(statusInfo != null) {
	    	//Update the run_id and reset the retry count and errors
	    	statusInfo.setRunId(runId);
	    	statusInfo.setError("");
	    	statusInfo.setRetryCount(0L);
	    	statusInfo = dmeSyncWorkflowService.getService(access).saveStatusInfo(statusInfo);
	    	// Delete the metadata info created for this object ID
	    	dmeSyncWorkflowService.getService(access).deleteMetadataInfoByObjectId(statusInfo.getId());
	    	// Send the incomplete objectId to the message queue for processing
	        DmeSyncMessageDto message = new DmeSyncMessageDto();
	        message.setObjectId(statusInfo.getId());
	
	        sender.send(message, "inbound.queue");
	      }
      }

      logger.info(
          "[Scheduler] Completed restaring files at {} for Run ID: {} base directory to reprocess {}",
          dateFormat.format(new Date()),
          runId,
          syncBaseDir);

    } catch (Exception e) {
      logger.error("[Scheduler] Failed to restart files, {}", syncBaseDir, e);
    } finally {
      MDC.clear();
      runId = null;
    }
  }
  private List<HpcPathAttributes> scanDirectory() throws HpcException {
    
    HpcLocalDirectoryListQuery impl = new HpcLocalDirectoryListQuery();
    List<HpcPathAttributes> result = new ArrayList<>();
    List<String> excludePatterns =
        excludePattern == null || excludePattern.isEmpty()
            ? null
            : new ArrayList<>(Arrays.asList(excludePattern.split(",")));
    List<String> includePatterns =
        includePattern == null || includePattern.isEmpty()
            ? null
            : new ArrayList<>(Arrays.asList(includePattern.split(",")));
    
    if (syncBaseDir != null) {
      if(tar && Integer.parseInt(depth) == 0) {
    	  result = toHpcPathAttribute(syncBaseDir);
    	  return result;
      }
      result = 
          impl.getPathAttributes(
              syncBaseDir,
              excludePatterns,
              includePatterns,
              tar ? Integer.parseInt(depth) : untar ? Integer.parseInt(depth) + 1 : 0);
    }
    return result;
  }
  
  private List<HpcPathAttributes> queryDataObjects() throws HpcException, IOException {
    List<HpcPathAttributes> result = new ArrayList<>();
    Path filePath = Paths.get(sourceSoftlinkFile);
    List<String> lines = Files.readAllLines(filePath);
    //process each collection
    for(String collectionPath: lines) {
      result.addAll(dmeSyncDataObjectListQuery.getPathAttributes(collectionPath));
    }
    return result;
  }

  private List<HpcPathAttributes> toHpcPathAttribute(String directory) {
	  List<HpcPathAttributes> pathAttributesList = new ArrayList<>();
	  File file = new File(directory);

	  HpcPathAttributes pathAttributes = new HpcPathAttributes();
	  pathAttributes.setName(file.getName());
	  pathAttributes.setPath(file.getPath());
	  pathAttributes.setUpdatedDate(new Date(file.lastModified()));
	  pathAttributes.setAbsolutePath(file.getAbsolutePath());
	  pathAttributes.setSize(file.length());
	  pathAttributes.setIsDirectory(true);
	  pathAttributesList.add(pathAttributes);

	return pathAttributesList;
  }

  private void processFiles(List<HpcPathAttributes> files) throws Exception {

    for (HpcPathAttributes file : files) {

      StatusInfo statusInfo = null;

      //If we need to verify previous upload, check
      if ("local".equals(verifyPrevUpload)) {
        // Checks the local db to see if it has been completed
        if (untar) {
          statusInfo =
              dmeSyncWorkflowService.getService(access).findFirstStatusInfoByOriginalFilePathAndSourceFileNameAndStatus(
                  file.getAbsolutePath(), file.getTarEntry(), "COMPLETED");
        } else {
          statusInfo =
              dmeSyncWorkflowService.getService(access).findFirstStatusInfoByOriginalFilePathAndStatus(
                  file.getAbsolutePath(), "COMPLETED");
        }
        if (statusInfo != null) {
          logger.debug(
              "[Scheduler] File has already been uploaded: {}", statusInfo.getOriginalFilePath());
          if(!replaceModifiedFiles)
        	  continue;
          
          Date modifiedTimestamp = file.getUpdatedDate();
          Date uploadedTimestamp = statusInfo.getUploadStartTimestamp();
          if(uploadedTimestamp.compareTo(modifiedTimestamp) > 0) {
        	  //Modified is before the last upload
        	  continue;
          }
          //Modified after the last upload, so we need to re-upload
        } else {
        	statusInfo =
                    dmeSyncWorkflowService.getService(access).findFirstStatusInfoByOriginalFilePathOrderByStartTimestampDesc(
                        file.getAbsolutePath());
          if(statusInfo != null) {
        	//Update the run_id and reset the retry count and errors
        	statusInfo.setRunId(runId);
        	statusInfo.setError("");
        	statusInfo.setRetryCount(0L);
        	statusInfo = dmeSyncWorkflowService.getService(access).saveStatusInfo(statusInfo);
        	// Delete the metadata info created for this object ID
        	dmeSyncWorkflowService.getService(access).deleteMetadataInfoByObjectId(statusInfo.getId());
        	// Send the incomplete objectId to the message queue for processing
            DmeSyncMessageDto message = new DmeSyncMessageDto();
            message.setObjectId(statusInfo.getId());

            sender.send(message, "inbound.queue");
            continue;
          }
        }

      }
      
      //If file has been modified with in days specified, skip
      if (!lastModfiedDays.isEmpty()
          && daysBetween(file.getUpdatedDate(), new Date()) <= Integer.parseInt(lastModfiedDays)) {
        logger.info(
            "[Scheduler] Skipping: {} File/folder has been modified within the last {} days. Last modified date: {}.",
            file.getAbsolutePath(),
            lastModfiedDays,
            file.getUpdatedDate());
        continue;
      }

		// If folder does not contain a specified file, skip
		if (!checkExistsFileUnderBaseDir
					&& (StringUtils.isNotEmpty(checkExistsFileExt) || StringUtils.isNotEmpty(checkExistsFile))) {

	    Path folder = Paths.get(file.getAbsolutePath());
        if(!tar) {
          folder = Paths.get(file.getAbsolutePath()).getParent();
        }
        if(StringUtils.isNotEmpty(checkExistsFile)) {
        	try (DirectoryStream<Path> stream = Files.newDirectoryStream(folder,
    				path -> path.getFileName().toString().equals(checkExistsFile))) {
        		if (!stream.iterator().hasNext()) {
    				logger.info("{} file not found for {}", checkExistsFile, folder.toString());
    				continue;
    			}
        	}  catch (IOException ex) {
	          throw new Exception("Error while listing directory: " + folder.toString(), ex);
	        }
        }
        if(StringUtils.isNotEmpty(checkExistsFileExt)) {
	        try (DirectoryStream<Path> stream = Files.newDirectoryStream(folder, path -> path.toString().endsWith("." + checkExistsFileExt))) {
	
	          if (!stream.iterator().hasNext()) {
	            String message ="The directory " + file.getAbsolutePath() + " does not contain any " + checkExistsFileExt + " file at depth 0";
	            logger.info(
	              "[Scheduler] Skipping: {} Folder to process does not contain the specified extention {}.",
	              file.getAbsolutePath(),
	              checkExistsFileExt);
	            //Insert record in DB as COMPLETED and send an email.
	            statusInfo = insertRecordDb(file, true);
	            dmeSyncMailServiceFactory.getService(doc).sendMail("WARNING: HPCDME during registration", message);
	            continue;
	          }
	        }  catch (IOException ex) {
	          throw new Exception("Error while listing directory: " + file.getAbsolutePath(), ex);
	        }
        }
      }
      
      //If folder under the base dir does not contain a specified file for both files and tar, skip
      if (checkExistsFileUnderBaseDir 
    		  && (StringUtils.isNotEmpty(checkExistsFileExt) || StringUtils.isNotEmpty(checkExistsFile))) {
        
        //Find the directory being archived under the base dir
        Path baseDirPath = Paths.get(syncBaseDir).toRealPath();
        Path filePath = Paths.get(file.getAbsolutePath());
        Path relativePath = baseDirPath.relativize(filePath);
        Path subPath1 = relativePath.subpath(0, Integer.parseInt(checkExistsFileUnderBaseDirDepth)+1);
        Path checkExistFilePath = baseDirPath.resolve(subPath1);
        
        if(StringUtils.isNotEmpty(checkExistsFile)) {
	        try (DirectoryStream<Path> stream = Files.newDirectoryStream(checkExistFilePath, path -> path.getFileName().toString().equals(checkExistsFile))) {
	
	          if (!stream.iterator().hasNext()) {
	            logger.info(
	              "[Scheduler] Skipping: {} Folder to process does not contain the specified file {}.",
	              checkExistFilePath,
	              checkExistsFile);
	            continue;
	          }
	        }  catch (IOException ex) {
	          throw new Exception("Error while listing directory: " + checkExistFilePath, ex);
	        }
        }
        if(StringUtils.isNotEmpty(checkExistsFileExt)) {
	        try (DirectoryStream<Path> stream = Files.newDirectoryStream(checkExistFilePath, path -> path.toString().endsWith("." + checkExistsFileExt))) {
	
	          if (!stream.iterator().hasNext()) {
	            logger.info(
	              "[Scheduler] Skipping: {} Folder to process does not contain the specified extention {}.",
	              checkExistFilePath,
	              checkExistsFileExt);
	            continue;
	          }
	        }  catch (IOException ex) {
	          throw new Exception("Error while listing directory: " + checkExistFilePath, ex);
	        }
        }
      }
      
      // Insert the record in local DB
      logger.info("[Scheduler] Including: {}", file.getAbsolutePath());
      statusInfo = insertRecordDb(file, false);

      // Send the objectId to the message queue for processing
      DmeSyncMessageDto message = new DmeSyncMessageDto();
      message.setObjectId(statusInfo.getId());

      sender.send(message, "inbound.queue");
    }
  }

  private StatusInfo insertRecordDb(HpcPathAttributes file, boolean completed) {
    StatusInfo statusInfo = new StatusInfo();
    statusInfo.setRunId(runId);
    statusInfo.setOrginalFileName(file.getName());
    statusInfo.setOriginalFilePath(file.getAbsolutePath());
    statusInfo.setSourceFileName(untar ? file.getTarEntry() : file.getName());
    statusInfo.setSourceFilePath(file.getAbsolutePath());
    statusInfo.setFilesize(file.getSize());
    statusInfo.setStartTimestamp(new Date());
    statusInfo.setDoc(doc);
    if(completed) {
      statusInfo.setStatus("COMPLETED");
      statusInfo.setError("specified file extension doesn't exist in correct depth");
    }
    statusInfo = dmeSyncWorkflowService.getService(access).saveStatusInfo(statusInfo);
    return statusInfo;
  }

  @Scheduled(cron = "0 0/1 * * * ?")
  public void checkForCompletedRun() {
    String currentRunId = null;
    if (shutDownFlag) {
      currentRunId = oneTimeRunId;
      //Check if we have already started the run
      List<StatusInfo> currentRun = dmeSyncWorkflowService.getService(access).findStatusInfoByRunIdAndDoc(currentRunId, doc);
      if(CollectionUtils.isEmpty(currentRun))
        return;
    } else {
      StatusInfo latest = null;
      if(createSoftlink) {
    	  latest = dmeSyncWorkflowService.getService(access).findTopStatusInfoByDocOrderByStartTimestampDesc(doc);
      } else {
	      //Add base path also to distinguish multiple docs running the workflow.
	      latest = dmeSyncWorkflowService.getService(access).findTopStatusInfoByDocAndOriginalFilePathStartsWithOrderByStartTimestampDesc(doc, syncBaseDir);
	  }
      if(latest != null)
        currentRunId = latest.getRunId();
    }

    //Check to make sure scheduler is completed, run has occurred and the queue is empty
    if (runId == null
        && currentRunId != null
        && !currentRunId.isEmpty()
        && sender.getQueueCount("inbound.queue") == 0) {

      //check if the latest export file is generated in log directory
      Path path = Paths.get(logFile);
      final String fileName = path.getParent().toString() + File.separatorChar + currentRunId + ".xlsx";
      File excel = new File(fileName);
      if (!excel.exists()) {
        //Export and send email for completed run
        dmeSyncMailServiceFactory.getService(doc).sendResult(currentRunId);

        if (shutDownFlag) {
          logger.info("[Scheduler] Queue is empty. Shutting down the application.");
          DmeSyncApplication.shutdown();
        }
      }
    }
  }

  private int daysBetween(Date d1, Date d2) {
    return (int) ((d2.getTime() - d1.getTime()) / (1000 * 60 * 60 * 24));
  }
}
