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
import java.util.stream.Collectors;
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
import gov.nih.nci.hpc.dmesync.workflow.impl.DmeSyncAWSScanDirectory;
import gov.nih.nci.hpc.dmesync.workflow.impl.DmeSyncDataObjectListQuery;
import gov.nih.nci.hpc.dmesync.workflow.impl.DmeSyncVerifyTaskImpl;
import gov.nih.nci.hpc.exception.HpcException;
import gov.nih.nci.hpc.dmesync.DmeSyncApplication;
import gov.nih.nci.hpc.dmesync.DmeSyncMailServiceFactory;
import gov.nih.nci.hpc.dmesync.DmeSyncWorkflowServiceFactory;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.dto.DmeSyncMessageDto;
import gov.nih.nci.hpc.dmesync.jms.DmeSyncConsumer;
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
  @Autowired private DmeSyncConsumer consumer;
  @Autowired private DmeSyncMailServiceFactory dmeSyncMailServiceFactory;
  @Autowired private DmeSyncWorkflowServiceFactory dmeSyncWorkflowService;
  @Autowired private DmeSyncDataObjectListQuery dmeSyncDataObjectListQuery;
  @Autowired private DmeSyncAWSScanDirectory dmeSyncAWSScanDirectory;
  @Autowired private DmeSyncVerifyTaskImpl dmeSyncVerifyTaskImpl;

  @Value("${dmesync.db.access:local}")
  private String access;
  
  @Value("${dmesync.doc.name}")
  private String doc;
  
  @Value("${dmesync.source.base.dir}")
  private String syncBaseDir;

  @Value("${dmesync.source.base.dir.folders:}")
  private String syncBaseDirFolders;

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
  

  @Value("${dmesync.multiple.tars.files.count:0}")
  private Integer filesPerTar;

  @Value("${dmesync.depth.leaf.folder.skip:true}")
  private boolean skipIfNotLeafFolder;

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
  
  @Value("${dmesync.file.noArchive.exist:}")
  private String checkNoArchiveExistsFile;
  
  @Value("${dmesync.file.archive.exist:}")
  private String checkArchiveExistsFile;
  
  @Value("${dmesync.tar.file.exist.ext:}")
  private String checkExistsFileExt;
  
  @Value("${dmesync.multiple.tars.dir.folders:}")
  private String multpleTarsFolders;
  
  @Value("${dmesync.process.multiple.tars:false}")
  private boolean processMultpleTars;

  @Value("${dmesync.file.exist.under.basedir:false}")
  private boolean checkExistsFileUnderBaseDir;
  
  @Value("${dmesync.file.exist.under.basedir.depth:0}")
  private String checkExistsFileUnderBaseDirDepth;
  
  @Value("${logging.file.name}")
  private String logFile;

  @Value("${dmesync.create.softlink:false}")
  private boolean createSoftlink;
  
  @Value("${dmesync.move.processed.files:false}")
  private boolean moveProcessedFiles;
  
  @Value("${dmesync.noscan.rerun:false}")
  private boolean noScanRerun;
  
  @Value("${dmesync.source.softlink.file:}")
  private String sourceSoftlinkFile;
  
  @Value("${dmesync.source.aws:false}")
  private boolean awsFlag;
  
  @Value("${dmesync.source.aws.bucket:}")
  private String awsBucket;

  @Value("${dmesync.source.aws.access.key:}")
  private String awsAccessKey;

  @Value("${dmesync.source.aws.secret.key:}")
  private String awsSecretKey;
  
  @Value("${dmesync.source.aws.region:}")
  private String awsRegion;
  
  @Value("${dmesync.tar.contents.file:false}")
  private boolean createTarContentsFile;
  
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
      } else if (noScanRerun) {
        findFilesToRerun();
        logger.info(
            "[Scheduler] Completed restaring files at {} for Run ID: {} base directory to reprocess {}",
            dateFormat.format(new Date()),
            runId,
            syncBaseDir);
        MDC.clear();
        return;
      } else if (awsFlag) {
    	  paths = dmeSyncAWSScanDirectory.getPathAttributes(syncBaseDir);
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
            if (tar && depth.equals("-1") && skipIfNotLeafFolder) {
              try(Stream<Path> stream = Files.list(Paths.get(pathAttr.getAbsolutePath()))) {
               if(stream.anyMatch(x -> x.toFile().isDirectory()))
                  continue;
              }
            }
			if (processMultpleTars) {
				// Only add the folder if the folder is not empty.
				File folder = new File(pathAttr.getAbsolutePath());
				if (folder.list() != null && folder.list().length > 0) {
					folders.add(pathAttr);
				} else {
					logger.info("[Scheduler] There are no files in the Folder  {}", pathAttr.getAbsolutePath());
				}
			} else {
				folders.add(pathAttr);
			}
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
      //Check to see if any records are being processed for this run, if not send email
      List<StatusInfo> currentRun = dmeSyncWorkflowService.getService(access).findStatusInfoByRunIdAndDoc(runId, doc);
      String emailBody= "There were no files/folders found for processing"+(!StringUtils.isEmpty(syncBaseDirFolders)?" in "+syncBaseDirFolders+" folders":"")+ ".";
      if(CollectionUtils.isEmpty(currentRun))
    	  dmeSyncMailServiceFactory.getService(doc).sendMail("HPCDME Auto Archival Result for " + doc + " - Base Path: " + syncBaseDir,
    			  emailBody);
      
    } catch (Exception e) {
      //Send email notification
	  logger.error("[Scheduler] Failed to access files in directory, {}", syncBaseDir, e);
	  dmeSyncMailServiceFactory.getService(doc).sendMail("HPCDME Auto Archival Error: " + e.getMessage(),
				e.getMessage() + "\n\n" + e.getCause().getMessage());
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
                dmeSyncWorkflowService.getService(access).findAllStatusInfoLikeOriginalFilePath(syncBaseDir + '%');
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
  
  /**
   * Pickup files to rerun from Database instead of file scan
   */
  private void findFilesToRerun() {

    List<String> syncBaseDirFolderList =
        syncBaseDirFolders == null || syncBaseDirFolders.isEmpty()
        ? null
            : new ArrayList<>(Arrays.asList(syncBaseDirFolders.split(",")));

    if (syncBaseDir != null) {
      int queryCount = syncBaseDirFolderList == null? 1: syncBaseDirFolderList.size();
      String queryPath = null;
      for(int i = 0; i < queryCount; i++) {
        if(syncBaseDirFolderList == null)
          queryPath = syncBaseDir;
        else
          queryPath = syncBaseDir + File.separatorChar + syncBaseDirFolderList.get(i);

        List<StatusInfo> statusInfoList =
            dmeSyncWorkflowService.getService(access).findAllStatusInfoLikeOriginalFilePath(queryPath+'%');

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
      }
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
    List<String> syncBaseDirFolderList =
    		syncBaseDirFolders == null || syncBaseDirFolders.isEmpty()
                ? null
                : new ArrayList<>(Arrays.asList(syncBaseDirFolders.split(",")));
    
    if (syncBaseDir != null) {
      int scanCount = syncBaseDirFolderList == null? 1: syncBaseDirFolderList.size();
      String scanDir = null;
      for(int i = 0; i < scanCount; i++) {
    	  if(syncBaseDirFolderList == null)
    		  scanDir = syncBaseDir;
    	  else
    		  scanDir = syncBaseDir + File.separatorChar + syncBaseDirFolderList.get(i);
	      if(tar && Integer.parseInt(depth) == 0) {
	    	  result = toHpcPathAttribute(scanDir);
	    	  return result;
	      }
	      result.addAll(impl.getPathAttributes(
	        		  scanDir,
	              excludePatterns,
	              includePatterns,
	              tar ? Integer.parseInt(depth) : untar ? Integer.parseInt(depth) + 1 : 0));
      }
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
		} else if (tar && filesPerTar > 0  && multpleTarsFolders != null
				&& StringUtils.containsIgnoreCase( multpleTarsFolders, file.getName())) {
			logger.info("checking if all the Multiple Tars got uploaded {}",file.getAbsolutePath());
			List<StatusInfo> mulitpleTarRequests = dmeSyncWorkflowService.getService(access)
					.findAllByDocAndLikeOriginalFilePath(doc,file.getAbsolutePath() + '%');
			
			if (!mulitpleTarRequests.isEmpty()) {
				// Retrieve the original Tar object where multiple tars are created mainly for rerun 
				statusInfo = dmeSyncWorkflowService.getService(access)
						.findTopStatusInfoByDocAndSourceFilePath(doc,
								file.getAbsolutePath());
				List<StatusInfo> statusInfoNotCompletedList = mulitpleTarRequests.stream().filter(c -> c.getStatus() == null)
						.collect(Collectors.toList());
				if (!statusInfoNotCompletedList.isEmpty() || ((statusInfo!=null && statusInfo.getTarContentsCount()>0))) {
					// use the same status Info rows with new Run Id for reupload
					for (StatusInfo object : statusInfoNotCompletedList) {
						if (object != null) {
							// Update the run_id and reset the retry count and errors
							object.setRunId(runId);
							object.setError("");
							object.setRetryCount(0L);
							object = dmeSyncWorkflowService.getService(access).saveStatusInfo(object);
							// Delete the metadata info created for this object ID
							dmeSyncWorkflowService.getService(access).deleteMetadataInfoByObjectId(object.getId());
						}
					}
					// if all the records are not completed send the original Folder object Id to JMS queue.
					dmeSyncWorkflowService.getService(access).deleteTaskInfoByObjectId(statusInfo.getId());
					// Send the incomplete objectId to the message queue for processing
					DmeSyncMessageDto message = new DmeSyncMessageDto();
					statusInfo.setRunId(runId);
					statusInfo.setError("");
					statusInfo.setRetryCount(0L);
					statusInfo = dmeSyncWorkflowService.getService(access).saveStatusInfo(statusInfo);
					message.setObjectId(statusInfo.getId());
					sender.send(message, "inbound.queue");
					continue;

				} else {
					// if all the records are completed no rerun needed.
					logger.info(
				            "[Scheduler] All the Multiple tar files for the folder got uploaded",
				            file.getAbsolutePath());
				}
			// no records for tar folder set status Info to null
			} else {
				statusInfo = null;
			}

		}else if (createTarContentsFile) {
			// to check the status of the folder itself instead of the the contents file record.
			StatusInfo tarFolderRecord =
		              dmeSyncWorkflowService.getService(access).findFirstStatusInfoByOriginalFilePathAndSourceFilePath(
		                  file.getAbsolutePath(), "TarContentsFile.txt");
			if(tarFolderRecord!=null)
			statusInfo = "COMPLETED".equals(tarFolderRecord.getStatus())? tarFolderRecord:null;
			
		}
		else {
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
          if(uploadedTimestamp == null || uploadedTimestamp.compareTo(modifiedTimestamp) > 0) {
        	  //There are some files where uploadedTimestamp is null so ignore these for now.
        	  //Modified is before the last upload
        	  continue;
          }
          //Modified after the last upload, so we need to re-upload
        } else {
        	statusInfo =
                    dmeSyncWorkflowService.getService(access).findFirstStatusInfoByOriginalFilePathOrderByStartTimestampDesc(
                        file.getAbsolutePath());
        	if(createTarContentsFile) {
        		statusInfo =
                        dmeSyncWorkflowService.getService(access).findFirstStatusInfoByOriginalFilePathAndSourceFilePath(
                            file.getAbsolutePath(),"TarContentsFile.txt");
        	}
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
    				String message ="The directory " + folder.toString() + " does not contain file " + checkExistsFile;
    	            logger.info(
    	              "[Scheduler] Skipping: {} folder which does not contain the specified file {}.",
    	              folder.toString(),
    	              checkExistsFile);
    	            //TBD: Check if we need to insert record in DB as COMPLETED before sending an email.
    	            dmeSyncMailServiceFactory.getService(doc).sendMail("WARNING: HPCDME during registration", message);
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
      
      //If folder under the base dir does contain a non_Archived specified file for both files and tar, skip
      if (checkExistsFileUnderBaseDir 
    		  && (StringUtils.isNotEmpty(checkNoArchiveExistsFile) && StringUtils.isNotEmpty(checkArchiveExistsFile))) {
        
        //Find the directory being archived under the base dir
        Path baseDirPath = Paths.get(syncBaseDir).toRealPath();
        Path filePath = Paths.get(file.getAbsolutePath());
        Path relativePath = baseDirPath.relativize(filePath);
        Path subPath1 = relativePath.subpath(0, Integer.parseInt(checkExistsFileUnderBaseDirDepth)+1);
        Path checkExistFilePath = baseDirPath.resolve(subPath1);
        
        if(StringUtils.isNotEmpty(checkNoArchiveExistsFile)) {
	        try (DirectoryStream<Path> stream = Files.newDirectoryStream(checkExistFilePath, path -> path.getFileName().toString().equals(checkNoArchiveExistsFile))) {
	
	          if (stream.iterator().hasNext()) {
	        	  
		        String message ="The directory " + file.getAbsolutePath() + " does contain any " + checkNoArchiveExistsFile  + "file";
	            logger.info(
	              "[Scheduler] Skipping: {} Folder to process does contain the specified file {}.",
	              checkExistFilePath,
	              checkNoArchiveExistsFile
	              );
	            dmeSyncMailServiceFactory.getService(doc).sendMail("WARNING: HPCDME during registration", message);

	            continue;
	          }else {
	        	  try (DirectoryStream<Path> streamCheck = Files.newDirectoryStream(checkExistFilePath, path -> path.getFileName().toString().equals(checkArchiveExistsFile))) {
	  		          String message ="The directory " + file.getAbsolutePath() + " does not contain any " + checkArchiveExistsFile + "file" ;	
	    	          if (!streamCheck.iterator().hasNext()) {
	    	            logger.info(
	    	              "[Scheduler] Skipping: {} Folder to process does contain the specified file {}.",
	    	              checkExistFilePath,
	    	              checkArchiveExistsFile);
	    	            dmeSyncMailServiceFactory.getService(doc).sendMail("WARNING: HPCDME during registration", message);
	    	            continue;
	        	  
	          }
	        }  catch (IOException ex) {
	          throw new Exception("Error while listing directory: " + checkExistFilePath, ex);
	        }
        }
      
        }catch (IOException ex) {
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
	if(awsFlag) return;
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
    

    logger.info("checking if scheduler is completed with queue count {} and active threads {} ", sender.getQueueCount("inbound.queue"), consumer.isProcessing())

    //Check to make sure scheduler is completed, run has occurred and the queue is empty
    if (runId == null
        && currentRunId != null
        && !currentRunId.isEmpty()
        && sender.getQueueCount("inbound.queue") == 0
        && !consumer.isProcessing()) {

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

  @Scheduled(cron = "0 0/1 * * * ?")
  public void checkForAWSCompletedRun() {
	if(!awsFlag) return;
	
	boolean completedFlag = true;
    String currentRunId = null;
    if (shutDownFlag) {
      currentRunId = oneTimeRunId;
      //Check if we have already started the run
      List<StatusInfo> currentRun = dmeSyncWorkflowService.getService(access).findStatusInfoByRunIdAndDoc(currentRunId, doc);
      if(CollectionUtils.isEmpty(currentRun))
        return;
    } else {
      StatusInfo latest = null;
      //Add base path also to distinguish multiple docs running the workflow.
      latest = dmeSyncWorkflowService.getService(access).findTopStatusInfoByDocAndOriginalFilePathStartsWithOrderByStartTimestampDesc(doc, syncBaseDir);

      if(latest != null)
        currentRunId = latest.getRunId();
    }

    //Check to make sure scheduler is completed, run has occurred and the queue is empty
    if (runId == null
        && currentRunId != null
        && !currentRunId.isEmpty()
        && sender.getQueueCount("inbound.queue") == 0
        && !consumer.isProcessing()) {

      //check if the latest export file is generated in log directory
      Path path = Paths.get(logFile);
      final String fileName = path.getParent().toString() + File.separatorChar + currentRunId + ".xlsx";
      File excel = new File(fileName);
      if (!excel.exists()) {
    	//Check if all the files have been processed by DME
    	List<StatusInfo> currentRun = dmeSyncWorkflowService.getService(access).findStatusInfoByRunIdAndDoc(currentRunId, doc);
    	for(StatusInfo statusInfo: currentRun) {
    		try {
				dmeSyncVerifyTaskImpl.processTask(statusInfo);
			} catch (Exception e) {
				logger.error("[Scheduler] Verify task for AWS completion error " + statusInfo.getSourceFilePath(), e.getMessage());
			}
    		if(StringUtils.contains(statusInfo.getError(), "Data_transfer_status is not in ARCHIVED"))
    			completedFlag = false;
    	}
    	
        //Export and send email for completed run
        if(completedFlag)
        	dmeSyncMailServiceFactory.getService(doc).sendResult(currentRunId);

        if (shutDownFlag && completedFlag) {
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
