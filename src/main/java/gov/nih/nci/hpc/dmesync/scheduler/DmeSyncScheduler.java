package gov.nih.nci.hpc.dmesync.scheduler;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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

import gov.nih.nci.hpc.dmesync.util.DmeMetadataBuilder;
import gov.nih.nci.hpc.dmesync.util.HpcLocalDirectoryListQuery;
import gov.nih.nci.hpc.dmesync.util.HpcPathAttributes;
import gov.nih.nci.hpc.dmesync.util.TarUtil;
import gov.nih.nci.hpc.dmesync.util.WorkflowConstants;
import gov.nih.nci.hpc.dmesync.workflow.impl.DmeSyncAWSScanDirectory;
import gov.nih.nci.hpc.dmesync.workflow.impl.DmeSyncDataObjectListQuery;
import gov.nih.nci.hpc.dmesync.workflow.impl.DmeSyncVerifyTaskImpl;
import gov.nih.nci.hpc.exception.HpcException;
import gov.nih.nci.hpc.dmesync.DmeSyncApplication;
import gov.nih.nci.hpc.dmesync.DmeSyncMailServiceFactory;
import gov.nih.nci.hpc.dmesync.DmeSyncWorkflowServiceFactory;
import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.domain.WorkflowRunInfo;
import gov.nih.nci.hpc.dmesync.dto.DmeSyncMessageDto;
import gov.nih.nci.hpc.dmesync.jms.DmeSyncConsumer;
import gov.nih.nci.hpc.dmesync.jms.DmeSyncProducer;
import gov.nih.nci.hpc.dmesync.service.DmeSyncWorkflowRunLogService;
import gov.nih.nci.hpc.dmesync.service.DocConfigService;

/**
 * DME Sync Scheduler to scan for files to be Archived
 * 
 * @author dinhys
 *
 */
@Component
public class DmeSyncScheduler implements DocWorkflowExecutor {

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
  @Autowired private DmeMetadataBuilder dmeMetadataBuilder;
  @Autowired private DmeSyncWorkflowRunLogService dmeSyncWorkflowRunLogService;
  @Autowired private DocConfigService configService;

  @Value("${dmesync.db.access:local}")
  private String access;
  
  @Value("${dmesync.run.once.and.shutdown:false}")
  private boolean shutDownFlag;

  @Value("${dmesync.run.once.run_id:}")
  private String oneTimeRunId;

  @Value("${dmesync.file.noArchive.exist:}")
  private String checkNoArchiveExistsFile;
  
  @Value("${dmesync.file.archive.exist:}")
  private String checkArchiveExistsFile;
  
  @Value("${logging.file.name}")
  private String logFile;
  
  @Value("${dmesync.source.aws.bucket:}")
  private String awsBucket;

  @Value("${dmesync.source.aws.access.key:}")
  private String awsAccessKey;

  @Value("${dmesync.source.aws.secret.key:}")
  private String awsSecretKey;
  
  @Value("${dmesync.source.aws.region:}")
  private String awsRegion;
  
  private String runId;

  /**
   * Main scheduler method to crawl the file system and find files to enqueue
   */
  @Override
  public void execute(DocConfig config) {
	  
	// Example: select tasks based on config
	DocConfig.SourceConfig sourceConfig = config.getSourceConfig();
	DocConfig.SourceRule sourceRule = config.getSourceRule();
	DocConfig.PreprocessingConfig pre = config.getPreprocessingConfig();
	DocConfig.PreprocessingRule preRule = config.getPreprocessingRule();
	DocConfig.UploadConfig upload = config.getUploadConfig();
	  
	dmeMetadataBuilder.evictMetadataMap();

	if (upload.moveProcessedFiles) {
		findFilesToMove(config);
		return;
	}
	
    // Make sure the work dir is different from the source dir so we don't cleanup any original files.
	if ((pre.tar || pre.untar) && (sourceConfig.workBaseDir == null
			|| Paths.get(sourceConfig.sourceBaseDir).startsWith(sourceConfig.workBaseDir))) {
      logger.error("[Scheduler] work directory must be different from the source dir");
      return;
    }
    
    if ((pre.tar && pre.untar)){
      logger.error("[Scheduler] Both tar and untar cannot be specified at the same time.");
      return;
    }
    
    runId = "Run_" + timestampFormat.format(new Date());
    
    WorkflowRunInfo workflowRunInfo=insertWorkflowRunInfo(config);
	  logger.info(
		        "[Scheduler] Workflow Run Information is inserted {}", workflowRunInfo);

    if (shutDownFlag) {
      //check if the one time run has already occurred
      List<StatusInfo> statusInfo = dmeSyncWorkflowService.getService(access).findStatusInfoByRunIdAndDoc(oneTimeRunId, config.getDocName());
      //If it has been called already, return
      if (!CollectionUtils.isEmpty(statusInfo)) {
        return;
      }
    }

    MDC.put("doc", config.getDocName());
    MDC.put("run.id", runId);

    if(upload.softlink)
    	 logger.info(
    		        "[Scheduler] Current time: {} executing Run ID: {} softlink file to read {}",
    		        dateFormat.format(new Date()),
    		        runId,
    		        upload.softlinkFile);
    else if(upload.collectionSoftlink) {
    	if(StringUtils.isNotBlank(upload.softlinkFile)) {
    		logger.info(
   		        "[Scheduler] Current time: {} executing Run ID: {} collection softlink file to read {}",
   		        dateFormat.format(new Date()),
   		        runId,
   		        upload.softlinkFile);
    	} else {
    		logger.info(
		        "[Scheduler] Current time: {} executing Run ID: {} collection softlink directory to scan {}",
		        dateFormat.format(new Date()),
		        runId,
		        sourceConfig.sourceBaseDir);
    	}
    }
    else
	    logger.info(
	        "[Scheduler] Current time: {} executing Run ID: {} base directory to scan {}",
	        dateFormat.format(new Date()),
	        runId,
	        sourceConfig.sourceBaseDir);

    // The scheduler will scan through the specified directories to find candidate for archival.
    // Find the eligible file/directory,Include/Exclude or existence of a file, modified date etc.
    // If it finds a candidate, it checks the local db to see if it has been completed.
    // If not, then it inserts the data and sends the details to the message queue for processing.

    try {
      List<HpcPathAttributes> paths = null;
      if(upload.softlink) {
    	  paths = queryDataObjectsForSoftlinkCreation(config);
      } else if (sourceRule.noscanRerun) {
        findFilesToRerun(config);
        logger.info(
            "[Scheduler] Completed restaring files at {} for Run ID: {} base directory to reprocess {}",
            dateFormat.format(new Date()),
            runId,
            sourceConfig.sourceBaseDir);
        MDC.clear();
        return;
      } else if (sourceRule.aws) {
    	  paths = dmeSyncAWSScanDirectory.getPathAttributes(sourceConfig.sourceBaseDir, config);
      } else {
      // Scan through the specified base directory and find candidates for processing
    	  paths = scanDirectory(config);
      }
      
      List<HpcPathAttributes> folders = new ArrayList<>();
      List<HpcPathAttributes> files = new ArrayList<>();
      if (paths != null && paths.isEmpty()) {
        logger.info("[Scheduler] No files/folders found for runID: {}", runId);
        MDC.clear();
        return;
      } else {
        for (HpcPathAttributes pathAttr : paths) {
          if (pathAttr.getIsDirectory() && !upload.collectionSoftlink) {
            //If depth of -1 is specified, skip if it is not a leaf folder
            if (pre.tar && pre.depth.equals(-1) && preRule.tarSkipNonLeafFolder) {
              try(Stream<Path> stream = Files.list(Paths.get(pathAttr.getAbsolutePath()))) {
               if(stream.anyMatch(x -> x.toFile().isDirectory()))
                  continue;
              }
            }
			// Only add the folder if the folder is not empty.
			File folder = new File(pathAttr.getAbsolutePath());
			String[] children = folder.list();
			if (children == null) {
				logger.warn("[Scheduler] Unable to list files in the Folder {}. It may be unreadable or inaccessible.",
						pathAttr.getAbsolutePath());
			} else if (children.length > 0) {
				folders.add(pathAttr);
			} else {
				logger.info("[Scheduler] There are no files in the Folder  {}", pathAttr.getAbsolutePath());
			}
          } else {
            files.add(pathAttr);
          }
        }
      }

   // --- Selective Scan Enhancement ---
      if (sourceRule.selectiveScan) {
       selectiveScanProcessing(folders, files, config);
      }
      
      // If tar is required, need to manipulate the list.
      else if (pre.tar) {
        processFiles(folders, config);
      } else if (pre.untar) {
        //If untar is required, need to find the files included in tar.
        for (HpcPathAttributes file : files) {
          //List tar files from each folder
          List<HpcPathAttributes> tarFileList = TarUtil.listTar(file.getAbsolutePath());
          processFiles(tarFileList, config);
        }
      } else {
        // Process the list of files
    	  if(upload.collectionSoftlink) {
    		  for (HpcPathAttributes file : files) {
    	          //List tar files from each folder
    	          List<HpcPathAttributes> collectionLinkList = listCollectionsToLink(file.getAbsolutePath());
    	          processFiles(collectionLinkList, config);
    	        }
    		  
    	  } else
    		  processFiles(files, config);
      }
	   if (sourceRule.retryPriorRunFailures) {
			includePriorRunFailuresInCurrentRunWorklist(config);
		}
      logger.info(
          "[Scheduler] Completed file scan at {} for Run ID: {} base directory to scan {}",
          dateFormat.format(new Date()),
          runId,
          sourceConfig.sourceBaseDir);
      //Check to see if any records are being processed for this run, if not send email
      List<StatusInfo> currentRun = dmeSyncWorkflowService.getService(access).findStatusInfoByRunIdAndDoc(runId, config.getDocName());
      String emailBody= "There were no files/folders found for processing"+(!StringUtils.isEmpty(sourceRule.sourceBaseDirFolders)?" in "+sourceRule.sourceBaseDirFolders+" folders":"")+ ".";
      if(CollectionUtils.isEmpty(currentRun)) {
    	  logger.info("[Scheduler] No files/folders found for RunID." + runId + " Doc "+ config.getDocName());
    	  dmeSyncMailServiceFactory.getService(config.getDocName()).sendMail("HPCDME Auto Archival Result for " + config.getDocName() + " - Base Path: " + sourceConfig.sourceBaseDir,
    			  emailBody);
          try {
              dmeSyncWorkflowRunLogService.updateWorkflowRunEnd(runId, config.getDocName(), WorkflowConstants.RunStatus.SKIPPED.toString(),null);
            } catch (IllegalArgumentException e) {
              logger.warn("[Scheduler] Workflow run not found when updating run end to SKIPPED for runId: {}, doc: {}", runId, config.getDocName(), e);
            }
		if (shutDownFlag) {
			logger.info("[Scheduler] No files/folders found. Shutting down the application.");
			DmeSyncApplication.shutdown();
		}
      }
    } catch (Exception e) {
      //Send email notification
	  logger.error("[Scheduler] Failed to access files in directory, {}", sourceConfig.sourceBaseDir, e);
	  dmeSyncMailServiceFactory.getService(config.getDocName()).sendMail("HPCDME Auto Archival Error: " + e.getMessage(),
				e.getMessage() + "\n\n" + e.getCause().getMessage());
    } finally {
      MDC.clear();
      runId = null;
    }
  }

  /**
   * Temporary scheduler to move SB Single Cell fastq from Sample to Run directory
   */
  private void findFilesToMove(DocConfig config) {
  
    DocConfig.SourceConfig sourceConfig = config.getSourceConfig();
    runId = shutDownFlag ? oneTimeRunId : "Run_" + timestampFormat.format(new Date());

    if (shutDownFlag) {
      //check if the one time run has already occurred
      List<StatusInfo> statusInfo = dmeSyncWorkflowService.getService(access).findStatusInfoByRunIdAndDoc(oneTimeRunId, config.getDocName());
      //If it has been called already, return
      if (!CollectionUtils.isEmpty(statusInfo)) {
        return;
      }
    }

    MDC.put("doc", config.getDocName());
    MDC.put("run.id", runId);

    logger.info(
        "[Scheduler] Current time: {} executing Run ID: {} base directory {}, restarting files from DB",
        dateFormat.format(new Date()),
        runId,
        sourceConfig.sourceBaseDir);

    try {

      List<StatusInfo> statusInfoList =
                dmeSyncWorkflowService.getService(access).findAllStatusInfoLikeOriginalFilePath(sourceConfig.sourceBaseDir + '%');
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
	        message.setDocConfigId(config.getId());
	        sender.send(message, "inbound.queue");
	      }
      }

      logger.info(
          "[Scheduler] Completed restaring files at {} for Run ID: {} base directory to reprocess {}",
          dateFormat.format(new Date()),
          runId,
          sourceConfig.sourceBaseDir);

    } catch (Exception e) {
      logger.error("[Scheduler] Failed to restart files, {}", sourceConfig.sourceBaseDir, e);
    } finally {
      MDC.clear();
      runId = null;
    }
  }
  
  /**
   * Pickup files to rerun from Database instead of file scan
   */
  private void findFilesToRerun(DocConfig config) {

	DocConfig.SourceConfig sourceConfig = config.getSourceConfig();
	DocConfig.SourceRule sourceRule = config.getSourceRule();
	
    List<String> syncBaseDirFolderList =
    		sourceRule.sourceBaseDirFolders == null || sourceRule.sourceBaseDirFolders.isEmpty()
        ? null
            : new ArrayList<>(Arrays.asList(sourceRule.sourceBaseDirFolders.split(",")));

    if (sourceConfig.sourceBaseDir != null) {
      int queryCount = syncBaseDirFolderList == null? 1: syncBaseDirFolderList.size();
      String queryPath = null;
      for(int i = 0; i < queryCount; i++) {
        if(syncBaseDirFolderList == null)
          queryPath = sourceConfig.sourceBaseDir;
        else
          queryPath = sourceConfig.sourceBaseDir + File.separatorChar + syncBaseDirFolderList.get(i);

        List<StatusInfo> statusInfoList =
            dmeSyncWorkflowService.getService(access).findAllStatusInfoLikeOriginalFilePath(queryPath+'%');

        for(StatusInfo statusInfo : statusInfoList) {
          if(statusInfo != null) {
            //Update the run_id and reset the retry count and errors
            statusInfo.setRunId(runId);
            statusInfo.setError("");
            statusInfo.setRetryCount(0L);
            statusInfo.setEndWorkflow(false);
            statusInfo = dmeSyncWorkflowService.getService(access).saveStatusInfo(statusInfo);
            // Delete the metadata info created for this object ID
            dmeSyncWorkflowService.getService(access).deleteMetadataInfoByObjectId(statusInfo.getId());
            // Send the incomplete objectId to the message queue for processing
            DmeSyncMessageDto message = new DmeSyncMessageDto();
            message.setObjectId(statusInfo.getId());
            message.setDocConfigId(config.getId());
            sender.send(message, "inbound.queue");
          }
        }
      }
    }

  }
  
  private List<HpcPathAttributes> scanDirectory(DocConfig config) throws HpcException {
    
	DocConfig.PreprocessingConfig pre = config.getPreprocessingConfig();
	DocConfig.SourceConfig sourceConfig = config.getSourceConfig();
	DocConfig.SourceRule sourceRule = config.getSourceRule();
	  
    HpcLocalDirectoryListQuery impl = new HpcLocalDirectoryListQuery();
    List<HpcPathAttributes> result = new ArrayList<>();
    List<String> excludePatterns =
    		sourceRule.excludePattern == null || sourceRule.excludePattern.isEmpty()
            ? null
            : new ArrayList<>(Arrays.asList(sourceRule.excludePattern.split(",")));
    List<String> includePatterns =
    		sourceRule.includePattern == null || sourceRule.includePattern.isEmpty()
            ? null
            : new ArrayList<>(Arrays.asList(sourceRule.includePattern.split(",")));
    List<String> syncBaseDirFolderList =
    		sourceRule.sourceBaseDirFolders == null || sourceRule.sourceBaseDirFolders.isEmpty()
                ? null
                : new ArrayList<>(Arrays.asList(sourceRule.sourceBaseDirFolders.split(",")));
    
    if (sourceConfig.sourceBaseDir != null) {
      int scanCount = syncBaseDirFolderList == null? 1: syncBaseDirFolderList.size();
      String scanDir = null;
      for(int i = 0; i < scanCount; i++) {
    	  if(syncBaseDirFolderList == null)
    		  scanDir = sourceConfig.sourceBaseDir;
    	  else
    		  scanDir = sourceConfig.sourceBaseDir + File.separatorChar + syncBaseDirFolderList.get(i);
	      if(pre.tar && pre.depth == 0) {
	    	  result = toHpcPathAttribute(scanDir);
	    	  return result;
	      }
	      result.addAll(impl.getPathAttributes(
	        		  scanDir,
	              excludePatterns,
	              includePatterns,
	              pre.tar ? pre.depth : pre.untar ? pre.depth + 1 : 0));
      }
    }
    return result;
  }
  
  private List<HpcPathAttributes> queryDataObjectsForSoftlinkCreation(DocConfig config) throws HpcException, IOException {
	DocConfig.UploadConfig upload = config.getUploadConfig();
    List<HpcPathAttributes> result = new ArrayList<>();
    Path filePath = Paths.get(upload.softlinkFile);
    List<String> lines = Files.readAllLines(filePath);
    //process each collection
    for(String collectionPath: lines) {
      result.addAll(dmeSyncDataObjectListQuery.getPathAttributes(collectionPath, config));
    }
    return result;
  }
  
  private List<HpcPathAttributes> listCollectionsToLink(String sourceSoftlinkFile) throws HpcException, IOException {
	    List<HpcPathAttributes> result = new ArrayList<>();
	    Path filePath = Paths.get(sourceSoftlinkFile);
	    List<String> lines = Files.readAllLines(filePath);
	    //process each collection
	    for(String collectionPath: lines) {
	      result.add(getPathAttributesOfCollection(sourceSoftlinkFile, collectionPath));
	    }
	    return result;
	  }

  /**
   * Get path attributes of a collection to link.
   * 
   * @param collectionPath The collection path
   * @return pathAttributes
   * @throws HpcException The exception
   */
  private HpcPathAttributes getPathAttributesOfCollection(String sourceSoftlinkFile, String collectionPath) {
	HpcPathAttributes pathAttributes = new HpcPathAttributes();
	pathAttributes.setName(Paths.get(collectionPath).getFileName().toString());
	pathAttributes.setPath(sourceSoftlinkFile);
	pathAttributes.setAbsolutePath(collectionPath);
	pathAttributes.setIsDirectory(true);
	return pathAttributes;
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

  private void processFiles(List<HpcPathAttributes> files, DocConfig config) throws Exception {

	DocConfig.PreprocessingConfig pre = config.getPreprocessingConfig();
	DocConfig.PreprocessingRule preRule = config.getPreprocessingRule();
	DocConfig.SourceConfig sourceConfig = config.getSourceConfig();
	DocConfig.SourceRule sourceRule = config.getSourceRule();
	DocConfig.UploadConfig upload = config.getUploadConfig();

    for (HpcPathAttributes file : files) {

      StatusInfo statusInfo = null;

      //If we need to verify previous upload, check
      if ("local".equals(upload.verifyPrevUpload)) {
        // Checks the local db to see if it has been completed
        if (pre.untar) {
          statusInfo =
              dmeSyncWorkflowService.getService(access).findFirstStatusInfoByOriginalFilePathAndSourceFileNameAndStatus(
                  file.getAbsolutePath(), file.getTarEntry(), "COMPLETED");
		} else if ( preRule.processMultipleTars && 
				   TarUtil.matchesAnyMultipleTarFolder( preRule.multipleTarsDirFolders , file.getName() )) {
			logger.info("checking if all the Multiple Tars got uploaded {}",file.getAbsolutePath());
			List<StatusInfo> mulitpleTarRequests = dmeSyncWorkflowService.getService(access)
					.findAllByDocAndLikeOriginalFilePath(config.getDocName(),file.getAbsolutePath() + '%');
			
			if (!mulitpleTarRequests.isEmpty()) {
				// Retrieve the original Tar object where multiple tars are created mainly for rerun 
				statusInfo = dmeSyncWorkflowService.getService(access)
						.findTopStatusInfoByDocAndSourceFilePath(config.getDocName(),
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
							object.setEndWorkflow(false);
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
					statusInfo.setEndWorkflow(false);
					statusInfo = dmeSyncWorkflowService.getService(access).saveStatusInfo(statusInfo);
					message.setObjectId(statusInfo.getId());
					message.setDocConfigId(config.getId());
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

		}else if (preRule.tarContentsFile) {
			// to check the status of the folder and the contents
			// file(included/excludedfiles record.
			logger.info("checking if all the folder and contents file got uploaded {}", file.getAbsolutePath());

			List<StatusInfo> tarFolderRequests = dmeSyncWorkflowService.getService(access)
					.findAllByDocAndLikeOriginalFilePath(config.getDocName(), file.getAbsolutePath() + '%');

			if (tarFolderRequests.isEmpty()) {
				// No records for the path folder in database; running this folder for first time
				statusInfo = null;

			} else {
				// Find the TAR file record
				Optional<StatusInfo> tarRecordOpt = tarFolderRequests.stream()
						.filter(p -> !p.getSourceFilePath().endsWith(WorkflowConstants.tarContentsFileEndswith)
								&& !p.getSourceFilePath().endsWith(WorkflowConstants.tarExcludedContentsFileEndswith))
						.findFirst();
				StatusInfo tarRecord = tarRecordOpt.get();

				boolean allRequestsCompleted = tarFolderRequests.stream().allMatch(p-> StringUtils.equals(p.getStatus(), WorkflowConstants.COMPLETED));

				if (tarRecord != null && StringUtils.equals(tarRecord.getStatus(), WorkflowConstants.COMPLETED)
						&& allRequestsCompleted) {

					// if all the records are completed no rerun needed.
					logger.info("[Scheduler] All the records for the folder got uploaded", file.getAbsolutePath());
					statusInfo = tarRecord;

				} else {

					if (tarRecord != null && !StringUtils.equals(tarRecord.getStatus(), WorkflowConstants.COMPLETED)) {
						// Tar folder is not uploaded - enqueue tar folder row to JMS
						sendRequestToJms(tarRecord, config);
						continue;
					}
					// Tar completed - enqueue all incomplete contents records
					List<StatusInfo> tarContentRows = tarFolderRequests.stream()
							.filter(p -> p.getSourceFilePath().endsWith(WorkflowConstants.tarContentsFileEndswith)
									|| p.getSourceFilePath().endsWith(WorkflowConstants.tarExcludedContentsFileEndswith))
							.collect(Collectors.toList());
					// Find the included contents file record
					for (StatusInfo row : tarContentRows) {
						if (!WorkflowConstants.COMPLETED.equals(row.getStatus())) {
							sendRequestToJms(row, config);
						}
					}
					continue;

				}
			}
		}
		else if(upload.collectionSoftlink) {
			statusInfo =
		              dmeSyncWorkflowService.getService(access).findFirstStatusInfoByOriginalFilePathAndSourceFilePathAndStatus(
		                  file.getAbsolutePath(), file.getPath(), "COMPLETED");
		}
		else {
          statusInfo =
              dmeSyncWorkflowService.getService(access).findFirstStatusInfoByOriginalFilePathAndStatus(
                  file.getAbsolutePath(), "COMPLETED");
        }
        if (statusInfo != null) {
          logger.debug(
              "[Scheduler] File has already been uploaded: {}", statusInfo.getOriginalFilePath());
          if(!upload.replaceModifiedFiles)
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
        	if(preRule.tarContentsFile) {
        		statusInfo =
                        dmeSyncWorkflowService.getService(access).findFirstStatusInfoByOriginalFilePathAndSourceFilePathNotEndsWith(
                            file.getAbsolutePath(),WorkflowConstants.tarContentsFileEndswith);
        	}
          if(statusInfo != null) {
        	//Update the run_id and reset the retry count and errors
        	statusInfo.setRunId(runId);
        	statusInfo.setError("");
        	statusInfo.setRetryCount(0L);
        	statusInfo.setEndWorkflow(false);
        	if(!file.getIsDirectory()) {
        	statusInfo.setFilesize(file.getSize());
        	}
        	statusInfo = dmeSyncWorkflowService.getService(access).saveStatusInfo(statusInfo);
        	// Delete the metadata info created for this object ID
        	dmeSyncWorkflowService.getService(access).deleteMetadataInfoByObjectId(statusInfo.getId());
        	// Send the incomplete objectId to the message queue for processing
            DmeSyncMessageDto message = new DmeSyncMessageDto();
            message.setObjectId(statusInfo.getId());
            message.setDocConfigId(config.getId());
            sender.send(message, "inbound.queue");
            continue;
          }
        }

      }
      
      //If file has been modified with in days specified, skip
      if (!sourceRule.lastModifiedUnderBaseDir && sourceRule.lastModifiedDays != null
          && daysBetween(file.getUpdatedDate(), new Date()) <= sourceRule.lastModifiedDays) {
        logger.info(
            "[Scheduler] Skipping: {} File/folder has been modified within the last {} days. Last modified date: {}.",
            file.getAbsolutePath(),
            sourceRule.lastModifiedDays,
            file.getUpdatedDate());
        continue;
      }
      
      //If parent folder has been modified with in days specified, skip
      
		if (sourceRule.lastModifiedUnderBaseDir && sourceRule.lastModifiedDays != null) {
			// Find the directory being archived under the base dir
			Path baseDirPath = Paths.get(sourceConfig.sourceBaseDir).toRealPath();
			Path filePath = Paths.get(file.getAbsolutePath());
			Path relativePath = baseDirPath.relativize(filePath);
			Path subPath1 = relativePath.subpath(0, sourceRule.lastModifiedUnderBaseDirDepth + 1);
	        Path checkExistFilePath = baseDirPath.resolve(subPath1);

			Date folderModifiedDate=new Date(Files.getLastModifiedTime(checkExistFilePath).toMillis());
			
			if (daysBetween(folderModifiedDate, new Date()) <= sourceRule.lastModifiedDays) {
				logger.info(
						"[Scheduler] Skipping: {} folder has been modified within the last {} days for child folder {}. Last modified date: {}.",
						checkExistFilePath.toAbsolutePath(),sourceRule.lastModifiedDays ,filePath.getFileName(),folderModifiedDate);
				continue;
			}
		}

		// If folder does not contain a specified file, skip
		if (!sourceRule.fileExistUnderBaseDir
					&& (StringUtils.isNotEmpty(preRule.tarFileExistExt) || StringUtils.isNotEmpty(preRule.tarFileExist))) {

	    Path folder = Paths.get(file.getAbsolutePath());
        if(!pre.tar) {
          folder = Paths.get(file.getAbsolutePath()).getParent();
        }
        if(StringUtils.isNotEmpty(preRule.tarFileExist)) {
        	try (DirectoryStream<Path> stream = Files.newDirectoryStream(folder,
    				path -> path.getFileName().toString().equals(preRule.tarFileExist))) {
        		if (!stream.iterator().hasNext()) {
    				String message ="The directory " + folder.toString() + " does not contain file " + preRule.tarFileExist;
    	            logger.info(
    	              "[Scheduler] Skipping: {} folder which does not contain the specified file {}.",
    	              folder.toString(),
    	              preRule.tarFileExist);
    	            //TBD: Check if we need to insert record in DB as COMPLETED before sending an email.
    	            dmeSyncMailServiceFactory.getService(config.getDocName()).sendMail("WARNING: HPCDME during registration", message);
    				continue;
    			}
        	}  catch (IOException ex) {
	          throw new Exception("Error while listing directory: " + folder.toString(), ex);
	        }
        }
        if(StringUtils.isNotEmpty(preRule.tarFileExistExt)) {
	        try (DirectoryStream<Path> stream = Files.newDirectoryStream(folder, path -> path.toString().endsWith("." + preRule.tarFileExistExt))) {
	
	          if (!stream.iterator().hasNext()) {
	            String message ="The directory " + file.getAbsolutePath() + " does not contain any " + preRule.tarFileExistExt + " file at depth 0";
	            logger.info(
	              "[Scheduler] Skipping: {} Folder to process does not contain the specified extention {}.",
	              file.getAbsolutePath(),
	              preRule.tarFileExistExt);
	            //Insert record in DB as COMPLETED and send an email.
	            statusInfo = insertRecordDb(file, true, config);
	            dmeSyncMailServiceFactory.getService(config.getDocName()).sendMail("WARNING: HPCDME during registration", message);
	            continue;
	          }
	        }  catch (IOException ex) {
	          throw new Exception("Error while listing directory: " + file.getAbsolutePath(), ex);
	        }
        }
      }
      
      //If folder under the base dir does not contain a specified file for both files and tar, skip
      if (sourceRule.fileExistUnderBaseDir 
    		  && (StringUtils.isNotEmpty(preRule.tarFileExistExt) || StringUtils.isNotEmpty(preRule.tarFileExist))) {
        
        //Find the directory being archived under the base dir
        Path baseDirPath = Paths.get(sourceConfig.sourceBaseDir).toRealPath();
        Path filePath = Paths.get(file.getAbsolutePath());
        Path relativePath = baseDirPath.relativize(filePath);
        Path subPath1 = relativePath.subpath(0, sourceRule.fileExistUnderBaseDirDepth+1);
        Path checkExistFilePath = baseDirPath.resolve(subPath1);
        
        if(StringUtils.isNotEmpty(preRule.tarFileExist)) {
	        try (DirectoryStream<Path> stream = Files.newDirectoryStream(checkExistFilePath, path -> path.getFileName().toString().equals(preRule.tarFileExist))) {
	
	          if (!stream.iterator().hasNext()) {
	            logger.info(
	              "[Scheduler] Skipping: {} Folder to process does not contain the specified file {}.",
	              checkExistFilePath,
	              preRule.tarFileExist);
	            continue;
	          }
	        }  catch (IOException ex) {
	          throw new Exception("Error while listing directory: " + checkExistFilePath, ex);
	        }
        }
        if(StringUtils.isNotEmpty(preRule.tarFileExistExt)) {
	        try (DirectoryStream<Path> stream = Files.newDirectoryStream(checkExistFilePath, path -> path.toString().endsWith("." + preRule.tarFileExistExt))) {
	
	          if (!stream.iterator().hasNext()) {
	            logger.info(
	              "[Scheduler] Skipping: {} Folder to process does not contain the specified extention {}.",
	              checkExistFilePath,
	              preRule.tarFileExistExt);
	            continue;
	          }
	        }  catch (IOException ex) {
	          throw new Exception("Error while listing directory: " + checkExistFilePath, ex);
	        }
        }
      }
      
      //If folder under the base dir does contain a non_Archived specified file for both files and tar, skip
      if (sourceRule.fileExistUnderBaseDir 
    		  && (StringUtils.isNotEmpty(checkNoArchiveExistsFile) && StringUtils.isNotEmpty(checkArchiveExistsFile))) {
        
        //Find the directory being archived under the base dir
        Path baseDirPath = Paths.get(sourceConfig.sourceBaseDir).toRealPath();
        Path filePath = Paths.get(file.getAbsolutePath());
        Path relativePath = baseDirPath.relativize(filePath);
        Path subPath1 = relativePath.subpath(0, sourceRule.fileExistUnderBaseDirDepth+1);
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
	            dmeSyncMailServiceFactory.getService(config.getDocName()).sendMail("WARNING: HPCDME during registration", message);

	            continue;
	          }else {
	        	  try (DirectoryStream<Path> streamCheck = Files.newDirectoryStream(checkExistFilePath, path -> path.getFileName().toString().equals(checkArchiveExistsFile))) {
	  		          String message ="The directory " + file.getAbsolutePath() + " does not contain any " + checkArchiveExistsFile + "file" ;	
	    	          if (!streamCheck.iterator().hasNext()) {
	    	            logger.info(
	    	              "[Scheduler] Skipping: {} Folder to process does contain the specified file {}.",
	    	              checkExistFilePath,
	    	              checkArchiveExistsFile);
	    	            dmeSyncMailServiceFactory.getService(config.getDocName()).sendMail("WARNING: HPCDME during registration", message);
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
      statusInfo = insertRecordDb(file, false, config);

      // Send the objectId to the message queue for processing
      DmeSyncMessageDto message = new DmeSyncMessageDto();
      message.setObjectId(statusInfo.getId());
      message.setDocConfigId(config.getId());
      sender.send(message, "inbound.queue");
    }
  }

  private StatusInfo insertRecordDb(HpcPathAttributes file, boolean completed, DocConfig config) {
	DocConfig.PreprocessingConfig pre = config.getPreprocessingConfig();
	DocConfig.UploadConfig upload = config.getUploadConfig();
	  
    StatusInfo statusInfo = new StatusInfo();
    statusInfo.setRunId(runId);
    statusInfo.setOrginalFileName(file.getName());
    statusInfo.setOriginalFilePath(file.getAbsolutePath());
    statusInfo.setSourceFileName(pre.untar ? file.getTarEntry() : file.getName());
    statusInfo.setSourceFilePath(upload.collectionSoftlink ? file.getPath() : file.getAbsolutePath());
    statusInfo.setFilesize(file.getSize());
    statusInfo.setStartTimestamp(new Date());
    statusInfo.setDoc(config.getDocName());
    if(completed) {
      statusInfo.setStatus("COMPLETED");
      statusInfo.setError("specified file extension doesn't exist in correct depth");
    }
    statusInfo = dmeSyncWorkflowService.getService(access).saveStatusInfo(statusInfo);
    return statusInfo;
  }

  @Scheduled(cron = "0 0/1 * * * ?")
  public void checkForCompletedRun() {
	  
   for (DocConfig config : configService.getEnabledDocs()) {
	   
	DocConfig.SourceConfig sourceConfig = config.getSourceConfig();
	DocConfig.SourceRule sourceRule = config.getSourceRule();
	DocConfig.UploadConfig upload = config.getUploadConfig();
	
	if(sourceRule.aws) return;
    String currentRunId = null;
    if (shutDownFlag) {
      currentRunId = oneTimeRunId;
      //Check if we have already started the run
      List<StatusInfo> currentRun = dmeSyncWorkflowService.getService(access).findStatusInfoByRunIdAndDoc(currentRunId, config.getDocName());
      if(CollectionUtils.isEmpty(currentRun)) {
    	  // check if there are any records for Run_Ignored
			List<StatusInfo> currentRunIgnored = dmeSyncWorkflowService.getService(access)
					.findStatusInfoByRunIdAndDoc(currentRunId + WorkflowConstants.IGNORED_RUN_SUFFIX, config.getDocName());
			if (CollectionUtils.isEmpty(currentRunIgnored))
				return;
			else {
				// There are records in Ignored Run, no records to upload send email
				String emailBody = "There were no files/folders found for processing"
						+ (!StringUtils.isEmpty(sourceRule.sourceBaseDirFolders) ? " in " + sourceRule.sourceBaseDirFolders + " folders" : "")
						+ ".";
				dmeSyncMailServiceFactory.getService(config.getDocName())
						.sendMail("HPCDME Auto Archival Result for " + config.getDocName() + " - Base Path: " + sourceConfig.sourceBaseDir, emailBody);
				logger.info("[Scheduler] No files/folders found. Shutting down the application.");
				DmeSyncApplication.shutdown();
			}
	    
      }     
	 } else {
      StatusInfo latest = null;
      if(upload.softlink || upload.collectionSoftlink) {
    	  latest = dmeSyncWorkflowService.getService(access).findTopStatusInfoByDocOrderByStartTimestampDesc(config.getDocName());
      } else {
	      //Add base path also to distinguish multiple docs running the workflow.
	      latest = dmeSyncWorkflowService.getService(access).findTopStatusInfoByDocAndOriginalFilePathStartsWithOrderByStartTimestampDesc(config.getDocName(), sourceConfig.sourceBaseDir);
	  }
      if(latest != null)
        currentRunId = latest.getRunId();
    }
    


    //Check to make sure scheduler is completed, run has occurred and the queue is empty
    if (runId == null
        && currentRunId != null
        && !currentRunId.isEmpty()
        && sender.getQueueCount("inbound.queue") == 0
        && consumer.isAllThreadsCompleted()) {
    	

      //check if the latest export file is generated in log directory
      Path path = Paths.get(logFile);
      final String fileName = path.getParent().toString() + File.separatorChar + currentRunId + ".xlsx";
      File excel = new File(fileName);
      if (!excel.exists()) {
        //Export and send email for completed run
        logger.info("checking if scheduler is completed with queue count {} and active threads completed {} ", sender.getQueueCount("inbound.queue"), consumer.isAllThreadsCompleted());
        dmeSyncMailServiceFactory.getService(config.getDocName()).sendResult(currentRunId);

        if (shutDownFlag) {
          logger.info("checking if scheduler is completed with queue count {} and active threads completed {} ", sender.getQueueCount("inbound.queue"), consumer.isAllThreadsCompleted());
          logger.info("[Scheduler] Queue is empty. Shutting down the application.");
          DmeSyncApplication.shutdown();
        }
      }
    }
  }
  }

  @Scheduled(cron = "0 0/1 * * * ?")
  public void checkForAWSCompletedRun() {
	
   for (DocConfig config : configService.getEnabledDocs()) {
	   
	DocConfig.SourceConfig sourceConfig = config.getSourceConfig();
	DocConfig.SourceRule sourceRule = config.getSourceRule();
	  
	if(!sourceRule.aws) return;
	
	boolean completedFlag = true;
    String currentRunId = null;
    if (shutDownFlag) {
      currentRunId = oneTimeRunId;
      //Check if we have already started the run
      List<StatusInfo> currentRun = dmeSyncWorkflowService.getService(access).findStatusInfoByRunIdAndDoc(currentRunId, config.getDocName());
      if(CollectionUtils.isEmpty(currentRun))
        return;
    } else {
      StatusInfo latest = null;
      //Add base path also to distinguish multiple docs running the workflow.
      latest = dmeSyncWorkflowService.getService(access).findTopStatusInfoByDocAndOriginalFilePathStartsWithOrderByStartTimestampDesc(config.getDocName(), sourceConfig.sourceBaseDir);

      if(latest != null)
        currentRunId = latest.getRunId();
    }

    //Check to make sure scheduler is completed, run has occurred and the queue is empty
    if (runId == null
        && currentRunId != null
        && !currentRunId.isEmpty()
        && sender.getQueueCount("inbound.queue") == 0
        && consumer.isAllThreadsCompleted()) {

      //check if the latest export file is generated in log directory
      Path path = Paths.get(logFile);
      final String fileName = path.getParent().toString() + File.separatorChar + currentRunId + ".xlsx";
      File excel = new File(fileName);
      if (!excel.exists()) {
    	//Check if all the files have been processed by DME
    	List<StatusInfo> currentRun = dmeSyncWorkflowService.getService(access).findStatusInfoByRunIdAndDoc(currentRunId, config.getDocName());
    	for(StatusInfo statusInfo: currentRun) {
    		try {
				dmeSyncVerifyTaskImpl.processTask(statusInfo, config);
			} catch (Exception e) {
				logger.error("[Scheduler] Verify task for AWS completion error " + statusInfo.getSourceFilePath(), e.getMessage());
			}
    		if(StringUtils.contains(statusInfo.getError(), "Data_transfer_status is not in ARCHIVED"))
    			completedFlag = false;
    	}
    	
        //Export and send email for completed run
        if(completedFlag)
        	dmeSyncMailServiceFactory.getService(config.getDocName()).sendResult(currentRunId);

        if (shutDownFlag && completedFlag) {
          logger.info("[Scheduler] Queue is empty. Shutting down the application.");
          DmeSyncApplication.shutdown();
        }
      }
    }
   }
  }
  
  private int daysBetween(Date d1, Date d2) {
    return (int) ((d2.getTime() - d1.getTime()) / (1000 * 60 * 60 * 24));
  }
  
	private void sendRequestToJms(StatusInfo statusInfo, DocConfig config) {

		statusInfo.setRunId(runId);
		statusInfo.setError("");
		statusInfo.setRetryCount(0L);
		statusInfo.setEndWorkflow(false);
		statusInfo = dmeSyncWorkflowService.getService(access).saveStatusInfo(statusInfo);
		// Delete the metadata info created for this object ID
		dmeSyncWorkflowService.getService(access).deleteMetadataInfoByObjectId(statusInfo.getId());
		// Send the incomplete objectId to the message queue for processing
		DmeSyncMessageDto message = new DmeSyncMessageDto();
		message.setObjectId(statusInfo.getId());
		message.setDocConfigId(config.getId());
		sender.send(message, "inbound.queue");
	}
	
	private WorkflowRunInfo insertWorkflowRunInfo(DocConfig config) {
	    DocConfig.SourceConfig sourceConfig = config.getSourceConfig();
		    Timestamp now = Timestamp.from(Instant.now());

		    WorkflowRunInfo workflowRunInfo = new WorkflowRunInfo();
		    workflowRunInfo.setRunId(runId);
		    workflowRunInfo.setRunStartTimestamp(now);
		    workflowRunInfo.setRunLastHeartbeatTimestamp(now);
	    workflowRunInfo.setWorkflowId(config.getWorkflowId());
	    workflowRunInfo.setDoc(config.getDocName());
	    workflowRunInfo.setServerId(config.getServerId());
	    workflowRunInfo.setDmeServerId(config.getDmeServerId());
		    workflowRunInfo.setStatus(WorkflowConstants.RunStatus.RUNNING.toString());
	    workflowRunInfo.setThreads(config.getThreads());
	    workflowRunInfo.setSourcePath(sourceConfig.sourceBaseDir);
	    workflowRunInfo.setCronExpression(config.getCronExpression());
	    workflowRunInfo.setDocId(config.getId());
		    workflowRunInfo = dmeSyncWorkflowRunLogService.saveWorkflowRunInfo(workflowRunInfo);
		    return workflowRunInfo;
	}
	/**
	 * Selective scan processing.
	 *
	 * Purpose:
	 *  Allows a mixed mode scan where some folders are archived as a single TAR unit,
	 *       while other folders are scanned and their individual files are archived separately.</li>
	 * Configuration:
	 *  {@code dmesync.selective.scan=true} enables this mode.
	 *  {@code dmesync.tar.include.pattern} is a comma-separated list of glob patterns that select
	 *       which folders should be treated as "tar folders".
	 *       
	 * Pattern semantics:
	 *   Patterns are matched against the folder path relative to {@code dmesync.source.base.dir}
	 *       (i.e., {@code syncBaseDir}).
	 *   Example:
	 *     syncBaseDir = /data/archive
	 *     folderPath  = /data/archive/2025/movies/CS-fam123
	 *     relative    = 2025/movies/CS-fam123
	 *     pattern     = 2025/movies/CS-fam*
	 *  Glob wildcards:
	 *       {@code *} matches characters within a single path segment
	 *       {@code **} matches across path separators (any depth)
	 *       {@code ?} matches a single character
	 *
	 * Algorithm overview:
	 *   Determine which candidate folders match {@code tar.include.pattern} (these become {@code foldersToTar}).
	 *   Remove subfolders underneath tarred parent folders to avoid double-processing.
	 *   Archive tar-selected folders as folders (TAR workflow) via {@link #processFiles(List)}.
	 *   For the remaining folders, scan direct children and archive individual files only.
	 *   Finally, process any "top-level" files that are not within any already-processed folder.
	 */
	private void selectiveScanProcessing(List<HpcPathAttributes> folders, List<HpcPathAttributes> files, DocConfig config)
			throws Exception {

		DocConfig.SourceConfig sourceConfig = config.getSourceConfig();
		DocConfig.PreprocessingRule preRule = config.getPreprocessingRule();
		
		logger.info("[Scheduler] Selective Scan mode Started. tarIncludePattern='{}'", preRule.tarIncludePattern);

		try {

			// Selected folders that will be processed as folder objects (tar workflow).
			List<HpcPathAttributes> foldersToTar = new ArrayList<>();

			Set<Path> tarredFolderPaths = new HashSet<>();

			// Build matchers from comma-separated glob patterns.
			// Examples:
			// 2025/movies/CS-fam*
			// 2025/**/CS-fam*
			final List<PathMatcher> tarPatternsMatcher = buildPathMatchers(preRule.tarIncludePattern);

			// Base directory used to compute relative paths.
			final Path baseDirPath = Paths.get(sourceConfig.sourceBaseDir).toRealPath();

			// Decide which folders should be tarred.

			// 1. Find folders to TAR
			for (HpcPathAttributes folder : folders) {
				Path folderPath = Paths.get(folder.getAbsolutePath()).normalize().toAbsolutePath();
				if (isFolderToTar(folderPath, tarPatternsMatcher, baseDirPath)) {
					foldersToTar.add(folder);
					tarredFolderPaths.add(folderPath);
					logger.info("[Scheduler][SelectiveScan] Tarring folder matched  {}", folderPath);
				}
			}

			// 1) Process tar-selected folders.
			if (!foldersToTar.isEmpty()) {
				logger.info("[Scheduler][SelectiveScan] Tarring folders count={}", foldersToTar.size());
				processFiles(foldersToTar, config);
			} else {
				logger.info("[Scheduler][SelectiveScan] No folders selected for TAR.");
			}

			// 2. Folders for individual scan = not tarred and not subfolder of tarred
			List<HpcPathAttributes> foldersToScanIndividually = folders.stream().filter(f -> {
				Path path = Paths.get(f.getAbsolutePath()).normalize().toAbsolutePath();
				return !tarredFolderPaths.contains(path) && !isDescendantOf(path, tarredFolderPaths);
			}).toList();

			// For remaining folders, enqueue child files .
			List<HpcPathAttributes> individualFiles = collectFilesFromFolders(foldersToScanIndividually);

			if (!individualFiles.isEmpty()) {
				logger.info("[Scheduler][SelectiveScan] Processing individual files count={}", individualFiles.size());
				processFiles(individualFiles, config);
			}

			// 3) Process files that are not under any processed folder (tarred or
			// scanned-individually).
			Set<Path> scannedFolderPaths = new HashSet<>(tarredFolderPaths);
			for (HpcPathAttributes f : foldersToScanIndividually) {
				scannedFolderPaths.add(Paths.get(f.getAbsolutePath()).toAbsolutePath().normalize());
			}

			// 4. Process top-level files (not under any already-processed folder)
			Set<Path> processedFolders = new HashSet<>(tarredFolderPaths);
			foldersToScanIndividually
					.forEach(f -> processedFolders.add(Paths.get(f.getAbsolutePath()).normalize().toAbsolutePath()));
			List<HpcPathAttributes> topLevelFiles = files.stream().filter(f -> {
				Path filePath = Paths.get(f.getAbsolutePath()).normalize().toAbsolutePath();
				return processedFolders.stream().noneMatch(filePath::startsWith);
			}).toList();

			if (!topLevelFiles.isEmpty()) {
				logger.info("[Scheduler][SelectiveScan] Processing top-level files count={}", topLevelFiles.size());
				processFiles(topLevelFiles, config);
			}
		} catch (Exception e) {
			logger.error("[Scheduler][SelectiveScan] Failed");
			throw new Exception("SelectiveScan caused excelption" + e.getMessage());
		}

		logger.info("[Scheduler] Selective Scan mode Completed");
	}

	private boolean isFolderToTar(Path folder, List<PathMatcher> matchers, Path baseDirPath) {
		if (!matchers.isEmpty()) {
			Path rel = baseDirPath.relativize(folder);
			String relUnix = rel.toString().replace('\\', '/');
			for (PathMatcher m : matchers) {
				if (m.matches(Paths.get(relUnix)))
					return true;
			}
			return false;
		}
		// treat as leaf folder if no patterns
		try (DirectoryStream<Path> ds = Files.newDirectoryStream(folder)) {
			for (Path p : ds)
				if (Files.isDirectory(p))
					return false;
		} catch (IOException e) {
			logger.warn("Error checking if folder is leaf: {}", folder, e);
			return false;
		}
		return true;
	}

	private List<PathMatcher> buildPathMatchers(String patterns) {
		if (patterns == null || patterns.isBlank())
			return List.of();
		return Arrays.stream(patterns.split(",")).map(String::trim).filter(s -> !s.isEmpty())
				.map(pat -> FileSystems.getDefault().getPathMatcher("glob:" + pat)).toList();
		
	}

	private static boolean isDescendantOf(Path test, Set<Path> parents) {
		Path parent = test.getParent();
		while (parent != null) {
			if (parents.contains(parent))
				return true;
			parent = parent.getParent();
		}
		return false;
	}

	private List<HpcPathAttributes> collectFilesFromFolders(List<HpcPathAttributes> folders) {
		List<HpcPathAttributes> files = new ArrayList<>();
		for (HpcPathAttributes folder : folders) {
			File[] child = new File(folder.getAbsolutePath()).listFiles();
			if (child == null)
				continue;
			for (File f : child) {
				if (!f.isFile())
					continue;
				HpcPathAttributes a = new HpcPathAttributes();
				a.setName(f.getName());
				a.setPath(f.getPath());
				a.setUpdatedDate(new Date(f.lastModified()));
				a.setAbsolutePath(f.getAbsolutePath());
				a.setSize(f.length());
				a.setIsDirectory(false);
				files.add(a);
			}
		}
		return files;
	}

	public boolean isLeafFolder(Path folder) throws IOException {
		if (!Files.isDirectory(folder)) {
			return false;
		}

		try (DirectoryStream<Path> stream = Files.newDirectoryStream(folder)) {
			for (Path child : stream) {
				if (Files.isDirectory(child)) {
					return false; // has a subdirectory → not leaf
				}
			}
		}

		return true; // no subdirectories → leaf
	}
	
	/**
	 * Include prior-run failures in the current run so they are retried automatically.
	 *
	 * What it does (simple/low-filter version):
	 *  1) Find the most recent previous run_id for (doc, baseDir) that is NOT the current runId.
	 *  2) Load all StatusInfo rows for that previous run_id + doc.
	 *  3) Take only rows under the baseDir and not COMPLETED.
	 *  4) Reset them to current runId and enqueue to JMS.
	 */
	private void includePriorRunFailuresInCurrentRunWorklist(DocConfig config) {

	  DocConfig.SourceConfig sourceConfig = config.getSourceConfig();
	  String doc = config.getDocName();
	  String syncBaseDir = sourceConfig.sourceBaseDir;

	  if (StringUtils.isBlank(runId) || StringUtils.isBlank(doc) || StringUtils.isBlank(syncBaseDir)) {
	    logger.warn("[Scheduler][PriorRunRetry] Missing context. runId='{}', doc='{}', syncBaseDir='{}'",
	        runId, doc, syncBaseDir);
	    return;
	  }

	  try {
	    // 1) Determine previousRunId (most recent runId for this doc/baseDir excluding current runId).
	    StatusInfo latestAny =
	        dmeSyncWorkflowService.getService(access)
	            .findTopStatusInfoByDocAndOriginalFilePathStartsWithOrderByStartTimestampDesc(doc, syncBaseDir);

	    if (latestAny == null || StringUtils.isBlank(latestAny.getRunId())) {
	      logger.info("[Scheduler][PriorRunRetry] No history found for doc='{}' base='{}'", doc, syncBaseDir);
	      return;
	    }

	    // derive previous run id by scanning recent rows under baseDir and picking the newest runId != current runId.
	    List<StatusInfo> baseRows =
	        dmeSyncWorkflowService.getService(access).findAllByDocAndLikeOriginalFilePath(doc, syncBaseDir + "%");

	    String previousRunId = baseRows.stream()
	        .filter(s -> s != null && StringUtils.isNotBlank(s.getRunId()) && !StringUtils.equals(s.getRunId(), runId))
	        .sorted((a, b) -> {
	          Date ad = a.getStartTimestamp();
	          Date bd = b.getStartTimestamp();
	          if (ad == null && bd == null) return 0;
	          if (ad == null) return 1;
	          if (bd == null) return -1;
	          return bd.compareTo(ad); // newest first
	        })
	        .map(StatusInfo::getRunId)
	        .findFirst()
	        .orElse(null);

	    if (StringUtils.isBlank(previousRunId)) {
	      logger.info("[Scheduler][PriorRunRetry] No previous run found (current runId='{}')", runId);
	      return;
	    }

	    // 2) Load previous run rows.
	    List<StatusInfo> prevRunRows =
	        dmeSyncWorkflowService.getService(access).findStatusInfoByRunIdAndDoc(previousRunId, doc);

	    if (CollectionUtils.isEmpty(prevRunRows)) {
	      logger.info("[Scheduler][PriorRunRetry] No rows for previousRunId='{}' doc='{}'", previousRunId, doc);
	      return;
	    }

	    // 3) retry anything under baseDir that is NOT COMPLETED, and also check if the source folder/file is still exists in source
	    

	    List<StatusInfo> toRetry = prevRunRows.stream()
	        .filter(s -> s != null)
	        .filter(s -> !WorkflowConstants.COMPLETED.equalsIgnoreCase(StringUtils.defaultString(s.getStatus())))
	        .filter(s -> {
	            String originalFilePath = s.getOriginalFilePath();
	            if (StringUtils.isBlank(originalFilePath)) return false;
	              Path baseDirPath = null, candidatePath;
	              try {
	            	baseDirPath = Paths.get(syncBaseDir).toRealPath();
	                candidatePath = Paths.get(originalFilePath).toRealPath();
	              } catch (IOException ioEx) {
	                // If real path cannot be resolved, fall back to a normalized absolute path.
	                candidatePath = Paths.get(originalFilePath).normalize().toAbsolutePath();
	              }
	              if (!candidatePath.startsWith(baseDirPath)) {
	                return false;
	              }
	              return Files.exists(candidatePath);
	          })
	          .toList();
	    

	    if (toRetry.isEmpty()) {
	      logger.info("[Scheduler][PriorRunRetry] No failures to retry from previousRunId='{}' under base='{}'",
	          previousRunId, syncBaseDir);
	      return;
	    }

	    // 4) Reset + enqueue.
	    int enqueued = 0;
	    for (StatusInfo s : toRetry) {

	      s.setRunId(runId);
	      s.setError("");
	      s.setRetryCount(0L);
	      s.setEndWorkflow(false);

	      s = dmeSyncWorkflowService.getService(access).saveStatusInfo(s);

	      //  clear old derived state so rerun is clean.
	      dmeSyncWorkflowService.getService(access).deleteMetadataInfoByObjectId(s.getId());

	      DmeSyncMessageDto message = new DmeSyncMessageDto();
	      message.setObjectId(s.getId());
	      sender.send(message, "inbound.queue");

	      enqueued++;
	    }

	    logger.info("[Scheduler][PriorRunRetry] Enqueued {} prior-run failure(s) from '{}' into current runId='{}'",
	        enqueued, previousRunId, runId);

	  } catch (Exception e) {
	    logger.error("[Scheduler][PriorRunRetry] Failed to include prior-run failures into current run", e);
	  }
	}
}
	
