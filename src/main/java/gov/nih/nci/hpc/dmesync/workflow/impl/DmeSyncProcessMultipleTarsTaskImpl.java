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
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.dto.DmeSyncMessageDto;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncStorageException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncVerificationException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.jms.DmeSyncProducer;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncTask;

/**
 * DME Sync Process Multiple Tar Task Implementation
 * 
 * @author konerum3
 */
@Component
public class DmeSyncProcessMultipleTarsTaskImpl extends AbstractDmeSyncTask implements DmeSyncTask {

	@Autowired
	private DmeSyncProducer sender;

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
	private String multipleTarsFolders;

	@Value("${dmesync.multiple.tars.files.count:0}")
	private Integer filesPerTar;

	@Value("${dmesync.multiple.tars.size.gb:0}")
	private Integer sizePerTarInGB;

	@Value("${dmesync.cleanup:false}")
	private boolean cleanup;

	@Value("${dmesync.verify.prev.upload:none}")
	private String verifyPrevUpload;

	@Value("${dmesync.multiple.tars.files.validation:true}")
	private boolean verifyTarFilesCount;

	@PostConstruct
	public boolean init() {
		super.setTaskName("ProcessMultipleTarsTask");
		super.setCheckTaskForCompletion(false);
		return true;
	}

	@Override
	public StatusInfo process(StatusInfo object)
			throws DmeSyncVerificationException, DmeSyncWorkflowException, DmeSyncStorageException {

		/**
		 * This task is only applicable for CSB movies folder in Dataset so below
		 * condition will skip other folders and individual tar file requests when dmesync.process.multiple.tars is true
		 * 
		 */
		String sourceDirLeafNode = object.getSourceFilePath() != null
				? ((Paths.get(object.getSourceFilePath())).getFileName()).toString()
				: null;
		if (StringUtils.containsIgnoreCase(multipleTarsFolders, sourceDirLeafNode)) {

			try {

				Path baseDirPath = Paths.get(syncBaseDir).toRealPath();
				Path workDirPath = Paths.get(syncWorkDir).toRealPath();
				Path sourceDirPath = Paths.get(object.getOriginalFilePath());
				Path relativePath = baseDirPath.relativize(sourceDirPath);
				String tarWorkDir = workDirPath.toString() + File.separatorChar + relativePath.toString();
				Path tarWorkDirPath = Paths.get(tarWorkDir);
				Files.createDirectories(tarWorkDirPath);
				File directory = new File(object.getOriginalFilePath());
				File[] files = directory.listFiles();

				
				String tarFileParentName = sourceDirPath.getParent().getFileName().toString();
				String tarFileNameFormat = tarFileParentName + "_" + object.getOrginalFileName();
				File tarMappingFile = new File(syncWorkDir + "/" + tarFileParentName,
						(tarFileNameFormat + "_TarContentsFile.txt"));
				BufferedWriter notesWriter = new BufferedWriter(new FileWriter(tarMappingFile));
                int totalFilesInTars = 0;

				logger.info("[{}] Started multiple tar files processing in {}", super.getTaskName(),
						object.getOriginalFilePath());

				// Check directory permission
				if (!Files.isReadable(Paths.get(object.getOriginalFilePath()))) {
					throw new DmeSyncStorageException("No Read permission to " + object.getOriginalFilePath());
				}

				if (files != null && files.length > 0) {
					Arrays.sort(files, Comparator.comparing(File::lastModified));
					List<File> fileList = new ArrayList<>(Arrays.asList(files));
					
					// Determine splitting strategy: size-based takes precedence over count-based
					int expectedTarRequests;
					List<List<File>> fileGroups = new ArrayList<>();
					
					if (sizePerTarInGB > 0) {
						// Size-based splitting
						long targetSizeInBytes = (long) sizePerTarInGB * 1024L * 1024L * 1024L; // Convert GB to bytes
						logger.info("[{}] Using size-based splitting with target size {} GB ({} bytes) per tar", 
							super.getTaskName(), sizePerTarInGB, targetSizeInBytes);
						
						fileGroups = groupFilesBySize(fileList, targetSizeInBytes);
						expectedTarRequests = fileGroups.size();
						
						logger.info("[{}] Size-based splitting created {} groups for {} files", 
							super.getTaskName(), expectedTarRequests, fileList.size());
					} else if (filesPerTar > 0) {
						// File count-based splitting (existing behavior)
						expectedTarRequests = (fileList.size() + filesPerTar - 1) / filesPerTar;
						logger.info("[{}] Using file count-based splitting with {} files per tar", 
							super.getTaskName(), filesPerTar);
						
						// Create groups based on file count
						for (int i = 0; i < expectedTarRequests; i++) {
							int start = i * filesPerTar;
							int end = Math.min(start + filesPerTar, fileList.size());
							fileGroups.add(fileList.subList(start, end));
						}
					} else {
						// No splitting configured
						throw new DmeSyncWorkflowException(
							"Neither dmesync.multiple.tars.size.gb nor dmesync.multiple.tars.files.count is configured");
					}
					
				   // setting the expected tars count in DB , In the case of rerun the statusInfo row will already have the getTarContentsCount value.
					int tarsCounter= object.getTarContentsCount()!=null?object.getTarContentsCount():expectedTarRequests;
					object.setTarContentsCount(tarsCounter);
					object = dmeSyncWorkflowService.getService(access).saveStatusInfo(object);
					logger.info("[{}] Updated the expected tars counter value column {} in the DB  ", super.getTaskName(),
							object.getTarContentsCount());
				

					logger.info("[{}] Started creating {} tars with total of  {} for the dataset {} with {} files ", super.getTaskName(),
							tarsCounter,expectedTarRequests, tarFileParentName, fileList.size());

					for (int i = 0; i < expectedTarRequests; i++) {
						List<File> subList = fileGroups.get(i);
						int start = fileList.indexOf(subList.get(0));
						int end = fileList.indexOf(subList.get(subList.size() - 1));
						totalFilesInTars = end+1;
						String tarFileName = tarFileNameFormat + "_part_" + (i + 1) + ".tar";
						String tarFilePath = tarWorkDir + File.separatorChar + tarFileName;
						tarFilePath = Paths.get(tarFilePath).normalize().toString();
						
                      /* Before creating new tar request If verifyPrevUpload is local check these two conditions
                       *   check if already uploaded to dme: if yes check Indexes in the statusInfo row are same: if above condition works write to contents file and skip the tar
                       *   check if there is already record inserted in Db: If yes check the indexes, if not reuse the row from Db record.
                       */
						
						if ("local".equals(verifyPrevUpload)) {
							
						  // This  block executes when  dmesync.verify.prev.upload=local
							StatusInfo recordForUploadedTar = dmeSyncWorkflowService.getService(access)
									.findFirstStatusInfoByOriginalFilePathAndSourceFileNameAndStatus(
											object.getOriginalFilePath(), tarFileName, "COMPLETED");
							StatusInfo recordForTarfile = dmeSyncWorkflowService.getService(access)
									.findTopBySourceFileNameAndRunId(tarFileName, object.getRunId());
							
							if (recordForUploadedTar != null) {
								/* if tar already got uploaded to DME(completed status in  statusInfo table,then verify indexes matched
								 *  If indexes matched , then write the contents to tar contents file and continue
								 *  If indexed doesn't matched, then insert row with error message in the database. 
								 **/
								
								if (start != recordForUploadedTar.getTarIndexStart()
										|| end != recordForUploadedTar.getTarIndexEnd()) {
								// If tar start index and tar end index doesn't match with indexes of uploaded file, insert a new row in status info table with error 
									String errorMsg = "Tar range indexes doesn't matched with expected index ranges tar start Index:"
											+ start + "UploadedStartIndex:" + recordForUploadedTar.getTarIndexStart()
											+ "tar end index:" + end + " uploadedEndIndex:"
											+ recordForUploadedTar.getTarIndexEnd();
									logger.info("[{}] UploadedTarIndexes {} mismatch Inserting error row in database {}", super.getTaskName(),
											tarFileName, errorMsg);
									StatusInfo indexErrorRow = insertErrorRowforTar(object, tarFileName, start, end,
											errorMsg);
									
									
								} else {
									//If tar start index matches and tar is already uploaded , then log the info.
									logger.info(
											"[{}] Skipping the Creation of tar request {} since this is already got uploaded  {} {}",
											super.getTaskName(), tarFileName, recordForUploadedTar.getId(),
											recordForUploadedTar.getStatus());
									List<StatusInfo> duplicateRows = dmeSyncWorkflowService.getService(access)
											.findByOriginalFilePathAndSourceFileNameAndStatusNull(object.getOriginalFilePath(),
													tarFileName);
									if (!duplicateRows.isEmpty()) {
										List<Long> objectIds = duplicateRows.stream().map(StatusInfo::getId)
												.collect(Collectors.toList());
										dmeSyncWorkflowService.getService(access).deleteStatusInfoByIds(objectIds);
										logger.info(
												"[{}] Deleting the duplicate row for {} with ID {} because there is already uploaded row {} with {} status",
												super.getTaskName(), tarFileName,objectIds, recordForUploadedTar.getId(),
												recordForUploadedTar.getStatus());
									}

								}
								writeToContentsFile(notesWriter, tarFileName, subList);
								continue;
								
							} else if (recordForTarfile != null) {
								// If there is already record in DB for this tar mainly for failed tar reruns, reuse the existing tar for the tar request 
								
									if (start != recordForTarfile.getTarIndexStart()
											|| end != recordForTarfile.getTarIndexEnd()) {
										
										// If tar start index and tar end index doesn't match with indexes of uploaded file, insert a new row in status info table with error 
										String errorMsg = "TarRange Indexes doesn't matched with expected Index ranges tarStartIndex:"
												+ start + "UploadedStartIndex:" + recordForTarfile.getTarIndexStart()
												+ "tarEndIndex:" + end + " uploadedEndIndex:"
												+ recordForTarfile.getTarIndexEnd();

										logger.info("[{}] UploadedTarIndexes {} mismatch {}", super.getTaskName(),
												tarFileName, errorMsg);
										StatusInfo indexErrorRow = insertErrorRowforTar(object, tarFileName, start, end,
												errorMsg);
										
									} else {
										//If tar start index matches and existing tar record is available in database , then send the existing Id to database
										logger.info("[{}]Enqueuing the existing tar request {} with Id{} from DB {}",
												super.getTaskName(), tarFileName ,recordForTarfile.getId(), tarFilePath);
										enqueueRequestToJms(recordForTarfile);
									}
									
							}else {
								
								// If tar is not uploaded or existing record is not in database, mainly for new tar request
								// add new row in status info table for tar, send the new row Id to JMS queue
								StatusInfo newTarRequest = insertNewRowforTar(object, tarFileName, true, start, end, null);
								// Send the objectId to the message queue for processing
								logger.info("[{}]Enqueuing the new tar request {} with Id {}", super.getTaskName(),
										tarFileName,newTarRequest.getId());
								enqueueRequestToJms(newTarRequest);
							}
							
						} else {
							// If verifyPrevUpload value is none. This means doesn't check the database for uploads then add new row in status info table for tar, send the new row Id to JMS queue
							StatusInfo newTarRequest = insertNewRowforTar(object, tarFileName, true, start, end, null);
							logger.info("[{}]Enqueuing the new tar request {}", super.getTaskName(),
									newTarRequest.getId());
							enqueueRequestToJms(newTarRequest);
						}

						writeToContentsFile(notesWriter, tarFileName, subList);
					}

					notesWriter.close();
					logger.info("[{}] Ended Multiple tar requests processing in {}", super.getTaskName());
					
					
					logger.info("[{}] Started Multiple tar requests Verification  in {}", super.getTaskName());
					List<StatusInfo> totalRequestsForFolder;
					if ("local".equals(verifyPrevUpload)) {
						// If local, get the query based on original file Path movies
						totalRequestsForFolder = dmeSyncWorkflowService.getService(access)
								.findAllByDocAndLikeOriginalFilePath(doc, object.getOriginalFilePath());
					} else {
						// If local, get the query based on run_id
						totalRequestsForFolder = dmeSyncWorkflowService.getService(access)
								.findAllByDocAndRunIdAndLikeOriginalFilePath(doc, object.getRunId(),
										object.getOriginalFilePath());
					}

					long totalTarsRequests = totalRequestsForFolder.stream().filter(t -> t.getTarIndexStart() != null)
							.count();
					;
					
					// verify if all the files in folder got added to files in tar requests. If not throw the exception
					if (totalFilesInTars != files.length) {
						object.setError((" Files in original folder " + files.length
								+ " doesn't match the files in multiple tars requests " + totalFilesInTars));
						dmeSyncWorkflowService.getService(access).recordError(object);
						object.setStatus(null);
						throw new DmeSyncVerificationException((" Files in original folder " + files.length
								+ " doesn't match the files in multiple created tars " + totalFilesInTars));
						
					} 
					if (expectedTarRequests != (totalTarsRequests)) {  
						// verify if all the tar requests are inserted in status_info table.If not throw the exception
						object.setError((" Expected tar creation Requests " + expectedTarRequests
								+ " doesn't match the creation requests in DB " + totalTarsRequests));
						object.setStatus(null);
						dmeSyncWorkflowService.getService(access).recordError(object);
						throw new DmeSyncVerificationException((" Expected tar creation Requests " + expectedTarRequests
								+ " doesn't match the creation requests in DB " + totalTarsRequests));
						
						
					} 
						// If all the verifications are true, This block gets executed.
						logger.info("[{}] Completed Multiple tar requests Verification Completed in {}",
								super.getTaskName());

						StatusInfo checkForUploadedContentsFile = dmeSyncWorkflowService.getService(access)
								.findTopStatusInfoByDocAndSourceFilePath(doc,
										tarMappingFile.getAbsolutePath());

						
						
						if ("local".equals(verifyPrevUpload) && checkForUploadedContentsFile!=null) {
							
						   if( StringUtils.equalsIgnoreCase("COMPLETED", checkForUploadedContentsFile.getStatus())) {
							   // Tar contents file request have already uplaoded in previous run
							   logger.info(
										"[{}] Tar Contents file already uploaded to DME  {} ,{} ,{}",
										super.getTaskName(), checkForUploadedContentsFile.getSourceFileName(), checkForUploadedContentsFile.getId(),
										checkForUploadedContentsFile.getStatus());
							   
						   } else if (object.getTarContentsCount()==0) {
								// tarContentsCounter : number of tars remaining to be uploaded .
							  // If the contents file is not uploaded and all the tars are uploaded, so enqueing the contents file 
							logger.info("[{}]Enqueuing the existing contents file upload request {}", super.getTaskName(),
										tarMappingFile.getName());
							enqueueRequestToJms(checkForUploadedContentsFile);
						   }
						} else {
							// If there is no content files record in DB, create a new one.
							logger.info("[{}]Inserting the new contents file upload request {}", super.getTaskName(),
									tarMappingFile.getName());
							// add new row in status info table for uploading tarContentsFile.
							StatusInfo contentsFileRecord = insertNewRowforTar(object, tarMappingFile.getName(), false, null,
									null, tarMappingFile);
							if (object.getTarContentsCount()==0) {
								// tarContentsCounter : number of tars remaining to be uploaded .
								  // If the contents file is not uploaded and all the tars are uploaded, so enqueing the contents file 
								logger.info("[{}]Enqueuing the contents file upload request {}", super.getTaskName(),
											tarMappingFile.getName());
								enqueueRequestToJms(contentsFileRecord);
							}
							// This contentsFileRecord objectId is send to the message queue in the cleanup task after all tars are uploaded
						 }
						
					    object = dmeSyncWorkflowService.getService(access).findStatusInfoById(object.getId()).orElseThrow();
						logger.info("[{}] Tars counter value column {} in the DB  ", super.getTaskName(),
								object.getTarContentsCount());
						
						// update the current status info row as completed so this workflow is completed and next task won't be processed.
						object.setStatus("COMPLETED");
						object.setEndWorkflow(true);
						object = dmeSyncWorkflowService.getService(access).saveStatusInfo(object);
						
						logger.info("[{}] Movies folder row status changed to {} in the DB for path {} ", super.getTaskName(),
								object.getStatus(),object.getOriginalFilePath() );
					
				}
			} catch (Exception e) {
				logger.error("[{}] error {}", super.getTaskName(), e.getMessage(), e);
				throw new DmeSyncStorageException("Error occurred during tar. " + e.getMessage(), e);
			}
		}return object;

	}

	private StatusInfo insertNewRowforTar(StatusInfo object, String sourceFileName, boolean isTarRequest,
			Integer tarStartIndex, Integer tarEndIndex, File sourceFile) throws IOException {

		StatusInfo statusInfo = new StatusInfo();
		statusInfo.setRunId(object.getRunId());
		statusInfo.setOrginalFileName(object.getOrginalFileName());
		statusInfo.setOriginalFilePath(object.getOriginalFilePath());
		statusInfo.setSourceFileName(sourceFileName);
		statusInfo.setStartTimestamp(new Date());
		statusInfo.setDoc(object.getDoc());
		if (isTarRequest) {
			statusInfo.setTarIndexEnd(tarEndIndex);
			statusInfo.setTarIndexStart(tarStartIndex);
			statusInfo.setFilesize(0L);
		} else {
			statusInfo.setSourceFilePath(sourceFile.getAbsolutePath());
			statusInfo.setFilesize(sourceFile.length());
		}
		statusInfo = dmeSyncWorkflowService.getService(access).saveStatusInfo(statusInfo);

		return statusInfo;

	}

	private StatusInfo insertErrorRowforTar(StatusInfo object, String sourceFileName, Integer tarStartIndex,
			Integer tarEndIndex, String error) throws IOException {

		StatusInfo statusInfo = new StatusInfo();
		statusInfo.setRunId(object.getRunId());
		statusInfo.setOrginalFileName(object.getOrginalFileName());
		statusInfo.setOriginalFilePath(object.getOriginalFilePath());
		statusInfo.setSourceFileName(sourceFileName);
		statusInfo.setStartTimestamp(new Date());
		statusInfo.setDoc(object.getDoc());
		statusInfo.setTarIndexEnd(tarEndIndex);
		statusInfo.setTarIndexStart(tarStartIndex);
		statusInfo.setSourceFilePath(object.getOriginalFilePath());
		statusInfo.setFilesize(0L);
		statusInfo.setError(error);

		statusInfo = dmeSyncWorkflowService.getService(access).saveStatusInfo(statusInfo);

		return statusInfo;

	}

	private void writeToContentsFile(BufferedWriter notesWriter, String tarFileName, List<File> subList)
			throws IOException {

		notesWriter.write("Tar File: " + tarFileName + "\n");
		for (File fileName : subList) {
			notesWriter.write(tarFileName + " " + fileName.getName() + "\n");
		}
		notesWriter.write("\n");
	}
	
	private void enqueueRequestToJms(StatusInfo object ) {
		DmeSyncMessageDto message = new DmeSyncMessageDto();
		message.setObjectId(object.getId());
		sender.send(message, "inbound.queue");
		logger.info("get queue count" + sender.getQueueCount("inbound.queue"));
		
	}

	/**
	 * Calculate the total size of a file or directory recursively
	 * @param file The file or directory
	 * @return Total size in bytes
	 */
	private long calculateSize(File file) throws IOException {
		if (file.isFile()) {
			return file.length();
		} else if (file.isDirectory()) {
			long totalSize = 0;
			File[] children = file.listFiles();
			if (children != null) {
				for (File child : children) {
					totalSize += calculateSize(child);
				}
			}
			return totalSize;
		}
		return 0;
	}

	/**
	 * Group files into sublists based on target size
	 * @param files List of files to group
	 * @param targetSizeInBytes Target size per group in bytes
	 * @return List of file groups
	 */
	private List<List<File>> groupFilesBySize(List<File> files, long targetSizeInBytes) throws IOException {
		List<List<File>> groups = new ArrayList<>();
		List<File> currentGroup = new ArrayList<>();
		long currentGroupSize = 0;

		for (File file : files) {
			long fileSize = calculateSize(file);
			
			// If adding this file would exceed the target size and current group is not empty,
			// start a new group
			if (currentGroupSize + fileSize > targetSizeInBytes && !currentGroup.isEmpty()) {
				groups.add(new ArrayList<>(currentGroup));
				currentGroup.clear();
				currentGroupSize = 0;
			}
			
			// Add file to current group
			currentGroup.add(file);
			currentGroupSize += fileSize;
		}

		// Add the last group if it has any files
		if (!currentGroup.isEmpty()) {
			groups.add(currentGroup);
		}

		return groups;
	}

}
