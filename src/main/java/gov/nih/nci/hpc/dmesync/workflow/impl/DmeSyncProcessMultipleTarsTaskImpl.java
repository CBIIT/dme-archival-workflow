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
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

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
	private String multpleTarsFolders;

	@Value("${dmesync.multiple.tars.files.count:0}")
	private Integer filesPerTar;

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
		 * condition will skip other folders and individual tar file requests
		 */
		String sourceDirLeafNode = object.getSourceFilePath() != null
				? ((Paths.get(object.getSourceFilePath())).getFileName()).toString()
				: null;
		if (StringUtils.equalsIgnoreCase(multpleTarsFolders, sourceDirLeafNode)) {

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

				// FYI,, If the contents file format has changed , these changes should also done in cleanup task, 
				  //since we are enqeueing contents file to upload logic in cleanup task. 
				String tarFileParentName = sourceDirPath.getParent().getFileName().toString();
				String tarFileNameFormat = tarFileParentName + "_" + object.getOrginalFileName();
				File tarMappingFile = new File(syncWorkDir + "/" + tarFileParentName,
						(tarFileNameFormat + "_TarContentsFile.txt"));
				BufferedWriter notesWriter = new BufferedWriter(new FileWriter(tarMappingFile));

				int lastTarIndex = 0;

				logger.info("[{}] Started multiple tar files processing in {}", super.getTaskName(),
						object.getOriginalFilePath());

				// Check directory permission
				if (!Files.isReadable(Paths.get(object.getOriginalFilePath()))) {
					throw new DmeSyncStorageException("No Read permission to " + object.getOriginalFilePath());
				}

				if (files != null && files.length>0) {
					// sorting the files based on the lastModified in asc, so every rerun we get
					// them in same order.
					Arrays.sort(files, Comparator.comparing(File::lastModified));
					List<File> fileList = new ArrayList<>(Arrays.asList(files));
					int expectedTarRequests = (fileList.size() + filesPerTar - 1) / filesPerTar;

					logger.info("[{}] Started creating {} tars for the dataset {} with {} files ", super.getTaskName(),
							expectedTarRequests, tarFileParentName, fileList.size());

					for (int i = 0; i < expectedTarRequests; i++) {
						int start = i * filesPerTar;
						int end = Math.min(start + filesPerTar, fileList.size());
						lastTarIndex = end;
						List<File> subList = fileList.subList(start, end);
						String tarFileName = tarFileNameFormat + "_part_" + (i + 1) + ".tar";
						String tarFilePath = tarWorkDir + File.separatorChar + tarFileName;
						tarFilePath = Paths.get(tarFilePath).normalize().toString();
						
                      /* Before creating new tar request If verifyPrevUpload is local check these two conditions
                       *   check if already uploaded to dme: if yes check Indexes in the statusInfo row are same: if above condition works write to contents file and skip the tar
                       *   check if there is already record inserted in Db: If yes check the indexes, if not reuse the row from Db record.
                       */
						
						if ("local".equals(verifyPrevUpload)) {
							// check if tarName already got uploaded to DME from statusInfo table */
							StatusInfo recordForUploadedTar = dmeSyncWorkflowService.getService(access)
									.findFirstStatusInfoByOriginalFilePathAndSourceFileNameAndStatus(
											object.getOriginalFilePath(), tarFileName, "COMPLETED");
							StatusInfo recordForTarfile = dmeSyncWorkflowService.getService(access)
									.findTopBySourceFileNameAndRunId(tarFileName, object.getRunId());
							if (recordForUploadedTar != null) {
								// Check the startIndex and EndIndex with start and End to make sure uploaded tars have same range
								if (start != recordForUploadedTar.getTarIndexStart()
										|| end != recordForUploadedTar.getTarIndexEnd()) {
                                  // If indexes doesn't match , insert the new row with error message
									String errorMsg = "TarRange Indexes doesn't matched with expected Index ranges tarStartIndex:"
											+ start + "UploadedStartIndex:" + recordForUploadedTar.getTarIndexStart()
											+ "tarEndIndex:" + end + " uploadedEndIndex:"
											+ recordForUploadedTar.getTarIndexEnd();
									logger.info("[{}] UploadedTarIndexes {} mismatch Inserting error row in database {}", super.getTaskName(),
											tarFileName, errorMsg);
									StatusInfo indexErrorRow = insertErrorRowforTar(object, tarFileName, start, end,
											errorMsg);
								} else {
									// This block is executed when the tar is already uploaded to DME
									logger.info(
											"[{}] Skipping the Creation of tar request {} since this is already got uploaded  {} {}",
											super.getTaskName(), tarFileName, recordForUploadedTar.getId(),
											recordForUploadedTar.getStatus());
								}
								writeToContentsFile(notesWriter, tarFileName, subList);
								continue;
							} else if (recordForTarfile != null) {
								// check is there is already record in DB for this runId and sourceFilePath mainly for failed tar reruns
									if (start != recordForTarfile.getTarIndexStart()
											|| end != recordForTarfile.getTarIndexEnd()) {
										// This block executes when there is already record in the Db for that tar and indexes are not same, just enter new row with error msge
										String errorMsg = "TarRange Indexes doesn't matched with expected Index ranges tarStartIndex:"
												+ start + "UploadedStartIndex:" + recordForTarfile.getTarIndexStart()
												+ "tarEndIndex:" + end + " uploadedEndIndex:"
												+ recordForTarfile.getTarIndexEnd();

										logger.info("[{}] UploadedTarIndexes {} mismatch {}", super.getTaskName(),
												tarFileName, errorMsg);
										StatusInfo indexErrorRow = insertErrorRowforTar(object, tarFileName, start, end,
												errorMsg);
									} else {
										// This block executes when there is already record in the Db so reusing the tar
										logger.info("[{}]Enqueuing the existing tar request {} with Id{} from DB {}",
												super.getTaskName(), tarFileName ,recordForTarfile.getId(), tarFilePath);
										DmeSyncMessageDto message = new DmeSyncMessageDto();
										message.setObjectId(recordForTarfile.getId());
										sender.send(message, "inbound.queue");
										logger.info("get queue count" + sender.getQueueCount("inbound.queue"));
									}
							}else {
								// This block executes when the tar is not uploaded or record is not created. mainly for new request 
								// add new row in status info table for created tar.
								StatusInfo statusInfo = insertNewRowforTar(object, tarFileName, true, start, end, null);
								// Send the objectId to the message queue for processing
								logger.info("[{}]Enqueuing the new tar request {} with Id {}", super.getTaskName(),
										tarFileName,statusInfo.getId());
								DmeSyncMessageDto message = new DmeSyncMessageDto();
								message.setObjectId(statusInfo.getId());
								sender.send(message, "inbound.queue");
								logger.info("get queue count" + sender.getQueueCount("inbound.queue"));
							}
						} else {
							// This block executes when the verifyPrevUpload value is none. This means doesn't check the database 
							StatusInfo statusInfo = insertNewRowforTar(object, tarFileName, true, start, end, null);
							logger.info("[{}]Enqueuing the new tar request {}", super.getTaskName(),
									statusInfo.getId());
							DmeSyncMessageDto message = new DmeSyncMessageDto();
							message.setObjectId(statusInfo.getId());
							sender.send(message, "inbound.queue");
							logger.info("get queue count" + sender.getQueueCount("inbound.queue"));
						}

						writeToContentsFile(notesWriter, tarFileName, subList);
					}

					notesWriter.close();
					logger.info("[{}] Ended Multiple tar requests processing in {}", super.getTaskName());
					logger.info("[{}] Started Multiple tar requests Verification  in {}", super.getTaskName());

					List<StatusInfo> totalRequestsForFolder;
					if ("local".equals(verifyPrevUpload)) {
						totalRequestsForFolder = dmeSyncWorkflowService.getService(access)
								.findAllByDocAndLikeOriginalFilePath(doc, object.getOriginalFilePath());
					} else {
						totalRequestsForFolder = dmeSyncWorkflowService.getService(access)
								.findAllByDocAndRunIdAndLikeOriginalFilePath(doc, object.getRunId(),
										object.getOriginalFilePath());
					}

					long totalTarsRequests = totalRequestsForFolder.stream().filter(t -> t.getTarIndexStart() != null)
							.count();
					;
					// checking if all the files in folder got tared
					if (lastTarIndex != files.length) {
						object.setError((" Files in original folder " + files.length
								+ " didn't match the files in multiple tars requests " + lastTarIndex));
						dmeSyncWorkflowService.getService(access).recordError(object);
						throw new DmeSyncVerificationException((" Files in original folder " + files.length
								+ " didn't match the files in multiple created tars " + lastTarIndex));
						// checking if all the tar requests are inserted in status_info table
					} else if (expectedTarRequests != (totalTarsRequests)) {
						object.setError((" Expected tar creation Requests " + expectedTarRequests
								+ " didn't match the creation requests in DB " + totalTarsRequests));
						dmeSyncWorkflowService.getService(access).recordError(object);
						throw new DmeSyncVerificationException((" Expected tar creation Requests " + expectedTarRequests
								+ " didn't match the creation requests in DB " + totalTarsRequests));
					} else {
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
							  // If the contents file is not uploaded and all the tars are uploaded, so enqueing the contents file 
							logger.info("[{}]Enqueuing the existing contents file upload request {}", super.getTaskName(),
										tarMappingFile.getName());
							DmeSyncMessageDto message = new DmeSyncMessageDto();
							message.setObjectId(checkForUploadedContentsFile.getId());
							sender.send(message, "inbound.queue");
							logger.info("get queue count" + sender.getQueueCount("inbound.queue"));
						   }
						} else {
							logger.info("[{}]Inserting the new contents file upload request {}", super.getTaskName(),
									tarMappingFile.getName());
							// add new row in status info table for uploading tarContentsFile.
							StatusInfo statusInfo = insertNewRowforTar(object, tarMappingFile.getName(), false, null,
									null, tarMappingFile);
							// Sending  the objectId to the message queue in the cleanup task after all tars are uploaded
						 }
						// update the current status info row as completed so this workflow is completed
						object.setStatus("COMPLETED");
						object.setTarContentsCount(object.getTarContentsCount()!=null?object.getTarContentsCount():expectedTarRequests);
						object = dmeSyncWorkflowService.getService(access).saveStatusInfo(object);
					}
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

}
