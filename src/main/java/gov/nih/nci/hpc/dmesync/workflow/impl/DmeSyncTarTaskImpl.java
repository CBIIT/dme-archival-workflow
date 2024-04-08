package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.dto.DmeSyncMessageDto;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
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

	private static final String multipleTarFileExtension = "part_";

	@PostConstruct
	public boolean init() {
		super.setTaskName("TarTask");
		if (tarIndividualFiles)
			super.setCheckTaskForCompletion(false);
		return true;
	}

	@Override
	public StatusInfo process(StatusInfo object) throws DmeSyncMappingException, DmeSyncWorkflowException {

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
			Files.createDirectories(tarWorkDirPath);

			// if this property is set performing multiple tars based on the number of files
			// in each tier
			if (filesPerTar > 0) {

				if (multpleTarsFolders != null
						&& StringUtils.contains(object.getOrginalFileName(), multpleTarsFolders)) {

					// using this map and notes file for tar tracking.
					Map<String, List<String>> tarFilesTracking = new HashMap<>();
					File tarMappingFile = new File(syncWorkDir, object.getOrginalFileName() + "_TarMappingNotes.txt");
					BufferedWriter notesWriter = new BufferedWriter(new FileWriter(tarMappingFile));

					File directory = new File(object.getOriginalFilePath());
					File[] files = directory.listFiles();

					// Check directory permission
					if (!Files.isReadable(Paths.get(object.getOriginalFilePath()))) {
						throw new Exception("No Read permission to " + object.getOriginalFilePath());
					}
					List<String> excludeFolders = excludeFolder == null || excludeFolder.isEmpty() ? null
							: new ArrayList<>(Arrays.asList(excludeFolder.split(",")));

					if (files != null) {

						// sorting the files based on the lastModified in asc
						Arrays.sort(files, Comparator.comparing(File::lastModified));
						List<File> fileList = new ArrayList<>(Arrays.asList(files));

						int tarLoopCount = (fileList.size() + filesPerTar - 1) / filesPerTar;
						// This is mainly for rerun to start the loop from last uploaded tar.
						int lastUploadedTarNumber = retrievePreviousUploadedTarNumber(object);

						int loopStart = lastUploadedTarNumber > 0 ? lastUploadedTarNumber : 0;
						for (int i = loopStart; i < tarLoopCount; i++) {

							int start = i * filesPerTar;
							int end = Math.min(start + filesPerTar, fileList.size());
							List<File> subList = fileList.subList(start, end);
							// int tarNumber = i + 1;
							String tarFileName = object.getOrginalFileName() + "_" + multipleTarFileExtension + (i + 1)
									+ ".tar";
							String tarFile = tarWorkDir + File.separatorChar + tarFileName;
							tarFile = Paths.get(tarFile).normalize().toString();

							logger.info("[{}] Creating tar file in {}", super.getTaskName(), tarFile);
							
							// Track files included in this tar file
							List<String> tarFileTracking = new ArrayList<>();
							tarFilesTracking.put(tarFile, tarFileTracking);
							
							File[] filesArray = new File[subList.size()];
							subList.toArray(filesArray);
							
							
							for (File ft : subList) {
								if (!ft.canRead()) {
									
									  object.setFilesize(tarMappingFile.length());
									  object.setError("No Read permission to " + ft.getAbsolutePath());
									  object.setSourceFileName(tarMappingFile.getName());
									  object.setSourceFilePath(tarMappingFile.getAbsolutePath());
									  object.setTarStartTimestamp(null); 
									  object =
									  dmeSyncWorkflowService.getService(access).saveStatusInfo(object);
									  upsertTask(object.getId());
									 
									throw new Exception("No Read permission to " + ft.getAbsolutePath());
								}
								tarFileTracking.add(ft.getName());

							}
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


							/*
							 * Write tracking information to notes file
							 */

							notesWriter.write("Tar File: " + tarFileName + "\n");
							for (String fileName : tarFileTracking) {
								notesWriter.write(" - " + fileName + "\n");
							}
							notesWriter.write("\n");
							
							
							// add new row in status info table for created tar.

							StatusInfo recordForTarfile = dmeSyncWorkflowService.getService(access)
									.findTopBySourceFilePathAndRunId(tarFile, object.getRunId());

							if (recordForTarfile == null) {
								
								StatusInfo statusInfo= insertNewRowforTar(object, tarFileName, tarFile);
								
								// Mark the tar task as completed for individual Tars.
								upsertTask(statusInfo.getId());
								
								// Send the  objectId to the message queue for processing
								DmeSyncMessageDto message = new DmeSyncMessageDto();
								message.setObjectId(statusInfo.getId());

								sender.send(message, "inbound.queue");

							} else {

								DmeSyncMessageDto message = new DmeSyncMessageDto();
								message.setObjectId(recordForTarfile.getId());

								sender.send(message, "inbound.queue");

							}

						}
					}
					notesWriter.close();

					// update the current statusInfo row with the TarMaapingNotesFile
					object.setFilesize(tarMappingFile.length());
					object.setSourceFileName(tarMappingFile.getName());
					object.setSourceFilePath(tarMappingFile.getAbsolutePath());
					object.setTarStartTimestamp(null);
					object = dmeSyncWorkflowService.getService(access).saveStatusInfo(object);

				}
			} else {
				String tarFileName = object.getOrginalFileName() + ".tar";
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
				// object.setTarEndTimestamp(new Date());
				object = dmeSyncWorkflowService.getService(access).saveStatusInfo(object);
			}
		} catch (Exception e) {

			logger.error("[{}] error {}", super.getTaskName(), e.getMessage(), e);
			// throw new DmeSyncWorkflowException("Error occurred during tar. " +
			// e.getMessage(), e);
		}

		return object;
	}

	private int retrievePreviousUploadedTarNumber(StatusInfo object) {

		int lastUploadedTarNumber = 0;

		StatusInfo latestTar = dmeSyncWorkflowService.getService(access)
				.findTopStatusInfoByDocAndOriginalFilePathStartsWithOrderByTarEndTimestampDesc(object.getDoc(),
						object.getOriginalFilePath());
		if (latestTar != null) {
			// get the last tar Number based on sourcfileName
			Pattern pattern = Pattern.compile(multipleTarFileExtension + "(\\d+)");
			Matcher matcher = pattern.matcher(latestTar.getSourceFileName());
			if (matcher.find()) {
				// Extract the number from the matched group
				String numberString = matcher.group(1);
				// Convert the extracted string to an integer
				lastUploadedTarNumber = Integer.parseInt(numberString);

			}
		}

		return lastUploadedTarNumber;

	}
	
	private StatusInfo insertNewRowforTar(StatusInfo object, String tarFileName, String tarFile) {
		
		File createdTarFile = new File(tarFile);

		StatusInfo statusInfo = new StatusInfo();
		statusInfo.setRunId(object.getRunId());
		statusInfo.setOrginalFileName(object.getOrginalFileName());
		statusInfo.setOriginalFilePath(object.getOriginalFilePath());
		statusInfo.setSourceFileName(tarFileName);
		statusInfo.setSourceFilePath(tarFile);
		statusInfo.setFilesize(createdTarFile.length());
		statusInfo.setStartTimestamp(new Date());
		statusInfo.setTarStartTimestamp(new Date());
		statusInfo.setTarEndTimestamp(new Date());
		statusInfo.setDoc(object.getDoc());

		statusInfo = dmeSyncWorkflowService.getService(access).saveStatusInfo(statusInfo);
		
		return statusInfo;
		
		
	}
}
