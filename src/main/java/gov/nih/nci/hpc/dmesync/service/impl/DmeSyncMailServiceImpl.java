package gov.nih.nci.hpc.dmesync.service.impl;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;

import org.apache.commons.codec.binary.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.mail.MailParseException;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;

import gov.nih.nci.hpc.dmesync.DmeSyncWorkflowServiceFactory;
import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import gov.nih.nci.hpc.dmesync.domain.MetadataInfo;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.dto.DmeCSBMailBodyDto;
import gov.nih.nci.hpc.dmesync.service.DmeSyncMailService;
import gov.nih.nci.hpc.dmesync.service.DmeSyncWorkflowRunLogService;
import gov.nih.nci.hpc.dmesync.util.ExcelUtil;
import gov.nih.nci.hpc.dmesync.util.WorkflowConstants;

@Service("defaultMailService")
public class DmeSyncMailServiceImpl implements DmeSyncMailService {
  @Autowired private JavaMailSender sender;
  @Autowired private DmeSyncWorkflowServiceFactory dmeSyncWorkflowService;
  @Autowired private DmeSyncWorkflowRunLogService dmeSyncWorkflowRunLogService;


  @Value("${dmesync.db.access:local}")
  private String access;

  @Value("${logging.file.name}")
  private String logFile;
  
  @Value("${dmesync.max.recommended.file.size}")
  private String maxRecommendedFileSize;
  
  @Value("${dmesync.min.tar.file.size:1024}")
  private String minTarFile;
  
  final Logger logger = LoggerFactory.getLogger(getClass().getName());
  
  
  
  
  @Override
  public String sendMail(String subject, String text, DocConfig config) {
	DocConfig.NotificationConfig mailConfig = config.getNotificationConfig();
    MimeMessage message = sender.createMimeMessage();
    MimeMessageHelper helper = new MimeMessageHelper(message);

    try {
      helper.setFrom("hpcdme-sync");
      helper.setTo(mailConfig.recipients.split(","));
      helper.setText(text);
      helper.setSubject(subject);
    } catch (MessagingException e) {
      logger.error("Error while sending mail", e);
      return "ERROR";
    }
    sender.send(message);
    return "SUCCESS";
  }
  
  @Override
  public String sendErrorMail(String subject, String text, DocConfig config) {
    MimeMessage message = sender.createMimeMessage();
    MimeMessageHelper helper = new MimeMessageHelper(message);

    try {
      helper.setFrom("hpcdme-sync");
      helper.setTo("HPC_DME_Admin@mail.nih.gov");
      helper.setText(text);
      helper.setSubject(subject);
    } catch (MessagingException e) {
      logger.error("Error while sending mail", e);
      return "ERROR";
    }
    sender.send(message);
    return "SUCCESS";
  }

  @Override
  public void sendResult(String runId, DocConfig config) {
	DocConfig.NotificationConfig mailConfig = config.getNotificationConfig();
	DocConfig.SourceConfig sourceConfig = config.getSourceConfig();
	DocConfig.PreprocessingRule preRule = config.getPreprocessingRule();
    MimeMessage message = sender.createMimeMessage();
    int minTarFileCount = 0; 
    String subject;

    try {

      List<StatusInfo> statusInfo = dmeSyncWorkflowService.getService(access).findStatusInfoByRunIdAndDoc(runId, config.getDocName());
     
      if(preRule.tarContentsFile) {
    	  statusInfo=generateAggregrateRecords(statusInfo, config);
      }
      List<MetadataInfo> metadataInfo = dmeSyncWorkflowService.getService(access).findAllMetadataInfoByRunIdAndDoc(runId, config.getDocName());
      // Sort in-place ascending (A->Z), nulls last
	  statusInfo.sort(Comparator.comparing(StatusInfo::getOriginalFilePath,
				Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER)));
      
      Path path = Paths.get(logFile);
      String excelFile = ExcelUtil.export(runId, statusInfo, metadataInfo, path.getParent().toString());
      
      Path safeDirectory = path.getParent().toAbsolutePath().normalize();
      Path filePath = Paths.get(excelFile).toAbsolutePath().normalize();
      if (!filePath.startsWith(safeDirectory)) {
    	  logger.info("Generated file path is outside the safe directory");;
          throw new IllegalArgumentException("Generated file path is outside the safe directory");
      }
      
      MimeMessageHelper helper = new MimeMessageHelper(message, MimeMessageHelper.MULTIPART_MODE_MIXED_RELATED, StandardCharsets.UTF_8.name());
      helper.setFrom("hpcdme-sync");
      helper.setTo(mailConfig.recipients.split(","));
        
      String body = "<p>The attached file contains results from DME auto-archive.</p>";
      body = body + "<p>Base Path: " + sourceConfig.sourceBaseDir + "</p>";
      body = body + "<p>Below is the summary:</p>";
              
      // Check to see if any files were over the recommended size and flag if it was.
      boolean exceedsMaxRecommendedFileSize = false;
      long maxFileSize = Long.parseLong(maxRecommendedFileSize);
      long processedCount = 0, successCount = 0, failedCount = 0;
      for (StatusInfo info : statusInfo) {
    	  processedCount ++;
    	 
    	  if (info.getFilesize() > maxFileSize) {
    		  
    		  exceedsMaxRecommendedFileSize = true;	  
    	     
    	  } 
    		     	  
    	  if (info.getOrginalFileName().contains(".tar")) {
	    	  if (info.getFilesize() < Integer.valueOf(minTarFile)) { 
	    		   
	    		  minTarFileCount++;	    	
	    	  }
    	  }   	 
   		      	  
    	  if (StringUtils.equals(info.getStatus(), "COMPLETED"))
    	  {	  successCount++; }
    	  else {
    		  failedCount++;
    	  }
      }
       
      body = body.concat("<ul>"
                                  +  "<li>"+ "Total processed: " + processedCount + "</li>"
    		                      + "<li>" + "Success: " + successCount +"</li>"
                                  + "<li>" + "Failure: " + failedCount + "</li>"  
    		                      + "<li>" + "Tar files with sizes smaller than " + ExcelUtil.humanReadableByteCount(Long.valueOf(minTarFile), true) + ": " + minTarFileCount +
    		             "</ul>");
     
        
      body = body.concat("<p><b><i> A Failure count of zero does not guarantee the accuracy of the metadata or the file size."
      		+ " Hence, please review the attached results and reply all to this email to report any discrepancy.</i> </b> </p>");
      
      
      if(exceedsMaxRecommendedFileSize)
    	  body = body.concat("<p><b><i>There was a file that exceeds the recommended file size of " + ExcelUtil.humanReadableByteCount(maxFileSize, true) + ".</p></b></i>");
      String updatedBody=body;
      
      if("csb".equals(config.getDocName())) {

			Set<String> folderPaths = statusInfo.stream().map(StatusInfo::getOriginalFilePath)
					.collect(Collectors.toSet());
			List<DmeCSBMailBodyDto> folders = new ArrayList<>();
			for (String name : folderPaths) {
				if (name.contains("movies")) {
					List<StatusInfo> allUploads = dmeSyncWorkflowService.getService(access)
							.findAllByDocAndLikeOriginalFilePath(config.getDocName(), name);
					// Removing the movies folder, tar contents file since we are only displaying the data for tars
					allUploads.removeIf(file -> {
			            String filename = file.getSourceFileName();
			            return !filename.isEmpty() && (filename.endsWith(".txt") || filename.equalsIgnoreCase("movies"));
			        });					
					// Fetch details for each folder name
					DmeCSBMailBodyDto folder = new DmeCSBMailBodyDto();
					File directory = new File(name);
					File[] files = directory.listFiles();
					int expectedTars = (files.length + preRule.multipleTarsFilesCount - 1) / preRule.multipleTarsFilesCount;
					folder.setFolderName(name);
					folder.setFilesCount(files.length);
					// adding + 1 to include tarMapping notes,movies folder statusInfo rows
					folder.setExpectedTars(expectedTars );
					folder.setCreatedTars(allUploads.size());
					folder.setUploadedTars(allUploads.stream().filter(tar -> ("COMPLETED".equals(tar.getStatus()))) 
							.count());
					folder.setFailedTars(allUploads.stream().filter(tar -> (tar.getStatus() == null ))
							.count());
					folders.add(folder);
				}
			}
          String htmlContent = buildHtmlTable(folders);

    	 updatedBody= body+ htmlContent;
      }
	  subject = (failedCount > 0) ? "Failed " : "Completed ";
	  helper.setSubject("DME Auto Archival " + subject + "for  " + config.getDocName().toUpperCase() + " - Run_ID: " + runId
				+ " - Base Path:  " + sourceConfig.sourceBaseDir);

      helper.setText(updatedBody,true);
      
	  String status= (failedCount > 0) ? WorkflowConstants.RunStatus.FAILED.toString() : WorkflowConstants.RunStatus.SUCCEEDED.toString();
      
      FileSystemResource file = new FileSystemResource(excelFile);
      helper.addAttachment(file.getFilename(), file);
      sender.send(message);
      logger.info("Workflow Run is completed");
      try {
          dmeSyncWorkflowRunLogService.updateWorkflowRunEnd(runId, config.getDocName(), status, null);
       } catch (IllegalArgumentException ex) {
          logger.warn("Unable to update workflow run log for runId {} and doc {}: {}", runId, config.getDocName(), ex.getMessage());
      }
      
    } catch (MessagingException e) {
      throw new MailParseException(e);
    }
    
  }
  
  
  
  public String buildHtmlTable(List<DmeCSBMailBodyDto> folders) {
      StringBuilder htmlBuilder = new StringBuilder();

      htmlBuilder.append("<p>Movies folder archival details</p>")
                 .append("<table border='1' style='border-collapse:collapse; width:100%;'>")
                 .append("<thead>")
                 .append("<tr>")
                 .append("<th>Folder Name</th>")
                 .append("<th>Files Count</th>")
                 .append("<th>Expected uploads</th>")
                 .append("<th>Processed</th>")
                 .append("<th>Uploaded</th>")
                 .append("<th>Failed</th>")
                 .append("</tr>")
                 .append("</thead>")
                 .append("<tbody>");

      for (DmeCSBMailBodyDto folder : folders) {
          htmlBuilder.append("<tr>")
                     .append("<td>").append(folder.getFolderName()).append("</td>")
                     .append("<td>").append(folder.getFilesCount()).append("</td>")
                     .append("<td>").append(folder.getExpectedTars()).append("</td>")
                     .append("<td>").append(folder.getCreatedTars()).append("</td>")
                     .append("<td>").append(folder.getUploadedTars()).append("</td>")
                     .append("<td>").append(folder.getFailedTars()).append("</td>")
                     .append("</tr>");
      }

      htmlBuilder.append("</tbody>")
                 .append("</table>");
                

      return htmlBuilder.toString();
  }
  
	
  private List<StatusInfo> generateAggregrateRecords(List<StatusInfo> runIdRecords, DocConfig config) {
	  
	  	DocConfig.PreprocessingRule preRule = config.getPreprocessingRule();
		List<StatusInfo> aggregateFolderRecords = new ArrayList<>();

		Set<String> folderPaths = runIdRecords.stream().map(StatusInfo::getOriginalFilePath)
				.collect(Collectors.toSet());

		for (String folder : folderPaths) {

			List<StatusInfo> folderRows = dmeSyncWorkflowService.getService(access)
					.findAllByDocAndLikeOriginalFilePath(config.getDocName(), folder);

			Optional<StatusInfo> tarRecordOpt = folderRows.stream()
					.filter(p -> !p.getSourceFilePath().endsWith(WorkflowConstants.tarContentsFileEndswith)
							&& !p.getSourceFilePath().endsWith(WorkflowConstants.tarExcludedContentsFileEndswith))
					.findFirst(); // Find the TAR file record

			StatusInfo includedFileRecord = folderRows.stream()
					.filter(p -> p.getSourceFilePath().endsWith(WorkflowConstants.tarContentsFileEndswith)).findFirst()
					.orElse(null); // Find the included contents file record
			StatusInfo tarRecord = tarRecordOpt.get();

			// Only check for both included and excluded contents file if the property is true
			StatusInfo excludedFileRecord = folderRows.stream().filter(
					p -> p.getSourceFilePath().endsWith(WorkflowConstants.tarExcludedContentsFileEndswith))
					.findFirst().orElse(null); // Find the excluded contents file record
			
			// Check for the TAR record
			if (tarRecordOpt.isPresent()) {

				// Handle contents files
				if (preRule.tarExcludedContentsFile && excludedFileRecord!=null ) {


					if (includedFileRecord != null 
							&&  WorkflowConstants.COMPLETED.equals(tarRecord.getStatus())
							&&  WorkflowConstants.COMPLETED.equals(includedFileRecord.getStatus())
							&&  WorkflowConstants.COMPLETED.equals(excludedFileRecord.getStatus())) {
						tarRecord.setStatus("COMPLETED");
						// tarRecord.setError(""); // No error
					} else {
						if ( WorkflowConstants.COMPLETED.equals(tarRecord.getStatus())) {
							if (includedFileRecord != null &&  WorkflowConstants.COMPLETED.equals(includedFileRecord.getStatus())) {
								tarRecord.setStatus(
										"FAILED.Excluded contents file not uploaded, only TAR file and Included contents file got uploaded.");
								tarRecord.setError(excludedFileRecord.getError());
							} else if ( WorkflowConstants.COMPLETED.equals(excludedFileRecord.getStatus())) {
								tarRecord.setStatus(
										"FAILED.Included contents file not uploaded, only TAR file and Excluded contents file got uploaded.");
								tarRecord.setError(includedFileRecord!=null? includedFileRecord.getError(): "Contents file has not been created, check if folder is empty.");

							} else {
								tarRecord.setStatus(
										"FAILED.Both included and excluded contents files not uploaded, only TAR file uploaded.");
								tarRecord.setError(includedFileRecord!=null? includedFileRecord.getError(): "Contents file has not been created, check if folder is empty.");

							}
						} else {
							if (includedFileRecord != null &&  WorkflowConstants.COMPLETED.equals(includedFileRecord.getStatus())
									&&  WorkflowConstants.COMPLETED.equals(excludedFileRecord.getStatus())) {
								tarRecord.setStatus("FAILED.Tar file not uploaded, only contents files got uploaded.");
							} else if ( WorkflowConstants.COMPLETED.equals(excludedFileRecord.getStatus())) {
								tarRecord.setStatus(
										"FAILED.Tar file and Included contents file not uploaded, only Excluded contents file got uploaded.");
							} else if (includedFileRecord != null
									&&  WorkflowConstants.COMPLETED.equals(includedFileRecord.getStatus())) {
								tarRecord.setStatus(
										"FAILED.Tar file and Excluded  contents file not uploaded, only Included contents file got uploaded.");
							} else {
								tarRecord.setStatus("FAILED.Tar file and both contents file not uploaded.");
							}
						}
					}
				} else {
					// If excluded contents file is not set, just handle included contents file
					if (includedFileRecord != null &&  WorkflowConstants.COMPLETED.equals(tarRecord.getStatus())
							&&  WorkflowConstants.COMPLETED.equals(includedFileRecord.getStatus())) {
						tarRecord.setStatus("COMPLETED");
						// tarRecord.setError(""); // No error
					} else {
						if ( WorkflowConstants.COMPLETED.equals(tarRecord.getStatus())) {
							tarRecord.setStatus("FAILED.Contents file not uploaded, only TAR file uploaded.");
							tarRecord.setError(includedFileRecord!=null ? includedFileRecord.getError(): "Contents file has not been created, check if folder is empty.");
						} else if (includedFileRecord != null &&  WorkflowConstants.COMPLETED.equals(includedFileRecord.getStatus())) {
							tarRecord.setStatus("FAILED.TAR file not uploaded, only contents file uploaded.");
						} else {
							tarRecord.setStatus("FAILED.TAR file and contents file not uploaded.");
						}
					}
				}
			}

			aggregateFolderRecords.add(tarRecord);
			// Save the updated StatusInfo record back to the database
			// StatusInfoRepository.save(StatusInfo);
		}
		return aggregateFolderRecords;
      }
  
	
}
