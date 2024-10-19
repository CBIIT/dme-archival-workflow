package gov.nih.nci.hpc.dmesync.service.impl;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
import gov.nih.nci.hpc.dmesync.domain.MetadataInfo;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.dto.DmeCSBMailBodyDto;
import gov.nih.nci.hpc.dmesync.service.DmeSyncMailService;
import gov.nih.nci.hpc.dmesync.util.ExcelUtil;

@Service("defaultMailService")
public class DmeSyncMailServiceImpl implements DmeSyncMailService {
  @Autowired private JavaMailSender sender;
  @Autowired private DmeSyncWorkflowServiceFactory dmeSyncWorkflowService;

  @Value("${dmesync.db.access:local}")
  private String access;

  @Value("${dmesync.admin.emails}")
  private String adminEmails;
  
  @Value("${logging.file}")
  private String logFile;
  
  @Value("${dmesync.doc.name}")
  private String doc;
  
  @Value("${dmesync.source.base.dir}")
  private String syncBaseDir;
  
  @Value("${dmesync.max.recommended.file.size}")
  private String maxRecommendedFileSize;
  
  @Value("${dmesync.min.tar.file.size:1024}")
  private String minTarFile;
  
  @Value("${dmesync.multiple.tars.files.count:0}")
  private Integer filesPerTar;
  
  final Logger logger = LoggerFactory.getLogger(getClass().getName());
  
  
  
  @Override
  public String sendMail(String subject, String text) {
    MimeMessage message = sender.createMimeMessage();
    MimeMessageHelper helper = new MimeMessageHelper(message);

    try {
      helper.setFrom("hpcdme-sync");
      helper.setTo(adminEmails.split(","));
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
  public String sendErrorMail(String subject, String text) {
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
  public void sendResult(String runId) {
    MimeMessage message = sender.createMimeMessage();
    int minTarFileCount = 0; 
    String subject;

    try {

      List<StatusInfo> statusInfo = dmeSyncWorkflowService.getService(access).findStatusInfoByRunIdAndDoc(runId, doc);
      List<MetadataInfo> metadataInfo = dmeSyncWorkflowService.getService(access).findAllMetadataInfoByRunIdAndDoc(runId, doc);
      Path path = Paths.get(logFile);
      String excelFile = ExcelUtil.export(runId, statusInfo, metadataInfo, path.getParent().toString());
      
      MimeMessageHelper helper = new MimeMessageHelper(message, MimeMessageHelper.MULTIPART_MODE_MIXED_RELATED, StandardCharsets.UTF_8.name());
      helper.setFrom("hpcdme-sync");
      helper.setTo(adminEmails.split(","));
        
      String body = "<p>The attached file contains results from DME auto-archive.</p>";
      body = body + "<p>Base Path: " + syncBaseDir + "</p>";
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
      
      if("csb".equals(doc)) {

			Set<String> folderPaths = statusInfo.stream().map(StatusInfo::getOriginalFilePath)
					.collect(Collectors.toSet());
			List<DmeCSBMailBodyDto> folders = new ArrayList<>();
			for (String name : folderPaths) {
				if (name.contains("movies")) {
					List<StatusInfo> allUploads = dmeSyncWorkflowService.getService(access)
							.findAllByDocAndLikeOriginalFilePath(doc, name);
					// Fetch details for each folder name
					DmeCSBMailBodyDto folder = new DmeCSBMailBodyDto();
					File directory = new File(name);
					File[] files = directory.listFiles();
					int expectedTars = (files.length + filesPerTar - 1) / filesPerTar;
					folder.setFolderName(name);
					folder.setFilesCount(files.length);
					// adding + 1 to include tarMapping notes,movies folder statusInfo rows
					folder.setExpectedTars(expectedTars );
					// Removing the movies folder, tar contents file since we are only displaying the data for tars
					folder.setCreatedTars(allUploads.size()-2);
					folder.setUploadedTars(allUploads.stream().filter(tar -> ("COMPLETED".equals(tar.getStatus()) && tar.getUploadEndTimestamp()!=null &&
							tar.getTarEndTimestamp()!=null)) 
							.count());
					folder.setFailedTars(allUploads.stream().filter(tar -> (tar.getStatus() == null  && tar.getUploadEndTimestamp()!=null &&
							tar.getTarEndTimestamp()!=null)) 
							.count());
					folders.add(folder);
				}
			}
          String htmlContent = buildHtmlTable(folders);

    	 updatedBody= body+ htmlContent;
      }
	  subject = (failedCount > 0) ? "Failed " : "Completed ";
	  helper.setSubject("DME Auto Archival " + subject + "for  " + doc.toUpperCase() + " - Run_ID: " + runId
				+ " - Base Path:  " + syncBaseDir);

      helper.setText(updatedBody,true);
      
      FileSystemResource file = new FileSystemResource(excelFile);
      helper.addAttachment(file.getFilename(), file);
      sender.send(message);
      
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
}
