package gov.nih.nci.hpc.dmesync.service.impl;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
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
  public void sendResult(String runId) {
    MimeMessage message = sender.createMimeMessage();

    try {

      List<StatusInfo> statusInfo = dmeSyncWorkflowService.getService(access).findStatusInfoByRunIdAndDoc(runId, doc);
      List<MetadataInfo> metadataInfo = dmeSyncWorkflowService.getService(access).findAllMetadataInfoByRunIdAndDoc(runId, doc);
      Path path = Paths.get(logFile);
      String excelFile = ExcelUtil.export(runId, statusInfo, metadataInfo, path.getParent().toString());

      MimeMessageHelper helper = new MimeMessageHelper(message, true);

      helper.setFrom("hpcdme-sync");
      helper.setTo(adminEmails.split(","));
      helper.setSubject("HPCDME Auto Archival Result for " + doc.toUpperCase() + " Run_ID: " + runId + " - Base Path: " + syncBaseDir);
      String body = "Attached results from DME auto archival runId, " + runId + " for " + doc.toUpperCase() + " and base path, " + syncBaseDir;
      // Check to see if any files were over the recommended size and flag if it was.
      boolean exceedsMaxRecommendedFileSize = false;
      long maxFileSize = Long.parseLong(maxRecommendedFileSize);
      long processedCount = 0, successCount = 0, failedCount = 0;
      for (StatusInfo info : statusInfo) {
    	  processedCount ++;
    	  if (info.getFilesize() > maxFileSize) {
    		  exceedsMaxRecommendedFileSize = true;
    	  }
    	  if (info.getStatus().equals("COMPLETED"))
    		  successCount++;
    	  else
    		  failedCount++;
      }
      
      body = body.concat("\n\nSummary - Total processed: " + processedCount + ", Success: " + successCount + ", Failure: " + failedCount);
      body = body.concat("\n\nPlease review the attached results for any discrepancies in expected file size, missing or incorrect metadata.");
      
      if(exceedsMaxRecommendedFileSize)
    	  body = body.concat("\n\nThere was a file that exceeds the recommended file size of " + ExcelUtil.humanReadableByteCount(maxFileSize, true));
      helper.setText(body);
      
      FileSystemResource file = new FileSystemResource(excelFile);
      helper.addAttachment(file.getFilename(), file);
      sender.send(message);
      
    } catch (MessagingException e) {
      throw new MailParseException(e);
    }
    
  }
}
