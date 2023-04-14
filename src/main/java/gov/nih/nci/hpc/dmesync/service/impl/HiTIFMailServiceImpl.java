package gov.nih.nci.hpc.dmesync.service.impl;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.mail.MailParseException;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;
import org.thymeleaf.util.StringUtils;

import gov.nih.nci.hpc.dmesync.DmeSyncWorkflowServiceFactory;
import gov.nih.nci.hpc.dmesync.domain.CollectionNameMapping;
import gov.nih.nci.hpc.dmesync.domain.MetadataInfo;
import gov.nih.nci.hpc.dmesync.domain.MetadataMapping;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.service.DmeSyncMailService;
import gov.nih.nci.hpc.dmesync.util.ExcelUtil;

@Service("hitifMailService")
public class HiTIFMailServiceImpl implements DmeSyncMailService {
  @Autowired private JavaMailSender sender;
  @Autowired private DmeSyncWorkflowServiceFactory dmeSyncWorkflowService;

  @Value("${dmesync.db.access:local}")
  private String access;
  
  @Value("${dmesync.doc.name}")
  private String doc;
  
  @Value("${dmesync.admin.emails}")
  private String adminEmails;

  @Value("${dmesync.send.user.emails:false}")
  private boolean sendUserEmails;

  @Value("${logging.file}")
  private String logFile;
  
  @Value("${dmesync.source.base.dir}")
  private String syncBaseDir;
  
  @Value("${dmesync.max.recommended.file.size}")
  private String maxRecommendedFileSize;
  
  final Logger logger = LoggerFactory.getLogger(getClass().getName());

  @Override
  public String sendMail(String subject, String text) {
    MimeMessage message = sender.createMimeMessage();
    MimeMessageHelper helper = new MimeMessageHelper(message);
    String userEmails = null;
    String allEmails = adminEmails;

    try {
      helper.setFrom("hpcdme-sync");
      // Check if missing mrf file email, then extract
      // the user from the path to send out to the user as well as admin.
      if (StringUtils.contains(text, "does not contain any").booleanValue()) {
        userEmails = extractUserEmailFromPath(text);
        if (!StringUtils.isEmpty(userEmails)) allEmails = String.join(",", userEmails, adminEmails);
      }
      if (sendUserEmails) {
        helper.setTo(allEmails.split(","));
        helper.setSubject(subject);
      } else {
        helper.setTo(adminEmails.split(","));
        helper.setSubject(subject + " [to: " + allEmails + "]");
      }
      helper.setText(text);
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
    String userEmails = null;
    String allEmails = adminEmails;

    try {

      List<StatusInfo> statusInfo = dmeSyncWorkflowService.getService(access).findStatusInfoByRunIdAndDoc(runId, doc);
      if (CollectionUtils.isNotEmpty(statusInfo)) {
        List<MetadataInfo> metadataInfo = dmeSyncWorkflowService.getService(access).findAllMetadataInfoByRunIdAndDoc(runId, doc);
        Path path = Paths.get(logFile);
        String excelFile = ExcelUtil.export(runId, statusInfo, metadataInfo, path.getParent().toString());

        MimeMessageHelper helper = new MimeMessageHelper(message, true);

        helper.setFrom("hpcdme-sync");
        // Extract all users from the original file path and add to the to line.
        userEmails = extractUserEmailsFromRun(runId);
        if (!StringUtils.isEmpty(userEmails)) allEmails = String.join(",", userEmails, adminEmails);
        if (sendUserEmails) {
          helper.setTo(allEmails.split(","));
          helper.setSubject("Archival Report for HiTIF - " + runId);
        } else {
          helper.setTo(adminEmails.split(","));
          helper.setSubject("Archival Report for HiTIF - " + runId + " [to: " + allEmails + "]");
        }
        String body = "Please find attached DME archival report for your data in " + syncBaseDir + " folder.";
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
      }
    } catch (MessagingException e) {
      throw new MailParseException(e);
    }
  }

  /**
   * Extract user email from the path provided in the message.
   *
   * @param text
   * @return the email address
   */
  private String extractUserEmailFromPath(String text) {
    String userEmail = null;
    try {
      String userPath = StringUtils.substringAfter(text, "MeasurementData");
      userPath = StringUtils.substringBefore(userPath, " does not");
      String user = userPath.replace("\\", "/").split("/")[1];
      CollectionNameMapping collectionName =
          dmeSyncWorkflowService.getService(access).findCollectionNameMappingByMapKeyAndCollectionTypeAndDoc(user, "User", doc);
      MetadataMapping metadataMapping =
          dmeSyncWorkflowService.getService(access)
              .findByMetadataMappingByCollectionTypeAndCollectionNameAndMetaDataKeyAndDoc(
                  "User", collectionName.getMapValue(), "email", doc);
      userEmail = metadataMapping.getMetaDataValue();
    } catch (Exception e) {
      logger.error("User email could not be extracted from message: {}", text);
    }
    return userEmail;
  }

  /**
   * Extract user emails for the specified runId.
   *
   * @param runId
   * @return user email addresses
   */
  private String extractUserEmailsFromRun(String runId) {
    String userEmail = null;
    try {
      List<MetadataInfo> metadataInfo =
          dmeSyncWorkflowService.getService(access).findAllMetadataInfoByRunIdAndMetaDataKeyAndDoc(runId, "email", doc);
      Set<String> set = new HashSet<>();
      for (MetadataInfo user : metadataInfo) {
        set.add(user.getMetaDataValue());
      }
      userEmail = String.join(",", set);
    } catch (Exception e) {
      logger.error("User emails could not be extracted for runId: {}", runId);
    }
    return userEmail;
  }
}
