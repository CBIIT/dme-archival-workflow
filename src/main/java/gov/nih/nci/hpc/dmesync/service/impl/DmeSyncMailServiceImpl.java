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
import gov.nih.nci.hpc.dmesync.domain.MetadataInfo;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.service.DmeSyncMailService;
import gov.nih.nci.hpc.dmesync.service.DmeSyncWorkflowService;
import gov.nih.nci.hpc.dmesync.util.ExcelUtil;

@Service("defaultMailService")
public class DmeSyncMailServiceImpl implements DmeSyncMailService {
  @Autowired private JavaMailSender sender;
  @Autowired private DmeSyncWorkflowService dmeSyncWorkflowService;

  @Value("${dmesync.admin.emails}")
  private String adminEmails;
  
  @Value("${logging.file}")
  private String logFile;
  
  @Value("${dmesync.doc.name}")
  private String doc;
  
  @Value("${dmesync.source.base.dir}")
  private String syncBaseDir;
  
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

      List<StatusInfo> statusInfo = dmeSyncWorkflowService.findStatusInfoByRunId(runId);
      List<MetadataInfo> metadataInfo = dmeSyncWorkflowService.findAllMetadataInfoByRunId(runId);
      Path path = Paths.get(logFile);
      String excelFile = ExcelUtil.export(runId, statusInfo, metadataInfo, path.getParent().toString());

      MimeMessageHelper helper = new MimeMessageHelper(message, true);

      helper.setFrom("hpcdme-sync");
      helper.setTo(adminEmails.split(","));
      helper.setSubject("HPCDME Auto Archival Result for " + doc.toUpperCase() + " Run_ID: " + runId + " - Base Path: " + syncBaseDir);
      helper.setText("Attached results from DME auto archival runId, " + runId + " for " + doc.toUpperCase() + " and base path, " + syncBaseDir);

      FileSystemResource file = new FileSystemResource(excelFile);
      helper.addAttachment(file.getFilename(), file);
      sender.send(message);
      
    } catch (MessagingException e) {
      throw new MailParseException(e);
    }
    
  }
}
