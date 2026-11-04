package gov.nih.nci.hpc.dmesync.service.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import javax.mail.BodyPart;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.internet.MimeMessage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.test.util.ReflectionTestUtils;

class DmeSyncMailServiceImplTest {
  @TempDir
  Path tempDir;

  @Test
  void attachesReviewCsvAlongsideReportForScafDryRun() throws Exception {
    Path csv = Files.writeString(tempDir.resolve("scaf-review.csv"), "OriginalFilePath,Approved\n/source,\n");
    DmeSyncMailServiceImpl service = service(true, "scaf", csv);
    MimeMessage message = new MimeMessage(Session.getInstance(new Properties()));
    MimeMessageHelper helper = new MimeMessageHelper(message, true);
    helper.setText("Workflow report");
    Path report = Files.writeString(tempDir.resolve("report.xlsx"), "report");
    helper.addAttachment("report.xlsx", report.toFile());

    assertTrue(service.attachScafReviewFile(helper).contains("also attached"));
    message.saveChanges();
    Multipart multipart = (Multipart) message.getContent();
    assertEquals(3, multipart.getCount());
    BodyPart attachment = multipart.getBodyPart(2);
    assertEquals("scaf-review.csv", attachment.getFileName());
    assertTrue(attachment.isMimeType("text/csv"));
    assertEquals(Files.readString(csv),
        new String(attachment.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
  }

  @Test
  void missingReviewFileProducesExplicitEmailNotice() throws Exception {
    DmeSyncMailServiceImpl service = service(true, "scaf", tempDir.resolve("missing.csv"));
    MimeMessageHelper helper = new MimeMessageHelper(new MimeMessage(Session.getInstance(new Properties())), true);
    assertTrue(service.attachScafReviewFile(helper).contains("missing or unreadable"));
  }

  @Test
  void leavesNonScafAndActualUploadEmailsUnchanged() throws Exception {
    Path csv = Files.writeString(tempDir.resolve("review.csv"), "Approved\n");
    MimeMessageHelper helper = new MimeMessageHelper(new MimeMessage(Session.getInstance(new Properties())), true);
    assertEquals("", service(false, "scaf", csv).attachScafReviewFile(helper));
    assertEquals("", service(true, "default", csv).attachScafReviewFile(helper));
    assertEquals("", service(true, "scaf", Path.of("")).attachScafReviewFile(helper));
  }

  private DmeSyncMailServiceImpl service(boolean dryRun, String doc, Path review) {
    DmeSyncMailServiceImpl service = new DmeSyncMailServiceImpl();
    ReflectionTestUtils.setField(service, "dryRun", dryRun);
    ReflectionTestUtils.setField(service, "doc", doc);
    ReflectionTestUtils.setField(service, "scafReviewFile", review.toString());
    return service;
  }
}
