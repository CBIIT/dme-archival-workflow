package gov.nih.nci.hpc.dmesync.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import gov.nih.nci.hpc.dmesync.DmeSyncApplication;
import gov.nih.nci.hpc.dmesync.DmeSyncMailServiceFactory;
import gov.nih.nci.hpc.dmesync.DmeSyncWorkflowServiceFactory;

@RestController
public class RestartController {

  @Autowired private DmeSyncWorkflowServiceFactory dmeSyncWorkflowService;
  @Autowired protected DmeSyncMailServiceFactory mailServiceFactory;

  @Value("${dmesync.db.access:local}")
  private String access;
  
  @Value("${dmesync.doc.name}")
  private String doc;
  
  @Value("${dmesync.source.base.dir}")
  private String syncBaseDir;
  
  @GetMapping("/restart")
  public void restart() {
    DmeSyncApplication.restart();
  }

  @GetMapping(value = {"/export", "/export/{runId}"})
  public void export(@PathVariable(required = false) String runId) {
    if (runId == null || runId.isEmpty()) {
      //find the latest runId
      runId = dmeSyncWorkflowService.getService(access).findTopStatusInfoByDocAndOriginalFilePathStartsWithOrderByStartTimestampDesc(doc, syncBaseDir).getRunId();
    }
    mailServiceFactory.getService(doc).sendResult(runId);
  }
}
