package gov.nih.nci.hpc.dmesync;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import gov.nih.nci.hpc.dmesync.service.DmeSyncWorkflowService;

@Component
public class DmeSyncWorkflowServiceFactory {
  @Autowired
  @Qualifier("local")
  private DmeSyncWorkflowService localService;

  @Autowired
  @Qualifier("remote")
  private DmeSyncWorkflowService remoteService;

  public DmeSyncWorkflowService getService(String access) {
    if ("remote".equals(access)) {
      return remoteService;
    } else
      return localService;
  }
}
