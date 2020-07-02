package gov.nih.nci.hpc.dmesync;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import gov.nih.nci.hpc.dmesync.service.DmeSyncMailService;

@Component
public class DmeSyncMailServiceFactory {
  @Autowired
  @Qualifier("defaultMailService")
  private DmeSyncMailService defaultMailService;

  @Autowired
  @Qualifier("hitifMailService")
  private DmeSyncMailService hitifMailService;

  public DmeSyncMailService getService(String doc) {
    if ("hitif".equals(doc)) {
      return hitifMailService;
    } else {
      return defaultMailService;
    }
  }
}
