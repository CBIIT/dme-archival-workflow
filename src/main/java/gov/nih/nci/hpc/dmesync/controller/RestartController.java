package gov.nih.nci.hpc.dmesync.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import gov.nih.nci.hpc.dmesync.DmeSyncApplication;

@RestController
public class RestartController {

  @GetMapping("/restart")
  public void restart() {
    DmeSyncApplication.restart();
  }

}
