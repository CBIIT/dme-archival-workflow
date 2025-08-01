package gov.nih.nci.hpc.dmesync.jms;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Message;
import javax.jms.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import gov.nih.nci.hpc.dmesync.DmeSyncWorkflowServiceFactory;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.dto.DmeSyncMessageDto;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncWorkflow;

/**
 * DME Sync Consumer (Listener)
 * 
 * @author dinhys
 */
@Component
public class DmeSyncConsumer {

  private static final Logger log = LoggerFactory.getLogger(DmeSyncConsumer.class);

  @Value("${dmesync.db.access:local}")
  private String access;
  
  @Autowired private DmeSyncWorkflow dmeSyncWorkflow;

  @Autowired private DmeSyncWorkflowServiceFactory dmeSyncWorkflowService;
  
  private final AtomicInteger activeThreads = new AtomicInteger(0);

  public void threadStarted() {
	  activeThreads.incrementAndGet();
  }

  public void threadCompleted() {
	  activeThreads.decrementAndGet();
  }

  public boolean isAllThreadsCompleted() {
      return activeThreads.get() == 0;
  }

  @JmsListener(destination = "inbound.queue")
  public String receiveMessage(
      @Payload DmeSyncMessageDto syncMessage,
      @Headers MessageHeaders headers,
      Message message,
      Session session)
      throws DmeSyncWorkflowException {

    log.debug("[JMS Listener] Received message <{}>", syncMessage);
    threadStarted();

    try {

      // Get StatusInfo from DB
      Optional<StatusInfo> statusInfo = dmeSyncWorkflowService.getService(access).findStatusInfoById(syncMessage.getObjectId());
      if(!statusInfo.isPresent()) {
        log.error("[JMS Listener] Received message < {} > it does not exist.", syncMessage);
        return null;
      }
      MDC.put("run.id", statusInfo.get().getRunId());
      MDC.put("object.id", statusInfo.get().getId().toString());
      MDC.put("object.path", statusInfo.get().getOriginalFilePath() + " - " + statusInfo.get().getSourceFileName());

      // Start the workflow
      dmeSyncWorkflow.start(statusInfo.get());

    } finally {
      MDC.clear();
      log.info("Thread completed execution {}");
      threadCompleted();
    }

    return null;
  }
}
