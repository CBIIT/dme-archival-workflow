package gov.nih.nci.hpc.dmesync.jms;

import java.util.Enumeration;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;
import gov.nih.nci.hpc.dmesync.dto.DmeSyncMessageDto;

/**
 * DME Sync JMS Message Producer 
 * 
 * @author dinhys
 */
@Component
public class DmeSyncProducer {

  private static final Logger log = LoggerFactory.getLogger(DmeSyncProducer.class);
  
  @Value("${dmesync.jms.transactional:true}")
  private boolean transactionalState;

  @Autowired private JmsTemplate jmsTemplate;

  public void send(DmeSyncMessageDto message, String queue) {
    log.debug("[JMS Producer] Sending message <{}>", message);
    jmsTemplate.setSessionTransacted(transactionalState);
    jmsTemplate.convertAndSend(queue, message);
   
  }

  public int getQueueCount(String queue) {
    return jmsTemplate.browse(
        queue,
        (session, browser) -> {
          Enumeration<?> messages = browser.getEnumeration();
          int total = 0;
          while (messages.hasMoreElements()) {
            messages.nextElement();
            total++;
          }
          return total;
        });
  }
  
  public boolean isMainThreadActive() {
	  
      Map<Thread, StackTraceElement[]> threadMap = Thread.getAllStackTraces();
	  
      for (Thread thread : threadMap.keySet()) {
          // Ignore threads that are not processing tasks
          if (thread.getState() == Thread.State.RUNNABLE || thread.getState() == Thread.State.WAITING ) {
              if (thread.getName().equals("main")) {
                  return true; 
              }
          }
      }
      return false; 
	  
  }
}
