package gov.nih.nci.hpc.dmesync.jms;

import java.util.Enumeration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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

  @Autowired private JmsTemplate jmsTemplate;

  public void send(DmeSyncMessageDto message, String queue) {
    log.debug("[JMS Producer] Sending message <{}>", message);
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
}
