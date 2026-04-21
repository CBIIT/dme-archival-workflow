package gov.nih.nci.hpc.dmesync.workflow;


import java.util.Locale;
import org.springframework.context.MessageSource;
import org.springframework.stereotype.Service;

@Service
public class MessageService {
  private final MessageSource messageSource;

  public MessageService(MessageSource messageSource) {
    this.messageSource = messageSource;
  }

  public String get(String code, Object... args) {
    return messageSource.getMessage(code, args, Locale.getDefault());
  }
}