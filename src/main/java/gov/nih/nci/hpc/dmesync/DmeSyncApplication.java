package gov.nih.nci.hpc.dmesync;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import javax.jms.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jms.DefaultJmsListenerContainerFactoryConfigurer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.support.converter.MappingJackson2MessageConverter;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.jms.support.converter.MessageType;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.util.ErrorHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.nih.nci.hpc.dmesync.scheduler.DmeSyncScheduler;

@SpringBootApplication
@EnableJms
@EnableScheduling
public class DmeSyncApplication {

  final Logger logger = LoggerFactory.getLogger(getClass().getName());

  private static ConfigurableApplicationContext context;

  private static String authToken;

  public static void main(String[] args) {
    String env = null;
    boolean runImmediateFlag = false;
    String tokenFilePath = "token_file";
    if (args.length >= 1) {
      env = args[0];
    }
    if (env == null || env.isEmpty()) {
      System.out.println("Usage: java -jar dme-sync<version>.jar env[dev|uat|prod]");
    } else {
      if (args.length >= 2 && "true".equals(args[1]))
        runImmediateFlag = true;
      else if (args.length == 2)
        tokenFilePath = args[1];
      else if (args.length == 3)
        tokenFilePath = args[2];
        
      try {
        readAuthToken(tokenFilePath);
      } catch (Exception e) {
        System.out.println("token_file is not found or empty");
        return;
      }
      System.setProperty("auth.token", authToken);
      System.setProperty("hpc.server.url", getServerUrl(env));
      context = SpringApplication.run(DmeSyncApplication.class, args);
      if (runImmediateFlag) {
        DmeSyncScheduler scheduler = context.getBean(DmeSyncScheduler.class);
        scheduler.findFilesToPush();
      }
    }
  }

  private static void readAuthToken(String tokenFilePath) throws IOException {
    //read json file data to String
    byte[] jsonData = Files.readAllBytes(Paths.get(tokenFilePath));

    //create ObjectMapper instance
    ObjectMapper objectMapper = new ObjectMapper();

    //read JSON like DOM Parser
    JsonNode rootNode = objectMapper.readTree(jsonData);
    JsonNode tokenNode = rootNode.path("token");
    authToken = tokenNode.asText();
  }

  @Bean
  public MessageConverter messageConverter() {
    MappingJackson2MessageConverter converter = new MappingJackson2MessageConverter();
    converter.setTargetType(MessageType.TEXT);
    converter.setTypeIdPropertyName("_type");
    return converter;
  }

  @Bean
  public DefaultJmsListenerContainerFactory jmsListenerContainerFactory(
      ConnectionFactory connectionFactory,
      DefaultJmsListenerContainerFactoryConfigurer configurer) {
    DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();

    factory.setErrorHandler(
        new ErrorHandler() {
          @Override
          public void handleError(Throwable t) {
            String error = "[JMS Listener] Failed to process message due to: " + t.getMessage();
            logger.error(error, t);
          }
        });

    configurer.configure(factory, connectionFactory);
    return factory;
  }

  public static void restart() {
    ApplicationArguments args = context.getBean(ApplicationArguments.class);

    Thread thread =
        new Thread(
            () -> {
              context.close();
              context = SpringApplication.run(DmeSyncApplication.class, args.getSourceArgs());
            });

    thread.setDaemon(false);
    thread.start();
  }

  public static void shutdown() {
    int exitCode = SpringApplication.exit(context, () -> 0);
    System.exit(exitCode);
  }

  private static String getServerUrl(String env) {
    String serverUrl = null;
    if ("local".equals(env)) {
      serverUrl = "https://localhost:7738/hpc-server";

    } else if ("dev".equals(env)) {
      serverUrl = "https://fsdsgl-dmeap01d.ncifcrf.gov:7738/hpc-server";
    } else if ("uat".equals(env)) {
      serverUrl = "https://fsdsgl-dmeap01t.ncifcrf.gov:7738/hpc-server";
    } else if ("prod".equals(env)) {
      serverUrl = "https://hpcdmeapi.nci.nih.gov:8080";
    } else if ("prod2".equals(env)) {
        serverUrl = "https://fsdsgl-dmeap02p.ncifcrf.gov:8080";
    } else if ("prod3".equals(env)) {
        serverUrl = "https://fsdmel-dsapi03p.ncifcrf.gov:8080";
    } else if ("prod4".equals(env)) {
        serverUrl = "https://fsdmel-dsapi04p.ncifcrf.gov:8080";
    } else if ("prod5".equals(env)) {
        serverUrl = "https://fsdmel-dsapi05p.ncifcrf.gov:8080";
    } else if ("prod7".equals(env)) {
        serverUrl = "https://fsdsgl-dmeap07p.ncifcrf.gov:8080";
    } else if ("prod_bp".equals(env)) {
        serverUrl = "https://fsdmel-dsapi06p.ncifcrf.gov:8080";
    } else {
      System.out.println("Invalid environment: " + env);
    }
    return serverUrl;
  }
}
