package gov.nih.nci.hpc.dmesync.jms;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.ConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.jms.listener.adapter.MessageListenerAdapter;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.ErrorHandler;

import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import gov.nih.nci.hpc.dmesync.service.DocConfigService;

/**
 * Manages one JMS {@link DefaultMessageListenerContainer} per enabled DOC.
 *
 * <p>Queue names are derived by {@link DocQueueNameResolver}: {@code dme.<normalized-doc-name>}.
 * Each container processes messages exclusively for its DOC lane, providing isolation so that a
 * backlog in one DOC does not delay another.
 *
 * <p>On startup, and periodically thereafter (every {@code app.config.refresh-ms} ms), the
 * manager reconciles the set of active containers against the Oracle-driven DOC configuration:
 * <ul>
 *   <li>Containers are created for newly-enabled DOCs.</li>
 *   <li>Container concurrency is updated in-place when {@link DocConfig#getThreads()} changes.</li>
 *   <li>Containers are stopped and removed for DOCs that are no longer enabled.</li>
 * </ul>
 */
@Component
public class DmeSyncListenerContainerManager implements DisposableBean {

  private static final Logger log = LoggerFactory.getLogger(DmeSyncListenerContainerManager.class);

  private final ConnectionFactory connectionFactory;
  private final MessageConverter messageConverter;
  private final DmeSyncConsumer consumer;
  private final DocConfigService configService;
  private final DocQueueNameResolver queueNameResolver;

  @Value("${spring.jms.listener.concurrency:10}")
  private int defaultConcurrency;

  /** Active listener containers, keyed by the resolved queue name. */
  private final Map<String, DefaultMessageListenerContainer> containers = new ConcurrentHashMap<>();

  public DmeSyncListenerContainerManager(
      ConnectionFactory connectionFactory,
      MessageConverter messageConverter,
      DmeSyncConsumer consumer,
      DocConfigService configService,
      DocQueueNameResolver queueNameResolver) {
    this.connectionFactory = connectionFactory;
    this.messageConverter = messageConverter;
    this.consumer = consumer;
    this.configService = configService;
    this.queueNameResolver = queueNameResolver;
  }

  /**
   * Reconciles listener containers against the current DOC configuration.
   *
   * <p>Called automatically at startup ({@code initialDelay = 0}) and then every
   * {@code app.config.refresh-ms} milliseconds so that container topology stays in sync with
   * Oracle-driven configuration changes.
   */
  @Scheduled(initialDelay = 0, fixedDelayString = "${app.config.refresh-ms:60000}")
  public synchronized void reconcile() {
    List<DocConfig> enabledDocs = configService.getEnabledDocs();

    Set<String> expectedQueues = new HashSet<>();
    for (DocConfig doc : enabledDocs) {
      String queueName = queueNameResolver.resolve(doc);
      expectedQueues.add(queueName);
      int threads = resolveThreads(doc);

      if (!containers.containsKey(queueName)) {
        createContainer(queueName, threads);
      } else {
        // Update concurrency in-place if it changed.
        DefaultMessageListenerContainer existing = containers.get(queueName);
        if (threads != existing.getMaxConcurrentConsumers()) {
          log.info("[JMS Manager] Updating concurrency for queue '{}' to {}",
              queueName, threads);
          existing.setConcurrency(String.valueOf(threads) + "-" + String.valueOf(threads));
        }
      }
    }

    // Stop and remove containers for DOCs that are no longer enabled.
    Set<String> toRemove = new HashSet<>(containers.keySet());
    toRemove.removeAll(expectedQueues);
    for (String queueName : toRemove) {
      stopAndRemoveContainer(queueName);
    }
  }

  /**
   * Returns the set of queue names currently served by active listener containers.
   *
   * @return an unmodifiable snapshot of the active queue names
   */
  public Set<String> getActiveQueueNames() {
    return Set.copyOf(containers.keySet());
  }

  @Override
  public void destroy() {
    log.info("[JMS Manager] Shutting down all listener containers");
    for (Map.Entry<String, DefaultMessageListenerContainer> entry : containers.entrySet()) {
      try {
        entry.getValue().stop();
        entry.getValue().destroy();
      } catch (Exception e) {
        log.warn("[JMS Manager] Error stopping container for queue '{}'", entry.getKey(), e);
      }
    }
    containers.clear();
  }

  // ── private helpers ────────────────────────────────────────────────────────

  private void createContainer(String queueName, int threads) {
    log.info("[JMS Manager] Creating listener container for queue '{}' (concurrency {})",
        queueName, threads);

    MessageListenerAdapter adapter = new MessageListenerAdapter(consumer);
    adapter.setDefaultListenerMethod("processMessage");
    adapter.setMessageConverter(messageConverter);

    DefaultMessageListenerContainer container = new DefaultMessageListenerContainer();
    container.setConnectionFactory(connectionFactory);
    container.setDestinationName(queueName);
    container.setMessageListener(adapter);
    container.setConcurrency(String.valueOf(threads) + "-" + String.valueOf(threads));
    container.setErrorHandler(new ErrorHandler() {
      @Override
      public void handleError(Throwable t) {
        log.error("[JMS Listener][{}] Failed to process message: {}", queueName, t.getMessage(), t);
      }
    });

    container.afterPropertiesSet();
    container.start();
    containers.put(queueName, container);

    log.info("[JMS Manager] Started listener container for queue '{}'", queueName);
  }

  private void stopAndRemoveContainer(String queueName) {
    DefaultMessageListenerContainer container = containers.remove(queueName);
    if (container != null) {
      log.info("[JMS Manager] Stopping listener container for queue '{}'", queueName);
      try {
        container.stop();
        container.destroy();
      } catch (Exception e) {
        log.warn("[JMS Manager] Error while stopping container for queue '{}'", queueName, e);
      }
    }
  }

  /** Returns the per-DOC thread count, falling back to the global default when not set. */
  private int resolveThreads(DocConfig doc) {
    Integer threads = doc.getThreads();
    return (threads != null && threads > 0) ? threads : defaultConcurrency;
  }
}
