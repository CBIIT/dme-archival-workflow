package gov.nih.nci.hpc.dmesync.jms;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import gov.nih.nci.hpc.dmesync.DmeSyncWorkflowServiceFactory;
import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.dto.DmeSyncMessageDto;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.service.DocConfigService;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncWorkflow;

/**
 * DME Sync Consumer (Listener)
 *
 * <p>Processes inbound JMS messages from per-DOC work queues. Listener containers are
 * registered dynamically by {@link DmeSyncListenerContainerManager}; this class no longer
 * carries a static {@code @JmsListener} annotation so that each DOC gets its own isolated lane.
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

  @Autowired private DocConfigService configService;

  /** Global active-thread count (all DOCs combined). */
  private final AtomicInteger activeThreads = new AtomicInteger(0);

  /** Per-DOC active-thread counts, keyed by DOC name. */
  private final ConcurrentHashMap<String, AtomicInteger> activeThreadsByDoc = new ConcurrentHashMap<>();

  // ── Thread-count accessors ─────────────────────────────────────────────────

  /** Returns {@code true} when no threads are active across all DOC lanes. */
  public boolean isAllThreadsCompleted() {
    return activeThreads.get() == 0;
  }

  /**
   * Returns {@code true} when no threads are active for the given DOC lane.
   *
   * @param docName the DOC name to query; if {@code null} the global count is used
   */
  public boolean isAllThreadsCompleted(String docName) {
    if (docName == null) {
      return isAllThreadsCompleted();
    }
    AtomicInteger counter = activeThreadsByDoc.get(docName);
    return counter == null || counter.get() == 0;
  }

  // ── Message processing ─────────────────────────────────────────────────────

  /**
   * Processes a deserialized {@link DmeSyncMessageDto} received from any per-DOC queue.
   *
   * <p>This method is the delegate target for each
   * {@link org.springframework.jms.listener.adapter.MessageListenerAdapter} created by
   * {@link DmeSyncListenerContainerManager}.
   *
   * @param syncMessage the inbound message DTO
   * @throws DmeSyncWorkflowException if workflow execution fails
   */
  public void processMessage(DmeSyncMessageDto syncMessage) throws DmeSyncWorkflowException {
    log.debug("[JMS Listener] Received message <{}>", syncMessage);

    // Increment global count immediately so callers never observe a false "idle" window.
    activeThreads.incrementAndGet();

    // Determine DOC name early (fast in-memory lookup) so per-DOC counter is incremented
    // atomically alongside the global counter, eliminating any visibility gap between the two.
    String docName = null;
    if (syncMessage.getDocConfigId() != null) {
      Optional<DocConfig> configForDocName = configService.getDocConfigById(syncMessage.getDocConfigId());
      if (configForDocName.isPresent()) {
        docName = configForDocName.get().getDocName();
      }
    }
    if (docName != null) {
      activeThreadsByDoc.computeIfAbsent(docName, k -> new AtomicInteger(0)).incrementAndGet();
    }

    try {
      Optional<StatusInfo> statusInfoOpt =
          dmeSyncWorkflowService.getService(access).findStatusInfoById(syncMessage.getObjectId());
      Optional<DocConfig> configOpt = configService.getDocConfigById(syncMessage.getDocConfigId());

      if (!statusInfoOpt.isPresent()) {
        log.error("[JMS Listener] Received message < {} > it does not exist.", syncMessage);
        return;
      }

      StatusInfo statusInfo = statusInfoOpt.get();
      // Reconcile: if docName wasn't resolved from the config lookup above, derive it from
      // StatusInfo and increment per-DOC counter now (best-effort for legacy messages).
      if (docName == null) {
        docName = statusInfo.getDoc();
        if (docName != null) {
          activeThreadsByDoc.computeIfAbsent(docName, k -> new AtomicInteger(0)).incrementAndGet();
        }
      }

      MDC.put("doc", docName != null ? docName : "");
      MDC.put("run.id", statusInfo.getRunId());
      MDC.put("object.id", statusInfo.getId().toString());
      MDC.put("object.path",
          statusInfo.getOriginalFilePath() + " - " + statusInfo.getSourceFileName());

      dmeSyncWorkflow.start(statusInfo, configOpt.get());

    } finally {
      if (docName != null) {
        AtomicInteger docCounter = activeThreadsByDoc.get(docName);
        if (docCounter != null) {
          docCounter.decrementAndGet();
        }
      }
      MDC.clear();
      activeThreads.decrementAndGet();
    }
  }
}
