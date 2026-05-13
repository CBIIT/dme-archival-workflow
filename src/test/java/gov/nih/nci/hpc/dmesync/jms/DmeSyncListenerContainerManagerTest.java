package gov.nih.nci.hpc.dmesync.jms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Session;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jms.support.converter.MessageConverter;

import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import gov.nih.nci.hpc.dmesync.service.DocConfigService;

/**
 * Unit tests for {@link DmeSyncListenerContainerManager}.
 *
 * <p>The tests use a real (but inactive) embedded broker backed by the minimal JMS mocks needed
 * to keep the containers from throwing NPEs during {@code afterPropertiesSet()}.
 */
class DmeSyncListenerContainerManagerTest {

  private DocConfigService configService;
  private DmeSyncListenerContainerManager manager;

  @BeforeEach
  void setUp() throws Exception {
    configService = mock(DocConfigService.class);

    // Minimal ConnectionFactory that returns a no-op Session so containers can initialise.
    Connection mockConnection = mock(Connection.class);
    Session mockSession = mock(Session.class);
    when(mockConnection.createSession(false, Session.AUTO_ACKNOWLEDGE)).thenReturn(mockSession);
    ConnectionFactory mockCf = mock(ConnectionFactory.class);
    when(mockCf.createConnection()).thenReturn(mockConnection);

    MessageConverter mockConverter = mock(MessageConverter.class);
    DmeSyncConsumer mockConsumer = mock(DmeSyncConsumer.class);

    manager = new DmeSyncListenerContainerManager(
        mockCf, mockConverter, mockConsumer, configService, new DocQueueNameResolver());
    // Inject default concurrency values (normally bound via @Value).
    org.springframework.test.util.ReflectionTestUtils.setField(manager, "defaultConcurrency", 5);
    org.springframework.test.util.ReflectionTestUtils.setField(manager, "defaultMaxConcurrency", 5);
  }

  // ── helper ─────────────────────────────────────────────────────────────────

  private static DocConfig docConfig(Long id, String docName, Integer threads) {
    DocConfig.SourceConfig src = new DocConfig.SourceConfig("/src", "/work", "/dst", 1);
    DocConfig.SourceRule rule = new DocConfig.SourceRule(
        null, null, null, null, null,
        false, false, null, null, false, null,
        false, false, false, 1);
    DocConfig.PreprocessingConfig pre = new DocConfig.PreprocessingConfig(false, null, false, false, false, 1);
    DocConfig.PreprocessingRule preRule = new DocConfig.PreprocessingRule(
        false, null, null, false, false, null, null,
        false, false, false, null, false, null,
        null, null, null, false, false, null, null, 1);
    DocConfig.UploadConfig upload = new DocConfig.UploadConfig(
        false, false, false, false, false,
        false, false, false, false, false, false, false, null, 1);
    DocConfig.NotificationConfig notif = new DocConfig.NotificationConfig(null, false, false, 1);
    return new DocConfig(id, docName, "srv1", "wf1", "dme1", "http://dme",
        threads, true, "0 0 * * * ?", 1, Instant.now(), Instant.now(),
        src, rule, pre, preRule, upload, notif);
  }

  // ── tests ──────────────────────────────────────────────────────────────────

  @Test
  void noEnabledDocs_noContainersCreated() {
    when(configService.getEnabledDocs()).thenReturn(Collections.emptyList());

    manager.reconcile();

    assertTrue(manager.getActiveQueueNames().isEmpty(),
        "No containers should exist when there are no enabled DOCs");
  }

  @Test
  void oneEnabledDoc_oneContainerCreated() {
    when(configService.getEnabledDocs()).thenReturn(List.of(docConfig(1L, "DOC1", 3)));

    manager.reconcile();

    Set<String> queues = manager.getActiveQueueNames();
    assertEquals(1, queues.size());
    assertTrue(queues.contains("dme.doc1"), "Expected queue 'dme.doc1'");
  }

  @Test
  void twoEnabledDocs_twoSeparateContainersCreated() {
    when(configService.getEnabledDocs()).thenReturn(
        List.of(docConfig(1L, "DOC1", 2), docConfig(2L, "DOC2", 4)));

    manager.reconcile();

    Set<String> queues = manager.getActiveQueueNames();
    assertEquals(2, queues.size());
    assertTrue(queues.contains("dme.doc1"));
    assertTrue(queues.contains("dme.doc2"));
  }

  @Test
  void docRemovedBetweenReconciles_containerStopped() {
    when(configService.getEnabledDocs())
        .thenReturn(List.of(docConfig(1L, "DOC1", 2), docConfig(2L, "DOC2", 2)))
        .thenReturn(List.of(docConfig(1L, "DOC1", 2)));  // DOC2 disabled on second call

    manager.reconcile();
    assertEquals(2, manager.getActiveQueueNames().size());

    manager.reconcile();
    Set<String> queues = manager.getActiveQueueNames();
    assertEquals(1, queues.size());
    assertTrue(queues.contains("dme.doc1"));
    assertFalse(queues.contains("dme.doc2"),
        "Container for disabled DOC2 should have been removed");
  }

  @Test
  void docAddedBetweenReconciles_newContainerCreated() {
    when(configService.getEnabledDocs())
        .thenReturn(List.of(docConfig(1L, "DOC1", 2)))
        .thenReturn(List.of(docConfig(1L, "DOC1", 2), docConfig(3L, "DOC3", 2)));

    manager.reconcile();
    assertEquals(1, manager.getActiveQueueNames().size());

    manager.reconcile();
    Set<String> queues = manager.getActiveQueueNames();
    assertEquals(2, queues.size());
    assertTrue(queues.contains("dme.doc3"), "New container for DOC3 should have been created");
  }

  @Test
  void nullThreadsInDocConfig_fallsBackToDefault() {
    when(configService.getEnabledDocs()).thenReturn(List.of(docConfig(1L, "DOC1", null)));

    manager.reconcile();

    // Container should be created without throwing even when threads is null.
    assertTrue(manager.getActiveQueueNames().contains("dme.doc1"));
  }

  @Test
  void destroyStopsAllContainers() {
    when(configService.getEnabledDocs()).thenReturn(
        List.of(docConfig(1L, "DOC1", 2), docConfig(2L, "DOC2", 2)));
    manager.reconcile();
    assertEquals(2, manager.getActiveQueueNames().size());

    manager.destroy();

    assertTrue(manager.getActiveQueueNames().isEmpty(),
        "All containers should be removed after destroy()");
  }
}
