
package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.nio.file.Path;
import java.util.Date;
import java.util.List;

import org.springframework.test.util.ReflectionTestUtils;

import gov.nih.nci.hpc.dmesync.DmeSyncWorkflowServiceFactory;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.dto.DmeSyncMessageDto;
import gov.nih.nci.hpc.dmesync.jms.DmeSyncProducer;
import gov.nih.nci.hpc.dmesync.scheduler.DmeSyncScheduler;
import gov.nih.nci.hpc.dmesync.service.DmeSyncWorkflowService;

/**
 * More tests in the same "simple style":
 *  - @Test only
 *  - Mockito mocks created inside each test
 *  - ReflectionTestUtils for private fields + private method invocation
 *  - NO mockito-inline: uses real temp paths for Files.exists()
 *
 * NOTE: Replace WorkflowServiceType with the actual service type returned by factory.getService(access).
 */
class DmeSyncSchedulerPriorRunRetryTest {


  private static DmeSyncScheduler newSchedulerWithContext(
      DmeSyncProducer sender,
      DmeSyncWorkflowServiceFactory factory,
      DmeSyncWorkflowService workflowSvc,
      String access,
      String doc,
      String baseDir,
      String runId) {

    DmeSyncScheduler scheduler = new DmeSyncScheduler();
	
    ReflectionTestUtils.setField(scheduler, "sender", sender);
    ReflectionTestUtils.setField(scheduler, "dmeSyncWorkflowService", factory);

    ReflectionTestUtils.setField(scheduler, "access", access);
    ReflectionTestUtils.setField(scheduler, "doc", doc);
    ReflectionTestUtils.setField(scheduler, "syncBaseDir", baseDir);
    ReflectionTestUtils.setField(scheduler, "runId", runId);

    when(factory.getService(access)).thenReturn((DmeSyncWorkflowService) workflowSvc);
    return scheduler;
  }

  @Test
  void returnsWhenMissingContext_runIdBlank() throws Exception {
    DmeSyncProducer sender = mock(DmeSyncProducer.class);
    DmeSyncWorkflowServiceFactory factory = mock(DmeSyncWorkflowServiceFactory.class);
    DmeSyncWorkflowService workflowSvc = mock(DmeSyncWorkflowService.class);

    // runId blank
    DmeSyncScheduler scheduler = newSchedulerWithContext(
        sender, factory, workflowSvc, "local", "DOC1", "/source", "");

    ReflectionTestUtils.invokeMethod(scheduler, "includePriorRunFailuresInCurrentRunWorklist");

    verifyNoInteractions(factory);
    verifyNoInteractions(sender);
    verifyNoInteractions(workflowSvc);
  }

  @Test
  void noPreviousRunId_noBaseRows_noEnqueue() throws Exception {
    DmeSyncProducer sender = mock(DmeSyncProducer.class);
    DmeSyncWorkflowServiceFactory factory = mock(DmeSyncWorkflowServiceFactory.class);
    DmeSyncWorkflowService workflowSvc = mock(DmeSyncWorkflowService.class);

    Path baseDir = java.nio.file.Files.createTempDirectory("dmesync-base-");
    DmeSyncScheduler scheduler = newSchedulerWithContext(
        sender, factory, workflowSvc, "local", "DOC1", baseDir.toString(), "Run_20260325010101");

    when(workflowSvc.findAllStatusInfoLikeOriginalFilePath(baseDir.toString() + "%"))
        .thenReturn(List.of());

    ReflectionTestUtils.invokeMethod(scheduler, "includePriorRunFailuresInCurrentRunWorklist");

    verify(sender, never()).send(any(DmeSyncMessageDto.class), anyString());
    verify(workflowSvc, never()).saveStatusInfo(any());

    java.nio.file.Files.deleteIfExists(baseDir);
  }

  @Test
  void previousRunRowsEmpty_noEnqueue() throws Exception {
    DmeSyncProducer sender = mock(DmeSyncProducer.class);
    DmeSyncWorkflowServiceFactory factory = mock(DmeSyncWorkflowServiceFactory.class);
    DmeSyncWorkflowService workflowSvc = mock(DmeSyncWorkflowService.class);

    Path baseDir = java.nio.file.Files.createTempDirectory("dmesync-base-");
    DmeSyncScheduler scheduler = newSchedulerWithContext(
        sender, factory, workflowSvc, "local", "DOC1", baseDir.toString(), "Run_20260325010101");

    StatusInfo baseRow = new StatusInfo();
    baseRow.setRunId("Run_20260225010101");
    baseRow.setStartTimestamp(new Date(1));

    when(workflowSvc.findAllStatusInfoLikeOriginalFilePath(baseDir.toString() + "%"))
        .thenReturn(List.of(baseRow));
    when(workflowSvc.findStatusInfoByRunIdAndDoc("Run_20260225010101", "DOC1"))
        .thenReturn(List.of());

    ReflectionTestUtils.invokeMethod(scheduler, "includePriorRunFailuresInCurrentRunWorklist");

    verify(sender, never()).send(any(DmeSyncMessageDto.class), anyString());
    verify(workflowSvc, never()).saveStatusInfo(any());

    java.nio.file.Files.deleteIfExists(baseDir);
  }

  @Test
  void completedOnly_nothingRetried() throws Exception {
    DmeSyncProducer sender = mock(DmeSyncProducer.class);
    DmeSyncWorkflowServiceFactory factory = mock(DmeSyncWorkflowServiceFactory.class);
    DmeSyncWorkflowService workflowSvc = mock(DmeSyncWorkflowService.class);

    Path baseDir = java.nio.file.Files.createTempDirectory("dmesync-base-");
    DmeSyncScheduler scheduler = newSchedulerWithContext(
        sender, factory, workflowSvc, "local", "DOC1", baseDir.toString(), "Run_20260325010101");

    StatusInfo baseRow = new StatusInfo();
    baseRow.setRunId("Run_20260225010101");
    baseRow.setStartTimestamp(new Date(1));

    StatusInfo completed = new StatusInfo();
    completed.setId(1L);
    completed.setRunId("Run_20260225010101");
    completed.setDoc("DOC1");
    completed.setOriginalFilePath(java.nio.file.Files.createDirectory(baseDir.resolve("done")).toString());
    completed.setStatus("COMPLETED");

    when(workflowSvc.findAllStatusInfoLikeOriginalFilePath(baseDir.toString() + "%"))
        .thenReturn(List.of(baseRow));
    when(workflowSvc.findStatusInfoByRunIdAndDoc("Run_20260225010101", "DOC1"))
        .thenReturn(List.of(completed));

    ReflectionTestUtils.invokeMethod(scheduler, "includePriorRunFailuresInCurrentRunWorklist");

    verify(sender, never()).send(any(DmeSyncMessageDto.class), anyString());
    verify(workflowSvc, never()).saveStatusInfo(any());
    verify(workflowSvc, never()).deleteMetadataInfoByObjectId(anyLong());
    verify(workflowSvc, never()).deleteTaskInfoByObjectId(anyLong());

    // cleanup
    java.nio.file.Files.deleteIfExists(Path.of(completed.getOriginalFilePath()));
    java.nio.file.Files.deleteIfExists(baseDir);
  }

  @Test
  void failedButOutsideBaseDir_notRetried() throws Exception {
    DmeSyncProducer sender = mock(DmeSyncProducer.class);
    DmeSyncWorkflowServiceFactory factory = mock(DmeSyncWorkflowServiceFactory.class);
    DmeSyncWorkflowService workflowSvc = mock(DmeSyncWorkflowService.class);

    Path baseDir = java.nio.file.Files.createTempDirectory("dmesync-base-");
    Path otherDir = java.nio.file.Files.createTempDirectory("dmesync-other-");

    DmeSyncScheduler scheduler = newSchedulerWithContext(
        sender, factory, workflowSvc, "local", "DOC1", baseDir.toString(), "Run_20260325010101");

    StatusInfo baseRow = new StatusInfo();
    baseRow.setRunId("Run_20260225010101");
    baseRow.setStartTimestamp(new Date(1));

    StatusInfo failedOutside = new StatusInfo();
    failedOutside.setId(2L);
    failedOutside.setRunId("Run_20260225010101");
    failedOutside.setDoc("DOC1");
    failedOutside.setOriginalFilePath(java.nio.file.Files.createDirectory(otherDir.resolve("retry")).toString());
    failedOutside.setStatus("FAILED");

    when(workflowSvc.findAllStatusInfoLikeOriginalFilePath(baseDir.toString() + "%"))
        .thenReturn(List.of(baseRow));
    when(workflowSvc.findStatusInfoByRunIdAndDoc("Run_20260225010101", "DOC1"))
        .thenReturn(List.of(failedOutside));

    ReflectionTestUtils.invokeMethod(scheduler, "includePriorRunFailuresInCurrentRunWorklist");

    verify(sender, never()).send(any(DmeSyncMessageDto.class), anyString());
    verify(workflowSvc, never()).saveStatusInfo(any());

    // cleanup
    java.nio.file.Files.deleteIfExists(Path.of(failedOutside.getOriginalFilePath()));
    java.nio.file.Files.deleteIfExists(otherDir);
    java.nio.file.Files.deleteIfExists(baseDir);
  }

  @Test
  void failedMissingPath_notRetried() throws Exception {
    DmeSyncProducer sender = mock(DmeSyncProducer.class);
    DmeSyncWorkflowServiceFactory factory = mock(DmeSyncWorkflowServiceFactory.class);
    DmeSyncWorkflowService workflowSvc = mock(DmeSyncWorkflowService.class);

    Path baseDir = java.nio.file.Files.createTempDirectory("dmesync-base-");
    Path missing = baseDir.resolve("missing"); // do not create

    DmeSyncScheduler scheduler = newSchedulerWithContext(
        sender, factory, workflowSvc, "local", "DOC1", baseDir.toString(), "Run_20260325010101");

    StatusInfo baseRow = new StatusInfo();
    baseRow.setRunId("Run_20260225010101");
    baseRow.setStartTimestamp(new Date(1));

    StatusInfo failedMissing = new StatusInfo();
    failedMissing.setId(3L);
    failedMissing.setRunId("Run_20260225010101");
    failedMissing.setDoc("DOC1");
    failedMissing.setOriginalFilePath(missing.toString());
    failedMissing.setStatus("FAILED");

    when(workflowSvc.findAllStatusInfoLikeOriginalFilePath(baseDir.toString() + "%"))
        .thenReturn(List.of(baseRow));
    when(workflowSvc.findStatusInfoByRunIdAndDoc("Run_20260225010101", "DOC1"))
        .thenReturn(List.of(failedMissing));

    ReflectionTestUtils.invokeMethod(scheduler, "includePriorRunFailuresInCurrentRunWorklist");

    verify(sender, never()).send(any(DmeSyncMessageDto.class), anyString());
    verify(workflowSvc, never()).saveStatusInfo(any());
    verify(workflowSvc, never()).deleteMetadataInfoByObjectId(anyLong());
    verify(workflowSvc, never()).deleteTaskInfoByObjectId(anyLong());

    java.nio.file.Files.deleteIfExists(baseDir);
  }

  @Test
  void failedExistingPath_isSavedAndEnqueued() throws Exception {
    // filesystem
    Path baseDir = java.nio.file.Files.createTempDirectory("dmesync-base-");
    Path existingFolder = java.nio.file.Files.createDirectory(baseDir.resolve("retryMe"));

    // mocks
    DmeSyncProducer sender = mock(DmeSyncProducer.class);
    DmeSyncWorkflowServiceFactory factory = mock(DmeSyncWorkflowServiceFactory.class);
    DmeSyncWorkflowService workflowSvc = mock(DmeSyncWorkflowService.class);

    // SUT
    DmeSyncScheduler scheduler = new DmeSyncScheduler();
    ReflectionTestUtils.setField(scheduler, "sender", sender);
    ReflectionTestUtils.setField(scheduler, "dmeSyncWorkflowService", factory);

    ReflectionTestUtils.setField(scheduler, "access", "local");
    ReflectionTestUtils.setField(scheduler, "doc", "DOC1");
    ReflectionTestUtils.setField(scheduler, "syncBaseDir", baseDir.toString());
    ReflectionTestUtils.setField(scheduler, "runId", "Run_CURR");

    when(factory.getService("local"))
        .thenReturn((DmeSyncWorkflowService) workflowSvc);

    // previous run id derivation input
    StatusInfo baseRow = new StatusInfo();
    baseRow.setRunId("Run_PREV");
    baseRow.setStartTimestamp(new Date(1));
    when(workflowSvc.findAllStatusInfoLikeOriginalFilePath(baseDir.toString() + "%"))
        .thenReturn(List.of(baseRow));

    // previous run rows
    StatusInfo failed = new StatusInfo();
    failed.setId(11L);
    failed.setRunId("Run_PREV");
    failed.setDoc("DOC1");
    failed.setOriginalFilePath(existingFolder.toString());
    failed.setStatus("FAILED");
    when(workflowSvc.findStatusInfoByRunIdAndDoc("Run_PREV", "DOC1"))
        .thenReturn(List.of(failed));

    when(workflowSvc.saveStatusInfo(any(StatusInfo.class))).thenAnswer(inv -> inv.getArgument(0));

    // act (private method)
    ReflectionTestUtils.invokeMethod(scheduler, "includePriorRunFailuresInCurrentRunWorklist");

    // assert: save invoked on workflowSvc (NOT on factory)
    verify(workflowSvc, times(1)).saveStatusInfo(any(StatusInfo.class));
    verify(sender, times(1)).send(any(DmeSyncMessageDto.class), eq("inbound.queue"));

    // cleanup
    java.nio.file.Files.deleteIfExists(existingFolder);
    java.nio.file.Files.deleteIfExists(baseDir);
  }

  @Test
  void previousRunIdEqualsCurrent_run_findsAnotherPreviousIfPresent() throws Exception {
    // This covers the scenario where the newest baseRow has current runId,
    // but an older row exists with a different runId that should be chosen as previous.
    DmeSyncProducer sender = mock(DmeSyncProducer.class);
    DmeSyncWorkflowServiceFactory factory = mock(DmeSyncWorkflowServiceFactory.class);
    DmeSyncWorkflowService workflowSvc = mock(DmeSyncWorkflowService.class);

    Path baseDir = java.nio.file.Files.createTempDirectory("dmesync-base-");
    Path existingFolder = java.nio.file.Files.createDirectory(baseDir.resolve("retryMe"));

    DmeSyncScheduler scheduler = newSchedulerWithContext(
        sender, factory, workflowSvc, "local", "DOC1", baseDir.toString(), "Run_CURR");

    StatusInfo newestCurrent = new StatusInfo();
    newestCurrent.setRunId("Run_CURR");
    newestCurrent.setStartTimestamp(new Date(100));

    StatusInfo olderPrev = new StatusInfo();
    olderPrev.setRunId("Run_PREV");
    olderPrev.setStartTimestamp(new Date(10));

    when(workflowSvc.findAllStatusInfoLikeOriginalFilePath(baseDir.toString() + "%"))
        .thenReturn(List.of(newestCurrent, olderPrev));

    StatusInfo failed = new StatusInfo();
    failed.setId(99L);
    failed.setRunId("Run_PREV");
    failed.setDoc("DOC1");
    failed.setOriginalFilePath(existingFolder.toString());
    failed.setStatus("FAILED");

    when(workflowSvc.findStatusInfoByRunIdAndDoc("Run_PREV", "DOC1"))
        .thenReturn(List.of(failed));
    when(workflowSvc.saveStatusInfo(any(StatusInfo.class))).thenAnswer(inv -> inv.getArgument(0));

    ReflectionTestUtils.invokeMethod(scheduler, "includePriorRunFailuresInCurrentRunWorklist");

    verify(sender, times(1)).send(any(DmeSyncMessageDto.class), eq("inbound.queue"));
    verify(workflowSvc, times(1)).saveStatusInfo(any(StatusInfo.class));

    // cleanup
    java.nio.file.Files.deleteIfExists(existingFolder);
    java.nio.file.Files.deleteIfExists(baseDir);
  }
}