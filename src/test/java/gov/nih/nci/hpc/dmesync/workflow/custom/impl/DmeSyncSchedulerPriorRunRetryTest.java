
package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.nio.file.Path;
import java.util.Date;
import java.util.List;

import org.springframework.test.util.ReflectionTestUtils;

import gov.nih.nci.hpc.dmesync.DmeSyncWorkflowServiceFactory;
import gov.nih.nci.hpc.dmesync.domain.DocConfig;
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
 *
 * 
 */
class DmeSyncSchedulerPriorRunRetryTest {

  static DocConfig config = null;

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
    ReflectionTestUtils.setField(scheduler, "runId", runId);
    DocConfig.SourceConfig sourceConfig = new DocConfig.SourceConfig(baseDir, null, "/TEST_Archive", 1);
	config = new DocConfig(null, doc, null, null, null, null, null, false, null, 0, null, null, sourceConfig, null, null, null, null, null);

    when(factory.getService(access)).thenReturn((DmeSyncWorkflowService) workflowSvc);
    return scheduler;
  }

  @Test
  void returnsWhenMissingContext_runIdBlank() throws Exception {
    DmeSyncProducer sender = mock(DmeSyncProducer.class);
    DmeSyncWorkflowServiceFactory factory = mock(DmeSyncWorkflowServiceFactory.class);
    DmeSyncWorkflowService workflowSvc = mock(DmeSyncWorkflowService.class);

    DmeSyncScheduler scheduler = newSchedulerWithContext(
        sender, factory, workflowSvc, "local", "DOC1", "/source", "");

    ReflectionTestUtils.invokeMethod(scheduler, "includePriorRunFailuresInCurrentRunWorklist", config);

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

    ReflectionTestUtils.invokeMethod(scheduler, "includePriorRunFailuresInCurrentRunWorklist", config);

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

    ReflectionTestUtils.invokeMethod(scheduler, "includePriorRunFailuresInCurrentRunWorklist", config);

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

    ReflectionTestUtils.invokeMethod(scheduler, "includePriorRunFailuresInCurrentRunWorklist", config);

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

    ReflectionTestUtils.invokeMethod(scheduler, "includePriorRunFailuresInCurrentRunWorklist", config);

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

    ReflectionTestUtils.invokeMethod(scheduler, "includePriorRunFailuresInCurrentRunWorklist", config);

    verify(sender, never()).send(any(DmeSyncMessageDto.class), anyString());
    verify(workflowSvc, never()).saveStatusInfo(any());
    verify(workflowSvc, never()).deleteMetadataInfoByObjectId(anyLong());
    verify(workflowSvc, never()).deleteTaskInfoByObjectId(anyLong());

    java.nio.file.Files.deleteIfExists(baseDir);
  }
  
  @Test
  void happyPath_failedExistingPath_retriedAndEnqueued() throws Exception {
    DmeSyncProducer sender = mock(DmeSyncProducer.class);
    DmeSyncWorkflowServiceFactory factory = mock(DmeSyncWorkflowServiceFactory.class);
    DmeSyncWorkflowService workflowSvc = mock(DmeSyncWorkflowService.class);

    Path baseDir = java.nio.file.Files.createTempDirectory("dmesync-base-");
    Path existing = java.nio.file.Files.createDirectory(baseDir.resolve("existing"));

    DmeSyncScheduler scheduler = newSchedulerWithContext(
        sender, factory, workflowSvc, "local", "DOC1", baseDir.toString(), "Run_20260325010101");

    // (1) first call: top row (avoid early return)
    StatusInfo latestAny = new StatusInfo();
    latestAny.setRunId("Run_20260225010101"); // previous runId used in interaction #3
    latestAny.setStartTimestamp(new Date(10));
    latestAny.setOriginalFilePath(existing.toString());

    when(workflowSvc.findTopStatusInfoByDocAndOriginalFilePathStartsWithOrderByStartTimestampDesc("DOC1", baseDir.toString()))
        .thenReturn(latestAny);

    // (2) second call: baseRows used to derive previous run id (can return anything, but must not break logic)
    StatusInfo baseRow = new StatusInfo();
    baseRow.setRunId("Run_20260225010101");
    baseRow.setStartTimestamp(new Date(9));
    baseRow.setOriginalFilePath(existing.toString());

    when(workflowSvc.findAllByDocAndLikeOriginalFilePath("DOC1", baseDir.toString() + "%"))
        .thenReturn(List.of(baseRow));

    // (3) third call: rows for the previous runId that will be retried
    StatusInfo failedExisting = new StatusInfo();
    failedExisting.setId(4L);
    failedExisting.setRunId("Run_20260225010101");
    failedExisting.setDoc("DOC1");
    failedExisting.setOriginalFilePath(existing.toString());
    failedExisting.setStatus("FAILED");
    failedExisting.setError("old error");
    failedExisting.setRetryCount(7L);
    failedExisting.setEndWorkflow(true);

    when(workflowSvc.findStatusInfoByRunIdAndDoc("Run_20260225010101", "DOC1"))
        .thenReturn(List.of(failedExisting));

    when(workflowSvc.saveStatusInfo(any(StatusInfo.class))).thenAnswer(inv -> inv.getArgument(0));

    // Act
    ReflectionTestUtils.invokeMethod(scheduler, "includePriorRunFailuresInCurrentRunWorklist", config);

    // Assert
    verify(workflowSvc, times(1)).saveStatusInfo(argThat(s ->
        "Run_20260325010101".equals(s.getRunId())
            && "".equals(s.getError())
            && Long.valueOf(0L).equals(s.getRetryCount())
            && Boolean.FALSE.equals(s.isEndWorkflow())
            && existing.toString().equals(s.getOriginalFilePath())
    ));

    verify(workflowSvc, times(1)).deleteMetadataInfoByObjectId(4L);
    verify(sender, times(1)).send(any(DmeSyncMessageDto.class), eq("inbound.queue"));

    java.nio.file.Files.deleteIfExists(existing);
    java.nio.file.Files.deleteIfExists(baseDir);
  }
}