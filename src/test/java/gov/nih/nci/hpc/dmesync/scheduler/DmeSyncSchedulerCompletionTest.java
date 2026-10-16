package gov.nih.nci.hpc.dmesync.scheduler;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.util.ReflectionTestUtils;

import gov.nih.nci.hpc.dmesync.DmeSyncMailServiceFactory;
import gov.nih.nci.hpc.dmesync.DmeSyncWorkflowServiceFactory;
import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.domain.WorkflowRunInfo;
import gov.nih.nci.hpc.dmesync.jms.DmeSyncConsumer;
import gov.nih.nci.hpc.dmesync.jms.DmeSyncProducer;
import gov.nih.nci.hpc.dmesync.jms.DocQueueNameResolver;
import gov.nih.nci.hpc.dmesync.service.*;

class DmeSyncSchedulerCompletionTest {
    @TempDir Path directory;
    private final DmeSyncScheduler scheduler = new DmeSyncScheduler();
    private final DmeSyncWorkflowService workflow = mock(DmeSyncWorkflowService.class);
    private final DmeSyncMailService mail = mock(DmeSyncMailService.class);
    private final DocConfig config = mock(DocConfig.class);
    private final StatusInfo tar = new StatusInfo();
    private final StatusInfo contents = new StatusInfo();

    @BeforeEach
    void setup() {
        DocConfigService configs = mock(DocConfigService.class);
        DmeSyncWorkflowServiceFactory workflowFactory = mock(DmeSyncWorkflowServiceFactory.class);
        DmeSyncMailServiceFactory mailFactory = mock(DmeSyncMailServiceFactory.class);
        DmeSyncWorkflowRunLogService runs = mock(DmeSyncWorkflowRunLogService.class);
        when(configs.getEnabledDocs()).thenReturn(List.of(config));
        when(config.getDocName()).thenReturn("test");
        when(config.getId()).thenReturn(1L);
        when(config.getSourceConfig()).thenReturn(new DocConfig.SourceConfig("/source", "/work", "/dest", 0));
        when(config.getSourceRule()).thenReturn(new DocConfig.SourceRule(
                null, null, null, null, null, false, false, null, null, false, null, false, false, false, 0));
        setMultipleTars(true);
        when(workflowFactory.getService("local")).thenReturn(workflow);
        when(mailFactory.getService("test")).thenReturn(mail);
        WorkflowRunInfo run = new WorkflowRunInfo();
        run.setRunId("run");
        run.setStatus("RUNNING");
        when(runs.findFirstByDocIdOrderByRunStartTimestampDesc(1L)).thenReturn(run);
        tar.setOriginalFilePath("/source/movies");
        tar.setSourceFileName("part.tar");
        tar.setTarIndexStart(0);
        tar.setTarIndexEnd(9);
        tar.setStatus("FAILED");
        contents.setOriginalFilePath("/source/movies");
        contents.setSourceFileName("dataset_movies_TarContentsFile.txt");
        when(workflow.findStatusInfoByRunIdAndDoc("run", "test")).thenReturn(List.of(tar, contents));
        StatusInfo parent = new StatusInfo();
        parent.setOriginalFilePath("/source/movies");
        parent.setStatus("COMPLETED");
        parent.setEndWorkflow(true);
        when(workflow.findStatusInfoByRunIdAndDoc("run_IGNORED", "test")).thenReturn(List.of(parent));

        ReflectionTestUtils.setField(scheduler, "configService", configs);
        ReflectionTestUtils.setField(scheduler, "dmeSyncWorkflowService", workflowFactory);
        ReflectionTestUtils.setField(scheduler, "dmeSyncMailServiceFactory", mailFactory);
        ReflectionTestUtils.setField(scheduler, "dmeSyncWorkflowRunLogService", runs);
        ReflectionTestUtils.setField(scheduler, "multipleTarCompletionEvaluator", new MultipleTarCompletionEvaluator());
        ReflectionTestUtils.setField(scheduler, "access", "local");
        ReflectionTestUtils.setField(scheduler, "logFile", directory.resolve("sync.log").toString());
        ReflectionTestUtils.setField(scheduler, "sender", mock(DmeSyncProducer.class));
        ReflectionTestUtils.setField(scheduler, "consumer", mock(DmeSyncConsumer.class));
        ReflectionTestUtils.setField(scheduler, "queueNameResolver", new DocQueueNameResolver());
    }

    private void setMultipleTars(boolean enabled) {
        when(config.getPreprocessingRule()).thenReturn(new DocConfig.PreprocessingRule(
                false, null, null, true, false, null, null, false, false, false, null,
                enabled, 100, "movies", null, null, false, false, "_", 2, 0));
    }

    @Test
    void reportsFailedBatchDespiteWaitingContentsFile() {
        scheduler.checkForCompletedRun();
        verify(mail).sendResult("run", config);
        verify(workflow, never()).countByDocAndRunIdAndStatus(anyString(), anyString(), any());
        verify(workflow, never()).saveStatusInfo(any());
    }

    @Test
    void successfulTarsWaitForContentsFile() {
        tar.setStatus("COMPLETED");
        scheduler.checkForCompletedRun();
        verifyNoInteractions(mail);
        contents.setStatus("COMPLETED");
        scheduler.checkForCompletedRun();
        verify(mail).sendResult("run", config);
    }

    @Test
    void pendingTarPreventsReport() {
        tar.setStatus(null);
        scheduler.checkForCompletedRun();
        verifyNoInteractions(mail);
    }

    @Test
    void normalModeKeepsExistingCountQuery() {
        setMultipleTars(false);
        when(workflow.countByDocAndRunIdAndStatus("test", "run", null)).thenReturn(1L, 0L);
        scheduler.checkForCompletedRun();
        verifyNoInteractions(mail);
        scheduler.checkForCompletedRun();
        verify(mail).sendResult("run", config);
        verify(workflow, times(2)).countByDocAndRunIdAndStatus("test", "run", null);
    }
}
