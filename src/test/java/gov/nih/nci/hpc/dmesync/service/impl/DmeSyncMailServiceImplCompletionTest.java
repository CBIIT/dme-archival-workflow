package gov.nih.nci.hpc.dmesync.service.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import javax.mail.Session;
import javax.mail.internet.MimeMessage;

import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.test.util.ReflectionTestUtils;

import gov.nih.nci.hpc.dmesync.DmeSyncWorkflowServiceFactory;
import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.service.DmeSyncWorkflowRunLogService;
import gov.nih.nci.hpc.dmesync.service.DmeSyncWorkflowService;
import gov.nih.nci.hpc.dmesync.service.MultipleTarCompletionEvaluator;

class DmeSyncMailServiceImplCompletionTest {
    @TempDir Path directory;

    @ParameterizedTest
    @ValueSource(strings = {"test", "csb"})
    void exportsEachTarAndBlockedContentsWithoutPersistingReportStatuses(String doc) throws Exception {
        DocConfig config = mock(DocConfig.class);
        when(config.getDocName()).thenReturn(doc);
        when(config.getWorkflowId()).thenReturn("workflow");
        when(config.getSourceConfig()).thenReturn(new DocConfig.SourceConfig("/source", "/work", "/dest", 0));
        when(config.getNotificationConfig()).thenReturn(
                new DocConfig.NotificationConfig("test@example.org", false, true, 0));
        when(config.getPreprocessingRule()).thenReturn(new DocConfig.PreprocessingRule(
                false, null, null, true, false, null, null, false, false, false, null,
                true, 100, "movies", null, null, false, false, "_", 2, 0));

        StatusInfo tar = record(1L, "group.tar", "FAILED");
        tar.setTarIndexStart(0);
        tar.setTarIndexEnd(9);
        // A TAR that failed before preprocessing has no sourceFilePath.
        StatusInfo contents = record(2L, "dataset_movies_TarContentsFile.txt", null);
        contents.setSourceFilePath("/work/dataset_movies_TarContentsFile.txt");
        StatusInfo parent = record(3L, "movies", "COMPLETED");
        parent.setEndWorkflow(true);
        parent.setRunId("run_IGNORED");

        DmeSyncWorkflowService workflow = mock(DmeSyncWorkflowService.class);
        when(workflow.findStatusInfoByRunIdAndDoc("run", doc)).thenReturn(List.of(tar, contents));
        when(workflow.findStatusInfoByRunIdAndDoc("run_IGNORED", doc)).thenReturn(List.of(parent));
        when(workflow.findAllMetadataInfoByRunIdAndDoc("run", doc)).thenReturn(new ArrayList<>());
        when(workflow.findAllByDocAndLikeOriginalFilePath(doc, "/source/movies"))
                .thenReturn(new ArrayList<>(List.of(tar, contents)));
        DmeSyncWorkflowServiceFactory factory = mock(DmeSyncWorkflowServiceFactory.class);
        when(factory.getService("local")).thenReturn(workflow);
        DmeSyncWorkflowRunLogService runs = mock(DmeSyncWorkflowRunLogService.class);
        JavaMailSender sender = mock(JavaMailSender.class);
        MimeMessage message = new MimeMessage(Session.getInstance(new Properties()));
        when(sender.createMimeMessage()).thenReturn(message);

        DmeSyncMailServiceImpl mail = new DmeSyncMailServiceImpl();
        ReflectionTestUtils.setField(mail, "sender", sender);
        ReflectionTestUtils.setField(mail, "dmeSyncWorkflowService", factory);
        ReflectionTestUtils.setField(mail, "dmeSyncWorkflowRunLogService", runs);
        ReflectionTestUtils.setField(mail, "multipleTarCompletionEvaluator", new MultipleTarCompletionEvaluator());
        ReflectionTestUtils.setField(mail, "access", "local");
        ReflectionTestUtils.setField(mail, "logFile", directory.resolve("sync.log").toString());
        ReflectionTestUtils.setField(mail, "maxRecommendedFileSize", "100000");
        ReflectionTestUtils.setField(mail, "minTarFile", "1024");

        mail.sendResult("run", config);

        verify(sender).send(message);
        assertTrue(message.getSubject().contains("Failed"));
        verify(runs).updateWorkflowRunEnd("run", config, "FAILED", null);
        verify(workflow, never()).saveStatusInfo(any());
        verify(workflow, times("csb".equals(doc) ? 1 : 0))
                .findAllByDocAndLikeOriginalFilePath(anyString(), anyString());
        assertNull(contents.getStatus());
        assertNull(contents.getError());
        try (Workbook workbook = WorkbookFactory.create(directory.resolve("workflow_run.xlsx").toFile())) {
            Sheet sheet = workbook.getSheet("RunResult");
            assertEquals(2, sheet.getLastRowNum());
            assertEquals("FAILED", sheet.getRow(2).getCell(6).getStringCellValue());
            assertTrue(sheet.getRow(2).getCell(15).getStringCellValue().startsWith("Blocked:"));
        }
    }

    private StatusInfo record(long id, String name, String status) {
        StatusInfo record = new StatusInfo();
        record.setId(id);
        record.setRunId("run");
        record.setDoc("test");
        record.setOriginalFilePath("/source/movies");
        record.setOrginalFileName("movies");
        record.setSourceFileName(name);
        record.setStatus(status);
        record.setFilesize(0L);
        record.setStartTimestamp(new Date());
        return record;
    }
}
