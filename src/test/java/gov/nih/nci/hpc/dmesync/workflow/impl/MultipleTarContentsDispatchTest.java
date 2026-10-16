package gov.nih.nci.hpc.dmesync.workflow.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;
import org.springframework.test.util.ReflectionTestUtils;

import gov.nih.nci.hpc.dmesync.DmeSyncWorkflowServiceFactory;
import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.dto.DmeSyncMessageDto;
import gov.nih.nci.hpc.dmesync.jms.DmeSyncProducer;
import gov.nih.nci.hpc.dmesync.jms.DocQueueNameResolver;
import gov.nih.nci.hpc.dmesync.service.DmeSyncWorkflowService;

class MultipleTarContentsDispatchTest {
    @TempDir Path directory;
    private final DmeSyncWorkflowService workflow = mock(DmeSyncWorkflowService.class);
    private final DmeSyncProducer sender = mock(DmeSyncProducer.class);
    private final DocConfig config = mock(DocConfig.class);
    private final StatusInfo contents = new StatusInfo();

    @BeforeEach
    void setup() {
        when(config.getId()).thenReturn(10L);
        when(config.getDocName()).thenReturn("test");
        when(config.getWorkflowId()).thenReturn("workflow");
        when(config.getSourceConfig()).thenReturn(new DocConfig.SourceConfig(
                "/source", directory.toString(), "/dest", 0));
        contents.setId(20L);
        contents.setRunId("run");
        contents.setSourceFileName("dataset_movies_TarContentsFile.txt");
        contents.setStatus("PENDING");
        contents.setError("Waiting for TAR uploads");
        when(workflow.saveStatusInfo(any(StatusInfo.class))).thenAnswer(call -> {
            StatusInfo row = call.getArgument(0);
            if (row == contents) {
                assertNull(row.getStatus());
                assertEquals("", row.getError());
            }
            return row;
        });
    }

    private void wire(AbstractDmeSyncTask task) {
        DmeSyncWorkflowServiceFactory factory = mock(DmeSyncWorkflowServiceFactory.class);
        when(factory.getService("local")).thenReturn(workflow);
        ReflectionTestUtils.setField(task, "access", "local");
        ReflectionTestUtils.setField(task, "dmeSyncWorkflowService", factory);
        ReflectionTestUtils.setField(task, "sender", sender);
        ReflectionTestUtils.setField(task, "queueNameResolver", new DocQueueNameResolver());
    }

    private void verifyContentsSavedBeforeSend() {
        InOrder order = inOrder(workflow, sender);
        order.verify(workflow).saveStatusInfo(argThat(row ->
                row == contents && row.getStatus() == null && "".equals(row.getError())));
        order.verify(sender).send(argThat(message ->
                Long.valueOf(20L).equals(message.getObjectId())
                        && Long.valueOf(10L).equals(message.getDocConfigId())), eq("dme.workflow"));
        assertNull(contents.getStatus());
        assertEquals("", contents.getError());
    }

    @Test
    void immediateContentsDispatchClearsAndPersistsPendingStateBeforeEnqueue() {
        DmeSyncProcessMultipleTarsTaskImpl task = new DmeSyncProcessMultipleTarsTaskImpl();
        wire(task);
        ReflectionTestUtils.invokeMethod(task, "enqueueRequestToJms", contents, config);
        verifyContentsSavedBeforeSend();
    }

    @Test
    void ordinaryTarDispatchDoesNotResetOrSaveStatus() {
        DmeSyncProcessMultipleTarsTaskImpl task = new DmeSyncProcessMultipleTarsTaskImpl();
        wire(task);
        StatusInfo tar = new StatusInfo();
        tar.setId(30L);
        tar.setSourceFileName("part_1_of_2.tar");
        tar.setStatus("FAILED");
        tar.setError("Existing failure");
        ReflectionTestUtils.invokeMethod(task, "enqueueRequestToJms", tar, config);
        verify(workflow, never()).saveStatusInfo(any());
        verify(sender).send(any(DmeSyncMessageDto.class), eq("dme.workflow"));
        assertEquals("FAILED", tar.getStatus());
        assertEquals("Existing failure", tar.getError());
    }

    @Test
    void cleanupContentsDispatchClearsAndPersistsPendingStateBeforeEnqueue() throws Exception {
        DmeSyncCleanupTaskImpl task = new DmeSyncCleanupTaskImpl();
        wire(task);
        StatusInfo tar = cleanupTar(1);
        ReflectionTestUtils.invokeMethod(task, "cleanUpTaskForMultipleTars", tar, config);
        verifyContentsSavedBeforeSend();
    }

    @Test
    void cleanupLeavesContentsPendingWhileOtherTarsRemain() throws Exception {
        DmeSyncCleanupTaskImpl task = new DmeSyncCleanupTaskImpl();
        wire(task);
        StatusInfo tar = cleanupTar(2);
        ReflectionTestUtils.invokeMethod(task, "cleanUpTaskForMultipleTars", tar, config);
        verify(workflow, never()).saveStatusInfo(same(contents));
        verifyNoInteractions(sender);
        assertEquals("PENDING", contents.getStatus());
        assertEquals("Waiting for TAR uploads", contents.getError());
    }

    @Test
    void immediatePersistenceFailurePreventsEnqueue() {
        DmeSyncProcessMultipleTarsTaskImpl task = new DmeSyncProcessMultipleTarsTaskImpl();
        wire(task);
        doThrow(new IllegalStateException("Database unavailable")).when(workflow).saveStatusInfo(contents);
        assertThrows(IllegalStateException.class,
                () -> ReflectionTestUtils.invokeMethod(task, "enqueueRequestToJms", contents, config));
        verifyNoInteractions(sender);
    }

    @Test
    void cleanupPersistenceFailurePreventsEnqueue() throws Exception {
        DmeSyncCleanupTaskImpl task = new DmeSyncCleanupTaskImpl();
        wire(task);
        StatusInfo tar = cleanupTar(1);
        doThrow(new IllegalStateException("Database unavailable")).when(workflow).saveStatusInfo(contents);
        assertThrows(IllegalStateException.class,
                () -> ReflectionTestUtils.invokeMethod(task, "cleanUpTaskForMultipleTars", tar, config));
        verifyNoInteractions(sender);
    }

    private StatusInfo cleanupTar(int remaining) throws Exception {
        Path work = Files.createDirectories(directory.resolve("dataset/movies"));
        Path tarFile = Files.createFile(work.resolve("part.tar"));
        StatusInfo tar = new StatusInfo();
        tar.setDoc("test");
        tar.setRunId("run");
        tar.setOriginalFilePath("/source/dataset/movies");
        tar.setOrginalFileName("movies");
        tar.setSourceFilePath(tarFile.toString());
        StatusInfo parent = new StatusInfo();
        parent.setTarContentsCount(remaining);
        when(workflow.findTopBySourceFileNameAndRunIdAndOriginalFilePath(
                "dataset_movies_TarContentsFile.txt", "run", "/source/dataset/movies")).thenReturn(contents);
        when(workflow.findTopStatusInfoByDocAndSourceFilePathAndLikeRunId(
                "test", "/source/dataset/movies", "run%")).thenReturn(parent);
        return tar;
    }
}
