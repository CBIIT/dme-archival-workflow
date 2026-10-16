package gov.nih.nci.hpc.dmesync.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.util.WorkflowConstants;

class MultipleTarCompletionEvaluatorTest {
    private final MultipleTarCompletionEvaluator evaluator = new MultipleTarCompletionEvaluator();
    private final DocConfig config = config(true);

    private static DocConfig config(boolean multipleTars) {
        DocConfig config = mock(DocConfig.class);
        when(config.getPreprocessingRule()).thenReturn(new DocConfig.PreprocessingRule(
                false, null, null, true, false, null, null, false, false, false, null,
                multipleTars, 100, "movies*,batch*", null, null, false, false, "_", 2, 0));
        return config;
    }

    private StatusInfo row(String path, String name, String status) {
        StatusInfo row = new StatusInfo();
        row.setOriginalFilePath(path);
        row.setSourceFileName(name);
        row.setStatus(status);
        return row;
    }

    private StatusInfo tar(String path, String status) {
        StatusInfo tar = row(path, "group.tar", status);
        tar.setTarIndexStart(0);
        tar.setTarIndexEnd(0);
        return tar;
    }

    private StatusInfo contents(String path, String status) {
        return row(path, "dataset_movies_TarContentsFile.txt", status);
    }

    private StatusInfo generated(String path) {
        StatusInfo parent = row(path, "movies", WorkflowConstants.COMPLETED);
        parent.setEndWorkflow(true);
        parent.setRunId("run_IGNORED");
        return parent;
    }

    @Test
    void failedBatchCompletesWithReportOnlyBlockedContents() {
        StatusInfo contents = contents("/source/movies", null);
        contents.setId(42L);
        contents.setError("");
        StatusInfo parent = generated("/source/movies");
        parent.setTarContentsCount(5);

        MultipleTarCompletionEvaluator.Evaluation result = evaluator.evaluate(
                List.of(tar("/source/movies", "FAILED"), contents), List.of(parent), config);

        assertTrue(result.isComplete());
        StatusInfo reportRow = result.getReportRecords().get(1);
        assertEquals("FAILED", reportRow.getStatus());
        assertTrue(reportRow.getError().startsWith("Blocked:"));
        assertEquals(42L, reportRow.getId());
        assertNotSame(contents, reportRow);
        assertNull(contents.getStatus());
        assertEquals("", contents.getError());
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", "PROCESSING"})
    void waitsForPendingOrRetryingTarEvenAfterAnotherTarFails(String status) {
        StatusInfo retrying = tar("/source/movies", status);
        retrying.setRetryCount(2L);
        retrying.setError("Retryable upload error");
        assertFalse(evaluator.evaluate(List.of(tar("/source/movies", "FAILED"), retrying,
                contents("/source/movies", null)), List.of(generated("/source/movies")), config).isComplete());
    }

    @Test
    void successfulTarsWaitForContentsUpload() {
        assertFalse(evaluator.evaluate(List.of(tar("/source/movies", "COMPLETED"),
                contents("/source/movies", null)), List.of(generated("/source/movies")), config).isComplete());
    }

    @Test
    void alreadyArchivedIgnoredTarDoesNotBlockContentsUpload() {
        StatusInfo contents = contents("/source/movies", null);
        List<StatusInfo> records = List.of(tar("/source/movies", "IGNORED"), contents);
        MultipleTarCompletionEvaluator.Evaluation result = evaluator.evaluate(
                records, List.of(generated("/source/movies")), config);
        assertFalse(result.isComplete());
        assertSame(contents, result.getReportRecords().get(1));
        assertNull(contents.getStatus());
        contents.setStatus("COMPLETED");
        assertTrue(evaluator.evaluate(records, List.of(generated("/source/movies")), config).isComplete());
    }

    @ParameterizedTest
    @ValueSource(strings = {"COMPLETED", "FAILED"})
    void successfulTarsFinishWhenContentsIsTerminal(String contentsStatus) {
        assertTrue(evaluator.evaluate(List.of(tar("/source/movies", "COMPLETED"),
                contents("/source/movies", contentsStatus)), List.of(generated("/source/movies")), config).isComplete());
    }

    @Test
    void doesNotOverwriteAnExistingContentsFailure() {
        StatusInfo contents = contents("/source/movies", "FAILED");
        contents.setError("Contents upload failed");
        MultipleTarCompletionEvaluator.Evaluation result = evaluator.evaluate(
                List.of(tar("/source/movies", "FAILED"), contents), List.of(generated("/source/movies")), config);
        assertTrue(result.isComplete());
        assertSame(contents, result.getReportRecords().get(1));
        assertEquals("Contents upload failed", contents.getError());
    }

    @Test
    void waitsForGenerationEvenWhenAllCurrentlyVisibleTarsAreTerminal() {
        StatusInfo parent = row("/source/movies", "movies", null);
        parent.setTarContentsCount(0);
        assertFalse(evaluator.evaluate(List.of(parent, tar("/source/movies", "FAILED"),
                contents("/source/movies", null)), List.of(), config).isComplete());
    }

    @Test
    void missingGenerationMarkerDoesNotCompletePartialBatch() {
        assertFalse(evaluator.evaluate(List.of(tar("/source/movies", "FAILED"),
                contents("/source/movies", null)), List.of(), config).isComplete());
    }

    @Test
    void currentGenerationTakesPrecedenceOverIgnoredMarker() {
        assertFalse(evaluator.evaluate(List.of(row("/source/movies", "movies", null),
                tar("/source/movies", "FAILED")), List.of(generated("/source/movies")), config).isComplete());
    }

    @Test
    void failedGenerationFinishesOnlyAfterAlreadyCreatedTarsFinish() {
        StatusInfo parent = row("/source/movies", "movies", "FAILED");
        StatusInfo tar = tar("/source/movies", null);
        StatusInfo contents = contents("/source/movies", null);
        assertFalse(evaluator.evaluate(List.of(parent, tar, contents), List.of(), config).isComplete());
        tar.setStatus("COMPLETED");
        MultipleTarCompletionEvaluator.Evaluation result = evaluator.evaluate(
                List.of(parent, tar, contents), List.of(), config);
        assertTrue(result.isComplete());
        assertTrue(result.getReportRecords().get(2).getError().startsWith("Blocked:"));
    }

    @Test
    void failedGenerationWithoutChildrenCanBeReported() {
        assertTrue(evaluator.evaluate(List.of(row("/source/movies", "movies", "FAILED")),
                List.of(), config).isComplete());
    }

    @Test
    void checksEveryBatchAndOrdinaryUploadIndependently() {
        StatusInfo ordinary = row("/source/ordinary.txt", "ordinary.txt", null);
        StatusInfo otherContents = contents("/source/batch_two", null);
        List<StatusInfo> records = List.of(tar("/source/movies", "FAILED"),
                contents("/source/movies", null), tar("/source/batch_two", "COMPLETED"),
                otherContents, ordinary);
        List<StatusInfo> parents = List.of(generated("/source/movies"), generated("/source/batch_two"));
        assertFalse(evaluator.evaluate(records, parents, config).isComplete());
        ordinary.setStatus("COMPLETED");
        assertFalse(evaluator.evaluate(records, parents, config).isComplete());
        otherContents.setStatus("COMPLETED");
        assertTrue(evaluator.evaluate(records, parents, config).isComplete());
    }

    @Test
    void supportsContentsOnlyRerunWithPreviouslySuccessfulTars() {
        StatusInfo contents = contents("/source/movies", null);
        assertFalse(evaluator.evaluate(List.of(contents), List.of(generated("/source/movies")), config).isComplete());
        contents.setStatus("COMPLETED");
        assertTrue(evaluator.evaluate(List.of(contents), List.of(generated("/source/movies")), config).isComplete());
    }

    @Test
    void generationMarkerMustMatchExactBatchPath() {
        assertFalse(evaluator.evaluate(List.of(tar("/source/movies", "FAILED"),
                contents("/source/movies", null)), List.of(generated("/other/movies")), config).isComplete());
    }

    @Test
    void usesLegacyNullStatusRuleOutsideMultipleTarMode() {
        StatusInfo contents = contents("/source/movies", null);
        assertFalse(evaluator.evaluate(List.of(tar("/source/movies", "FAILED"), contents),
                List.of(generated("/source/movies")), config(false)).isComplete());
        assertNull(contents.getStatus());
        assertFalse(evaluator.isBatchRecord(contents, config(false)));
    }
}
