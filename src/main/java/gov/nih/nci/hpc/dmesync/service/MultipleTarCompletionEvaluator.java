package gov.nih.nci.hpc.dmesync.service;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Component;

import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.util.TarUtil;
import gov.nih.nci.hpc.dmesync.util.WorkflowConstants;

@Component
public class MultipleTarCompletionEvaluator {

    public boolean isBatchRecord(StatusInfo record, DocConfig config) {
        if (!config.getPreprocessingRule().processMultipleTars || record.getOriginalFilePath() == null) {
            return false;
        }
        Path name = Path.of(record.getOriginalFilePath()).getFileName();
        return name != null && TarUtil.matchesAnyMultipleTarFolder(
                config.getPreprocessingRule().multipleTarsDirFolders, name.toString());
    }

    public Evaluation evaluate(List<StatusInfo> records, List<StatusInfo> ignoredRecords, DocConfig config) {
        boolean complete = true;
        Set<StatusInfo> blockedContents = new HashSet<>();
        Map<String, List<StatusInfo>> batches = records.stream()
                .filter(row -> isBatchRecord(row, config))
                .collect(Collectors.groupingBy(StatusInfo::getOriginalFilePath));

        for (StatusInfo row : records) {
            if (!isBatchRecord(row, config) && row.getStatus() == null) {
                complete = false;
            }
        }

        for (Map.Entry<String, List<StatusInfo>> batch : batches.entrySet()) {
            List<StatusInfo> rows = batch.getValue();
            StatusInfo parent = rows.stream().filter(this::isParent).findFirst()
                    .orElseGet(() -> ignoredRecords.stream()
                            .filter(row -> Objects.equals(batch.getKey(), row.getOriginalFilePath()))
                            .filter(this::isParent).findFirst().orElse(null));

            // The folder moves to the ignored run only after request generation and
            // verification finish. The cleanup counter is not a generation marker.
            boolean generated = parent != null && WorkflowConstants.isCompletedStatus(parent.getStatus())
                    && Boolean.TRUE.equals(parent.isEndWorkflow());
            boolean generationFailed = parent != null && WorkflowConstants.isFailedStatus(parent.getStatus());
            if ((!generated && !generationFailed)
                    || rows.stream().filter(row -> !isContentsFile(row)).anyMatch(row -> !isTerminal(row))) {
                complete = false;
                continue;
            }

            boolean failed = generationFailed || rows.stream().filter(this::isTar)
                    .anyMatch(row -> !WorkflowConstants.isCompletedStatus(row.getStatus()));
            for (StatusInfo row : rows) {
                if (isContentsFile(row) && !isTerminal(row)) {
                    if (failed && row.getStatus() == null) {
                        blockedContents.add(row);
                    } else {
                        complete = false;
                    }
                }
            }
        }

        List<StatusInfo> reportRecords = new ArrayList<>();
        for (StatusInfo row : records) {
            if (blockedContents.contains(row)) {
                StatusInfo reportRow = new StatusInfo();
                BeanUtils.copyProperties(row, reportRow);
                reportRow.setStatus(WorkflowConstants.FAILED);
                reportRow.setError("Blocked: contents file not uploaded because dependent TAR uploads or batch generation failed.");
                reportRecords.add(reportRow);
            } else {
                reportRecords.add(row);
            }
        }
        return new Evaluation(complete, reportRecords);
    }

    private boolean isTar(StatusInfo row) {
        return row.getTarIndexStart() != null || row.getTarIndexEnd() != null;
    }

    private boolean isContentsFile(StatusInfo row) {
        return isContentsPath(row.getSourceFileName()) || isContentsPath(row.getSourceFilePath());
    }

    private boolean isContentsPath(String path) {
        return path != null && (path.endsWith(WorkflowConstants.tarContentsFileEndswith)
                || path.endsWith(WorkflowConstants.tarExcludedContentsFileEndswith));
    }

    private boolean isParent(StatusInfo row) {
        return !isTar(row) && !isContentsFile(row);
    }

    private boolean isTerminal(StatusInfo row) {
        return WorkflowConstants.isCompletedStatus(row.getStatus())
                || WorkflowConstants.isFailedStatus(row.getStatus())
                || WorkflowConstants.isIgnoredStatus(row.getStatus());
    }

    public static class Evaluation {
        private final boolean complete;
        private final List<StatusInfo> reportRecords;

        private Evaluation(boolean complete, List<StatusInfo> reportRecords) {
            this.complete = complete;
            this.reportRecords = reportRecords;
        }

        public boolean isComplete() {
            return complete;
        }

        public List<StatusInfo> getReportRecords() {
            return reportRecords;
        }
    }
}
