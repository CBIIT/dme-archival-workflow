package gov.nih.nci.hpc.dmesync.scheduler;

import gov.nih.nci.hpc.dmesync.dao.WorkflowRunInfoDao;
import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import gov.nih.nci.hpc.dmesync.service.DocConfigService;
import gov.nih.nci.hpc.dmesync.service.DocExecutionLockService;
import gov.nih.nci.hpc.dmesync.util.CronDueEvaluator;

import org.springframework.core.task.TaskExecutor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import java.time.Instant;

@Service
public class DocScheduleCoordinator {
    private final DocConfigService configService;
    private final CronDueEvaluator cronDueEvaluator;
    private final DocExecutionLockService lockService;
    private final DocWorkflowExecutor executor;
    private final WorkflowRunInfoDao<?> runInfoDao;
    private final TaskExecutor taskExecutor;
    public DocScheduleCoordinator(
        DocConfigService configService,
        CronDueEvaluator cronDueEvaluator,
        DocExecutionLockService lockService,
        DocWorkflowExecutor executor,
        WorkflowRunInfoDao<?> runInfoDao,
        @Qualifier("taskExecutor") TaskExecutor taskExecutor) {
        this.configService = configService;
        this.cronDueEvaluator = cronDueEvaluator;
        this.lockService = lockService;
        this.executor = executor;
        this.runInfoDao = runInfoDao;
        this.taskExecutor = taskExecutor;
    }
    public void dispatchDueDocs() {
    	Instant now = Instant.now();
        for (DocConfig doc : configService.getEnabledDocs()) {
        	Instant lastScheduled = runInfoDao.findLastScheduledTime(doc.getId());
            if (!cronDueEvaluator.isDue(doc, now, lastScheduled)) {
                continue;
            }
            if (!lockService.tryAcquire(doc)) {
                continue;
            }
            taskExecutor.execute(() -> {
                try {
                    executor.execute(doc);
                } finally {
                    lockService.release(doc);
                }
            });
        }
    }
}