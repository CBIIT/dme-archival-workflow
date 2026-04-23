package gov.nih.nci.hpc.dmesync.scheduler;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class DocSchedulePollingJob {
    private final DocScheduleCoordinator coordinator;
    public DocSchedulePollingJob(DocScheduleCoordinator coordinator) {
        this.coordinator = coordinator;
    }
    @Scheduled(fixedDelayString = "${app.scheduler.poll-ms:60000}")
    public void poll() {
        coordinator.dispatchDueDocs();
    }
}
