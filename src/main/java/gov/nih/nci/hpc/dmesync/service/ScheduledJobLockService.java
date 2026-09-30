package gov.nih.nci.hpc.dmesync.service;

public interface ScheduledJobLockService {
    boolean tryAcquire(String jobName, String runKey);
    void markCompleted(String jobName, String runKey, boolean success);
}
