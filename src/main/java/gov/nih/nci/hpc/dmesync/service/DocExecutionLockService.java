package gov.nih.nci.hpc.dmesync.service;

import gov.nih.nci.hpc.dmesync.domain.DocConfig;

public interface DocExecutionLockService {
    boolean tryAcquire(DocConfig doc);
    void release(DocConfig doc);
}
