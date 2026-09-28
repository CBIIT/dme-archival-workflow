package gov.nih.nci.hpc.dmesync.scheduler;

import gov.nih.nci.hpc.dmesync.domain.DocConfig;

public interface DocWorkflowExecutor {
    void execute(DocConfig doc);
}
