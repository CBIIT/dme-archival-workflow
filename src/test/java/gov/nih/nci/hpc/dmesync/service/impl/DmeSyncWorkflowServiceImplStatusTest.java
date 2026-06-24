package gov.nih.nci.hpc.dmesync.service.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import gov.nih.nci.hpc.dmesync.dao.MetadataInfoDao;
import gov.nih.nci.hpc.dmesync.dao.StatusInfoDao;
import gov.nih.nci.hpc.dmesync.dao.TaskInfoDao;
import gov.nih.nci.hpc.dmesync.domain.MetadataInfo;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.domain.TaskInfo;
import gov.nih.nci.hpc.dmesync.util.WorkflowConstants;

class DmeSyncWorkflowServiceImplStatusTest {

 

  @SuppressWarnings("unchecked")
  @Test
  void recordErrorPreservesIgnoredStatus() {
    DmeSyncWorkflowServiceImpl service = new DmeSyncWorkflowServiceImpl();
    StatusInfoDao<StatusInfo> statusInfoDao = Mockito.mock(StatusInfoDao.class);
    MetadataInfoDao<MetadataInfo> metadataInfoDao = Mockito.mock(MetadataInfoDao.class);

    ReflectionTestUtils.setField(service, "statusInfoDao", statusInfoDao);
    ReflectionTestUtils.setField(service, "metadataInfoDao", metadataInfoDao);

    StatusInfo statusInfo = new StatusInfo();
    statusInfo.setId(21L);
    statusInfo.setStatus(WorkflowConstants.IGNORED);

    service.retryWorkflow(statusInfo, true, new RuntimeException("ignored"));

    assertEquals(WorkflowConstants.IGNORED, statusInfo.getStatus());
    verify(statusInfoDao).saveAndFlush(statusInfo);
    verify(metadataInfoDao).deleteByObjectId(21L);
  }

  @SuppressWarnings("unchecked")
  @Test
  void recordErrorMarksNonIgnoredStatusFailed() {
    DmeSyncWorkflowServiceImpl service = new DmeSyncWorkflowServiceImpl();
    StatusInfoDao<StatusInfo> statusInfoDao = Mockito.mock(StatusInfoDao.class);

    ReflectionTestUtils.setField(service, "statusInfoDao", statusInfoDao);

    StatusInfo statusInfo = new StatusInfo();
    statusInfo.setStatus(WorkflowConstants.COMPLETED);

    service.recordError(statusInfo, true);

    assertEquals(WorkflowConstants.FAILED, statusInfo.getStatus());
    verify(statusInfoDao).saveAndFlush(statusInfo);
  }
}
