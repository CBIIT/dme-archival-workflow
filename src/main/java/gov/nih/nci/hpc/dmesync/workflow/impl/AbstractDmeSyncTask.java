package gov.nih.nci.hpc.dmesync.workflow.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import gov.nih.nci.hpc.dmesync.DmeSyncWorkflowServiceFactory;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.domain.TaskInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncVerificationException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncTask;

public abstract class AbstractDmeSyncTask implements DmeSyncTask {

  @Value("${dmesync.db.access:local}")
  protected String access;

  @Autowired
  protected DmeSyncWorkflowServiceFactory dmeSyncWorkflowService;
  private String taskName;
  private boolean checkTaskForCompletion = true;

  final Logger logger = LoggerFactory.getLogger(getClass().getName());

  public StatusInfo processTask(StatusInfo object)
      throws DmeSyncMappingException, DmeSyncWorkflowException, DmeSyncVerificationException {

    if (!checkComplete(object.getId())) {

      logger.info("[{}] Started", taskName);

      object = process(object);

      //record task completion
      upsertTask(object.getId());
      logger.info("[{}] Completed", taskName);
    }

    return object;
  }

  protected abstract StatusInfo process(StatusInfo object)
      throws DmeSyncMappingException, DmeSyncWorkflowException, DmeSyncVerificationException;

  private boolean checkComplete(Long objectId) {

    TaskInfo task = dmeSyncWorkflowService.getService(access).findFirstTaskInfoByObjectIdAndTaskName(objectId, taskName);

    return (checkTaskForCompletion && task != null && task.isCompleted());
  }

  protected void upsertTask(Long objectId) {
    
    TaskInfo task = dmeSyncWorkflowService.getService(access).findFirstTaskInfoByObjectIdAndTaskName(objectId, taskName);
    if (task == null) {
      task = new TaskInfo();
      task.setObjectId(objectId);
      task.setTaskName(taskName);
    }
    task.setCompleted(true);
    dmeSyncWorkflowService.getService(access).saveTaskInfo(task);
  }
  
  public String getTaskName() {
    return taskName;
  }

  public void setTaskName(String taskName) {
    this.taskName = taskName;
  }

  public void setCheckTaskForCompletion(boolean checkTaskForCompletion) {
    this.checkTaskForCompletion = checkTaskForCompletion;
  }
}
