package gov.nih.nci.hpc.dmesync.domain;

import javax.persistence.*;

@Entity
public class TaskInfo {
  private Long id;
  private Long objectId; //StatusInfo.id
  private String taskName;
  private boolean completed;

  @Id
  @Column(name = "ID", nullable = false, precision = 0)
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "TASK_INFO_SEQ")
  @SequenceGenerator(name = "TASK_INFO_SEQ", sequenceName = "TASK_INFO_SEQ", allocationSize = 1)
  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Long getObjectId() {
    return objectId;
  }

  public void setObjectId(Long objectId) {
    this.objectId = objectId;
  }

  public String getTaskName() {
    return taskName;
  }

  public void setTaskName(String taskName) {
    this.taskName = taskName;
  }

  public boolean isCompleted() {
    return completed;
  }

  public void setCompleted(boolean completed) {
    this.completed = completed;
  }
}
