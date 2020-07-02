package gov.nih.nci.hpc.dmesync.dao;

import gov.nih.nci.hpc.dmesync.domain.TaskInfo;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TaskInfoDao<T extends TaskInfo> extends JpaRepository<T, Long> {

  TaskInfo findFirstByObjectIdAndTaskName(Long id, String taskName);

  void deleteByObjectId(Long objectId);

}
