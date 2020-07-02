package gov.nih.nci.hpc.dmesync.dao;

import gov.nih.nci.hpc.dmesync.domain.PermissionBookmarkInfo;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PermissionBookmarkInfoDao<T extends PermissionBookmarkInfo>
    extends JpaRepository<T, Long> {

  /**
   * findAllByCreated
   * @param flag the flag Y or N
   * @return list of PermissionBookmarkInfo
   */
  List<PermissionBookmarkInfo> findAllByCreated(String flag);
}
