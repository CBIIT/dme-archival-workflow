package gov.nih.nci.hpc.dmesync.dao;

import gov.nih.nci.hpc.dmesync.domain.MetadataInfo;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface MetadataInfoDao<T extends MetadataInfo> extends JpaRepository<T, Long> {

  /**
   * deleteByObjectId
   * @param objectId the id
   */
  void deleteByObjectId(Long objectId);

  /**
   * findAllByRunId
   * @param runId the runId
   * @return list of MetadataInfo
   */
  @Query("select m from MetadataInfo m, StatusInfo s where s.id = m.objectId and s.runId = ?1")
  List<MetadataInfo> findAllByRunId(String runId);
  
  /**
   * findAllByRunIdAndMetaDataKey
   * @param runId the runId
   * @param metaDataKey the metaDataKey
   * @return list of MetadataInfo
   */
  @Query("select m from MetadataInfo m, StatusInfo s where s.id = m.objectId and s.runId = ?1 and m.metaDataKey = ?2")
  List<MetadataInfo> findAllByRunIdAndMetaDataKey(String runId, String metaDataKey);
}
