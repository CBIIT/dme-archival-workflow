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
   * findAllByRunIdAndDoc
   * @param runId the runId
   * @param doc the doc
   * @return list of MetadataInfo
   */
  @Query("select m from MetadataInfo m, StatusInfo s where s.id = m.objectId and s.runId = ?1 and s.doc = ?2")
  List<MetadataInfo> findAllByRunIdAndDoc(String runId, String doc);
  
  /**
   * findAllByRunIdAndMetaDataKeyAndDoc
   * @param runId the runId
   * @param metaDataKey the metaDataKey
   * @param doc the doc
   * @return list of MetadataInfo
   */
  @Query("select m from MetadataInfo m, StatusInfo s where s.id = m.objectId and s.runId = ?1 and s.doc = ?3 and m.metaDataKey = ?2")
  List<MetadataInfo> findAllByRunIdAndMetaDataKeyAndDoc(String runId, String metaDataKey, String doc);
}
