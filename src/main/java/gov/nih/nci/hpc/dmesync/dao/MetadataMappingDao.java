package gov.nih.nci.hpc.dmesync.dao;

import gov.nih.nci.hpc.dmesync.domain.MetadataMapping;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface MetadataMappingDao<T extends MetadataMapping> extends JpaRepository<T, Long> {

  /**
   * findAllByCollectionTypeAndCollectionName
   * @param collectionType the collection type
   * @param collectionName the collection name
   * @return List of MetadataMapping
   */
  @Query("select m from MetadataMapping m where m.collectionType = ?1 and m.collectionName = ?2")
  List<MetadataMapping> findAllByCollectionTypeAndCollectionName(
      String collectionType, String collectionName);

  MetadataMapping findByCollectionTypeAndCollectionNameAndMetaDataKey(
      String collectionType, String collectionName, String metaDataKey);
}
