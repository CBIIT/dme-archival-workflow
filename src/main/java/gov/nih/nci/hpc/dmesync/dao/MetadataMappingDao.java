package gov.nih.nci.hpc.dmesync.dao;

import gov.nih.nci.hpc.dmesync.domain.MetadataMapping;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface MetadataMappingDao<T extends MetadataMapping> extends JpaRepository<T, Long> {

  /**
   * findAllByCollectionTypeAndCollectionNameAndDoc
   * @param collectionType the collection type
   * @param collectionName the collection name
   * @param doc the doc
   * @return List of MetadataMapping
   */
  @Query("select m from MetadataMapping m where m.collectionType = ?1 and m.collectionName = ?2 and m.doc = ?3")
  List<MetadataMapping> findAllByCollectionTypeAndCollectionNameAndDoc(
      String collectionType, String collectionName, String doc);

  MetadataMapping findByCollectionTypeAndCollectionNameAndMetaDataKeyAndDoc(
      String collectionType, String collectionName, String metaDataKey, String doc);
}
