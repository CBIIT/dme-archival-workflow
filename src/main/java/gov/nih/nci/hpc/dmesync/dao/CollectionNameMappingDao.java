package gov.nih.nci.hpc.dmesync.dao;

import gov.nih.nci.hpc.dmesync.domain.CollectionNameMapping;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface CollectionNameMappingDao<T extends CollectionNameMapping>
    extends JpaRepository<T, Long> {

  /**
   * findByMapKeyAndCollectionTypeAndDoc
   * 
   * @param key the key 
   * @param collectionType the collection type
   * @param doc the doc
   * @return CollectionNameMapping
   */
  @Query("select m from CollectionNameMapping m where m.mapKey = ?1 and m.collectionType = ?2 and m.doc = ?3")
  CollectionNameMapping findByMapKeyAndCollectionTypeAndDoc(String key, String collectionType, String doc);
}
