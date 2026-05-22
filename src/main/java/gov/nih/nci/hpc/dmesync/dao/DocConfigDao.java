package gov.nih.nci.hpc.dmesync.dao;

import java.util.List;
import java.util.Optional;

import gov.nih.nci.hpc.dmesync.domain.DocConfig;

/**
 * Repository for loading aggregate DOC configuration from Oracle.
 */
public interface DocConfigDao {
    List<DocConfig> findEnabledDocs();
    Optional<DocConfig> findByName(String docName);
}
