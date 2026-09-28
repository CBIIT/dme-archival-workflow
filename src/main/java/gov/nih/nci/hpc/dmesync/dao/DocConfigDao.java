package gov.nih.nci.hpc.dmesync.dao;

import java.util.List;
import java.util.Optional;

import gov.nih.nci.hpc.dmesync.domain.DocConfig;

/**
 * Repository for loading aggregate DOC configuration from Oracle.
 */
public interface DocConfigDao {
    /**
     * Finds all DOC configurations that are currently enabled.
     *
     * @return a list of enabled DOC configurations; empty if none are enabled
     */
    List<DocConfig> findEnabledDocs();

    /**
     * Finds a DOC configuration by its DOC name.
     *
     * @param docName DOC name to look up
     * @return an {@link Optional} containing the matching DOC configuration,
     *         or {@link Optional#empty()} if no match is found
     */
    Optional<DocConfig> findByName(String docName);
}