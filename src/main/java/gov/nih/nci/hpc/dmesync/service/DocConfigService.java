package gov.nih.nci.hpc.dmesync.service;

import java.util.List;
import java.util.Optional;

import gov.nih.nci.hpc.dmesync.domain.DocConfig;

public interface DocConfigService {
    List<DocConfig> getEnabledDocConfigs();
    Optional<DocConfig> getDocConfigByName(String docName);
    Optional<DocConfig> getDocConfigById(Long id);
    void refresh();

    default java.util.List<DocConfig> getEnabledDocs() {
        return getEnabledDocConfigs();
    }
}