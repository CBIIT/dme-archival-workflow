package gov.nih.nci.hpc.dmesync.service.impl;

import gov.nih.nci.hpc.dmesync.dao.DocConfigDao;
import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import gov.nih.nci.hpc.dmesync.service.DocConfigService;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import javax.transaction.Transactional;

@Service
public class DocConfigServiceImpl implements DocConfigService {
    private final DocConfigDao configDao;
    private volatile List<DocConfig> enabledDocs = List.of();
    private volatile Map<String, DocConfig> docsByName = new ConcurrentHashMap<>();
    private volatile Map<Long, DocConfig> docsById = new ConcurrentHashMap<>();

    public DocConfigServiceImpl(DocConfigDao configDao) {
        this.configDao = configDao;
        refresh();
    }

    @Scheduled(fixedDelayString = "${app.config.refresh-ms:60000}")
    @Override
    public synchronized void refresh() {
        List<DocConfig> docs = configDao.findEnabledDocs();
        // TODO: Add validation logic here
        this.enabledDocs = List.copyOf(docs);
        Map<String, DocConfig> byName = new ConcurrentHashMap<>();
        Map<Long, DocConfig> byId = new ConcurrentHashMap<>();
        docs.forEach(doc -> byName.put(doc.getDocName(), doc));
        docs.forEach(doc -> byId.put(doc.getId(), doc));
        // Publishing the fully built snapshot at once so readers never see a partial refresh.
        this.enabledDocs = List.copyOf(docs);
        this.docsByName = byName;
        this.docsById = byId;
    }

    @Override
    public List<DocConfig> getEnabledDocConfigs() {
        return enabledDocs;
    }

    @Override
    public Optional<DocConfig> getDocConfigByName(String docName) {
        return Optional.ofNullable(docsByName.get(docName));
    }
    
    @Override
    public Optional<DocConfig> getDocConfigById(Long id) {
        return Optional.ofNullable(docsById.get(id));
    }
    
    @Override
    @Transactional
    public synchronized boolean updateSourceIncludePattern(Long docId, String includePattern , int version) {
        boolean updated = configDao.updateIncludePattern(docId, includePattern , version);
        if (updated) {
            refresh();
        }
        return updated;
    }
}
