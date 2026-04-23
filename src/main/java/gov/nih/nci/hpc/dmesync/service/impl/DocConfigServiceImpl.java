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

@Service
public class DocConfigServiceImpl implements DocConfigService {
    private final DocConfigDao docConfigDao;
    private volatile List<DocConfig> enabledDocs = List.of();
    private final Map<String, DocConfig> docsByName = new ConcurrentHashMap<>();
    private final Map<Long, DocConfig> docsById = new ConcurrentHashMap<>();

    public DocConfigServiceImpl(DocConfigDao docConfigDao) {
        this.docConfigDao = docConfigDao;
        refresh();
    }

    @Scheduled(fixedDelayString = "${app.config.refresh-ms:60000}")
    @Override
    public synchronized void refresh() {
        List<DocConfig> docs = docConfigDao.findEnabledDocs();
        // TODO: Add validation logic here
        this.enabledDocs = List.copyOf(docs);
        this.docsByName.clear();
        docs.forEach(doc -> this.docsByName.put(doc.getDocName(), doc));
        docs.forEach(doc -> this.docsById.put(doc.getId(), doc));
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
}
