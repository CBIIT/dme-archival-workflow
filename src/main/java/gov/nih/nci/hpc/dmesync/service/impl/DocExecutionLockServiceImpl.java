package gov.nih.nci.hpc.dmesync.service.impl;

import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import gov.nih.nci.hpc.dmesync.service.DocExecutionLockService;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import java.util.Map;

@Service
public class DocExecutionLockServiceImpl implements DocExecutionLockService {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public DocExecutionLockServiceImpl(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public boolean tryAcquire(DocConfig doc) {
        String sql = """
            UPDATE DOC_CONFIG
            SET RUNNING_FLAG = '1'
            WHERE ID = :docId
              AND RUNNING_FLAG = '0'
            """;
        return jdbcTemplate.update(sql, Map.of("docId", doc.getId())) == 1;
    }

    @Override
    public void release(DocConfig doc) {
        String sql = """
            UPDATE DOC_CONFIG
            SET RUNNING_FLAG = '0'
            WHERE ID = :docId
            """;
        jdbcTemplate.update(sql, Map.of("docId", doc.getId()));
    }
}