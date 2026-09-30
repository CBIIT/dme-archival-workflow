package gov.nih.nci.hpc.dmesync.service.impl;

import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import gov.nih.nci.hpc.dmesync.service.ScheduledJobLockService;

@Service
public class ScheduledJobLockServiceImpl implements ScheduledJobLockService {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Value("${dmesync.workflow.server.id:}")
    private String serverId;

    public ScheduledJobLockServiceImpl(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public boolean tryAcquire(String jobName, String runKey) {
        Map<String, Object> params = Map.of(
            "jobName", jobName,
            "runKey", runKey,
            "serverId", serverId
        );

        String updateSql = """
            UPDATE SCHEDULED_JOB_LOCK
               SET RUN_KEY = :runKey,
                   LOCKED_BY = :serverId,
                   STATUS = 'RUNNING',
                   LOCKED_AT = SYSTIMESTAMP,
                   UPDATED_AT = SYSTIMESTAMP,
                   COMPLETED_AT = NULL
             WHERE JOB_NAME = :jobName
               AND (RUN_KEY IS NULL OR RUN_KEY <> :runKey)
            """;
        if (jdbcTemplate.update(updateSql, params) == 1) {
            return true;
        }

        String insertSql = """
            INSERT INTO SCHEDULED_JOB_LOCK (
                JOB_NAME,
                RUN_KEY,
                LOCKED_BY,
                STATUS,
                LOCKED_AT,
                UPDATED_AT
            ) VALUES (
                :jobName,
                :runKey,
                :serverId,
                'RUNNING',
                SYSTIMESTAMP,
                SYSTIMESTAMP
            )
            """;
        try {
            return jdbcTemplate.update(insertSql, params) == 1;
        } catch (DataIntegrityViolationException ex) {
            return false;
        }
    }

    @Override
    public void markCompleted(String jobName, String runKey, boolean success) {
        String sql = """
            UPDATE SCHEDULED_JOB_LOCK
               SET STATUS = :status,
                   COMPLETED_AT = SYSTIMESTAMP,
                   UPDATED_AT = SYSTIMESTAMP
             WHERE JOB_NAME = :jobName
               AND RUN_KEY = :runKey
               AND LOCKED_BY = :serverId
            """;
        jdbcTemplate.update(sql, Map.of(
            "jobName", jobName,
            "runKey", runKey,
            "serverId", serverId,
            "status", success ? "COMPLETED" : "FAILED"
        ));
    }
}
