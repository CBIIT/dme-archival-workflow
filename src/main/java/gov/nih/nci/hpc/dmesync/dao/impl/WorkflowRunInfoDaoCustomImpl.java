package gov.nih.nci.hpc.dmesync.dao.impl;

import gov.nih.nci.hpc.dmesync.dao.WorkflowRunInfoDaoCustom;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.Instant;

@Repository
public class WorkflowRunInfoDaoCustomImpl implements WorkflowRunInfoDaoCustom {

    @Autowired
    private JdbcTemplate jdbcTemplate;


    @Override
    public Instant findLastScheduledTime(Long docId) {
        String sql = "SELECT RUN_START_TIMESTAMP FROM WORKFLOW_RUN_INFO WHERE WORKFLOW_ID = (SELECT WORKFLOW_ID FROM DOC_CONFIG WHERE ID = ?) ORDER BY RUN_START_TIMESTAMP DESC FETCH FIRST 1 ROWS ONLY";
        return jdbcTemplate.query(sql, rs -> rs.next() ? rs.getTimestamp(1).toInstant() : null, docId);
    }
}