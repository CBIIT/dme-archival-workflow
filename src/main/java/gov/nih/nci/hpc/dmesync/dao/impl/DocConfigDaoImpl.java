package gov.nih.nci.hpc.dmesync.dao.impl;

import gov.nih.nci.hpc.dmesync.dao.DocConfigDao;
import gov.nih.nci.hpc.dmesync.domain.DocConfig;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Repository
public class DocConfigDaoImpl implements DocConfigDao {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${dmesync.server.id:}")
    private String serverId;
    
    @Override
    public List<DocConfig> findEnabledDocs() {
        // Query DOC_CONFIG for enabled docs
        String sql = "SELECT * FROM DOC_CONFIG WHERE ENABLED = 1 AND SERVER_ID = ?";
        List<Long> docIds = jdbcTemplate.query(sql, (rs, rowNum) -> rs.getLong("ID"), serverId);
        List<DocConfig> configs = new ArrayList<>();
        for (Long docId : docIds) {
            findById(docId).ifPresent(configs::add);
        }
        return configs;
    }

    @Override
    public Optional<DocConfig> findByName(String docName) {
        String sql = "SELECT ID FROM DOC_CONFIG WHERE DOC_NAME = ? AND ENABLED = 1 AND SERVER_ID = ?";
        List<Long> ids = jdbcTemplate.query(sql, (rs, rowNum) -> rs.getLong("ID"), docName, serverId);
        if (ids.isEmpty()) return Optional.empty();
        return findById(ids.get(0));
    }

    private Optional<DocConfig> findById(Long docId) {
        // Aggregate all config sections for the given docId
        // 1. DOC_CONFIG
        String sql = "SELECT * FROM DOC_CONFIG WHERE ID = ?";
        DocConfig config = jdbcTemplate.queryForObject(sql, (rs, rowNum) -> mapDocConfig(rs), docId);
        if (config == null) return Optional.empty();
        // 2. DOC_SOURCE_CONFIG
        DocConfig.SourceConfig sourceConfig = jdbcTemplate.queryForObject(
            "SELECT * FROM DOC_SOURCE_CONFIG WHERE DOC_ID = ? ORDER BY VERSION DESC FETCH FIRST 1 ROWS ONLY",
            (rs, rowNum) -> new DocConfig.SourceConfig(
                rs.getString("SOURCE_BASE_DIR"),
                rs.getString("WORK_BASE_DIR"),
                rs.getString("DESTINATION_BASE_DIR"),
                rs.getInt("VERSION")
            ), docId);
        // 3. DOC_SOURCE_RULE
        DocConfig.SourceRule sourceRule = jdbcTemplate.queryForObject(
            "SELECT * FROM DOC_SOURCE_RULE WHERE DOC_ID = ? ORDER BY VERSION DESC FETCH FIRST 1 ROWS ONLY",
            (rs, rowNum) -> new DocConfig.SourceRule(
                rs.getString("SOURCE_BASE_DIR_FOLDERS"),
                rs.getString("INCLUDE_PATTERN"),
                rs.getString("EXCLUDE_PATTERN"),
                rs.getString("METADATA_FILE"),
                rs.getString("PI_METADATA_FILE"),
                "1".equals(rs.getString("NOSCAN_RERUN")),
                "1".equals(rs.getString("FILE_EXIST_UNDER_BASEDIR")),
                rs.getInt("FILE_EXIST_UNDER_BASEDIR_DEPTH"),
                rs.getInt("LAST_MODIFIED_DAYS"),
                "1".equals(rs.getString("LAST_MODIFIED_UNDER_BASE_DIR")),
                rs.getInt("LAST_MODIFIED_UNDER_BASE_DIR_DEPTH"),
                "1".equals(rs.getString("SELECTIVE_SCAN")),
                "1".equals(rs.getString("RETRY_PRIOR_RUN_FAILURES")),
                "1".equals(rs.getString("AWS")),
                rs.getInt("VERSION")
            ), docId);
        // 4. DOC_PREPROCESSING_CONFIG
        DocConfig.PreprocessingConfig preprocessingConfig = jdbcTemplate.queryForObject(
            "SELECT * FROM DOC_PREPROCESSING_CONFIG WHERE DOC_ID = ? ORDER BY VERSION DESC FETCH FIRST 1 ROWS ONLY",
            (rs, rowNum) -> new DocConfig.PreprocessingConfig(
                "1".equals(rs.getString("TAR")),
                rs.getInt("DEPTH"),
                "1".equals(rs.getString("FILE_TAR")),
                "1".equals(rs.getString("UNTAR")),
                "1".equals(rs.getString("COMPRESS_TAR")),
                rs.getInt("VERSION")
            ), docId);
        // 5. DOC_PREPROCESSING_RULE
        DocConfig.PreprocessingRule preprocessingRule = jdbcTemplate.queryForObject(
            "SELECT * FROM DOC_PREPROCESSING_RULE WHERE DOC_ID = ? ORDER BY VERSION DESC FETCH FIRST 1 ROWS ONLY",
            (rs, rowNum) -> new DocConfig.PreprocessingRule(
            	"1".equals(rs.getString("EXTRACT_METADATA")),
            	rs.getString("EXTRACT_METADATA_EXT"),
            	rs.getString("TAR_EXCLUDE_FOLDER"),
                "1".equals(rs.getString("TAR_CONTENTS_FILE")),
                "1".equals(rs.getString("TAR_EXCLUDED_CONTENTS_FILE")),
                rs.getString("TAR_FILE_EXIST"),
                rs.getString("TAR_FILE_EXIST_EXT"),
                "1".equals(rs.getString("TAR_FILENAME_EXCEL_EXIST")),
                "1".equals(rs.getString("TAR_IGNORE_BROKEN_LINK")),
                "1".equals(rs.getString("TAR_SKIP_LEAF_FOLDER")),
                rs.getString("TAR_INCLUDE_PATTERN"),
                "1".equals(rs.getString("PROCESS_MULTIPLE_TARS")),
                rs.getInt("MULTIPLE_TARS_FILES_COUNT"),
                rs.getString("MULTIPLE_TARS_DIR_FOLDERS"),
                rs.getString("MULTIPLE_TARS_DIR_FOLDERS_PREFIX"),
                rs.getString("MULTIPLE_TARS_EXCLUDE_FOLDERS_PREFIX"),
                "1".equals(rs.getString("MULTIPLE_TARS_FILES_VALIDATION")),
                "1".equals(rs.getString("MULTIPLE_TARS_BATCH_FOLDERS")),
                rs.getString("MULTIPLE_TARS_BATCH_FOLDER_DELIMITER"),
                rs.getInt("MULTIPLE_TARS_BATCH_FOLDER_DELIMITER_LEVEL"),
                rs.getInt("VERSION")
            ), docId);
        // 6. DOC_UPLOAD_CONFIG
        DocConfig.UploadConfig upload = jdbcTemplate.queryForObject(
            "SELECT * FROM DOC_UPLOAD_CONFIG WHERE DOC_ID = ? ORDER BY VERSION DESC FETCH FIRST 1 ROWS ONLY",
            (rs, rowNum) -> new DocConfig.UploadConfig(
                "1".equals(rs.getString("VERIFY_PREV_UPLOAD")),
                "1".equals(rs.getString("DRY_RUN")),
                "1".equals(rs.getString("CHECKSUM")),
                "1".equals(rs.getString("CLEANUP_WORKDIR")),
                "1".equals(rs.getString("CHECK_END_WORKFLOW")),
                "1".equals(rs.getString("UPLOAD_MODIFIED_FILES")),
                "1".equals(rs.getString("REPLACE_MODIFIED_FILES")),
                "1".equals(rs.getString("METADATA_UPDATE_ONLY")),
                "1".equals(rs.getString("MOVE_PROCESSED_FILES")),
                "1".equals(rs.getString("FILESYSTEM_UPLOAD")),
                "1".equals(rs.getString("SOFTLINK")),
                "1".equals(rs.getString("COLLECTION_SOFTLINK")),
                rs.getString("SOFTLINK_FILE"),
                rs.getInt("VERSION")
            ), docId);
        // 7. DOC_NOTIFICATION_CONFIG
        DocConfig.NotificationConfig notificationConfig = jdbcTemplate.queryForObject(
            "SELECT * FROM DOC_NOTIFICATION_CONFIG WHERE DOC_ID = ? AND ENABLED = 'Y'",
            (rs, rowNum) -> new DocConfig.NotificationConfig(
                rs.getString("NOTIFY_TYPE"),
                rs.getString("RECIPIENT"),
                "Y".equals(rs.getString("ENABLED")),
                rs.getInt("VERSION")
            ), docId);
        // Compose DocConfig
        return Optional.of(new DocConfig(
            config.getId(),
            config.getDocName(),
            config.getServerId(),
            config.getWorkflowId(),
            config.getDmeServerId(),
            config.getDmeServerUrl(),
            config.getThreads(),
            config.isEnabled(),
            config.getCronExpression(),
            config.getVersion(),
            config.getCreatedAt(),
            config.getUpdatedAt(),
            sourceConfig,
            sourceRule,
            preprocessingConfig,
            preprocessingRule,
            upload,
            notificationConfig
        ));
    }

    private DocConfig mapDocConfig(ResultSet rs) throws SQLException {
        return new DocConfig(
            rs.getLong("ID"),
            rs.getString("DOC_NAME"),
            rs.getString("SERVER_ID"),
            rs.getString("WORKFLOW_ID"),
            rs.getString("DME_SERVER_ID"),
            rs.getString("DME_SERVER_URL"),
            rs.getInt("THREADS"),
            "1".equals(rs.getString("ENABLED")),
            rs.getString("CRON_EXPRESSION"),
            rs.getInt("VERSION"),
            rs.getTimestamp("CREATED_AT").toInstant(),
            rs.getTimestamp("UPDATED_AT").toInstant(),
            null, null, null, null, null, null // Will be filled in by findById
        );
    }
}
