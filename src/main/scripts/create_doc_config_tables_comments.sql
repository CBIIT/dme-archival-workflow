--------------------------------------------------------------------------------
-- COMMENT for DOC configuration tables
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Table: DOC_CONFIG
--------------------------------------------------------------------------------
COMMENT ON TABLE DOC_CONFIG IS 'Stores the DOC workflow configuration.';

COMMENT ON COLUMN DOC_CONFIG.ID IS 'Primary key for DOC_CONFIG.';
COMMENT ON COLUMN DOC_CONFIG.DOC_NAME IS 'DOC name.';
COMMENT ON COLUMN DOC_CONFIG.SERVER_ID IS 'Server identifier.';
COMMENT ON COLUMN DOC_CONFIG.WORKFLOW_ID IS 'Unique workflow identifier.';
COMMENT ON COLUMN DOC_CONFIG.DME_SERVER_ID IS 'DME server identifier.';
COMMENT ON COLUMN DOC_CONFIG.DME_SERVER_URL IS 'DME server URL.';
COMMENT ON COLUMN DOC_CONFIG.THREADS IS 'Number of processing threads.';
COMMENT ON COLUMN DOC_CONFIG.ENABLED IS 'Indicates whether this DOC configuration is enabled. 1 = enabled, 0 = disabled.';
COMMENT ON COLUMN DOC_CONFIG.CRON_EXPRESSION IS 'Cron expression used to schedule the workflow.';
COMMENT ON COLUMN DOC_CONFIG.VERSION IS 'Configuration version number.';
COMMENT ON COLUMN DOC_CONFIG.CREATED_AT IS 'Timestamp when the record was created.';
COMMENT ON COLUMN DOC_CONFIG.UPDATED_AT IS 'Timestamp when the record was last updated.';
COMMENT ON COLUMN DOC_CONFIG.RUNNING_FLAG IS 'Indicates whether the workflow is currently running. 1 = running, 0 = not running.';

--------------------------------------------------------------------------------
-- Table: DOC_SOURCE_CONFIG
--------------------------------------------------------------------------------
COMMENT ON TABLE DOC_SOURCE_CONFIG IS 'Stores source, work, and destination directory configuration for a DOC workflow.';

COMMENT ON COLUMN DOC_SOURCE_CONFIG.ID IS 'Primary key for DOC_SOURCE_CONFIG.';
COMMENT ON COLUMN DOC_SOURCE_CONFIG.DOC_ID IS 'Foreign key reference to DOC_CONFIG.ID.';
COMMENT ON COLUMN DOC_SOURCE_CONFIG.SOURCE_BASE_DIR IS 'Base source directory where the app will scan for files/folders to upload.';
COMMENT ON COLUMN DOC_SOURCE_CONFIG.WORK_BASE_DIR IS 'Base working directory used during processing of tar/untar and compression.';
COMMENT ON COLUMN DOC_SOURCE_CONFIG.DESTINATION_BASE_DIR IS 'Base collection path in DME.';
COMMENT ON COLUMN DOC_SOURCE_CONFIG.VERSION IS 'Configuration version number.';
COMMENT ON COLUMN DOC_SOURCE_CONFIG.CREATED_AT IS 'Timestamp when the record was created.';
COMMENT ON COLUMN DOC_SOURCE_CONFIG.UPDATED_AT IS 'Timestamp when the record was last updated.';

--------------------------------------------------------------------------------
-- Table: DOC_SOURCE_RULE
--------------------------------------------------------------------------------
COMMENT ON TABLE DOC_SOURCE_RULE IS 'Stores source selection and scanning rules for a DOC workflow.';

COMMENT ON COLUMN DOC_SOURCE_RULE.ID IS 'Primary key for DOC_SOURCE_RULE.';
COMMENT ON COLUMN DOC_SOURCE_RULE.DOC_ID IS 'Foreign key reference to DOC_CONFIG.ID.';
COMMENT ON COLUMN DOC_SOURCE_RULE.SOURCE_BASE_DIR_FOLDERS IS 'Source base directory folders to include or process.';
COMMENT ON COLUMN DOC_SOURCE_RULE.INCLUDE_PATTERN IS 'File or folder include pattern. Comma-separated multiple patterns can be specified.';
COMMENT ON COLUMN DOC_SOURCE_RULE.EXCLUDE_PATTERN IS 'File or folder exclude pattern. If both include and exclude pattern is applicable for a file/folder, the file/folder will be excluded as the exclusion takes precedence.';
COMMENT ON COLUMN DOC_SOURCE_RULE.METADATA_FILE IS 'Metadata file name or path. If specified, application will load the custom metadata excel file supplied by the user.';
COMMENT ON COLUMN DOC_SOURCE_RULE.PI_METADATA_FILE IS 'PI metadata file name or path. If specified, application will load the custom data owner PI metadata excel file supplied by the user.';
COMMENT ON COLUMN DOC_SOURCE_RULE.NOSCAN_RERUN IS 'Indicates whether rerun should occur without scanning. 1 = yes, 0 = no. If true, instead of scanning for files under the base dir, it will reprocess files from the database.';
COMMENT ON COLUMN DOC_SOURCE_RULE.FILE_EXIST_UNDER_BASEDIR IS 'Check if the marker file specified in DOC_PREPROCESSING_RULE.TAR_FILE_EXIST_EXT is directly under the base directory.. 1 = yes, 0 = no.';
COMMENT ON COLUMN DOC_SOURCE_RULE.FILE_EXIST_UNDER_BASEDIR_DEPTH IS 'If specified, it will check for the file under the specified depth from the basedir.';
COMMENT ON COLUMN DOC_SOURCE_RULE.LAST_MODIFIED_DAYS IS 'Number of days used for last-modified filtering. If specified, if modified date of the file/folder is within the number of days specified, it will not be archived.';
COMMENT ON COLUMN DOC_SOURCE_RULE.LAST_MODIFIED_UNDER_BASE_DIR IS 'Indicates whether last-modified filtering under the base directory is enabled. 1 = yes, 0 = no. If specified, checks modified date of the folder in the base directory instead of folder/file';
COMMENT ON COLUMN DOC_SOURCE_RULE.LAST_MODIFIED_UNDER_BASE_DIR_DEPTH IS 'Directory depth for last-modified filtering under the base directory. If specified, it will check for the last modified folder under the specified depth from the basedir.';
COMMENT ON COLUMN DOC_SOURCE_RULE.SELECTIVE_SCAN IS 'Indicates whether selective scan is enabled. 1 = yes, 0 = no.';
COMMENT ON COLUMN DOC_SOURCE_RULE.RETRY_PRIOR_RUN_FAILURES IS 'Indicates whether prior run failures should be retried. 1 = yes, 0 = no. It is mainly used when we are not scanning the entire source path, but only specific months.';
COMMENT ON COLUMN DOC_SOURCE_RULE.AWS IS 'Indicates whether AWS-based source handling is enabled. 1 = yes, 0 = no.';
COMMENT ON COLUMN DOC_SOURCE_RULE.VERSION IS 'Configuration version number.';
COMMENT ON COLUMN DOC_SOURCE_RULE.CREATED_AT IS 'Timestamp when the record was created.';
COMMENT ON COLUMN DOC_SOURCE_RULE.UPDATED_AT IS 'Timestamp when the record was last updated.';

--------------------------------------------------------------------------------
-- Table: DOC_PREPROCESSING_CONFIG
--------------------------------------------------------------------------------
COMMENT ON TABLE DOC_PREPROCESSING_CONFIG IS 'Stores preprocessing configuration for a DOC workflow.';

COMMENT ON COLUMN DOC_PREPROCESSING_CONFIG.ID IS 'Primary key for DOC_PREPROCESSING_CONFIG.';
COMMENT ON COLUMN DOC_PREPROCESSING_CONFIG.DOC_ID IS 'Foreign key reference to DOC_CONFIG.ID.';
COMMENT ON COLUMN DOC_PREPROCESSING_CONFIG.TAR IS 'Indicates whether tar processing is enabled. 1 = yes, 0 = no. If true, it will tar the collection specified by DOC_PREPROCESSING_CONFIG.DEPTH from DOC_SOURCE_CONFIG.SOURCE_BASE_DIR';
COMMENT ON COLUMN DOC_PREPROCESSING_CONFIG.DEPTH IS 'Directory depth used during tar or untar. It is used to determine the folder to tar/untar. If -1 is specified with tar option, it will tar the leaf folder only.';
COMMENT ON COLUMN DOC_PREPROCESSING_CONFIG.FILE_TAR IS 'Indicates whether file-level tar processing is enabled. 1 = yes, 0 = no.';
COMMENT ON COLUMN DOC_PREPROCESSING_CONFIG.UNTAR IS 'Indicates whether untar processing is enabled. 1 = yes, 0 = no. If true, it will untar the collection specified by DOC_PREPROCESSING_CONFIG.DEPTH from DOC_SOURCE_CONFIG.SOURCE_BASE_DIR';
COMMENT ON COLUMN DOC_PREPROCESSING_CONFIG.COMPRESS_TAR IS 'Indicates whether tar compression is enabled. 1 = yes, 0 = no. If true, the data object will be compressed prior to upload.';
COMMENT ON COLUMN DOC_PREPROCESSING_CONFIG.VERSION IS 'Configuration version number.';
COMMENT ON COLUMN DOC_PREPROCESSING_CONFIG.CREATED_AT IS 'Timestamp when the record was created.';
COMMENT ON COLUMN DOC_PREPROCESSING_CONFIG.UPDATED_AT IS 'Timestamp when the record was last updated.';

--------------------------------------------------------------------------------
-- Table: DOC_PREPROCESSING_RULE
--------------------------------------------------------------------------------
COMMENT ON TABLE DOC_PREPROCESSING_RULE IS 'Stores detailed preprocessing rules for a DOC workflow.';

COMMENT ON COLUMN DOC_PREPROCESSING_RULE.ID IS 'Primary key for DOC_PREPROCESSING_RULE.';
COMMENT ON COLUMN DOC_PREPROCESSING_RULE.DOC_ID IS 'Foreign key reference to DOC_CONFIG.ID.';
COMMENT ON COLUMN DOC_PREPROCESSING_RULE.EXTRACT_METADATA IS 'Indicates whether metadata extraction is enabled. 1 = yes, 0 = no.';
COMMENT ON COLUMN DOC_PREPROCESSING_RULE.EXTRACT_METADATA_EXT IS 'File extension used for metadata extraction.';
COMMENT ON COLUMN DOC_PREPROCESSING_RULE.TAR_EXCLUDE_FOLDER IS 'List of comma separated folders to exclude from tar processing. If specified, folders that matches the folder names in a comma separated list will be excluded from the tar ball.';
COMMENT ON COLUMN DOC_PREPROCESSING_RULE.TAR_CONTENTS_FILE IS 'Indicates whether a tar contents file should be generated which includes files in the each tar and archive. 1 = yes, 0 = no.';
COMMENT ON COLUMN DOC_PREPROCESSING_RULE.TAR_EXCLUDED_CONTENTS_FILE IS 'Indicates whether the system will enable creation of a tar contents file to list the files that are included in a tar archive which are broken symlinks.. 1 = yes, 0 = no.';
COMMENT ON COLUMN DOC_PREPROCESSING_RULE.TAR_FILE_EXIST IS 'If specified, it will check whether a file with the specified file name exists before tar operation is performed.';
COMMENT ON COLUMN DOC_PREPROCESSING_RULE.TAR_FILE_EXIST_EXT IS 'If specified, it will check whether a file with the specified file extension exists before tar operation is performed.';
COMMENT ON COLUMN DOC_PREPROCESSING_RULE.TAR_FILENAME_EXCEL_EXIST IS 'Indicates whether to retrieve the tar name from the excel spreadsheet. 1 = yes, 0 = no.';
COMMENT ON COLUMN DOC_PREPROCESSING_RULE.TAR_IGNORE_BROKEN_LINK IS 'Indicates whether broken links should be ignored during tar processing. 1 = yes, 0 = no. If set to false, the tar will not be created and the error from broken links will be recorded in the email report. If it is set to true, these errors will be ignored and the tar will be created. Default will be false.';
COMMENT ON COLUMN DOC_PREPROCESSING_RULE.TAR_SKIP_NON_LEAF_FOLDER IS 'Indicates whether non-leaf folders should be skipped during tar processing. 1 = yes, 0 = no.';
COMMENT ON COLUMN DOC_PREPROCESSING_RULE.TAR_INCLUDE_PATTERN IS 'Include pattern for tar processing.';
COMMENT ON COLUMN DOC_PREPROCESSING_RULE.PROCESS_MULTIPLE_TARS IS 'Indicates whether to create batch tars for the folder with the count specified by MULTIPLE_TARS_FILES_COUNT. 1 = yes, 0 = no.';
COMMENT ON COLUMN DOC_PREPROCESSING_RULE.MULTIPLE_TARS_FILES_COUNT IS 'The number of files to be batched in a single tar when the multiple tar mode it enabled.';
COMMENT ON COLUMN DOC_PREPROCESSING_RULE.MULTIPLE_TARS_DIR_FOLDERS IS 'Directory folders used for multiple tar processing.';
COMMENT ON COLUMN DOC_PREPROCESSING_RULE.MULTIPLE_TARS_DIR_FOLDERS_PREFIX IS 'Folder prefix used for multiple tar directory selection.';
COMMENT ON COLUMN DOC_PREPROCESSING_RULE.MULTIPLE_TARS_EXCLUDE_FOLDERS_PREFIX IS 'Folder prefix used to exclude folders from multiple tar processing.';
COMMENT ON COLUMN DOC_PREPROCESSING_RULE.MULTIPLE_TARS_FILES_VALIDATION IS 'Indicates whether multiple tar file validation is enabled. 1 = yes, 0 = no.';
COMMENT ON COLUMN DOC_PREPROCESSING_RULE.MULTIPLE_TARS_BATCH_FOLDERS IS 'Indicates whether grouped/batched folder mode for multiple tar processing is enabled. 1 = yes, 0 = no. If true, the workflow will create one tar per folder-group derived from the folder naming convention, instead of creating tars based on count';
COMMENT ON COLUMN DOC_PREPROCESSING_RULE.MULTIPLE_TARS_BATCH_FOLDER_DELIMITER IS 'Delimiter used for multiple tar batch folder parsing. Delimiter used to split each immediate child folder name (under the dataset directory) into segments to derive the group key.';
COMMENT ON COLUMN DOC_PREPROCESSING_RULE.MULTIPLE_TARS_BATCH_FOLDER_DELIMITER_LEVEL IS 'Delimiter level used for multiple tar batch folder parsing. Number of leading segments (after splitting by dmesync.multiple.tars.batch.folder.delimiter) to join back together to form the group key.';
COMMENT ON COLUMN DOC_PREPROCESSING_RULE.VERSION IS 'Configuration version number.';
COMMENT ON COLUMN DOC_PREPROCESSING_RULE.CREATED_AT IS 'Timestamp when the record was created.';
COMMENT ON COLUMN DOC_PREPROCESSING_RULE.UPDATED_AT IS 'Timestamp when the record was last updated.';

--------------------------------------------------------------------------------
-- Table: DOC_UPLOAD_CONFIG
--------------------------------------------------------------------------------
COMMENT ON TABLE DOC_UPLOAD_CONFIG IS 'Stores upload behavior and post-processing configuration for a DOC workflow.';

COMMENT ON COLUMN DOC_UPLOAD_CONFIG.ID IS 'Primary key for DOC_UPLOAD_CONFIG.';
COMMENT ON COLUMN DOC_UPLOAD_CONFIG.DOC_ID IS 'Foreign key reference to DOC_CONFIG.ID.';
COMMENT ON COLUMN DOC_UPLOAD_CONFIG.VERIFY_PREV_UPLOAD IS 'Indicates whether previous uploads should be verified. 1 = yes, 0 = no. If true, it will check the local db if it has previously been uploaded, and skip the file.';
COMMENT ON COLUMN DOC_UPLOAD_CONFIG.DRY_RUN IS 'Indicates whether upload should run in dry-run mode. 1 = yes, 0 = no. If true, only records the files to be processed in the local DB without running the workflow.';
COMMENT ON COLUMN DOC_UPLOAD_CONFIG.CHECKSUM IS 'Indicates whether checksum validation is enabled. 1 = yes, 0 = no.';
COMMENT ON COLUMN DOC_UPLOAD_CONFIG.CLEANUP_WORKDIR IS 'Indicates whether the work directory should be cleaned up after processing. 1 = yes, 0 = no.';
COMMENT ON COLUMN DOC_UPLOAD_CONFIG.CHECK_END_WORKFLOW IS 'Indicates whether end-of-workflow checks are enabled. 1 = yes, 0 = no. If true, the system will check for endWorkflow flag to end task processing if no further processing is necessary.';
COMMENT ON COLUMN DOC_UPLOAD_CONFIG.UPLOAD_MODIFIED_FILES IS 'Indicates whether modified files should be uploaded. 1 = yes, 0 = no. If true, the system will compare the new file checksum and file length with archived file and if doesn''t match will append _ver_fileLastModifiedDate to the filename and archive to DME.';
COMMENT ON COLUMN DOC_UPLOAD_CONFIG.REPLACE_MODIFIED_FILES IS 'Indicates whether modified files should replace existing files. 1 = yes, 0 = no. If true, the system will compare the modified date against the last uploaded and reupload if modified.';
COMMENT ON COLUMN DOC_UPLOAD_CONFIG.METADATA_UPDATE_ONLY IS 'Indicates whether only metadata should be updated. 1 = yes, 0 = no.';
COMMENT ON COLUMN DOC_UPLOAD_CONFIG.MOVE_PROCESSED_FILES IS 'Indicates whether processed files should be moved after upload. 1 = yes, 0 = no.';
COMMENT ON COLUMN DOC_UPLOAD_CONFIG.FILESYSTEM_UPLOAD IS 'Indicates whether filesystem upload mode is enabled. 1 = yes, 0 = no.';
COMMENT ON COLUMN DOC_UPLOAD_CONFIG.SOFTLINK IS 'Indicates whether softlink creation is enabled. 1 = yes, 0 = no.';
COMMENT ON COLUMN DOC_UPLOAD_CONFIG.COLLECTION_SOFTLINK IS 'Indicates whether collection-level softlink creation is enabled. 1 = yes, 0 = no.';
COMMENT ON COLUMN DOC_UPLOAD_CONFIG.SOFTLINK_FILE IS 'Softlink file path or file name.';
COMMENT ON COLUMN DOC_UPLOAD_CONFIG.VERSION IS 'Configuration version number.';
COMMENT ON COLUMN DOC_UPLOAD_CONFIG.CREATED_AT IS 'Timestamp when the record was created.';
COMMENT ON COLUMN DOC_UPLOAD_CONFIG.UPDATED_AT IS 'Timestamp when the record was last updated.';

--------------------------------------------------------------------------------
-- Table: DOC_NOTIFICATION_CONFIG
--------------------------------------------------------------------------------
COMMENT ON TABLE DOC_NOTIFICATION_CONFIG IS 'Stores notification settings for a DOC workflow.';

COMMENT ON COLUMN DOC_NOTIFICATION_CONFIG.ID IS 'Primary key for DOC_NOTIFICATION_CONFIG.';
COMMENT ON COLUMN DOC_NOTIFICATION_CONFIG.DOC_ID IS 'Foreign key reference to DOC_CONFIG.ID.';
COMMENT ON COLUMN DOC_NOTIFICATION_CONFIG.RECIPIENTS IS 'Notification recipient list. Once a run completes, the run result will be emailed to this address.';
COMMENT ON COLUMN DOC_NOTIFICATION_CONFIG.SEND_USER IS 'Indicates whether notifications are enabled for HiTIF configured users. 1 = yes, 0 = no.';
COMMENT ON COLUMN DOC_NOTIFICATION_CONFIG.ENABLED IS 'Indicates whether notifications are enabled. 1 = enabled, 0 = disabled.';
COMMENT ON COLUMN DOC_NOTIFICATION_CONFIG.VERSION IS 'Configuration version number.';
COMMENT ON COLUMN DOC_NOTIFICATION_CONFIG.CREATED_AT IS 'Timestamp when the record was created.';
COMMENT ON COLUMN DOC_NOTIFICATION_CONFIG.UPDATED_AT IS 'Timestamp when the record was last updated.';

--------------------------------------------------------------------------------
-- Table: DOC_CONFIG_AUDIT
--------------------------------------------------------------------------------
COMMENT ON TABLE DOC_CONFIG_AUDIT IS 'Stores audit history for DOC configuration changes.';

COMMENT ON COLUMN DOC_CONFIG_AUDIT.ID IS 'Primary key for DOC_CONFIG_AUDIT.';
COMMENT ON COLUMN DOC_CONFIG_AUDIT.DOC_ID IS 'DOC configuration identifier associated with the audited change.';
COMMENT ON COLUMN DOC_CONFIG_AUDIT.CONFIG_TYPE IS 'Configuration table or configuration category that changed.';
COMMENT ON COLUMN DOC_CONFIG_AUDIT.CONFIG_ID IS 'Primary key value of the configuration record that changed.';
COMMENT ON COLUMN DOC_CONFIG_AUDIT.ACTION IS 'Audit action performed, such as INSERT, UPDATE, or DELETE.';
COMMENT ON COLUMN DOC_CONFIG_AUDIT.OLD_VALUE IS 'Serialized old value before the change.';
COMMENT ON COLUMN DOC_CONFIG_AUDIT.NEW_VALUE IS 'Serialized new value after the change.';
COMMENT ON COLUMN DOC_CONFIG_AUDIT.ACTION_BY IS 'Database or application user who performed the change.';
COMMENT ON COLUMN DOC_CONFIG_AUDIT.ACTION_AT IS 'Timestamp when the audited action occurred.';
