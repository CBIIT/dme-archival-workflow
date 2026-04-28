package gov.nih.nci.hpc.dmesync.domain;

import java.time.Instant;

/**
 * Aggregate immutable DOC configuration loaded from Oracle.
 */
public class DocConfig {
    private final Long id;
    private final String docName;
    private final String serverId;
    private final String workflowId;
    private final String dmeServerId;
    private final String dmeServerUrl;
    private final Integer threads;
    private final boolean enabled;
    private final String cronExpression;
    private final int version;
    private final Instant createdAt;
    private final Instant updatedAt;
    private final SourceConfig sourceConfig;
    private final SourceRule sourceRule;
    private final PreprocessingConfig preprocessingConfig;
    private final PreprocessingRule preprocessingRule;
    private final UploadConfig upload;
    private final NotificationConfig notificationConfig;

    public DocConfig(Long id, String docName, String serverId, String workflowId, String dmeServerId, String dmeServerUrl, Integer threads,
                     boolean enabled, String cronExpression, int version, Instant createdAt, Instant updatedAt,
                     SourceConfig sourceConfig, SourceRule sourceRule,
                     PreprocessingConfig preprocessingConfig, PreprocessingRule preprocessingRule,
                     UploadConfig upload, NotificationConfig notificationConfig) {
        this.id = id;
        this.docName = docName;
        this.serverId = serverId;
        this.workflowId = workflowId;
        this.dmeServerId = dmeServerId;
        this.dmeServerUrl = dmeServerUrl;
        this.threads = threads;
        this.enabled = enabled;
        this.cronExpression = cronExpression;
        this.version = version;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.sourceConfig = sourceConfig;
        this.sourceRule = sourceRule;
        this.preprocessingConfig = preprocessingConfig;
        this.preprocessingRule = preprocessingRule;
        this.upload = upload;
        this.notificationConfig = notificationConfig;
    }

    public Long getId() { return id; }
    public String getDocName() { return docName; }
    public String getServerId() { return serverId; }
    public String getWorkflowId() { return workflowId; }
    public String getDmeServerId() { return dmeServerId; }
    public String getDmeServerUrl() { return dmeServerUrl; }
    public Integer getThreads() { return threads; }
    public boolean isEnabled() { return enabled; }
    public String getCronExpression() { return cronExpression; }
    public int getVersion() { return version; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public SourceConfig getSourceConfig() { return sourceConfig; }
    public SourceRule getSourceRule() { return sourceRule; }
    public PreprocessingConfig getPreprocessingConfig() { return preprocessingConfig; }
    public PreprocessingRule getPreprocessingRule() { return preprocessingRule; }
    public UploadConfig getUploadConfig() { return upload; }
    public NotificationConfig getNotificationConfig() { return notificationConfig; }

    // Nested static classes for each config section
    public static class SourceConfig {
        public final String sourceBaseDir;
        public final String workBaseDir;
        public final String destinationBaseDir;
        public final int version;
        public SourceConfig(String sourceBaseDir, String workBaseDir, String destinationBaseDir, int version) {
            this.sourceBaseDir = sourceBaseDir;
            this.workBaseDir = workBaseDir;
            this.destinationBaseDir = destinationBaseDir;
            this.version = version;
        }
    }
    public static class SourceRule {
        public final String sourceBaseDirFolders;
        public final String includePattern;
        public final String excludePattern;
        public final String metadataFile;
        public final String piMetadataFile;
        public final boolean noscanRerun;
        public final boolean fileExistUnderBaseDir;
        public final Integer fileExistUnderBaseDirDepth;
        public final Integer lastModifiedDays;
        public final boolean lastModifiedUnderBaseDir;
        public final Integer lastModifiedUnderBaseDirDepth;
        public final boolean selectiveScan;
        public final boolean retryPriorRunFailures;
        public final boolean aws;
        public final int version;
        public SourceRule(String sourceBaseDirFolders, String includePattern, String excludePattern, String metadataFile, String piMetadataFile,
                          boolean noscanRerun, boolean fileExistUnderBaseDir, Integer fileExistUnderBaseDirDepth, Integer lastModifiedDays,
                          boolean lastModifiedUnderBaseDir, Integer lastModifiedUnderBaseDirDepth, 
                          boolean selectiveScan, boolean retryPriorRunFailures, boolean aws, int version) {
            this.sourceBaseDirFolders = sourceBaseDirFolders;
            this.includePattern = includePattern;
            this.excludePattern = excludePattern;
            this.metadataFile = metadataFile;
            this.piMetadataFile = piMetadataFile;
            this.noscanRerun = noscanRerun;
            this.fileExistUnderBaseDir = fileExistUnderBaseDir;
            this.fileExistUnderBaseDirDepth = fileExistUnderBaseDirDepth;
            this.lastModifiedDays = lastModifiedDays;
            this.lastModifiedUnderBaseDir = lastModifiedUnderBaseDir;
            this.lastModifiedUnderBaseDirDepth = lastModifiedUnderBaseDirDepth;
            this.selectiveScan = selectiveScan;
            this.retryPriorRunFailures = retryPriorRunFailures;
            this.aws = aws;
            this.version = version;
        }
    }
    public static class PreprocessingConfig {
        public final boolean tar;
        public final Integer depth;
        public final boolean fileTar;
        public final boolean untar;
        public final boolean compressTar;
        public final int version;
        public PreprocessingConfig(boolean tar, Integer depth, boolean fileTar, boolean untar, boolean compressTar, int version) {
            this.tar = tar;
            this.depth = depth;
            this.fileTar = fileTar;
            this.untar = untar;
            this.compressTar = compressTar;
            this.version = version;
        }
    }

	public static class PreprocessingRule {
		public final boolean extractMetadata;
		public final String extractMetadataExt;
		public final String tarExcludeFolder;
		public final boolean tarContentsFile;
		public final boolean tarExcludedContentsFile;
		public final String tarFileExist;
		public final String tarFileExistExt;
		public final boolean tarFilenameExcelExist;
		public final boolean tarIgnoreBrokenLink;
		public final boolean tarSkipLeafFolder;
		public final String tarIncludePattern;
		public final boolean processMultipleTars;
		public final Integer multipleTarsFilesCount;
		public final String multipleTarsDirFolders;
		public final String multipleTarsDirFoldersPrefix;
		public final String multipleTarsExcludeFoldersPrefix;
		public final boolean multipleTarsFilesValidation;
		public final boolean multipleTarsBatchFolders;
		public final String multipleTarsBatchFolderDelimiter;
		public final Integer multipleTarsBatchFolderLevel;
		public final int version;

		public PreprocessingRule(boolean extractMetadata, String extractMetadataExt, String tarExcludeFolder,
				boolean tarContentsFile, boolean tarExcludedContentsFile, String tarFileExist, String tarFileExistExt,
				boolean tarFilenameExcelExist, boolean tarIgnoreBrokenLink, boolean tarSkipLeafFolder,
				String tarIncludePattern, boolean processMultipleTars, Integer multipleTarsFilesCount,
				String multipleTarsDirFolders, String multipleTarsDirFoldersPrefix,
				String multipleTarsExcludeFoldersPrefix, boolean multipleTarsFilesValidation,
				boolean multipleTarsBatchFolders, String multipleTarsBatchFolderDelimiter,
				Integer multipleTarsBatchFolderLevel, int version) {
			this.extractMetadata = extractMetadata;
			this.extractMetadataExt = extractMetadataExt;
			this.tarExcludeFolder = tarExcludeFolder;
			this.tarContentsFile = tarContentsFile;
			this.tarExcludedContentsFile = tarExcludedContentsFile;
			this.tarFileExist = tarFileExist;
			this.tarFileExistExt = tarFileExistExt;
			this.tarFilenameExcelExist = tarFilenameExcelExist;
			this.tarIgnoreBrokenLink = tarIgnoreBrokenLink;
			this.tarSkipLeafFolder = tarSkipLeafFolder;
			this.tarIncludePattern = tarIncludePattern;
			this.processMultipleTars = processMultipleTars;
			this.multipleTarsFilesCount = multipleTarsFilesCount;
			this.multipleTarsDirFolders = multipleTarsDirFolders;
			this.multipleTarsDirFoldersPrefix = multipleTarsDirFoldersPrefix;
			this.multipleTarsExcludeFoldersPrefix = multipleTarsExcludeFoldersPrefix;
			this.multipleTarsFilesValidation = multipleTarsFilesValidation;
			this.multipleTarsBatchFolders = multipleTarsBatchFolders;
			this.multipleTarsBatchFolderDelimiter = multipleTarsBatchFolderDelimiter;
			this.multipleTarsBatchFolderLevel = multipleTarsBatchFolderLevel;
			this.version = version;
		}
	}
    public static class UploadConfig {
        public final boolean verifyPrevUpload;
        public final boolean dryRun;
        public final boolean checksum;
        public final boolean cleanupWorkdir;
        public final boolean checkEndWorkflow;
        public final boolean uploadModifiedFiles;
        public final boolean replaceModifiedFiles;
        public final boolean metadataUpdateOnly;
        public final boolean moveProcessedFiles;
        public final boolean fileSystemUpload;
        public final boolean softlink;
        public final boolean collectionSoftlink;
        public final String softlinkFile;
        public final int version;
        public UploadConfig(boolean verifyPrevUpload, boolean dryRun, boolean checksum, boolean cleanupWorkdir, boolean checkEndWorkflow,
                           boolean uploadModifiedFiles, boolean replaceModifiedFiles, boolean metadataUpdateOnly,
                           boolean moveProcessedFiles, boolean fileSystemUpload, boolean softlink, boolean collectionSoftlink,
                           String softlinkFile, int version) {
            this.verifyPrevUpload = verifyPrevUpload;
            this.dryRun = dryRun;
            this.checksum = checksum;
            this.cleanupWorkdir = cleanupWorkdir;
            this.checkEndWorkflow = checkEndWorkflow;
            this.uploadModifiedFiles = uploadModifiedFiles;
            this.replaceModifiedFiles = replaceModifiedFiles;
            this.metadataUpdateOnly = metadataUpdateOnly;
            this.moveProcessedFiles = moveProcessedFiles;
            this.fileSystemUpload = fileSystemUpload;
            this.softlink = softlink;
            this.collectionSoftlink = collectionSoftlink;
            this.softlinkFile = softlinkFile;
            this.version = version;
        }
    }
    public static class NotificationConfig {
        public final String notifyType;
        public final String recipient;
        public final boolean enabled;
        public final int version;
        public NotificationConfig(String notifyType, String recipient, boolean enabled, int version) {
            this.notifyType = notifyType;
            this.recipient = recipient;
            this.enabled = enabled;
            this.version = version;
        }
    }
}