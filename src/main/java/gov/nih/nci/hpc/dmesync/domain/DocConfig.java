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
    private final UploadConfig uploadConfig;
    private final NotificationConfig notificationConfig;

    public DocConfig(Long id, String docName, String serverId, String workflowId, String dmeServerId, Integer threads,
                     boolean enabled, String cronExpression, int version, Instant createdAt, Instant updatedAt,
                     SourceConfig sourceConfig, SourceRule sourceRule,
                     PreprocessingConfig preprocessingConfig, PreprocessingRule preprocessingRule,
                     UploadConfig uploadConfig, NotificationConfig notificationConfig) {
        this.id = id;
        this.docName = docName;
        this.serverId = serverId;
        this.workflowId = workflowId;
        this.dmeServerId = dmeServerId;
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
        this.uploadConfig = uploadConfig;
        this.notificationConfig = notificationConfig;
    }

    public Long getId() { return id; }
    public String getDocName() { return docName; }
    public String getServerId() { return serverId; }
    public String getWorkflowId() { return workflowId; }
    public String getDmeServerId() { return dmeServerId; }
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
    public UploadConfig getUploadConfig() { return uploadConfig; }
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
        public final int version;
        public SourceRule(String sourceBaseDirFolders, String includePattern, String excludePattern, String metadataFile, String piMetadataFile,
                          boolean noscanRerun, boolean fileExistUnderBaseDir, Integer fileExistUnderBaseDirDepth, Integer lastModifiedDays,
                          boolean lastModifiedUnderBaseDir, Integer lastModifiedUnderBaseDirDepth, int version) {
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
        public final String tarExcludeFolder;
        public final boolean tarContentsFile;
        public final boolean tarExcludedContentsFile;
        public final boolean tarFileExist;
        public final String tarFileExistExt;
        public final boolean tarFilenameExcelExist;
        public final boolean processMultipleTars;
        public final Integer multipleTarsFilesCount;
        public final boolean tarIgnoreBrokenLink;
        public final int version;
        public PreprocessingRule(String tarExcludeFolder, boolean tarContentsFile, boolean tarExcludedContentsFile, boolean tarFileExist,
                                String tarFileExistExt, boolean tarFilenameExcelExist, boolean processMultipleTars, Integer multipleTarsFilesCount,
                                boolean tarIgnoreBrokenLink, int version) {
            this.tarExcludeFolder = tarExcludeFolder;
            this.tarContentsFile = tarContentsFile;
            this.tarExcludedContentsFile = tarExcludedContentsFile;
            this.tarFileExist = tarFileExist;
            this.tarFileExistExt = tarFileExistExt;
            this.tarFilenameExcelExist = tarFilenameExcelExist;
            this.processMultipleTars = processMultipleTars;
            this.multipleTarsFilesCount = multipleTarsFilesCount;
            this.tarIgnoreBrokenLink = tarIgnoreBrokenLink;
            this.version = version;
        }
    }
    public static class UploadConfig {
        public final boolean verifyPrevUpload;
        public final boolean dryRun;
        public final boolean cleanupWorkdir;
        public final boolean checkEndWorkflow;
        public final boolean uploadModifiedFiles;
        public final boolean replaceModifiedFiles;
        public final int version;
        public UploadConfig(boolean verifyPrevUpload, boolean dryRun, boolean cleanupWorkdir, boolean checkEndWorkflow,
                           boolean uploadModifiedFiles, boolean replaceModifiedFiles, int version) {
            this.verifyPrevUpload = verifyPrevUpload;
            this.dryRun = dryRun;
            this.cleanupWorkdir = cleanupWorkdir;
            this.checkEndWorkflow = checkEndWorkflow;
            this.uploadModifiedFiles = uploadModifiedFiles;
            this.replaceModifiedFiles = replaceModifiedFiles;
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