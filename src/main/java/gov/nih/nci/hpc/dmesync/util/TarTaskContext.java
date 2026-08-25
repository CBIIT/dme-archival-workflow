package gov.nih.nci.hpc.dmesync.util;

import java.io.File;
import java.util.List;

/**
 * Thread-local context that carries the resolved file lists produced by
 * {@link gov.nih.nci.hpc.dmesync.workflow.impl.DmeSyncTarTaskImpl} forward to
 * {@link gov.nih.nci.hpc.dmesync.workflow.impl.DmeSyncTarContentsFileTaskImpl}
 * so that the source directory does not have to be walked a second time.
 *
 * <p>Both tasks execute sequentially on the same thread for a given message,
 * so a {@link ThreadLocal} provides safe, zero-overhead isolation between
 * concurrent message-processing threads.
 *
 * <p>Call {@link #clear()} once the context has been consumed to avoid
 * memory leaks in thread-pool environments.
 */
public final class TarTaskContext {

    private static final ThreadLocal<List<File>> includedFiles = new ThreadLocal<>();
    private static final ThreadLocal<List<File>> excludedFiles = new ThreadLocal<>();

    private TarTaskContext() {
    }

    /** Store the list of files that were included in the tar archive. */
    public static void setIncludedFiles(List<File> files) {
        includedFiles.set(files);
    }

    /**
     * Returns the list of files included in the tar archive, or {@code null} if
     * the context has not been populated (e.g. contents task runs standalone).
     */
    public static List<File> getIncludedFiles() {
        return includedFiles.get();
    }

    /** Store the list of files that were excluded from the tar archive. */
    public static void setExcludedFiles(List<File> files) {
        excludedFiles.set(files);
    }

    /**
     * Returns the list of files excluded from the tar archive, or {@code null} if
     * the context has not been populated.
     */
    public static List<File> getExcludedFiles() {
        return excludedFiles.get();
    }

    /** Remove all thread-local values to prevent memory leaks. */
    public static void clear() {
        includedFiles.remove();
        excludedFiles.remove();
    }
}
