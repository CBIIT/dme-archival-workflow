package gov.nih.nci.hpc.dmesync.util;

import org.apache.commons.lang3.StringUtils;

public class WorkflowConstants {
	
	public static final String tarContentsFileEndswith ="_TarContentsFile.txt";
	public static final String tarExcludedContentsFileEndswith ="_TarExcludedContentsFile.txt";
	public static final String COMPLETED ="COMPLETED";
	public static final String FAILED ="FAILED";
	public static final String IGNORED ="IGNORED";
	public static final String IGNORED_RUN_SUFFIX="_IGNORED";
	public enum RunStatus {
		  RUNNING, SUCCEEDED, FAILED, SKIPPED, CANCELLED
		}

	public static boolean isCompletedStatus(String status) {
		return COMPLETED.equalsIgnoreCase(StringUtils.trimToEmpty(status));
	}

	public static boolean isFailedStatus(String status) {
		String normalizedStatus = StringUtils.trimToEmpty(status);
		return FAILED.equalsIgnoreCase(normalizedStatus);
	}

	public static boolean isIgnoredStatus(String status) {
		return IGNORED.equalsIgnoreCase(StringUtils.trimToEmpty(status));
	}

	public static boolean isRetryableStatus(String status) {
		return isFailedStatus(status);
	}

	public static String getDisplayStatus(String status) {
		if (isCompletedStatus(status)) {
			return COMPLETED;
		}
		if (isIgnoredStatus(status)) {
			return IGNORED;
		}
		if (isFailedStatus(status)) {
			return FAILED;
		}
		return status;
	}

	public static String toIgnoredRunId(String runId) {
		if (StringUtils.endsWith(runId, IGNORED_RUN_SUFFIX)) {
			return runId;
		}
		return runId + IGNORED_RUN_SUFFIX;
	}
	

}
