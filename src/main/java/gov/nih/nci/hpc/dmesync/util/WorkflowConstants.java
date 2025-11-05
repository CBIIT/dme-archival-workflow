package gov.nih.nci.hpc.dmesync.util;

public class WorkflowConstants {
	
	public static final String tarContentsFileEndswith ="_TarContentsFile.txt";
	public static final String tarExcludedContentsFileEndswith ="_TarExcludedContentsFile.txt";
	public static final String COMPLETED ="COMPLETED";
	
	public enum RunStatus {
		  RUNNING, SUCCEEDED, FAILED, SKIPPED, CANCELLED
		}

}
