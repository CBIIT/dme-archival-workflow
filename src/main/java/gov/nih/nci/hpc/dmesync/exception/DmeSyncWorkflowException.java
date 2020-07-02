package gov.nih.nci.hpc.dmesync.exception;

public class DmeSyncWorkflowException extends Exception {
  
  public DmeSyncWorkflowException(String s) {
    super(s);
  }

  public DmeSyncWorkflowException(String message, Throwable cause) {
    super(message, cause);
  }

  public DmeSyncWorkflowException(Throwable cause) {
    super(cause);
  }

  public DmeSyncWorkflowException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public DmeSyncWorkflowException() {}
}
