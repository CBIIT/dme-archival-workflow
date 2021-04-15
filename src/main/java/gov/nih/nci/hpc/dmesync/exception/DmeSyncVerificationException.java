package gov.nih.nci.hpc.dmesync.exception;

public class DmeSyncVerificationException extends Exception {
  
  public DmeSyncVerificationException(String s) {
    super(s);
  }

  public DmeSyncVerificationException(String message, Throwable cause) {
    super(message, cause);
  }

  public DmeSyncVerificationException(Throwable cause) {
    super(cause);
  }

  public DmeSyncVerificationException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public DmeSyncVerificationException() {}
}
