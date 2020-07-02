package gov.nih.nci.hpc.dmesync.exception;

public class DmeSyncMappingException extends Exception {
  
  public DmeSyncMappingException(String s) {
    super(s);
  }

  public DmeSyncMappingException(String message, Throwable cause) {
    super(message, cause);
  }

  public DmeSyncMappingException(Throwable cause) {
    super(cause);
  }

  public DmeSyncMappingException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public DmeSyncMappingException() {}
}
