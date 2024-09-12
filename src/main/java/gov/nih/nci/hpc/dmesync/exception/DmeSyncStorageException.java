package gov.nih.nci.hpc.dmesync.exception;

public class DmeSyncStorageException extends Exception {
  
  public DmeSyncStorageException(String s) {
    super(s);
  }

  public DmeSyncStorageException(String message, Throwable cause) {
    super(message, cause);
  }

  public DmeSyncStorageException(Throwable cause) {
    super(cause);
  }

  public DmeSyncStorageException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public DmeSyncStorageException() {}
}
