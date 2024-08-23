package gov.nih.nci.hpc.dmesync.exception;

public class DmeSyncWebException extends Exception {

	public DmeSyncWebException(String s) {
		super(s);
	}

	public DmeSyncWebException(String message, Throwable cause) {
		super(message, cause);
	}

	public DmeSyncWebException(Throwable cause) {
		super(cause);
	}

	public DmeSyncWebException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public DmeSyncWebException() {
	}
}
