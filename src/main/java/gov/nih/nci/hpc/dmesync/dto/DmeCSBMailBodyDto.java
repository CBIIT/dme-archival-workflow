package gov.nih.nci.hpc.dmesync.dto;

import java.io.Serializable;

public class DmeCSBMailBodyDto implements Serializable {

   private String folderName;
   int filesCount;
   int expectedTars;
   int createdTars;
   long uploadedTars;
   long failedTars;
   
  public DmeCSBMailBodyDto() {}





  public String getFolderName() {
	return folderName;
}





public void setFolderName(String folderName) {
	this.folderName = folderName;
}





public int getFilesCount() {
	return filesCount;
}





public void setFilesCount(int length) {
	this.filesCount = length;
}





public int getExpectedTars() {
	return expectedTars;
}





public void setExpectedTars(int expectedTars) {
	this.expectedTars = expectedTars;
}





public int getCreatedTars() {
	return createdTars;
}





public void setCreatedTars(int createdTars) {
	this.createdTars = createdTars;
}





public long getUploadedTars() {
	return uploadedTars;
}





public void setUploadedTars(long uploadedTars) {
	this.uploadedTars = uploadedTars;
}





public long getFailedTars() {
	return failedTars;
}





public void setFailedTars(long failedTars) {
	this.failedTars = failedTars;
}





public DmeCSBMailBodyDto(String folderName, int filesCount, int expectedTars, int createdTars, long uploadedTars,
		long failedTars) {
	super();
	this.folderName = folderName;
	this.filesCount = filesCount;
	this.expectedTars = expectedTars;
	this.createdTars = createdTars;
	this.uploadedTars = uploadedTars;
	this.failedTars = failedTars;
}





@Override
public String toString() {
	return "DmeCSBMailBodyDto [folderName=" + folderName + ", filesCount=" + filesCount + ", expectedTars="
			+ expectedTars + ", createdTars=" + createdTars + ", uploadedTars=" + uploadedTars + ", failedTars="
			+ failedTars + "]";
}
}
