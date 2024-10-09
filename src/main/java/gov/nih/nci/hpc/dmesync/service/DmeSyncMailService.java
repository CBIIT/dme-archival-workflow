package gov.nih.nci.hpc.dmesync.service;

/**
 * DME Sync Mail Service Interface
 * 
 * @author dinhys
 */
public interface DmeSyncMailService {
  
  /**
   * Send mail with subject and text
   * 
   * @param subject the mail subject
   * @param text the mail text
   * @return SUCCESS or ERROR
   */
  public String sendMail(String subject, String text);
  
  /**
   * Send the run result spreadsheet
   * 
   * @param runId the runId
   */
  public void sendResult(String runId);
  
  /**
   * Send Error mail with subject and text to NCI HPC_DME_Admin 
   * @param subject the mail subject
   * @param text the mail text
   * @return SUCCESS or ERROR
   */
  
  public String sendErrorMail(String subject, String text);

  
}
