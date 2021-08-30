package gov.nih.nci.hpc.dmesync.dao;

import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface StatusInfoDao<T extends StatusInfo> extends JpaRepository<T, Long> {

  /**
   * findFirstByOriginalFilePathAndStatus
   * @param originalFilePath the original file path
   * @param status the status
   * @return the StatusInfo object
   */
  StatusInfo findFirstByOriginalFilePathAndStatus(String originalFilePath, String status);

  /**
   * findAllByOriginalFilePathAndStatus
   * 
   * @param originalFilePath the original file path
   * @param status the status
   * @return the list of StatusInfo objects
   */
  @Query("select s from StatusInfo s where s.originalFilePath = ?1 and s.status = ?2")
  List<StatusInfo> findAllByOriginalFilePathAndStatus(String originalFilePath, String status);

  /**
   * findByRunId
   * @param runId the runId
   * @param doc the doc
   * @return the list of StatusInfo objects
   */
  List<StatusInfo> findByRunIdAndDoc(String runId, String doc);

  /**
   * findFirstByOriginalFilePathAndSourceFileNameAndStatus
   * @param originalFilePath the original file path
   * @param sourceFileName the source file name
   * @param status the status
   * @return the StatusInfo object
   */
  StatusInfo findFirstByOriginalFilePathAndSourceFileNameAndStatus(
      String originalFilePath, String sourceFileName, String status);

  /**
   * findTopStatusInfoByDocAndOriginalFilePathStartsWithOrderByStartTimestampDesc
   * @param doc the doc
   * @param baseDir the base directory
   * @return the StatusInfo object
   */
  StatusInfo findTopStatusInfoByDocAndOriginalFilePathStartsWithOrderByStartTimestampDesc(String doc, String baseDir);

  /**
   * findAllByOriginalFilePathAndStatusAndRunId
   * 
   * @param originalFilePath the original file path
   * @param status the status
   * @param runId the runId
   * @return  the list of StatusInfo objects
   */
  List<StatusInfo> findAllByOriginalFilePathAndStatusAndRunId(
      String originalFilePath, String status, String runId);

  /**
   * findFirstStatusInfoByOriginalFilePathOrderByStartTimestampDesc
   * @param originalFilePath the original file path
   * @return the StatusInfo object
   */
  StatusInfo findFirstStatusInfoByOriginalFilePathOrderByStartTimestampDesc(
      String originalFilePath);

}
