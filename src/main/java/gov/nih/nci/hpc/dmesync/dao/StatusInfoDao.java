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
  StatusInfo findFirstByOriginalFilePathAndStatusOrderByStartTimestampDesc(String originalFilePath, String status);

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
   * findAllLikeOriginalFilePath
   * 
   * @param originalFilePath the original file path
   * @return the list of StatusInfo objects
   */
  @Query("select s from StatusInfo s where s.originalFilePath like ?1 and s.status is null")
  List<StatusInfo> findAllLikeOriginalFilePath(String originalFilePath);
  
  /**
   * findAllLikeOriginalFilePath
   * 
   * @param originalFilePath the original file path
   * @param doc the docName
   * @return the list of StatusInfo objects
   */
  @Query("select s from StatusInfo s where s.originalFilePath like ?2 and s.doc =?1")
  List<StatusInfo> findAllByDocAndLikeOriginalFilePath(String Doc,String originalFilePath);

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
   * findTopStatusInfoByDocAndOriginalFilePathStartsWithOrderByStartTimestampDesc
   * @param doc the doc
   * @param baseDir the base directory
   * @return the StatusInfo object
   */
  StatusInfo findTopStatusInfoByDocAndOriginalFilePathStartsWithAndTarEndTimestampNull(String doc, String baseDir);

  /**
   * findTopStatusInfoByDocOrderByStartTimestampDesc
   * @param doc the doc
   * @return the StatusInfo object
   */
  StatusInfo findTopStatusInfoByDocOrderByStartTimestampDesc(String doc);

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
   * findAllByOriginalFilePathAndStatusAndRunId
   * 
   * @param originalFilePath the original file path
   * @param status the status
   * @param runId the runId
   * @return  the list of StatusInfo objects
   */
  StatusInfo findTopBySourceFilePathAndRunId(
      String originalFilePath, String runId);

  /**
   * findFirstStatusInfoByOriginalFilePathOrderByStartTimestampDesc
   * @param originalFilePath the original file path
   * @return the StatusInfo object
   */
  StatusInfo findFirstStatusInfoByOriginalFilePathOrderByStartTimestampDesc(
      String originalFilePath);

  /**
   * findStatusInfoByDocAndStatus
   * 
   * @param doc the doc
   * @param status the status
   * @return  the list of StatusInfo objects
   */
  List<StatusInfo> findStatusInfoByDocAndStatus(String doc, String status);

}
