package gov.nih.nci.hpc.dmesync.dao;

import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.domain.StatusInfoStats;

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

  /**
   * getTotalSizeTBByDate
   * 
   * @param doc the doc
   * @param days the number of days of interest
   * @return the list of StatusInfoStats objects
   */
  @Query("select new gov.nih.nci.hpc.dmesync.domain.StatusInfoStats(to_char(trunc(s.uploadEndTimestamp),'YYYY-MM-DD'), trunc(sum(s.filesize)/(1000*1000*1000*1000))) "
		  + "from StatusInfo s where s.doc=?1 and s.status='COMPLETED' and s.uploadEndTimestamp >= trunc(sysdate) - ?2 "
		  + "and s.orginalFileName <> 'metadata' group by to_char(trunc(s.uploadEndTimestamp),'YYYY-MM-DD') order by to_char(trunc(s.uploadEndTimestamp),'YYYY-MM-DD')")
  List<StatusInfoStats> getTotalSizeTBByDate(String doc, Long days);
  
  /**
   * getRunCountByDate
   * 
   * @param doc the doc
   * @param days the number of days of interest
   * @return the list of StatusInfoStats objects
   */
  @Query("select new gov.nih.nci.hpc.dmesync.domain.StatusInfoStats(to_char(trunc(s.uploadEndTimestamp),'YYYY-MM-DD'), count(distinct run_id)) "
		  + "from StatusInfo s where s.doc=?1 and s.status='COMPLETED' and s.uploadEndTimestamp >= trunc(sysdate) - ?2 "
		  + "and s.orginalFileName <> 'metadata' group by to_char(trunc(s.uploadEndTimestamp),'YYYY-MM-DD') order by to_char(trunc(s.uploadEndTimestamp),'YYYY-MM-DD')")
 List<StatusInfoStats> getRunCountByDate(String doc, Long days);
}
