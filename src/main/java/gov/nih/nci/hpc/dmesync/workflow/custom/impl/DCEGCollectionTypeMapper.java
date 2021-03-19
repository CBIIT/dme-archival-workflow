package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.springframework.stereotype.Component;

/**
 * Mapping of DCEG CGR collection type for various instrument/platfrom.
 *
 * @author <a href="mailto:yuri.dinh@nih.gov">Yuri Dinh</a>
 */
@Component
public class DCEGCollectionTypeMapper {
  //---------------------------------------------------------------------//
  // Constants
  //---------------------------------------------------------------------//

  // Collection type
  public static final String COLLECTION_TYPE_PLATFORM = "Platform";
  public static final String COLLECTION_TYPE_DEVELOPMENT = "Development";
  public static final String COLLECTION_TYPE_PROJECT = "Project";
  public static final String COLLECTION_TYPE_MANIFEST = "Manifest";
  public static final String COLLECTION_TYPE_RUN = "Run";
  public static final String COLLECTION_TYPE_SAMPLE = "Sample";
  public static final String COLLECTION_TYPE_ANALYSIS = "Analysis";
  public static final String COLLECTION_TYPE_REPORT = "Report";
  public static final String COLLECTION_TYPE_LANE = "Lane";
  public static final String COLLECTION_TYPE_SUBCATEGORY = "Subcategory";

  //---------------------------------------------------------------------//
  // Instance members
  //---------------------------------------------------------------------//

  // List of collection types for each platform.
  private List<String> pacBioCollectionTypeList = new ArrayList<>();
  private List<String> illuminaMiSeqProjectCollectionTypeList = new ArrayList<>();
  private List<String> illuminaMiSeqReportCollectionTypeList = new ArrayList<>();
  private List<String> illuminaHiSeqProjectCollectionTypeList = new ArrayList<>();

  //---------------------------------------------------------------------//
  // Constructors
  //---------------------------------------------------------------------//

  /** Constructor for Spring Dependency Injection. */
  private DCEGCollectionTypeMapper() {
    List<String> pacBioCollectionTypes =
        Arrays.asList(
    		COLLECTION_TYPE_PLATFORM,
    		COLLECTION_TYPE_DEVELOPMENT,
    		COLLECTION_TYPE_PROJECT,
    		COLLECTION_TYPE_RUN,
    		COLLECTION_TYPE_SAMPLE);
    
    List<String> illuminaMiSeqProjectCollectionTypes =
            Arrays.asList(
        		COLLECTION_TYPE_PLATFORM,
        		COLLECTION_TYPE_ANALYSIS,
        		COLLECTION_TYPE_PROJECT,
        		COLLECTION_TYPE_RUN,
        		COLLECTION_TYPE_SAMPLE,
        		COLLECTION_TYPE_LANE,
        		COLLECTION_TYPE_SUBCATEGORY,
        		COLLECTION_TYPE_SAMPLE);
    
    List<String> illuminaMiSeqReportCollectionTypes =
            Arrays.asList(
        		COLLECTION_TYPE_PLATFORM,
        		COLLECTION_TYPE_ANALYSIS,
        		COLLECTION_TYPE_REPORT);
    
    List<String> illuminaHiSeqProjectCollectionTypes =
            Arrays.asList(
        		COLLECTION_TYPE_PLATFORM,
        		COLLECTION_TYPE_ANALYSIS,
        		COLLECTION_TYPE_PROJECT,
        		COLLECTION_TYPE_MANIFEST,
        		COLLECTION_TYPE_RUN,
        		COLLECTION_TYPE_SAMPLE,
        		COLLECTION_TYPE_LANE,
        		COLLECTION_TYPE_SUBCATEGORY,
        		COLLECTION_TYPE_SAMPLE);

    pacBioCollectionTypeList.addAll(pacBioCollectionTypes);
    illuminaMiSeqProjectCollectionTypeList.addAll(illuminaMiSeqProjectCollectionTypes);
    illuminaMiSeqReportCollectionTypeList.addAll(illuminaMiSeqReportCollectionTypes);
    illuminaHiSeqProjectCollectionTypeList.addAll(illuminaHiSeqProjectCollectionTypes);
  }

  //---------------------------------------------------------------------//
  // Methods
  //---------------------------------------------------------------------//

  /**
   * Return the list of collection types for PacBio
   *
   * @return The List of collection types for PacBio
   */
  public List<String> getPacBioCollectonTypes() {
    return pacBioCollectionTypeList;
  }

  /**
   * Return the list of collection types for Illumina Mi-Seq Project
   *
   * @return The List of collection types for Illumina Mi-Seq Project
   */
  public List<String> getIlluminaMiSeqProjectCollectionTypes() {
    return illuminaMiSeqProjectCollectionTypeList;
  }
  
  /**
   * Return the list of collection types for Illumina Mi-Seq Report
   *
   * @return The List of collection types for Illumina Mi-Seq Report
   */
  public List<String> getIlluminaMiSeqReportCollectionTypes() {
    return illuminaMiSeqReportCollectionTypeList;
  }
  
  /**
   * Return the list of collection types for Illumina Hi-Seq Project
   *
   * @return The List of collection types for Illumina Hi-Seq Project
   */
  public List<String> getIlluminaHiSeqProjectCollectionTypes() {
    return illuminaHiSeqProjectCollectionTypeList;
  }
  
}
