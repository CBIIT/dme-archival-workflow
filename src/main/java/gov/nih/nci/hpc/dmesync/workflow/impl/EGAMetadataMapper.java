package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.springframework.stereotype.Component;

/**
 * Mapping of EGA metadata to EGA study and run.
 *
 * @author <a href="mailto:yuri.dinh@nih.gov">Yuri Dinh</a>
 */
@Component
public class EGAMetadataMapper {
  //---------------------------------------------------------------------//
  // Constants
  //---------------------------------------------------------------------//

  // Study metadata attributes.
  public static final String STUDY_TITLE_ATTRIBUTE = "STUDY_TITLE";
  public static final String STUDY_ABSTRACT_ATTRIBUTE = "STUDY_ABSTRACT";
  public static final String STUDY_DESCRIPTION_ATTRIBUTE = "STUDY_DESCRIPTION";

  // (Run) Experiment metadata attributes.
  public static final String DESIGN_DESCRIPTION_ATTRIBUTE = "DESIGN_DESCRIPTION";
  public static final String LIBRARY_STRATEGY_ATTRIBUTE = "LIBRARY_STRATEGY";
  public static final String LIBRARY_SOURCE_ATTRIBUTE = "LIBRARY_SOURCE";
  public static final String LIBRARY_SELECTION_ATTRIBUTE = "LIBRARY_SELECTION";
  public static final String LIBRARY_CONSTRUCTION_PROTOCOL_ATTRIBUTE = "LIBRARY_CONSTRUCTION_PROTOCOL";
  public static final String INSTRUMENT_MODEL_ATTRIBUTE = "INSTRUMENT_MODEL";
 
  // (Run) Sample metadata attributes.
  public static final String SUBMITTER_ID_ATTRIBUTE = "SUBMITTER_ID";
  public static final String TAXON_ID_ATTRIBUTE = "TAXON_ID";
  public static final String SCIENTIFIC_NAME_ATTRIBUTE = "SCIENTIFIC_NAME";
  public static final String COMMON_NAME_ATTRIBUTE = "COMMON_NAME";

  // (Run) Metadata attributes from the Map file
  public static final String INSTRUMENT_PLATFORM_ATTRIBUTE = "INSTRUMENT_PLATFORM";
  public static final String LIBRARY_LAYOUT_ATTRIBUTE = "LIBRARY_LAYOUT";
  public static final String LIBRARY_NAME_ATTRIBUTE =  "LIBRARY_NAME";
  public static final String SUBMISSION_CENTER_NAME_ATTRIBUTE =  "SUBMISSION CENTER_NAME";
  public static final String RUN_CENTER_NAME_ATTRIBUTE =  "RUN_CENTER_NAME";
  public static final String SAMPLE_ALIAS_ATTRIBUTE =  "SAMPLE_ALIAS";

  //---------------------------------------------------------------------//
  // Instance members
  //---------------------------------------------------------------------//

  // List of metadata attributes.
  private List<String> studyMetadataAttributeNames = new ArrayList<>();
  private List<String> experimentMetadataAttributeNames = new ArrayList<>();
  private List<String> runMetadataAttributeNames = new ArrayList<>();
  private List<String> runMetadataAttributeNamesFromMap = new ArrayList<>();

  //---------------------------------------------------------------------//
  // Constructors
  //---------------------------------------------------------------------//

  /** Constructor for Spring Dependency Injection. */
  private EGAMetadataMapper() {
    List<String> studyAttributes =
        Arrays.asList(
            STUDY_TITLE_ATTRIBUTE,
            STUDY_ABSTRACT_ATTRIBUTE,
            STUDY_DESCRIPTION_ATTRIBUTE);
    
    List<String> experimentAttributes =
        Arrays.asList(
            DESIGN_DESCRIPTION_ATTRIBUTE,
            LIBRARY_STRATEGY_ATTRIBUTE,
            LIBRARY_SOURCE_ATTRIBUTE,
            LIBRARY_SELECTION_ATTRIBUTE,
            LIBRARY_CONSTRUCTION_PROTOCOL_ATTRIBUTE,
            INSTRUMENT_MODEL_ATTRIBUTE);
    
    List<String> runAttributes =
        Arrays.asList(
            SUBMITTER_ID_ATTRIBUTE,
            TAXON_ID_ATTRIBUTE,
            SCIENTIFIC_NAME_ATTRIBUTE,
            COMMON_NAME_ATTRIBUTE);
    
    List<String> runMapAttributes =
        Arrays.asList(
            INSTRUMENT_PLATFORM_ATTRIBUTE,
            LIBRARY_LAYOUT_ATTRIBUTE,
            LIBRARY_NAME_ATTRIBUTE,
            SUBMISSION_CENTER_NAME_ATTRIBUTE,
            RUN_CENTER_NAME_ATTRIBUTE,
            SAMPLE_ALIAS_ATTRIBUTE);

    studyMetadataAttributeNames.addAll(studyAttributes);
    experimentMetadataAttributeNames.addAll(experimentAttributes);
    runMetadataAttributeNames.addAll(runAttributes);
    runMetadataAttributeNamesFromMap.addAll(runMapAttributes);
  }

  //---------------------------------------------------------------------//
  // Methods
  //---------------------------------------------------------------------//

  /**
   * Return the list of study metadata attributes
   *
   * @return The List of metadata attributes for a study.
   */
  public List<String> getStudyMetadataAttributeNames() {
    return studyMetadataAttributeNames;
  }

  /**
   * Return the list of experiment metadata attributes
   *
   * @return The List of metadata attributes for experiment.
   */
  public List<String> getExperimentMetadataAttributeNames() {
    return experimentMetadataAttributeNames;
  }
  
  /**
   * Return the list of run metadata attributes
   *
   * @return The List of metadata attributes for run.
   */
  public List<String> getRunMetadataAttributeNames() {
    return runMetadataAttributeNames;
  }
  
  /**
   * Return the list of run metadata attributes from the mapping file
   *
   * @return The List of metadata attributes for run from the mapping file.
   */
  public List<String> getRunMetadataAttributeNamesFromMap() {
    return runMetadataAttributeNamesFromMap;
  }
}
