package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.springframework.stereotype.Component;

/**
 * Mapping of dbGap metadata to dbGap project and run.
 *
 * @author <a href="mailto:yuri.dinh@nih.gov">Yuri Dinh</a>
 */
@Component
public class DbGapMetadataMapper {
  //---------------------------------------------------------------------//
  // Constants
  //---------------------------------------------------------------------//

  // Project metadata attributes.
  public static final String BIO_PROJECT_ATTRIBUTE = "BioProject";
  public static final String BIOSPECIMEN_REPO_ATTRIBUTE = "biospecimen_repository";
  public static final String GAP_ACCESSION_ATTRIBUTE = "gap_accession";
  public static final String ORGANISM_ATTRIBUTE = "Organism";
  public static final String SRA_STUDY_ATTRIBUTE = "SRA Study";
  public static final String STUDY_DESIGN_ATTRIBUTE = "study_design";
  public static final String STUDY_NAME_ATTRIBUTE = "study_name";

  // Dataset metadata attributes.
  public static final String CONSENT_CODE_ATTRIBUTE = "Consent_Code";
  public static final String CONSENT_ATTRIBUTE = "Consent";
 
  // Run metadata attributes.
  public static final String RUN_ATTRIBUTE = "Run";
  public static final String ANALYTE_TYPE_ATTRIBUTE = "analyte_type";
  public static final String ASSAY_TYPE_ATTRIBUTE = "Assay Type";
  public static final String ASSEMBLY_NAME_ATTRIBUTE = "AssemblyName";
  public static final String AVGSPOTLEN_ATTRIBUTE = "AvgSpotLen";
  public static final String BASES_ATTRIBUTE = "Bases";
  public static final String BI_GSSR_SAMPLE_ID_EXP_ATTRIBUTE = "BI_GSSR_sample_ID (exp)";
  public static final String BI_GSSR_SAMPLE_ID_RUN_ATTRIBUTE = "BI_GSSR_Sample_ID (run)";
  public static final String BI_GSSR_SAMPLE_LSID_EXP_ATTRIBUTE = "BI_GSSR_sample_LSID (exp)";
  public static final String BI_GSSR_SAMPLE_LSID_RUN_ATTRIBUTE = "BI_GSSR_sample_LSID (run)";
  public static final String BI_PROJECT_NAME_EXP_ATTRIBUTE = "BI_project_name (exp)";
  public static final String BI_PROJECT_NAME_RUN_ATTRIBUTE = "BI_project_name (run)";
  public static final String BI_RUN_BARCODE_RUN_ATTRIBUTE = "BI_Run_Barcode (run)";
  public static final String BI_RUN_NAME_RUN_ATTRIBUTE = "BI_run_name (run)";
  public static final String BI_WORK_REQUEST_ID_EXP_ATTRIBUTE = "BI_work_request_ID (exp)";
  public static final String BI_WORK_REQUEST_ID_RUN_ATTRIBUTE = "BI_work_request_ID (run)";
  public static final String BIOSAMPLE_ATTRIBUTE = "BioSample";
  public static final String BIOSPECIMEN_REPO_SAMPLE_ID_ATTRIBUTE =
      "biospecimen_repository_sample_id";
  public static final String BODY_SITE_ATTRIBUTE = "body_site";
  public static final String BYTES_ATTRIBUTE = "Bytes";
  public static final String CENTER_NAME_ATTRIBUTE = "Center Name"; 
  public static final String DATASTORE_FILETYPE_ATTRIBUTE = "DATASTORE filetype";
  public static final String DATASTORE_PROVIDER_ATTRIBUTE = "DATASTORE provider";
  public static final String DATASTORE_REGION_ATTRIBUTE = "DATASTORE region";
  public static final String EXPERIMENT_ATTRIBUTE = "Experiment";
  public static final String INSTRUMENT_ATTRIBUTE = "Instrument";
  public static final String IS_TUMOR_ATTRIBUTE = "is_tumor";
  public static final String LIBRARY_NAME_ATTRIBUTE = "Library Name";
  public static final String LIBRARY_LAYOUT_ATTRIBUTE = "LibraryLayout";
  public static final String LIBRARY_SELECTION_ATTRIBUTE = "LibrarySelection";
  public static final String LIBRARY_SOURCE_ATTRIBUTE = "LibrarySource";
  public static final String PLATFORM_ATTRIBUTE = "Platform";
  public static final String RELEASE_DATE_ATTRIBUTE = "ReleaseDate";
  public static final String SAMPLE_NAME_ATTRIBUTE = "Sample Name";
  public static final String SEX_ATTRIBUTE = "sex";
  public static final String X_ATTRIBUTE = "X";
  public static final String STUDY_DISEASE_ATTRIBUTE = "study_disease";
  public static final String SUBJECT_IS_AFFECTED_ATTRIBUTE = "subject_is_affected";
  public static final String BI_MOLECULAR_BARCODE_RUN_ATTRIBUTE = "BI_molecular_barcode (run)";
  public static final String BI_TARGET_SET_EXP_ATTRIBUTE = "BI_target_set (exp)";
  public static final String HISTOLOGICAL_TYPE_ATTRIBUTE = "histological_type";
  public static final String SUBMITTED_SUBJECT_ID_ATTRIBUTE = "submitted_subject_id";
  public static final String FLOWCELL_BARCODE_RUN_ATTRIBUTE = "flowcell_barcode (run)";
  public static final String INSTRUMENT_NAME_RUN_ATTRIBUTE = "instrument_name (run)";
  public static final String LDID_EXP_ATTRIBUTE = "lsid (exp)";
  public static final String LSID_RUN_ATTRIBUTE = "lsid (run)";
  public static final String MATERIAL_TYPE_ATTRIBUTE = "material_type (exp)";
  public static final String RUN_BARCODE_ATTRIBUTE = "run_barcode (run)";
  public static final String ANALYSIS_TYPE_EXP_ATTRIBUTE = "analysis_type (exp)";
  public static final String ANALYSIS_TYPE_RUN_ATTRIBUTE = "analysis_type (run)";
  public static final String DATA_TYPE_RUN_ATTRIBUTE = "data_type (run)";
  public static final String LIBRARY_TYPE_EXP_ATTRIBUTE = "library_type (exp)";
  public static final String LIBRARY_TYPE_RUN_ATTRIBUTE = "library_type (run)";
  public static final String RESEARCH_PROJECT_EXP_ATTRIBUTE = "research_project (exp)";
  public static final String RESEARCH_PROJECT_RUN_ATTRIBUTE = "research_project (run)";
  public static final String RUN_NAME_RUN_ATTRIBUTE = "run_name (run)";
  public static final String READ_GROUP_ID_RUN_ATTRIBUTE = "read_group_id (run)";
  public static final String TARGET_SET_EXP_ATTRIBUTE = "target_set (exp)";
  public static final String AGGREGATION_PROJECT_EXP_ATTRIBUTE = "aggregation_project (exp)";
  public static final String AGGREGATION_PROJECT_RUN_ATTRIBUTE = "aggregation_project (run)";
  public static final String LIBRARY_EXP_ATTRIBUTE = "library (exp)";
  public static final String LIBRARY_RUN_ATTRIBUTE = "library (run)";
  public static final String MOLECULAR_IDX_SCHEME_RUN_ATTRIBUTE = "molecular_idx_scheme (run)";
  public static final String WORK_REQUEST_OR_PDO_EXP_ATTRIBUTE = "work_request_or_pdo (exp)";
  public static final String WORK_REQUEST_OR_PDO_RUN_ATTRIBUTE = "work_request_or_pdo (run)";
  public static final String GSSR_ID_EXP_ATTRIBUTE = "gssr_id (exp)";
  public static final String GSSR_ID_RUN_ATTRIBUTE = "gssr_id (run)";
  public static final String PROJECT_EXP_ATTRIBUTE = "project (exp)";
  public static final String PROJECT_RUN_ATTRIBUTE = "project (run)";
  public static final String BAIT_SET_RUN_ATTRIBUTE = "bait_set (run)";
  public static final String LANE_RUN_ATTRIBUTE = "lane (run)";
  public static final String PRIMARY_DISEASE_EXP_ATTRIBUTE = "primary_disease (exp)";
  public static final String ROOT_SAMPLE_ID_EXP_ATTRIBUTE = "root_sample_id (exp)";
  public static final String ROOT_SAMPLE_ID_RUN_ATTRIBUTE = "root_sample_id (run)";
  public static final String SAMPLE_ID_EXP_ATTRIBUTE = "sample_id (exp)";
  public static final String SAMPLE_ID_RUN_ATTRIBUTE = "Sample_ID (run)";
  public static final String SAMPLE_TYPE_EXP_ATTRIBUTE = "sample_type (exp)";
  public static final String WORK_REQUEST_EXP_ATTRIBUTE = "work_request (exp)";
  public static final String WORK_REQUEST_RUN_ATTRIBUTE = "work_request (run)";
  public static final String DATA_TYPE_EXP_ATTRIBUTE = "data_type (exp)";
  public static final String PRODUCT_ORDER_EXP_ATTRIBUTE = "product_order (exp)";
  public static final String PRODUCT_ORDER_RUN_ATTRIBUTE = "product_order (run)";
  public static final String PRODUCT_PART_NUM_EXP_ATTRIBUTE = "product_part_number (exp)";
  public static final String PRODUCT_PART_NUM_RUN_ATTRIBUTE = "product_part_number (run)";
  public static final String MOLECULAR_DATA_TYPE_ATTRIBUTE = "molecular_data_type";
  public static final String RG_PLATFORM_UNIT_RUN_ATTRIBUTE = "rg_platform_unit (run)";
  public static final String RG_PLATFORM_UNIT_LIB_RUN_ATTRIBUTE = "rg_platform_unit_lib (run)";
  public static final String SECONDARY_ACCESSIONS_EXP_ATTRIBUTE = "secondary_accessions (exp)";
  public static final String ALIGNMENT_SOFTWARE_EXP_ATTRIBUTE = "alignment_software (exp)";

  //---------------------------------------------------------------------//
  // Instance members
  //---------------------------------------------------------------------//

  // List of metadata attributes.
  private List<String> projectMetadataAttributeNames = new ArrayList<>();
  private List<String> datasetMetadataAttributeNames = new ArrayList<>();
  private List<String> runMetadataAttributeNames = new ArrayList<>();

  //---------------------------------------------------------------------//
  // Constructors
  //---------------------------------------------------------------------//

  /** Constructor for Spring Dependency Injection. */
  private DbGapMetadataMapper() {
    List<String> attributes =
        Arrays.asList(
            BIO_PROJECT_ATTRIBUTE,
            BIOSPECIMEN_REPO_ATTRIBUTE,
            GAP_ACCESSION_ATTRIBUTE,
            ORGANISM_ATTRIBUTE,
            SRA_STUDY_ATTRIBUTE,
            STUDY_DESIGN_ATTRIBUTE,
            STUDY_NAME_ATTRIBUTE);
    List<String> datasetAttributes =
        Arrays.asList(
            CONSENT_CODE_ATTRIBUTE,
            CONSENT_ATTRIBUTE);
    List<String> runAttributes =
        Arrays.asList(
            ANALYTE_TYPE_ATTRIBUTE,
            ASSAY_TYPE_ATTRIBUTE,
            ASSEMBLY_NAME_ATTRIBUTE,
            AVGSPOTLEN_ATTRIBUTE,
            BASES_ATTRIBUTE,
            BI_GSSR_SAMPLE_ID_EXP_ATTRIBUTE,
            BI_GSSR_SAMPLE_ID_RUN_ATTRIBUTE,
            BI_GSSR_SAMPLE_LSID_EXP_ATTRIBUTE,
            BI_GSSR_SAMPLE_LSID_RUN_ATTRIBUTE,
            BI_PROJECT_NAME_EXP_ATTRIBUTE,
            BI_PROJECT_NAME_RUN_ATTRIBUTE,
            BI_RUN_BARCODE_RUN_ATTRIBUTE,
            BI_RUN_NAME_RUN_ATTRIBUTE,
            BI_WORK_REQUEST_ID_EXP_ATTRIBUTE,
            BI_WORK_REQUEST_ID_RUN_ATTRIBUTE,
            BIOSAMPLE_ATTRIBUTE,
            BIOSPECIMEN_REPO_SAMPLE_ID_ATTRIBUTE,
            BODY_SITE_ATTRIBUTE,
            BYTES_ATTRIBUTE,
            CENTER_NAME_ATTRIBUTE,
            DATASTORE_FILETYPE_ATTRIBUTE,
            DATASTORE_PROVIDER_ATTRIBUTE,
            DATASTORE_REGION_ATTRIBUTE,
            EXPERIMENT_ATTRIBUTE,
            INSTRUMENT_ATTRIBUTE,
            IS_TUMOR_ATTRIBUTE,
            LIBRARY_NAME_ATTRIBUTE,
            LIBRARY_LAYOUT_ATTRIBUTE,
            LIBRARY_SELECTION_ATTRIBUTE,
            LIBRARY_SOURCE_ATTRIBUTE,
            PLATFORM_ATTRIBUTE,
            RELEASE_DATE_ATTRIBUTE,
            SAMPLE_NAME_ATTRIBUTE,
            SEX_ATTRIBUTE,
            X_ATTRIBUTE,
            STUDY_DISEASE_ATTRIBUTE,
            SUBJECT_IS_AFFECTED_ATTRIBUTE,
            BI_MOLECULAR_BARCODE_RUN_ATTRIBUTE,
            BI_TARGET_SET_EXP_ATTRIBUTE,
            HISTOLOGICAL_TYPE_ATTRIBUTE,
            SUBMITTED_SUBJECT_ID_ATTRIBUTE,
            FLOWCELL_BARCODE_RUN_ATTRIBUTE,
            INSTRUMENT_NAME_RUN_ATTRIBUTE,
            LDID_EXP_ATTRIBUTE,
            LSID_RUN_ATTRIBUTE,
            MATERIAL_TYPE_ATTRIBUTE,
            RUN_BARCODE_ATTRIBUTE,
            ANALYSIS_TYPE_EXP_ATTRIBUTE,
            ANALYSIS_TYPE_RUN_ATTRIBUTE,
            DATA_TYPE_RUN_ATTRIBUTE,
            LIBRARY_TYPE_EXP_ATTRIBUTE,
            LIBRARY_TYPE_RUN_ATTRIBUTE,
            RESEARCH_PROJECT_EXP_ATTRIBUTE,
            RESEARCH_PROJECT_RUN_ATTRIBUTE,
            RUN_NAME_RUN_ATTRIBUTE,
            READ_GROUP_ID_RUN_ATTRIBUTE,
            TARGET_SET_EXP_ATTRIBUTE,
            AGGREGATION_PROJECT_EXP_ATTRIBUTE,
            AGGREGATION_PROJECT_RUN_ATTRIBUTE,
            LIBRARY_EXP_ATTRIBUTE,
            LIBRARY_RUN_ATTRIBUTE,
            MOLECULAR_IDX_SCHEME_RUN_ATTRIBUTE,
            WORK_REQUEST_OR_PDO_EXP_ATTRIBUTE,
            WORK_REQUEST_OR_PDO_RUN_ATTRIBUTE,
            GSSR_ID_EXP_ATTRIBUTE,
            GSSR_ID_RUN_ATTRIBUTE,
            PROJECT_EXP_ATTRIBUTE,
            PROJECT_RUN_ATTRIBUTE,
            BAIT_SET_RUN_ATTRIBUTE,
            LANE_RUN_ATTRIBUTE,
            PRIMARY_DISEASE_EXP_ATTRIBUTE,
            ROOT_SAMPLE_ID_EXP_ATTRIBUTE,
            ROOT_SAMPLE_ID_RUN_ATTRIBUTE,
            SAMPLE_ID_EXP_ATTRIBUTE,
            SAMPLE_ID_RUN_ATTRIBUTE,
            SAMPLE_TYPE_EXP_ATTRIBUTE,
            WORK_REQUEST_EXP_ATTRIBUTE,
            WORK_REQUEST_RUN_ATTRIBUTE,
            DATA_TYPE_EXP_ATTRIBUTE,
            PRODUCT_ORDER_EXP_ATTRIBUTE,
            PRODUCT_ORDER_RUN_ATTRIBUTE,
            PRODUCT_PART_NUM_EXP_ATTRIBUTE,
            PRODUCT_PART_NUM_RUN_ATTRIBUTE,
            MOLECULAR_DATA_TYPE_ATTRIBUTE,
            RG_PLATFORM_UNIT_RUN_ATTRIBUTE,
            RG_PLATFORM_UNIT_LIB_RUN_ATTRIBUTE,
            SECONDARY_ACCESSIONS_EXP_ATTRIBUTE,
            ALIGNMENT_SOFTWARE_EXP_ATTRIBUTE);

    projectMetadataAttributeNames.addAll(attributes);
    datasetMetadataAttributeNames.addAll(datasetAttributes);
    runMetadataAttributeNames.addAll(runAttributes);
  }

  //---------------------------------------------------------------------//
  // Methods
  //---------------------------------------------------------------------//

  /**
   * Return the list of project metadata attributes
   *
   * @return The List of metadata attributes for a project.
   */
  public List<String> getProjectMetadataAttributeNames() {
    return projectMetadataAttributeNames;
  }
  
  /**
   * Return the list of dataset metadata attributes
   *
   * @return The List of metadata attributes for a Dataset.
   */
  public List<String> getDatasetMetadataAttributeNames() {
    return datasetMetadataAttributeNames;
  }

  /**
   * Return the list of run metadata attributes
   *
   * @return The List of metadata attributes for run.
   */
  public List<String> getRunMetadataAttributeNames() {
    return runMetadataAttributeNames;
  }
}
