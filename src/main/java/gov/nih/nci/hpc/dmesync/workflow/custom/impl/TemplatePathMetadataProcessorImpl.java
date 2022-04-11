package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import javax.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncPathMetadataProcessor;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntries;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;
import gov.nih.nci.hpc.dmesync.util.ExcelUtil;

/**
 * Metadata template DME Path and Meta-data Processor Implementation
 *
 * @author dinhys
 */
@Service("template")
public class TemplatePathMetadataProcessorImpl extends AbstractPathMetadataProcessor
    implements DmeSyncPathMetadataProcessor {

  // Metadata template version 12 of DME path construction and meta data creation

  @Value("${dmesync.additional.metadata.excel:}")
  private String metadataFile;
  
  @Value("${dmesync.doc.name}")
  private String doc;
  
  @Value("${dmesync.source.base.dir}")
  private String sourceDir;
    
  @Override
  public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

    logger.info("[PathMetadataTask] Metadata template getArchivePath called");

    // Example source path -
    // /mnt/lgcp_images/HudsonAlpha_3680/H7C3KCCXX_s1_1_GSLv3-7_28_SL112746.fastq.gz
    String fileName = Paths.get(object.getSourceFileName()).toFile().getName();
    String archivePath = null;
    
    archivePath =
        destinationBaseDir
            + "/PI_"
            + getPiCollectionName(object)
            + "/Project_"
            + getProjectCollectionName(object)
            + "/Sample_"
            + getSampleId(object)
            + "/"
            + fileName;

    // replace spaces with underscore
    archivePath = archivePath.replace(" ", "_");

    logger.info("Archive path for {} : {}", object.getOriginalFilePath(), archivePath);
    
    return archivePath;
  }

  @Override
  public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
      throws DmeSyncMappingException, DmeSyncWorkflowException {

    // Add to HpcBulkMetadataEntries for path attributes
    HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

    // Add path metadata entries for "PI_XXX" collection
    // Example row: collectionType - PI, collectionName - XXX (derived)
    // key = pi_name, value = Adam Sowalsky (supplied)
    // key = pi_lab, value = LGCP, CCR (supplied)

    String piCollectionName = getPiCollectionName(object);
    String piCollectionPath = destinationBaseDir + "/PI_" + piCollectionName;
    HpcBulkMetadataEntry pathEntriesPI = new HpcBulkMetadataEntry();
    pathEntriesPI.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "PI_Lab"));
    pathEntriesPI.setPath(piCollectionPath);
    hpcBulkMetadataEntries
        .getPathsMetadataEntries()
        .add(populateTemplateMetadataEntries(pathEntriesPI, "PI_Lab", "Project"));

    // Add path metadata entries for "Project_XXX" collection
    // Example row: collectionType - Project, collectionName - HudsonAlpha_3680
    // project_id key = Project Identifier
    // project_poc key = Project POC
    // project_poc_email key = POC email
    // project_start_date key = Start Date
    // project_title key = Project Title
    // project_description key = Project Description
    // organism key = Organism
    // is_cell_line key = ?
    // summary_of_samples key = Summary of samples
    // project_type key = Type pf project
    // project_status key = Status
    // project_completed_date key = Completed Date (Optional)
    // origin key = Origin of data
    // deposition key = Deposition (Optional)
    // publications key = Publications (Optional)
    // collaborators key = Collaborators (Optional)
    

    String projectCollectionName = getProjectCollectionName(object);
    String projectCollectionPath = piCollectionPath + "/Project_" + projectCollectionName;
    projectCollectionPath = projectCollectionPath.replace(" ", "_");
    HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
    pathEntriesProject.getPathMetadataEntries().add(createPathEntry(COLLECTION_TYPE_ATTRIBUTE, "Project"));
	pathEntriesProject.setPath(projectCollectionPath);
    hpcBulkMetadataEntries
	    .getPathsMetadataEntries()
	    .add(populateTemplateMetadataEntries(pathEntriesProject, "Project", "Project"));


    // Add path metadata entries for "Sample" collection
    // Example row: collectionType - Sample, collectionName - Sample_<SampleId>
    // sample_id, value = SL112746 (derived)
    // sample_name, value = SL112746 (derived)
    // disease key = Study disease
    // method key = Type pf project
    // sample_type key = ?
    String fileName = Paths.get(object.getOriginalFilePath()).toFile().getName();
    String sampleId = getSampleId(object);
    String sampleCollectionPath = projectCollectionPath + "/Sample_" + sampleId;
    sampleCollectionPath = sampleCollectionPath.replace(" ", "_");
    HpcBulkMetadataEntry pathEntriesSample = new HpcBulkMetadataEntry();
    pathEntriesSample.setPath(sampleCollectionPath);
    hpcBulkMetadataEntries
	    .getPathsMetadataEntries()
	    .add(populateTemplateMetadataEntries(pathEntriesSample, "Sample", sampleId));

    // Set it to dataObjectRegistrationRequestDTO
    HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO =
        new HpcDataObjectRegistrationRequestDTO();
    dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
    dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
    dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(
        hpcBulkMetadataEntries);

    // Add object metadata
    // key = object_name, value = H7G3CCCXX_s4_2_GSLv3-7_27_SL112745.fastq.gz (derived)
    // key = file_type, value = fastq, bam (derived)
    String fileType = StringUtils.substringBefore(fileName, ".gz");
    fileType = fileType.substring(fileType.lastIndexOf('.') + 1);
    dataObjectRegistrationRequestDTO
        .getMetadataEntries()
        .add(createPathEntry("object_name", fileName));
    dataObjectRegistrationRequestDTO
        .getMetadataEntries()
        .add(createPathEntry("file_type", fileType));
    dataObjectRegistrationRequestDTO
        .getMetadataEntries()
        .add(createPathEntry("source_path",  object.getOriginalFilePath()));
    logger.info(
        "Metadata template custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
    return dataObjectRegistrationRequestDTO;
  }

  private String getCollectionNameFromParent(StatusInfo object, String parentName) {
    Path fullFilePath = Paths.get(object.getOriginalFilePath());
    logger.info("Full File Path = {}", fullFilePath);
    int count = fullFilePath.getNameCount();
    for (int i = 0; i <= count; i++) {
      if (fullFilePath.getParent().getFileName().toString().equals(parentName)) {
        return fullFilePath.getFileName().toString();
      }
      fullFilePath = fullFilePath.getParent();
    }
    return null;
  }

  private String getPiCollectionName(StatusInfo object) throws DmeSyncMappingException {
    String piCollectionName = null;
    // Example: If originalFilePath is
    // /mnt/lgcp_images/HudsonAlpha_3680/H7C3KCCXX_s1_1_GSLv3-7_28_SL112746.fastq.gz
    // then return the mapped PI from /mnt/lgcp_images
    piCollectionName = getCollectionMappingValue(sourceDir, "PI_Lab");

    logger.info("PI Collection Name: {}", piCollectionName);
    return piCollectionName;
  }

  private String getProjectCollectionName(StatusInfo object) throws DmeSyncMappingException {
    String projectCollectionName = null;
    // Example: If originalFilePath is
    // /mnt/lgcp_images/HudsonAlpha_3680/H7C3KCCXX_s1_1_GSLv3-7_28_SL112746.fastq.gz
    // then return the mapped Project from HudsonAlpha_3680
    String parentName = Paths.get(sourceDir).getFileName().toString();
    projectCollectionName = getCollectionNameFromParent(object, parentName);

    logger.info("projectCollectionName: {}", projectCollectionName);
    return projectCollectionName;
  }

  private String getSampleId(StatusInfo object) throws DmeSyncMappingException {
	  String path = Paths.get(object.getOriginalFilePath()).toString();
	  String fileName = Paths.get(object.getOriginalFilePath()).getFileName().toString();
	  String sampleId = null;
	// 1) If Sample_xxx folder exists in the path, then use the name
	// 2) If HudsonAlpha_xxx projects, extract Sample name from file name
	// If file name is H7C3KCCXX_s1_1_GSLv3-7_28_SL112746.fastq.gz
	// then the sampleId after the last underscore will be SL112746 (Library ID)
	// 3) Else use project name
	if (path.contains("Sample_")) {
	  sampleId = path.substring(path.indexOf("Sample_") + "Sample_".length(),
			 path.indexOf(File.separator, path.indexOf("Sample_")));
	}
	else if (path.contains("HudsonAlpha_")) {
	  sampleId = StringUtils.substringBefore(StringUtils.substringAfterLast(fileName, "_"),".");
	} else {
	  sampleId = getProjectCollectionName(object);
	}
	return "CL0106_T1";
  }
  
  @PostConstruct
  private void init() throws DmeSyncMappingException {
	if("template".equalsIgnoreCase(doc)) {
		metadataMap = ExcelUtil.parseMetadataTemplateEntries(metadataFile);
	}
  }
}
