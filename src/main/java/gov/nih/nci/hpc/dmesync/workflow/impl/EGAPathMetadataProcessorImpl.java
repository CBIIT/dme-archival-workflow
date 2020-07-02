package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncPathMetadataProcessor;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntries;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntry;
import gov.nih.nci.hpc.domain.metadata.HpcMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;

/**
 * EGA DME Path and Meta-data Processor Implementation
 *
 * @author dinhys
 */
@Service("ega")
public class EGAPathMetadataProcessorImpl extends AbstractPathMetadataProcessor
    implements DmeSyncPathMetadataProcessor {

  //EGA Custom logic for DME path construction and meta data creation

  @Value("${dmesync.additional.metadata.excel:}")
  private String mappingFile;

  @Autowired private EGAMetadataMapper mapper;

  @Override
  public String getArchivePath(StatusInfo object) throws DmeSyncMappingException {

    logger.info("[PathMetadataTask] EGA getArchivePath called");

    threadLocalMap.set(loadMetadataFile(mappingFile, "FILE_ACCESSION"));


    Path path = Paths.get(object.getSourceFilePath());
    String fileName = path.getFileName().toString();
    String fileId = path.getParent().getFileName().toString();
    String datasetId = path.getParent().getParent().getFileName().toString();
    String runId = getAttrValueWithKey(fileId, "RUN EGA_ID");
    String studyId = getAttrValueWithKey(fileId, "STUDY EGA_ID");

    String archivePath = null;

    if (runId == null) {
      archivePath =
          destinationBaseDir
              + "/Study_"
              + getCollectionMappingValue(datasetId, "Study")
              + "/Dataset_"
              + datasetId
              + "/"
              + fileName;
    } else {
      archivePath =
          destinationBaseDir
              + "/Study_"
              + studyId
              + "/Dataset_"
              + datasetId
              + "/Run_"
              + runId
              + "/"
              + fileName;
    }

    //replace spaces with underscore
    archivePath = archivePath.replaceAll(" ", "_");

    logger.info("Archive path for {} : {}", object.getOriginalFilePath(), archivePath);

    return archivePath;
  }

  @Override
  public HpcDataObjectRegistrationRequestDTO getMetaDataJson(StatusInfo object)
      throws DmeSyncMappingException, DmeSyncWorkflowException {

    HpcDataObjectRegistrationRequestDTO dataObjectRegistrationRequestDTO =
        new HpcDataObjectRegistrationRequestDTO();
    try {
      Path path = Paths.get(object.getSourceFilePath());
      String fileName = path.getFileName().toString();
      String fileId = path.getParent().getFileName().toString();
      String datasetId = path.getParent().getParent().getFileName().toString();
      Path xmlDirPath = Paths.get(path.getParent().getParent() + "/xmls");

      
      //Add to HpcBulkMetadataEntries for path attributes
      HpcBulkMetadataEntries hpcBulkMetadataEntries = new HpcBulkMetadataEntries();

      //Add path metadata entries for "Study_XXX" collection
      //Example row: collectionType - Project, collectionName - EGAS00001003178 (derived),
      //key = project_title, value = "Neoadjuvant immune checkpoint blockade in high-risk resectable melanoma" (STUDY_TITLE)
      //EGA project metadata attributes
      String runId = getAttrValueWithKey(fileId, "RUN EGA_ID");
      String studyId = getAttrValueWithKey(fileId, "STUDY EGA_ID");
      if (studyId == null) {
        studyId = getCollectionMappingValue(datasetId, "Study");
      }
      String projectCollectionName = getCollectionMappingValue(datasetId, "Study");
      String projectCollectionPath = destinationBaseDir + "/Study_" + projectCollectionName;
      HpcBulkMetadataEntry pathEntriesProject = new HpcBulkMetadataEntry();
      pathEntriesProject
          .getPathMetadataEntries()
          .add(createPathEntry("collection_type", "Project"));
      pathEntriesProject.getPathMetadataEntries().add(createPathEntry("access", "Closed Access"));
      if (runId != null) {
        pathEntriesProject.getPathMetadataEntries().add(createPathEntry("STUDY EGA_ID", studyId));
        pathEntriesProject.getPathMetadataEntries().add(createPathEntry("STUDY_TYPE", getAttrValueWithKey(fileId, "STUDY_TYPE")));
        pathEntriesProject
            .getPathMetadataEntries()
            .addAll(
                populateXmlMetadataEntries(
                    xmlDirPath, mapper.getStudyMetadataAttributeNames(), studyId));
      }
      pathEntriesProject.setPath(projectCollectionPath);
      hpcBulkMetadataEntries
          .getPathsMetadataEntries()
          .add(populateStoredMetadataEntries(pathEntriesProject, "Project", projectCollectionName));

      //Add path metadata entries for "Dataset_XXX" collection
      //Example row: collectionType - Dataset, collectionName - EGAD00001004352 (derived),
      //EGA dataset metadata attributes

      String datasetCollectionName = datasetId;
      String datasetCollectionPath = projectCollectionPath + "/Dataset_" + datasetCollectionName;
      HpcBulkMetadataEntry pathEntriesDataset = new HpcBulkMetadataEntry();
      pathEntriesDataset
          .getPathMetadataEntries()
          .add(createPathEntry("collection_type", "Dataset"));
      pathEntriesDataset.getPathMetadataEntries().add(createPathEntry("DATASET EGA_ID", datasetId));
      pathEntriesDataset.setPath(datasetCollectionPath);
      hpcBulkMetadataEntries
          .getPathsMetadataEntries()
          .add(populateStoredMetadataEntries(pathEntriesDataset, "Dataset", datasetCollectionName));

      //Add path metadata entries for "Run_XXX" collection
      //Example row: collectionType - Run, collectionName - EGAF00002167731 (derived)
      //EGA run metadata attributes
      if (runId !=null) {
        String sampleId = getAttrValueWithKey(fileId, "EGA_SAMPLE_ID");
        String experimentId = getAttrValueWithKey(fileId, "EXPERIMENT EGA_ID");
        String runCollectionName = runId;
        String runCollectionPath = datasetCollectionPath + "/Run_" + runCollectionName;
        HpcBulkMetadataEntry pathEntriesRun = new HpcBulkMetadataEntry();
        pathEntriesRun.getPathMetadataEntries().add(createPathEntry("collection_type", "Run"));
        pathEntriesRun.getPathMetadataEntries().add(createPathEntry("EXPERIMENT EGA_ID", experimentId));
        pathEntriesRun.getPathMetadataEntries().add(createPathEntry("RUN EGA_ID", runId));
        pathEntriesRun.getPathMetadataEntries().add(createPathEntry("EGA_SAMPLE_ID", sampleId));
        
        for (String metadataAttrName : mapper.getRunMetadataAttributeNamesFromMap()) {
          if(StringUtils.isNotBlank(getAttrValueWithKey(fileId, metadataAttrName)))
            pathEntriesRun.getPathMetadataEntries().add(createPathEntry(metadataAttrName, getAttrValueWithKey(fileId, metadataAttrName)));
        }
        
        pathEntriesRun.setPath(runCollectionPath);
        pathEntriesRun
            .getPathMetadataEntries()
            .addAll(
                populateXmlMetadataEntries(
                    xmlDirPath, mapper.getRunMetadataAttributeNames(), sampleId));
        pathEntriesRun
            .getPathMetadataEntries()
            .addAll(
                populateXmlMetadataEntries(
                    xmlDirPath, mapper.getExperimentMetadataAttributeNames(), experimentId));
        hpcBulkMetadataEntries.getPathsMetadataEntries().add(pathEntriesRun);
      }

      //Set it to dataObjectRegistrationRequestDTO
      dataObjectRegistrationRequestDTO.setCreateParentCollections(true);
      dataObjectRegistrationRequestDTO.setGenerateUploadRequestURL(true);
      dataObjectRegistrationRequestDTO.setParentCollectionsBulkMetadataEntries(
          hpcBulkMetadataEntries);

      //Add object metadata
      dataObjectRegistrationRequestDTO
          .getMetadataEntries()
          .add(createPathEntry("object_name", fileName));
      dataObjectRegistrationRequestDTO
          .getMetadataEntries()
          .add(createPathEntry("file_type", FilenameUtils.getExtension(fileName)));
      dataObjectRegistrationRequestDTO
          .getMetadataEntries()
          .add(createPathEntry("FILE_ACCESSION", fileId));
      if(runId !=null && StringUtils.isNotBlank(getAttrValueWithKey(fileId, "FILE_NAME")))
        dataObjectRegistrationRequestDTO
        .getMetadataEntries()
        .add(createPathEntry("FILE_NAME", getAttrValueWithKey(fileId, "FILE_NAME")));

    } finally {
      threadLocalMap.remove();
    }
    logger.info(
        "EGA custom DmeSyncPathMetadataProcessor getMetaDataJson for object {}", object.getId());
    return dataObjectRegistrationRequestDTO;
  }

  public List<HpcMetadataEntry> populateXmlMetadataEntries(
      Path xmlDirPath, List<String> attrNames, String egaId) throws DmeSyncMappingException {

    List<HpcMetadataEntry> entries = new ArrayList<HpcMetadataEntry>();

    //Retrieve xml metadata mapping if present
    Optional<Path> xmlFilePath = null;
    try (Stream<Path> xmlFiles = Files.walk(xmlDirPath)) {
      xmlFilePath = xmlFiles.filter(p -> p.getFileName().toString().startsWith(egaId)).findFirst();
    } catch (IOException e) {
      //Ignore since some datasets does not have metadata
    }
    if (xmlFilePath.isPresent()) {
      logger.info("metadata xml file for {} is {}", egaId, xmlFilePath.get().toString());


      String attrTagName = getAttrTagNameFromEgaId(egaId);
      try {

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(xmlFilePath.get().toString());
        
        //Add metedata if specified in list.
        for (String metadataAttrName : attrNames) {
          NodeList nList = document.getElementsByTagName(metadataAttrName);
          Element el = (Element) nList.item(0);
          if (el != null) {
            HpcMetadataEntry entry = new HpcMetadataEntry();
            entry.setAttribute(metadataAttrName);
            entry.setValue(el.getTextContent());
            entries.add(entry);
          }
        }

        if (attrTagName != null) {
          //Get *_ATTRIBUTE tags
          NodeList nList = document.getElementsByTagName(attrTagName);

          if (nList.getLength() > 0 && nList.item(0).hasChildNodes()) {
            for (int i = 0; i < nList.getLength(); i++) {
              Element el = (Element) nList.item(i);
              Element tagEl = (Element) el.getElementsByTagName("TAG").item(0);
              Element valueEl = (Element) el.getElementsByTagName("VALUE").item(0);
              if(tagEl != null && valueEl != null) {
                //Add metadata
                HpcMetadataEntry entry = new HpcMetadataEntry();
                entry.setAttribute(tagEl.getTextContent());
                entry.setValue(valueEl.getTextContent());
                entries.add(entry);
              }
            }
          }
        }

      } catch (SAXException e) {
        throw new DmeSyncMappingException(
            "Unable to parse metadata file, " + xmlFilePath.get().toString(), e);
      } catch (ParserConfigurationException e) {
        throw new DmeSyncMappingException(
            "Unable to parse metadata file, " + xmlFilePath.get().toString(), e);
      } catch (IOException e) {
        throw new DmeSyncMappingException(
            "Unable to parse metadata file, " + xmlFilePath.get().toString(), e);
      }
    }

    return entries;
  }

  private String getAttrTagNameFromEgaId(String egaId) {
    String tagName = null;
    if (StringUtils.startsWith(egaId, "EGAS")) {
      tagName = "STUDY_ATTRIBUTE";
    } else if (StringUtils.startsWith(egaId, "EGAN")) {
      tagName = "SAMPLE_ATTRIBUTE";
    }
    return tagName;
  }
  
}