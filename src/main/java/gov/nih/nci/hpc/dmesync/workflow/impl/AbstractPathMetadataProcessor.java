package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.xml.sax.SAXException;
import gov.nih.nci.hpc.dmesync.domain.CollectionNameMapping;
import gov.nih.nci.hpc.dmesync.domain.MetadataMapping;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.service.DmeSyncWorkflowService;
import gov.nih.nci.hpc.dmesync.util.ExcelUtil;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncPathMetadataProcessor;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntry;
import gov.nih.nci.hpc.domain.metadata.HpcMetadataEntry;

public abstract class AbstractPathMetadataProcessor implements DmeSyncPathMetadataProcessor {

  @Value("${dmesync.destination.base.dir}")
  protected String destinationBaseDir;

  @Autowired DmeSyncWorkflowService dmeSyncWorkflowService;
  
  protected static ThreadLocal<Map<String, Map<String, String>>> threadLocalMap = new ThreadLocal<Map<String, Map<String, String>>>() {
    @Override
    protected HashMap<String, Map<String, String>> initialValue() {
        return new HashMap<>();
    }
  };
  
  final Logger logger = LoggerFactory.getLogger(getClass().getName());

  
  public String getCollectionMappingValue(String key, String collectionType)
      throws DmeSyncMappingException {

    //Retrieve collection name mapping for a given key and collection type
    CollectionNameMapping collectionNameMapping =
        dmeSyncWorkflowService.findCollectionNameMappingByMapKeyAndCollectionType(key, collectionType);

    if (collectionNameMapping == null) {
      String msg =
          "CollectionType "
              + collectionType
              + " with key "
              + key
              + " does not have mapping in collectionNameMapping";
      logger.error(msg);
      throw new DmeSyncMappingException(msg);
    }

    return collectionNameMapping.getMapValue();
  }

  public HpcBulkMetadataEntry populateStoredMetadataEntries(
      HpcBulkMetadataEntry bulkMetadataEntry, String collectionType, String collectionName) {

    //Retrieve custom metadata mapping if present
    List<MetadataMapping> metadataMappings =
        dmeSyncWorkflowService.findAllMetadataMappingByCollectionTypeAndCollectionName(collectionType, collectionName);
    if (metadataMappings != null && !metadataMappings.isEmpty()) {
      for (MetadataMapping mappingEntry : metadataMappings) {
        bulkMetadataEntry
            .getPathMetadataEntries()
            .add(createPathEntry(mappingEntry.getMetaDataKey(), mappingEntry.getMetaDataValue()));
      }
    } else {
      String msg =
          "No metadata entries found for CollectionType "
              + collectionType
              + " and CollectionName "
              + collectionName;
      logger.info(msg);
    }

    return bulkMetadataEntry;
  }

  public HpcMetadataEntry createPathEntry(String key, String value) {
    HpcMetadataEntry pathEntry = new HpcMetadataEntry();
    pathEntry.setAttribute(key);
    pathEntry.setValue(value);

    return pathEntry;
  }
  
  public String getAttrValueWithKey(String rowKey, String attrKey) {
	String key = null;
	if(threadLocalMap.get() == null)
	  return null;
	for (String partialKey : threadLocalMap.get().keySet()) {
		if (StringUtils.contains(rowKey, partialKey)) {
	      key = partialKey;
	      break;
	    }
    }
	if(StringUtils.isEmpty(key)) {
      logger.error("Excel mapping not found for {}", rowKey);
      return null;
    }
    return (threadLocalMap.get().get(key) == null? null : threadLocalMap.get().get(key).get(attrKey));
  }
  
  public Map<String, Map<String, String>> loadMetadataFile(String metadataFile, String key) throws DmeSyncMappingException {
      return ExcelUtil.parseBulkMatadataEntries(metadataFile, key);
  }
  
  public Map<String, Map<String, String>> loadMetadataFile(String metadataFile, String key1, String key2) throws DmeSyncMappingException {
    return ExcelUtil.parseBulkMatadataEntries(metadataFile, key1, key2);
  }

  public List<HpcMetadataEntry> extractMetadataFromFile(File dataObjectFile) throws DmeSyncMappingException {
      Parser parser = new AutoDetectParser();
      Metadata extractedMetadata = new Metadata();

      try (InputStream dataObjectInputStream = new FileInputStream(dataObjectFile)) {
          // Extract metadata from the file.
          parser.parse(dataObjectInputStream, new BodyContentHandler(), extractedMetadata, new ParseContext());

          // Map the Tika extracted metadata to HPC metadata entry list.
          List<HpcMetadataEntry> metadataEntries = new ArrayList<>();
          for (String name : extractedMetadata.names()) {
              HpcMetadataEntry metadataEntry = new HpcMetadataEntry();
              metadataEntry.setAttribute(name);
              metadataEntry.setValue(extractedMetadata.get(name));
              metadataEntries.add(metadataEntry);
          }

          return metadataEntries;

      } catch (IOException | SAXException | TikaException e) {
          throw new DmeSyncMappingException("Failed to extract metadata from file");

      }
  }
}
