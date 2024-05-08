package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import gov.nih.nci.hpc.dmesync.DmeSyncWorkflowServiceFactory;
import gov.nih.nci.hpc.dmesync.domain.CollectionNameMapping;
import gov.nih.nci.hpc.dmesync.domain.MetadataMapping;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.util.CsvFileUtil;
import gov.nih.nci.hpc.dmesync.util.ExcelUtil;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncPathMetadataProcessor;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntry;
import gov.nih.nci.hpc.domain.metadata.HpcMetadataEntry;

public abstract class AbstractPathMetadataProcessor implements DmeSyncPathMetadataProcessor {

  protected static final String COLLECTION_TYPE_ATTRIBUTE = "collection_type";
	
  @Value("${dmesync.db.access:local}")
  private String access;
  
  @Value("${dmesync.doc.name}")
  private String doc;
  
  @Value("${dmesync.destination.base.dir}")
  protected String destinationBaseDir;

  @Autowired DmeSyncWorkflowServiceFactory dmeSyncWorkflowService;
  
  Map<String, Map<String, String>> metadataMap = null;

  protected static ThreadLocal<Map<String, Map<String, String>>> threadLocalMap = new ThreadLocal<Map<String, Map<String, String>>>() {
    @Override
    protected HashMap<String, Map<String, String>> initialValue() {
        return new HashMap<>();
    }
  };
  
  final Logger logger = LoggerFactory.getLogger(getClass().getName());

  
  public String getCollectionMappingValue(String key, String collectionType, String doc)
      throws DmeSyncMappingException {

    //Retrieve collection name mapping for a given key and collection type
    CollectionNameMapping collectionNameMapping =
        dmeSyncWorkflowService.getService(access).findCollectionNameMappingByMapKeyAndCollectionTypeAndDoc(key, collectionType, doc);

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
      HpcBulkMetadataEntry bulkMetadataEntry, String collectionType, String collectionName, String doc) {

    //Retrieve custom metadata mapping if present
    List<MetadataMapping> metadataMappings =
        dmeSyncWorkflowService.getService(access).findAllMetadataMappingByCollectionTypeAndCollectionNameAndDoc(collectionType, collectionName, doc);
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
  
  public HpcBulkMetadataEntry populateTemplateMetadataEntries(
	      HpcBulkMetadataEntry bulkMetadataEntry, String collectionType, String collectionName) {

    //Retrieve and populate required metadata
	Map<String, String> mandatoryEntryMap = metadataMap.get(collectionType + "_Required");
	for(Map.Entry<String, String> entry : mandatoryEntryMap.entrySet()) {
		if(metadataMap.get(collectionName) != null && metadataMap.get(collectionName).get(entry.getKey()) != null)
			bulkMetadataEntry
	        .getPathMetadataEntries()
	        .add(createPathEntry(entry.getValue(), metadataMap.get(collectionName).get(entry.getKey())));
		else {
	      String msg =
	              "No metadata entries found for CollectionType "
	                  + collectionType
	                  + " and CollectionName "
	                  + collectionName;
	      logger.error(msg);
		}
    }
	//Retrieve and populate optional metadata
	Map<String, String> optionalEntryMap = metadataMap.get(collectionType + "_Optional");
	if(optionalEntryMap != null) {
		for(Map.Entry<String, String> entry : optionalEntryMap.entrySet()) {
			if(metadataMap.get(collectionName) != null && metadataMap.get(collectionName).get(entry.getKey()) != null)
				bulkMetadataEntry
		        .getPathMetadataEntries()
		        .add(createPathEntry(entry.getValue(), metadataMap.get(collectionName).get(entry.getKey())));
	    }
	}
    
    return bulkMetadataEntry;
  }

  public HpcMetadataEntry createPathEntry(String key, String value) {
    HpcMetadataEntry pathEntry = new HpcMetadataEntry();
    pathEntry.setAttribute(key);
    pathEntry.setValue(value);

    return pathEntry;
  }
  
  public HpcMetadataEntry createPathEntry(String key, String value, String dateFormat) {
	    HpcMetadataEntry pathEntry = new HpcMetadataEntry();
	    pathEntry.setAttribute(key);
	    pathEntry.setValue(value);
	    pathEntry.setDateFormat(dateFormat);

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
  
  public String getAttrValueWithExactKey(String key, String attrKey) {
	if(StringUtils.isEmpty(key)) {
      logger.error("Excel mapping not found for {}", key);
      return null;
    }
    return (threadLocalMap.get().get(key) == null? null : threadLocalMap.get().get(key).get(attrKey));
  }
  
  public Map<String, Map<String, String>> loadMetadataFile(String metadataFile, String key) throws DmeSyncMappingException {
      return ExcelUtil.parseBulkMetadataEntries(metadataFile, key);
  }
  
  public Map<String, Map<String, String>> loadCsvMetadataFile(String metadataFile, String key) throws DmeSyncMappingException {
      return CsvFileUtil.parseBulkMetadataEntries(metadataFile, key);
  }
  
  public Map<String, Map<String, String>> loadJsonMetadataFile(String metadataFile, String key)
		 throws DmeSyncMappingException {
	ObjectMapper mapper = new ObjectMapper();
	Map<String, Map<String, String>> metdataMap = new HashMap<>();

	try {
		JsonNode rootNode = mapper.readTree(new File(metadataFile));
		if (rootNode.isObject()) {
			Iterator<String> iter = rootNode.fieldNames();
			Map<String, String> metadata = new HashMap<>();
			while (iter.hasNext()) {
				String nodeName = iter.next();
				JsonNode node = rootNode.path(nodeName);
				metadata.put(nodeName, node.asText());
			}
			metdataMap.put(key, metadata);
		}
	} catch (JsonProcessingException e) {
		throw new DmeSyncMappingException("Failed to load json metadata from file");
	} catch (IOException e) {
		throw new DmeSyncMappingException("Failed to parse json metadata from file");
	}
	return metdataMap;
  }
  
  public Map<String, Map<String, String>> loadMetadataFile(String metadataFile, String key1, String key2) throws DmeSyncMappingException {
    return ExcelUtil.parseBulkMetadataEntries(metadataFile, key1, key2);
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
