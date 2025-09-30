package gov.nih.nci.hpc.dmesync.util;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;

import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;

/**
 * Metadata Model Builder. Gets Metadata Map from cache if present, else
 * retrieves from the spreadsheet. reloads whenever scheduler runs
 *
 * @author konerum3
 */
@Service
public class DmeMetadataBuilder {

	protected Logger logger = LoggerFactory.getLogger(this.getClass());

	@Cacheable(value = "metadata", key = "'dmeMetadata'", sync = true)
	public Map<String, Map<String, String>> getMetadataMap(String metadataFile, String key)
			throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {

		logger.info("Parsing the Metadata Spreadsheet and creating Metadata Map");
		return ExcelUtil.parseBulkMetadataEntries(metadataFile, key);
	}

	@CachePut(value = "metadata", key = "'dmeMetadata'")
	public Map<String, Map<String, String>> updateMetadataMap(String metadataFile, String key)
			throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {

		logger.info("Updating the Metadata Spreadsheet and creating Metadata Map");
		return ExcelUtil.parseBulkMetadataEntries(metadataFile, key);
	}

	@Cacheable(value = "metadata", key = "'piMetadata'", sync = true)
	public Map<String, Map<String, String>> getPIMetadataMap(String metadataFile, String key)
			throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {
		logger.info("Parsing the PI Metadata Spreadsheet and creating Map");
		return ExcelUtil.parseBulkMetadataEntries(metadataFile, key);
	}

	@CachePut(value = "metadata", key = "'piMetadata'")
	public Map<String, Map<String, String>> updatePIMetadataMap(String metadataFile, String key)
			throws DmeSyncMappingException, DmeSyncWorkflowException, IOException {
		logger.info("Updating the PI Metadata Spreadsheet and creating Map");
		return ExcelUtil.parseBulkMetadataEntries(metadataFile, key);
	}

	@CacheEvict(value = "metadata", allEntries = true)
	public void evictMetadataMap() {
		logger.info("Clearing the cached Metadata Map");

	}

}
