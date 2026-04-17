package gov.nih.nci.hpc.dmesync.workflow.impl;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.apache.commons.io.FilenameUtils;
import gov.nih.nci.hpc.dmesync.util.ExcelUtil;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncStorageException;
import gov.nih.nci.hpc.dmesync.exception.DmeSyncWorkflowException;
import gov.nih.nci.hpc.dmesync.workflow.DmeSyncTask;

/**
 * DME Sync Tar Pre Process Task Implementation
 * 
 * @author konerum3
 */
@Component
public class DmeSyncTarPreProcessTaskImpl extends AbstractDmeSyncTask implements DmeSyncTask {

	@Value("${dmesync.doc.name}")
	private String doc;

	@Value("${dmesync.compress:false}")
	private boolean compress;

	@Value("${dmesync.source.base.dir}")
	private String syncBaseDir;

	@Value("${dmesync.work.base.dir}")
	private String syncWorkDir;

	@Value("${dmesync.file.tar:false}")
	private boolean tarIndividualFiles;

	@Value("${dmesync.multiple.tars.files.count:0}")
	private Integer filesPerTar;

	@Value("${dmesync.tar.filename.excel.exist:false}")
	private boolean tarNameinExcelFile;

	@Value("${dmesync.additional.metadata.excel:}")
	private String metadataFile;

	@Value("${dmesync.tar.contents.file:false}")
	private boolean createTarContentsFile;

	@Value("${dmesync.selective.scan:false}")
	private boolean selectiveScan;

	@Value("${dmesync.process.multiple.tars:false}")
	private boolean processMultipleTars;

	@PostConstruct
	public boolean init() {
		super.setTaskName("TarTask");
		if (tarIndividualFiles)
			super.setCheckTaskForCompletion(false);
		return true;
	}

	protected static ThreadLocal<Map<String, Map<String, String>>> threadLocalMap = new ThreadLocal<Map<String, Map<String, String>>>() {
		@Override
		protected HashMap<String, Map<String, String>> initialValue() {
			return new HashMap<>();
		}
	};

	@Override
	public StatusInfo process(StatusInfo object)
			throws DmeSyncMappingException, DmeSyncWorkflowException, DmeSyncStorageException {


		// Task: Create tar file in work directory for processing
		try {

			String tarFileName;
			if (tarNameinExcelFile) {
				threadLocalMap.set(loadMetadataFile(metadataFile, "Path"));
				String path = FilenameUtils.separatorsToUnix(object.getOriginalFilePath() + "/");
				tarFileName = getAttrValueWithKey(path, "tar_name");
			} else {
				tarFileName = object.getOrginalFileName() + ".tar";
			}

			logger.info("[{}] Updating source file name in {}", super.getTaskName(), tarFileName);

			if (compress) {
				tarFileName = tarFileName + ".gz";

			}

			// Update the record for metadata processing
			object.setSourceFileName(tarFileName);
			object = dmeSyncWorkflowService.getService(access).saveStatusInfo(object);

		} catch (Exception e) {
			logger.error("[{}] error {}", super.getTaskName(), e.getMessage(), e);
			throw new DmeSyncStorageException("Error occurred during pre processing for TarTask. " + e.getMessage(), e);
		} finally {
			threadLocalMap.remove();
		}
		return object;

	}

	public Map<String, Map<String, String>> loadMetadataFile(String metadataFile, String key)
			throws DmeSyncMappingException {
		return ExcelUtil.parseBulkMetadataEntries(metadataFile, key);
	}

	public String getAttrValueWithKey(String rowKey, String attrKey) throws Exception {
		String key = null;
		if (threadLocalMap.get() == null)
			return null;
		for (String partialKey : threadLocalMap.get().keySet()) {
			if (StringUtils.contains(rowKey, partialKey)) {
				key = partialKey;
				break;
			}
		}
		if (StringUtils.isEmpty(key)) {
			logger.error("Excel mapping not found for {}", rowKey);
			throw new Exception("Excel mapping not found for {} " + rowKey);
		}
		return (threadLocalMap.get().get(key) == null ? null : threadLocalMap.get().get(key).get(attrKey));
	}

}