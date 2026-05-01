package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.nio.file.Paths;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;

import static org.junit.jupiter.api.Assertions.*;

class GBOmicsPathMetadataProcessorImplTest {

	private DocConfig config;

	@BeforeEach
	public void init() {
		// Create the statusInfo object with the test Path
		DocConfig.SourceConfig sourceConfig = new DocConfig.SourceConfig(Paths.get("/data/khanlab3/gb_omics/projects").toString(), null, "/GB_OMICS_Archive", 1);
		DocConfig.UploadConfig upload = new DocConfig.UploadConfig(false, false, false, false, false, false, false, false, false, false, false, false, null, 0);
		config = new DocConfig(null, null, null, null, null, null, null, false, null, 0, null, null, sourceConfig, null,
				null, null, upload, null);

	}
    
    @Test
    void testGetProjectCollectionNameCCRGB() throws Exception {
        GBOmicsPathMetadataProcessorImpl processor = new GBOmicsPathMetadataProcessorImpl();
        StatusInfo statusInfo = new StatusInfo();
        // Simulate a path where the project folder is named "ccrgb-13"
        String testPath = Paths.get("/data/khanlab3/gb_omics/projects/caplen/ccrgb-13/file.txt").toString();
        statusInfo.setOriginalFilePath(testPath);
        statusInfo.setSourceFilePath(testPath);
        // The PI folder is "caplen" in this path
        String result = processor.getProjectCollectionName(statusInfo, config);
        assertEquals("CCRGB-13", result);
    }
    
    @Test
    void testGetProjectCollectionNameOther() throws Exception {
        GBOmicsPathMetadataProcessorImpl processor = new GBOmicsPathMetadataProcessorImpl();
        StatusInfo statusInfo = new StatusInfo();
        // Simulate a path where the project folder is a non-ccrgb project named "CENPA_cutrun"
        String testPath = Paths.get("/data/khanlab3/gb_omics/projects/caplen/CENPA_cutrun/file.txt").toString();
        statusInfo.setOriginalFilePath(testPath);
        statusInfo.setSourceFilePath(testPath);
        // The PI folder is "caplen" in this path
        String result = processor.getProjectCollectionName(statusInfo, config);
        assertEquals("CENPA_cutrun", result);
    }
}