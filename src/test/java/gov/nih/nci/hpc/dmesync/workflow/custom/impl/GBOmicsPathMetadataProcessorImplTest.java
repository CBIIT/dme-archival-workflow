package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import gov.nih.nci.hpc.dmesync.domain.DocConfig;
import gov.nih.nci.hpc.dmesync.domain.StatusInfo;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

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

	@ParameterizedTest
	@ValueSource(strings = {"fastq_pass", "fastq_fail", "bam_pass", "bam_fail"})
	void testMultiplexedOntArchivePath(String dataFolder) throws Exception {
		GBOmicsPathMetadataProcessorImpl processor = ontProcessor(Map.of(
				"FLO123_Tumor_barcode01", Map.of("Sample name", "Tumor_barcode01")));
		StatusInfo status = status("/unrelated/nested/FLO123/" + dataFolder + "/barcode01");

		assertEquals("/GB_OMICS_Archive/Lab_Lab1/DATA/Year_2024/Flowcell_FLO123/Sample_Tumor_barcode01/"
				+ dataFolder + "_barcode01", processor.getArchivePath(status, ontConfig()));
		assertNotEquals(Boolean.TRUE, status.isEndWorkflow());
	}

	@Test
	void testMultiplexedOntWithoutSampleUsesFlowcellFolder() throws Exception {
		GBOmicsPathMetadataProcessorImpl processor = ontProcessor(Map.of());
		StatusInfo status = status("/unrelated/nested/FLO123/fastq_pass/barcode01");

		assertEquals("/GB_OMICS_Archive/Lab_Lab1/DATA/Year_2024/Flowcell_FLO123/fastq_pass_barcode01",
				processor.getArchivePath(status, ontConfig()));
		assertNotEquals(Boolean.TRUE, status.isEndWorkflow());
	}

	@ParameterizedTest
	@ValueSource(strings = {"fastq_pass", "fastq_fail", "bam_pass", "bam_fail"})
	void testSingleSampleOntArchivePath(String dataFolder) throws Exception {
		GBOmicsPathMetadataProcessorImpl processor = ontProcessor(Map.of());
		StatusInfo status = status("/unrelated/nested/FLO123/" + dataFolder);

		assertEquals("/GB_OMICS_Archive/Lab_Lab1/DATA/Year_2024/Flowcell_FLO123/Sample_Single/" + dataFolder,
				processor.getArchivePath(status, ontConfig()));
	}

	@Test
	void testOtherOntFileUsesImmediateParent() throws Exception {
		GBOmicsPathMetadataProcessorImpl processor = ontProcessor(Map.of());
		StatusInfo status = status("/unrelated/nested/FLO123/report.txt");

		assertEquals("/GB_OMICS_Archive/Lab_Lab1/DATA/Year_2024/Flowcell_FLO123/report.txt",
				processor.getArchivePath(status, ontConfig()));
	}

	private DocConfig ontConfig() {
		DocConfig.SourceConfig source = new DocConfig.SourceConfig("/data/GB_OMICS_ONT", null, "/GB_OMICS_Archive", 1);
		DocConfig.SourceRule rule = new DocConfig.SourceRule(null, null, null, "metadata.xlsx", null, false, false, null, null, false, null, false, false, false, 0);
		DocConfig.UploadConfig upload = new DocConfig.UploadConfig(false, false, false, false, false, false, false, false, false, false, false, false, null, 0);
		return new DocConfig(null, null, null, null, null, null, null, false, null, 0, null, null, source, rule, null, null, upload, null);
	}

	private GBOmicsPathMetadataProcessorImpl ontProcessor(Map<String, Map<String, String>> multiplexed) throws Exception {
		GBOmicsPathMetadataProcessorImpl processor = spy(new GBOmicsPathMetadataProcessorImpl());
		doReturn(Map.of("FLO123", Map.of("Archive", "yes", "Sample name", "Single", "PI", "lab1", "Run Start Date", "01/01/24")))
				.when(processor).loadMetadataFile("metadata.xlsx", "FCID");
		doReturn(new HashMap<>(multiplexed)).when(processor).loadMetadataFile("metadata.xlsx", "FCID", "Sample name");
		doReturn("Lab1").when(processor).getCollectionMappingValue("lab1", "DataOwner_Lab", "gb-omics");
		return processor;
	}

	private StatusInfo status(String path) {
		StatusInfo status = new StatusInfo();
		status.setOriginalFilePath(path);
		status.setSourceFilePath(path);
		status.setOrginalFileName(Paths.get(path).getFileName().toString());
		return status;
	}
}