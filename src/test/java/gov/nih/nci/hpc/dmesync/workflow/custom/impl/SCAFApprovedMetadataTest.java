package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.FileWriter;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.util.ReflectionTestUtils;

import com.opencsv.CSVWriter;

import gov.nih.nci.hpc.dmesync.domain.StatusInfo;
import gov.nih.nci.hpc.domain.metadata.HpcBulkMetadataEntry;
import gov.nih.nci.hpc.domain.metadata.HpcMetadataEntry;
import gov.nih.nci.hpc.dto.datamanagement.v2.HpcDataObjectRegistrationRequestDTO;

class SCAFApprovedMetadataTest {
	@TempDir
	Path tempDir;

	@Test
	void metadataUsesEditedDestinationInsteadOfSpreadsheetAndCsvHierarchy() throws Exception {
		String original = "/mnt/scaf-data/CS123_Old_Project/02_PrimaryAnalysisOutput/"
				+ "00_FullCellrangerOutputs/GEX/OLD_SAMPLE.tar";
		String destination = "/Archive/test_lab/CS0_Man/SCAFID_Product_1/Primary_Analysis_Output/"
				+ "SCAFID_Product_2_PA_GEX.tar";
		Path reviewFile = tempDir.resolve("review.csv");
		try (CSVWriter writer = new CSVWriter(new FileWriter(reviewFile.toFile()))) {
			writer.writeNext(new String[] { "RunId", "OriginalFilePath", "SampleId", "SampleCollectionType",
					"Chemistry", "Flowcell", "ProposedDmePath", "ReviewStatus", "ReviewNotes", "Approved" });
			writer.writeNext(new String[] { "Run_1", original, "OLD_SAMPLE", "FASTQ", "ATAC", "OLD_FC",
					destination, "READY", "", "YES" });
		}
		SCAFPathMetadataProcessorImpl processor = processor();
		ReflectionTestUtils.setField(processor, "reviewFile", reviewFile.toString());
		ReflectionTestUtils.setField(processor, "destinationBaseDir", "/Archive");
		StatusInfo object = new StatusInfo();
		object.setOriginalFilePath(original);
		object.setOrginalFileName("OLD_SAMPLE.tar");
		object.setSourceFilePath("/work/project/GEX");
		assertEquals(destination, processor.getArchivePath(object));
		assertEquals("/work/project/GEX/SCAFID_Product_2_PA_GEX/SCAFID_Product_2_PA_GEX.tar",
				object.getSourceFilePath());
		try {
			HpcDataObjectRegistrationRequestDTO dto = processor.getMetaDataJson(object);
			Map<String, HpcBulkMetadataEntry> collections = dto.getParentCollectionsBulkMetadataEntries()
					.getPathsMetadataEntries().stream().collect(Collectors.toMap(HpcBulkMetadataEntry::getPath, e -> e));
			assertEquals(4, collections.size());
			assertEquals("PI_Lab", value(collections.get("/Archive/test_lab"), "collection_type"));
			assertEquals("CS0_Man", value(collections.get("/Archive/test_lab/CS0_Man"), "project_id"));
			assertEquals("SCAFID_Product_1",
					value(collections.get("/Archive/test_lab/CS0_Man/SCAFID_Product_1"), "sample_id"));
			assertEquals("Primary_Analysis_Output", value(collections.get(
					"/Archive/test_lab/CS0_Man/SCAFID_Product_1/Primary_Analysis_Output"), "collection_type"));
			assertEquals("GEX", dto.getMetadataEntries().stream()
					.filter(e -> "chemistry".equals(e.getAttribute())).findFirst().orElseThrow().getValue());
			assertEquals("SCAFID_Product_2_PA_GEX.tar", dto.getMetadataEntries().stream()
					.filter(e -> "object_name".equals(e.getAttribute())).findFirst().orElseThrow().getValue());
		} finally {
			AbstractPathMetadataProcessor.threadLocalMap.remove();
		}
	}

	@Test
	void projectAnalysisMetadataUsesOnlyLabProjectAndAnalysis() throws Exception {
		String original = "/mnt/scaf-data/CS123_Old_Project/Analysis/04_RawMatricesH5.tar";
		String destination = "/CCR/X_lab/CS035_test/Analysis/04_RawMatricesH5.tar";
		Path reviewFile = tempDir.resolve("analysis.csv");
		try (CSVWriter writer = new CSVWriter(new FileWriter(reviewFile.toFile()))) {
			writer.writeNext(new String[] { "RunId", "OriginalFilePath", "ProposedDmePath", "SampleId",
					"SampleCollectionType", "Chemistry", "Flowcell", "ReviewStatus", "ReviewNotes", "Approved" });
			writer.writeNext(new String[] { "Run_1", original, destination, "", "", "", "", "READY", "", "YES" });
		}
		SCAFPathMetadataProcessorImpl processor = processor();
		ReflectionTestUtils.setField(processor, "reviewFile", reviewFile.toString());
		ReflectionTestUtils.setField(processor, "destinationBaseDir", "/CCR");
		StatusInfo object = new StatusInfo();
		object.setOriginalFilePath(original);
		object.setOrginalFileName("04_RawMatricesH5.tar");
		object.setSourceFilePath(original);
		try {
			assertEquals(destination, processor.getArchivePath(object));
			HpcDataObjectRegistrationRequestDTO dto = processor.getMetaDataJson(object);
			Map<String, HpcBulkMetadataEntry> collections = dto.getParentCollectionsBulkMetadataEntries()
					.getPathsMetadataEntries().stream().collect(Collectors.toMap(HpcBulkMetadataEntry::getPath, e -> e));
			assertEquals(3, collections.size());
			assertEquals("PI_Lab", value(collections.get("/CCR/X_lab"), "collection_type"));
			assertEquals("CS035_test", value(collections.get("/CCR/X_lab/CS035_test"), "project_id"));
			assertEquals("Analysis", value(collections.get("/CCR/X_lab/CS035_test/Analysis"), "collection_type"));
			assertEquals("04_RawMatricesH5.tar", dto.getMetadataEntries().stream()
					.filter(e -> "object_name".equals(e.getAttribute())).findFirst().orElseThrow().getValue());
			org.junit.jupiter.api.Assertions.assertFalse(dto.getMetadataEntries().stream()
					.anyMatch(e -> "chemistry".equals(e.getAttribute()) || "sample_id".equals(e.getAttribute())));
		} finally {
			AbstractPathMetadataProcessor.threadLocalMap.remove();
		}
	}

	private SCAFPathMetadataProcessorImpl processor() {
		return new SCAFPathMetadataProcessorImpl() {
			@Override
			public Map<String, Map<String, String>> loadMetadataFile(String file, String key) {
				Map<String, String> row = new HashMap<>();
				row.put("data_owner_full_name", "Old Owner");
				row.put("project_id", "OLD_PROJECT");
				row.put("platform_name", "Illumina");
				return Map.of("CS123_Old_Project", row);
			}

			@Override
			public Map<String, Map<String, String>> loadCsvMetadataFile(String file, String key) {
				return Map.of();
			}
		};
	}

	private String value(HpcBulkMetadataEntry entry, String attribute) {
		return entry.getPathMetadataEntries().stream().filter(e -> attribute.equals(e.getAttribute()))
				.map(HpcMetadataEntry::getValue).findFirst().orElseThrow();
	}
}
