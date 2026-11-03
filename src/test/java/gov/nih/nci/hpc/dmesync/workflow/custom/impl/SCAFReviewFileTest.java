package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.FileWriter;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.opencsv.CSVWriter;

import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;

class SCAFReviewFileTest {

	@TempDir
	Path tempDir;

	@Test
	void appendWritesReviewRowsAndFlagsRepeatedDestination() throws Exception {
		Path reviewFile = tempDir.resolve("scaf-review.csv");

		assertEquals("READY", SCAFReviewFile.append(reviewFile.toString(), "Run_1", "/source/first", "SCAF1",
				"FASTQ", "GEX", "FC1", "/DME/Project/SCAF1/FASTQ/SCAF1.tar", "READY", ""));
		assertEquals("DUPLICATE", SCAFReviewFile.append(reviewFile.toString(), "Run_1", "/source/second", "SCAF1",
				"FASTQ", "GEX", "FC1", "/DME/Project/SCAF1/FASTQ/SCAF1.tar", "READY", ""));

		String review = java.nio.file.Files.readString(reviewFile);
		assertEquals(3, review.lines().count());
		org.junit.jupiter.api.Assertions.assertTrue(review.contains("DUPLICATE"));
	}

	@Test
	void requireApprovedUsesReviewedFieldsAndRejectsDuplicateApprovedPaths() throws Exception {
		Path reviewFile = tempDir.resolve("scaf-review.csv");
		try (CSVWriter writer = new CSVWriter(new FileWriter(reviewFile.toFile()))) {
			writer.writeNext(new String[] { "RunId", "OriginalFilePath", "SampleId", "SampleCollectionType",
					"Chemistry", "Flowcell", "ProposedDmePath", "ReviewStatus", "ReviewNotes", "Approved" });
			writer.writeNext(new String[] { "Run_1", "/source/first", "SCAF1", "FASTQ", "GEX", "FC1",
					"/DME/Project/SCAF1/FASTQ/SCAF1.tar", "READY", "", "YES" });
		}

		SCAFReviewFile.ReviewMapping mapping = SCAFReviewFile.requireApproved(reviewFile.toString(), "/source/first",
				"/DME");
		assertEquals("SCAF1", mapping.sampleId);
		assertEquals("GEX", mapping.chemistry);
		assertEquals("FC1", mapping.flowcell);

		try (CSVWriter writer = new CSVWriter(new FileWriter(reviewFile.toFile(), true))) {
			writer.writeNext(new String[] { "Run_1", "/source/second", "SCAF2", "FASTQ", "GEX", "FC2",
					"/DME/Project/SCAF1/FASTQ/SCAF1.tar", "DUPLICATE", "", "YES" });
		}

		assertThrows(DmeSyncMappingException.class, () -> SCAFReviewFile.requireApproved(reviewFile.toString(),
				"/source/first", "/DME"));
	}
}
