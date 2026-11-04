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
					"/DME/test_lab/Project/SCAF1/FASTQ/SCAF1_FQ_FC1_GEX.tar", "READY", "", "YES" });
		}

		SCAFReviewFile.ReviewMapping mapping = SCAFReviewFile.requireApproved(reviewFile.toString(), "/source/first",
				"/DME");
		assertEquals("SCAF1", mapping.sampleId);
		assertEquals("GEX", mapping.chemistry);
		assertEquals("FC1", mapping.flowcell);

		try (CSVWriter writer = new CSVWriter(new FileWriter(reviewFile.toFile(), true))) {
			writer.writeNext(new String[] { "Run_1", "/source/second", "SCAF2", "FASTQ", "GEX", "FC2",
					"/DME/test_lab/Project/SCAF1/FASTQ/SCAF1_FQ_FC1_GEX.tar", "DUPLICATE", "", "YES" });
		}

		assertThrows(DmeSyncMappingException.class, () -> SCAFReviewFile.requireApproved(reviewFile.toString(),
				"/source/first", "/DME"));
	}

	@Test
	void editedDestinationOverridesStaleCsvFields() throws Exception {
		Path reviewFile = tempDir.resolve("edited.csv");
		writeMapping(reviewFile,
				"/Archive/test_lab/CS0_Man/SCAFID_Product_1/Primary_Analysis_Output/SCAFID_Product_1_PA_GEX.tar",
				"YES");

		SCAFReviewFile.ReviewMapping mapping = SCAFReviewFile.requireApproved(
				reviewFile.toString(), "/source/first", "/Archive");
		assertEquals("SCAFID_Product_1", mapping.sampleId);
		assertEquals("Primary_Analysis_Output", mapping.collectionType);
		assertEquals("GEX", mapping.chemistry);
		assertEquals("", mapping.flowcell);
		assertEquals("/Archive/test_lab", mapping.labPath());
		assertEquals("/Archive/test_lab/CS0_Man", mapping.projectPath());
		assertEquals("/Archive/test_lab/CS0_Man/SCAFID_Product_1", mapping.samplePath());
	}

	@Test
	void fastqDestinationOverridesStaleSampleChemistryAndFlowcell() throws Exception {
		Path reviewFile = tempDir.resolve("fastq.csv");
		writeMapping(reviewFile, "/Archive/new_lab/Project/SCAF2/FASTQ/SCAF2_FQ_FC2_ATAC.tar", "YES");
		SCAFReviewFile.ReviewMapping mapping = SCAFReviewFile.requireApproved(
				reviewFile.toString(), "/source/first", "/Archive");
		assertEquals("SCAF2", mapping.sampleId);
		assertEquals("FASTQ", mapping.collectionType);
		assertEquals("ATAC", mapping.chemistry);
		assertEquals("FC2", mapping.flowcell);
	}

	@Test
	void sampleFolderCanDifferFromFilenamePrefix() throws Exception {
		Path reviewFile = tempDir.resolve("grouped-sample.csv");
		String destination = "/CCR_SCAF_Archive/Adam_Sowalsky_lab/CS033964156_Baj/"
				+ "SCAF4232_Product_1_2/Primary_Analysis_Output/SCAF4232_Product_2_PA_GEX.tar";
		writeMapping(reviewFile, destination, "YES");
		SCAFReviewFile.ReviewMapping mapping = SCAFReviewFile.requireApproved(
				reviewFile.toString(), "/source/first", "/CCR_SCAF_Archive");
		assertEquals("SCAF4232_Product_1_2", mapping.sampleId);
		assertEquals("GEX", mapping.chemistry);
		assertEquals("Primary_Analysis_Output", mapping.collectionType);
		assertEquals(destination, mapping.proposedDmePath);
		assertEquals("/CCR_SCAF_Archive/Adam_Sowalsky_lab/CS033964156_Baj/SCAF4232_Product_1_2",
				mapping.samplePath());
	}

	@Test
	void fastqSampleFolderCanDifferFromFilenamePrefix() throws Exception {
		Path reviewFile = tempDir.resolve("grouped-fastq.csv");
		writeMapping(reviewFile, "/Archive/lab/Project/CombinedSample/FASTQ/SCAF2_FQ_FC2_ATAC.tar.gz", "YES");
		SCAFReviewFile.ReviewMapping mapping = SCAFReviewFile.requireApproved(
				reviewFile.toString(), "/source/first", "/Archive");
		assertEquals("CombinedSample", mapping.sampleId);
		assertEquals("FC2", mapping.flowcell);
		assertEquals("ATAC", mapping.chemistry);
	}

	@Test
	void rejectsMalformedOrUnapprovedDestination() throws Exception {
		String[] paths = {
				"/Archive/lab/Project/SCAF2/FASTQ/unrecognized.tar",
				"/Archive/lab/Project/SCAF2/Unknown/SCAF2_PA_GEX.tar",
				"/Archive/lab/Project/SCAF2/Primary_Analysis_Output/other_FQ_FC1_GEX.tar",
				"/Elsewhere/lab/Project/SCAF2/Primary_Analysis_Output/SCAF2_PA_GEX.tar",
				"/Archive/lab/Project/SCAF2/extra/Primary_Analysis_Output/SCAF2_PA_GEX.tar"
		};
		for (int i = 0; i < paths.length; i++) {
			Path reviewFile = tempDir.resolve("invalid-" + i + ".csv");
			writeMapping(reviewFile, paths[i], "YES");
			assertThrows(DmeSyncMappingException.class, () -> SCAFReviewFile.requireApproved(
					reviewFile.toString(), "/source/first", "/Archive"));
		}
		Path unapproved = tempDir.resolve("unapproved.csv");
		writeMapping(unapproved, "/Archive/lab/Project/SCAF2/FASTQ/SCAF2_FQ_FC2_GEX.tar", "");
		assertThrows(DmeSyncMappingException.class, () -> SCAFReviewFile.requireApproved(
				unapproved.toString(), "/source/first", "/Archive"));
	}

	private void writeMapping(Path file, String destination, String approved) throws Exception {
		try (CSVWriter writer = new CSVWriter(new FileWriter(file.toFile()))) {
			writer.writeNext(new String[] { "RunId", "OriginalFilePath", "SampleId", "SampleCollectionType",
					"Chemistry", "Flowcell", "ProposedDmePath", "ReviewStatus", "ReviewNotes", "Approved" });
			writer.writeNext(new String[] { "Run_1", "/source/first", "OLD_SAMPLE", "FASTQ", "OLD_CHEMISTRY",
					"OLD_FLOWCELL", destination, "READY", "", approved });
		}
	}
}
