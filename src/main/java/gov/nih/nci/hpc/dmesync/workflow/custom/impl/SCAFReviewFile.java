package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;

final class SCAFReviewFile {
	private static final String[] HEADERS = { "RunId", "OriginalFilePath", "SampleId", "SampleCollectionType",
			"Chemistry", "Flowcell", "ProposedDmePath", "ReviewStatus", "ReviewNotes", "Approved" };
	private static final Object WRITE_LOCK = new Object();
	private static String activeFile;
	private static String activeRunId;
	private static final Set<String> reviewedSourcePaths = new HashSet<>();
	private static final Set<String> proposedPaths = new HashSet<>();
	private static volatile ReviewIndex cachedIndex;

	private SCAFReviewFile() {
	}

	static String append(String fileName, String runId, String originalPath, String sampleId, String collectionType,
			String chemistry, String flowcell, String proposedPath, String reviewStatus, String notes)
			throws IOException {
		synchronized (WRITE_LOCK) {
			Path reviewPath = Paths.get(fileName).toAbsolutePath().normalize();
			String normalizedFile = reviewPath.toString();
			if (!normalizedFile.equals(activeFile) || !StringUtils.equals(runId, activeRunId)) {
				Path parent = reviewPath.getParent();
				if (parent != null) {
					Files.createDirectories(parent);
				}
				try (CSVWriter writer = new CSVWriter(new FileWriter(reviewPath.toFile(), false))) {
					writer.writeNext(HEADERS);
				}
				activeFile = normalizedFile;
				activeRunId = runId;
				reviewedSourcePaths.clear();
				proposedPaths.clear();
				cachedIndex = null;
			}

			if (!reviewedSourcePaths.add(originalPath)) {
				return reviewStatus;
			}

			String finalStatus = reviewStatus;
			String finalNotes = notes;
			if (StringUtils.isNotBlank(proposedPath) && !proposedPaths.add(proposedPath)) {
				finalStatus = "DUPLICATE";
				finalNotes = StringUtils.isBlank(notes) ? "Duplicate proposed DME path"
						: notes + "; duplicate proposed DME path";
			}

			try (CSVWriter writer = new CSVWriter(new FileWriter(reviewPath.toFile(), true))) {
				writer.writeNext(new String[] { runId, originalPath, sampleId, collectionType, chemistry, flowcell,
						proposedPath, finalStatus, finalNotes, "" });
			}
			cachedIndex = null;
			return finalStatus;
		}
	}

	static ReviewMapping requireApproved(String fileName, String originalPath, String destinationBaseDir)
			throws DmeSyncMappingException {
		ReviewIndex index = loadIndex(fileName);
		List<ReviewMapping> mappings = index.byOriginalPath.get(originalPath);
		if (mappings == null || mappings.isEmpty()) {
			throw new DmeSyncMappingException("No SCAF review mapping found for " + originalPath);
		}
		if (mappings.size() != 1) {
			throw new DmeSyncMappingException("Multiple SCAF review mappings found for " + originalPath);
		}

		ReviewMapping mapping = mappings.get(0);
		if (!"YES".equalsIgnoreCase(StringUtils.trimToEmpty(mapping.approved))) {
			throw new DmeSyncMappingException("SCAF review mapping is not approved for " + originalPath);
		}
		if (StringUtils.isAnyBlank(mapping.sampleId, mapping.collectionType, mapping.chemistry,
				mapping.proposedDmePath)) {
			throw new DmeSyncMappingException("Approved SCAF mapping is missing required hierarchy fields for "
					+ originalPath);
		}
		if (!"FASTQ".equals(mapping.collectionType)
				&& !"Primary_Analysis_Output".equals(mapping.collectionType)) {
			throw new DmeSyncMappingException("Approved SCAF mapping has an unsupported collection type for "
					+ originalPath);
		}
		if ("FASTQ".equalsIgnoreCase(mapping.collectionType) && StringUtils.isBlank(mapping.flowcell)) {
			throw new DmeSyncMappingException("Approved FASTQ mapping is missing a flowcell for " + originalPath);
		}
		Path normalizedDestination = Paths.get(destinationBaseDir).normalize();
		Path normalizedProposedPath = Paths.get(mapping.proposedDmePath).normalize();
		if (!normalizedProposedPath.startsWith(normalizedDestination)
				|| normalizedProposedPath.equals(normalizedDestination)) {
			throw new DmeSyncMappingException("Approved SCAF mapping is outside the configured DME base path: "
					+ mapping.proposedDmePath);
		}

		for (ReviewMapping other : index.approvedMappings) {
			if (other != mapping && mapping.proposedDmePath.equals(other.proposedDmePath)) {
				throw new DmeSyncMappingException("Multiple approved SCAF mappings target "
						+ mapping.proposedDmePath);
			}
		}
		return mapping;
	}

	private static ReviewIndex loadIndex(String fileName) throws DmeSyncMappingException {
		Path path = Paths.get(fileName).toAbsolutePath().normalize();
		if (!Files.isRegularFile(path)) {
			throw new DmeSyncMappingException("SCAF review file does not exist: " + path);
		}
		try {
			long modified = Files.getLastModifiedTime(path).toMillis();
			long size = Files.size(path);
			ReviewIndex current = cachedIndex;
			if (current != null && current.path.equals(path) && current.lastModified == modified && current.size == size) {
				return current;
			}
			synchronized (WRITE_LOCK) {
				current = cachedIndex;
				if (current != null && current.path.equals(path) && current.lastModified == modified
						&& current.size == size) {
					return current;
				}
				ReviewIndex loaded = new ReviewIndex(path, modified, size);
				try (CSVReader reader = new CSVReader(new FileReader(path.toFile()))) {
					String[] headers = reader.readNext();
					if (headers == null) {
						throw new DmeSyncMappingException("SCAF review file has no header: " + path);
					}
					Map<String, Integer> columns = new HashMap<>();
					for (int i = 0; i < headers.length; i++) {
						columns.put(headers[i].trim(), i);
					}
					validateHeaders(columns, path);
					String[] values;
					while ((values = reader.readNext()) != null) {
						ReviewMapping mapping = new ReviewMapping(value(values, columns, "RunId"),
								value(values, columns, "OriginalFilePath"), value(values, columns, "SampleId"),
								value(values, columns, "SampleCollectionType"), value(values, columns, "Chemistry"),
								value(values, columns, "Flowcell"), value(values, columns, "ProposedDmePath"),
								value(values, columns, "ReviewStatus"), value(values, columns, "ReviewNotes"),
								value(values, columns, "Approved"));
						loaded.byOriginalPath.computeIfAbsent(mapping.originalFilePath, key -> new ArrayList<>())
								.add(mapping);
						if ("YES".equalsIgnoreCase(StringUtils.trimToEmpty(mapping.approved))) {
							loaded.approvedMappings.add(mapping);
						}
					}
				}
				cachedIndex = loaded;
				return loaded;
			}
		} catch (DmeSyncMappingException e) {
			throw e;
		} catch (Exception e) {
			throw new DmeSyncMappingException("Failed to read SCAF review file " + path, e);
		}
	}

	private static void validateHeaders(Map<String, Integer> columns, Path path) throws DmeSyncMappingException {
		for (String header : HEADERS) {
			if (!columns.containsKey(header)) {
				throw new DmeSyncMappingException("SCAF review file is missing required column " + header + ": "
						+ path);
			}
		}
	}

	private static String value(String[] row, Map<String, Integer> columns, String column) {
		Integer index = columns.get(column);
		return index == null || index >= row.length ? "" : StringUtils.trimToEmpty(row[index]);
	}

	static final class ReviewMapping {
		final String runId;
		final String originalFilePath;
		final String sampleId;
		final String collectionType;
		final String chemistry;
		final String flowcell;
		final String proposedDmePath;
		final String reviewStatus;
		final String reviewNotes;
		final String approved;

		ReviewMapping(String runId, String originalFilePath, String sampleId, String collectionType, String chemistry,
				String flowcell, String proposedDmePath, String reviewStatus, String reviewNotes, String approved) {
			this.runId = runId;
			this.originalFilePath = originalFilePath;
			this.sampleId = sampleId;
			this.collectionType = collectionType;
			this.chemistry = chemistry;
			this.flowcell = flowcell;
			this.proposedDmePath = proposedDmePath;
			this.reviewStatus = reviewStatus;
			this.reviewNotes = reviewNotes;
			this.approved = approved;
		}
	}

	private static final class ReviewIndex {
		private final Path path;
		private final long lastModified;
		private final long size;
		private final Map<String, List<ReviewMapping>> byOriginalPath = new HashMap<>();
		private final List<ReviewMapping> approvedMappings = new ArrayList<>();

		private ReviewIndex(Path path, long lastModified, long size) {
			this.path = path;
			this.lastModified = lastModified;
			this.size = size;
		}
	}
}
