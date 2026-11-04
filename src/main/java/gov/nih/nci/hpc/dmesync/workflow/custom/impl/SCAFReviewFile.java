package gov.nih.nci.hpc.dmesync.workflow.custom.impl;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;

final class SCAFReviewFile {
	private static final String[] HEADERS = { "RunId", "OriginalFilePath", "ProposedDmePath", "SampleId",
			"SampleCollectionType", "Chemistry", "Flowcell", "ReviewStatus", "ReviewNotes", "Approved" };
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
				writer.writeNext(new String[] { runId, originalPath, proposedPath, sampleId, collectionType, chemistry,
						flowcell, finalStatus, finalNotes, "" });
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
		if (StringUtils.isBlank(mapping.proposedDmePath)) {
			throw new DmeSyncMappingException("Approved SCAF mapping is missing ProposedDmePath for " + originalPath);
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
		return mapping.fromDestination(normalizedDestination);
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

		private ReviewMapping fromDestination(Path base) throws DmeSyncMappingException {
			Path destination = Paths.get(proposedDmePath);
			Path relative = base.relativize(destination);
			if (!destination.equals(destination.normalize()) || relative.getNameCount() != 5) {
				throw new DmeSyncMappingException("Expected SCAF path <base>/<lab>/<project>/<sample>/<type>/<file>: "
						+ proposedDmePath);
			}
			String sample = relative.getName(2).toString();
			String type = relative.getName(3).toString();
			String file = relative.getName(4).toString();
			String expression;
			if ("FASTQ".equals(type)) {
				expression = ".+_FQ_(.+)_([^_]+)\\.tar(?:\\.gz)?";
			} else if ("Primary_Analysis_Output".equals(type)) {
				expression = ".+_PA_(.+)\\.tar(?:\\.gz)?";
			} else {
				throw new DmeSyncMappingException("Unsupported SCAF collection type in " + proposedDmePath);
			}
			Matcher matcher = Pattern.compile(expression).matcher(file);
			if (!matcher.matches()) {
				throw new DmeSyncMappingException("SCAF filename must use <prefix>_PA_<chemistry>.tar or "
						+ "<prefix>_FQ_<flowcell>_<chemistry>.tar for its collection type in "
						+ proposedDmePath);
			}
			boolean fastq = "FASTQ".equals(type);
			return new ReviewMapping(runId, originalFilePath, sample, type,
					matcher.group(fastq ? 2 : 1), fastq ? matcher.group(1) : "", proposedDmePath,
					reviewStatus, reviewNotes, approved);
		}

		String samplePath() {
			return Paths.get(proposedDmePath).getParent().getParent().toString();
		}

		String projectPath() {
			return Paths.get(samplePath()).getParent().toString();
		}

		String labPath() {
			return Paths.get(projectPath()).getParent().toString();
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
