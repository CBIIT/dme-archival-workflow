package gov.nih.nci.hpc.dmesync.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.Hashing;

import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;

public class TarContentsFileUtil {

	static final Logger logger = LoggerFactory.getLogger(TarContentsFileUtil.class);

	// Column header written at the top of every manifest.
	private static final String HEADER_ROW = "TYPE\tPATH\tSIZE\tMD5\tSYMLINK_TARGET";

	private TarContentsFileUtil() {
	}

	/**
	 * Writes a structured manifest to {@code textWriter}.
	 *
	 * <p>Each entry is a tab-separated line with the following columns:
	 * <pre>
	 * TYPE    PATH                        SIZE        MD5             SYMLINK_TARGET
	 * FILE    data/sample.fastq           123456      md5:<hex>       -
	 * SYMLINK data/link.fastq             -           -               data/sample.fastq
	 * </pre>
	 *
	 * <p>Entries are sorted by normalized relative path before writing.
	 *
	 * @param textWriter    writer for the manifest file (will be closed by this method)
	 * @param tarFileHeader absolute path of the source directory (used as the relativization root)
	 * @param subList       list of {@link File} objects to include in the manifest
	 * @return {@code true} if the manifest was written successfully
	 * @throws IOException            on I/O error
	 * @throws DmeSyncMappingException on any other failure while building the manifest
	 */
	public static boolean writeToTarContentsFile(BufferedWriter textWriter, String tarFileHeader, List<File> subList)
			throws IOException, DmeSyncMappingException {

		try {
			logger.info("Writing Files list to TarContents file Started {}", tarFileHeader);

			Path sourcePath = new File(tarFileHeader).toPath();

			// Build all manifest rows first so we can sort them.
			List<String[]> rows = new ArrayList<>();
			for (File fileName : subList) {
				if (fileName == null) {
					continue;
				}
				Path filePath = fileName.toPath();
				// Normalize: relativize, then convert separators to '/' for portability.
				Path relativePath = sourcePath.relativize(filePath);
				String normalizedRelPath = relativePath.toString().replace(File.separatorChar, '/');

				if (Files.isSymbolicLink(filePath)) {
					Path target = Files.readSymbolicLink(filePath);
					Path resolvedTarget = filePath.getParent().resolve(target).normalize();
					// Express the symlink target relative to the source dir when possible.
					String targetStr;
					try {
						targetStr = sourcePath.relativize(resolvedTarget).toString().replace(File.separatorChar, '/');
					} catch (IllegalArgumentException e) {
						// Target is outside the source tree; record the absolute path.
						targetStr = resolvedTarget.toString().replace(File.separatorChar, '/');
					}
					rows.add(new String[]{"SYMLINK", normalizedRelPath, "-", "-", targetStr});
				} else if (fileName.isDirectory()) {
					rows.add(new String[]{"DIR", normalizedRelPath, "-", "-", "-"});
				} else {
					long size = fileName.length();
					String md5 = computeMd5(fileName);
					rows.add(new String[]{"FILE", normalizedRelPath, String.valueOf(size), "md5:" + md5, "-"});
				}
			}

			// Sort rows by the normalized relative path (column index 1) for consistent ordering.
			rows.sort((a, b) -> a[1].compareTo(b[1]));

			// Write header and rows.
			textWriter.write("Tar File: " + tarFileHeader + "\n");
			textWriter.write(HEADER_ROW + "\n");
			for (String[] row : rows) {
				textWriter.write(String.join("\t", row) + "\n");
			}
			textWriter.write("\n");

			logger.info("Writing Files list to TarContents file Completed {}", tarFileHeader);
			return true;

		} finally {
			textWriter.close();
		}
	}

	/**
	 * Computes the MD5 hex digest of {@code file} using Guava, consistent with
	 * how {@code DmeSyncCreateChecksumTaskImpl} computes checksums.
	 *
	 * @param file the file to hash
	 * @return lower-case hex string of the MD5 digest
	 */
	@SuppressWarnings("deprecation")
	private static String computeMd5(File file) throws IOException {
		return com.google.common.io.Files.hash(file, Hashing.md5()).toString();
	}
}
