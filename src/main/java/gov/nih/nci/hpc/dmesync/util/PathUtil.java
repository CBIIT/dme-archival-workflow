package gov.nih.nci.hpc.dmesync.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PathUtil {

	static final Logger logger = LoggerFactory.getLogger(PathUtil.class);

	private PathUtil() {
	}

	/**
	 * Resolves the source file path for archival.
	 *
	 * <p>
	 * For regular files, this returns the file's absolute path. For symbolic links,
	 * this returns the absolute path of the symlink target,
	 *
	 * <p>
	 * If the symbolic link target is relative, it is resolved against the symlink's
	 * parent directory and normalized before being converted to an absolute path.
	 *
	 * @param absolutePath the absolute path of the scanned file or symlink
	 * @return the resolved source file path; for symlinks, the target path,
	 *         otherwise the original path
	 * @throws IOException if an I/O error occurs while reading the symbolic link
	 */
	public static String resolveSourceFilePath(String absolutePath)  {

		if (absolutePath == null) {
			return null;
		}

		try {

			Path fileFullPath = Paths.get(absolutePath);
			if (!Files.isSymbolicLink(fileFullPath)) {
				return absolutePath;
			}

			Path targetPath = Files.readSymbolicLink(fileFullPath);
			if (!targetPath.isAbsolute()) {
				targetPath = fileFullPath.getParent().resolve(targetPath).normalize();
			}

			return targetPath.toAbsolutePath().toString();
		} catch (IOException e) {
			logger.error("Error resolving source file path for {}: {}", absolutePath, e);
			return null;
		}
	}

}
