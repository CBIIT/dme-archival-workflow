/*******************************************************************************
 * Copyright SVG, Inc.
 * Copyright Leidos Biomedical Research, Inc.
 *  
 * Distributed under the OSI-approved BSD 3-Clause License.
 * See https://github.com/CBIIT/HPC_DME_APIs/LICENSE.txt for details.
 ******************************************************************************/
package gov.nih.nci.hpc.dmesync.util;


import gov.nih.nci.hpc.domain.error.HpcErrorType;
import gov.nih.nci.hpc.exception.HpcException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HpcLocalDirectoryListQuery {

  private static String genFileSizeDisplayString(long sizeInBytes) {
    final String formattedFig =
      NumberFormat.getInstance(Locale.US).format(sizeInBytes);

    final String storageUnit = (sizeInBytes == 1) ? "byte" : "bytes";

    return String.format(
      "Aggregate file size: %s %s", formattedFig, storageUnit);

  }

	// ---------------------------------------------------------------------//
	// Constants
	// ---------------------------------------------------------------------//

	// Globus transfer status strings.
	private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

	/**
	 * Get attributes of a file/directory.
	 * 
	 * @param fileLocation The endpoint/path to check.
	 * @param excludePattern The exclude pattern
	 * @param includePattern The include pattern
	 * @param depth The depth to scan
	 * @return The list of HpcPathAttributes
	 * @throws HpcException The exception
	 */
	public List<HpcPathAttributes> getPathAttributes(String fileLocation, List<String> excludePattern,
			List<String> includePattern, int depth) throws HpcException {
		List<HpcPathAttributes> pathAttributes = new ArrayList<>();

		try {
		    logger.debug("getPathAttributes: fileLocation: {}", fileLocation);
		    logger.debug("getPathAttributes: excludePattern: {}", excludePattern);
		    logger.debug("getPathAttributes: includePattern: {}", includePattern);
			List<File> dirContent = listDirectory(fileLocation, excludePattern, includePattern, depth);
			getPathAttributes(pathAttributes, dirContent);
		} catch (Exception e) {
		  logger.error(e.getMessage(), e);
			throw new HpcException("Failed to get path attributes: " + fileLocation,
					HpcErrorType.DATA_TRANSFER_ERROR, e);
		}

		return pathAttributes;
	}

	/**
	 * Get attributes of a file/directory.
	 * 
	 * @param localBasePath The local base path.
	 * @param fileLocation The endpoint/path to check.
	 * @return The path attributes.
	 * @throws HpcException The exception
	 */
	public List<HpcPathAttributes> getFileListPathAttributes(String localBasePath, String fileLocation) throws HpcException {
		List<HpcPathAttributes> pathAttributes = new ArrayList<>();

		try {
			List<String> files = readFileListfromFile(fileLocation);
			long totalSize = 0L;
			for(String filePath : files)
			{
				HpcPathAttributes filePathAttr = new HpcPathAttributes();
				String fullPath = localBasePath + File.separator + filePath;
				fullPath = fullPath.replace("\\", "/");
				filePathAttr.setAbsolutePath(fullPath);
				String name = filePath.substring(filePath.lastIndexOf('/') > 0 ? filePath.lastIndexOf('/') : 0,
						filePath.length());
				filePathAttr.setName(name);
				File fileToCheckDir = new File(filePath);
				filePathAttr.setIsDirectory(fileToCheckDir.isDirectory());
				filePathAttr.setPath(filePath);
				totalSize = totalSize + fileToCheckDir.length();
				logger.debug("Including: {}", fullPath);
				pathAttributes.add(filePathAttr);
			}
			logger.debug("\nAggreate file size: {}", totalSize);
		} catch (Exception e) {
			throw new HpcException("Failed to get path attributes: " + fileLocation,
					HpcErrorType.INVALID_REQUEST_INPUT, e);
		}

		return pathAttributes;
	}
	private void getPathAttributes(List<HpcPathAttributes> attributes, List<File> dirContent) {
		try {
			if (dirContent != null) {
				for (File file : dirContent) {
					HpcPathAttributes pathAttributes = new HpcPathAttributes();
					pathAttributes.setName(file.getName());
					pathAttributes.setPath(file.getPath());
					pathAttributes.setUpdatedDate(new Date(file.lastModified()));
					pathAttributes.setAbsolutePath(file.getAbsolutePath());
					pathAttributes.setSize(file.length());
					if (file.isDirectory())
						pathAttributes.setIsDirectory(true);
					attributes.add(pathAttributes);
				}
			}

		} catch (Exception e) {
			// Unexpected error. Eat this.
			logger.error("Failed to build directory listing", e);
		}
	}

	private List<String> readFileListfromFile(String fileName) throws Exception {
		if (fileName == null || fileName.isEmpty())
			return null;
		List<String> patterns = new ArrayList<>();
		try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
			String line;
			while ((line = reader.readLine()) != null) {
				patterns.add(line);
			}

		} catch (IOException e) {
			throw new Exception("Failed to read files list due to: " + e.getMessage());
		}
		return patterns;
	}

  /**
   * List Directory
   * 
   * @param directoryName The directory to list.
   * @param excludePattern The exclude pattern.
   * @param includePattern The include pattern.
   * @param depth The depth to scan.
   * @return List of Files.
   * @throws HpcException The exception
   */
  public List<File> listDirectory(String directoryName, List<String> excludePattern,
      List<String> includePattern, int depth) throws HpcException {
    		File directory = new File(directoryName);
    List<File> resultList = new ArrayList<>();

    // get all the files from a directory
    if (!directory.isDirectory()) {
      logger.debug("Invalid source folder");
      throw new HpcException("Invalid source folder " + directoryName,
          HpcErrorType.DATA_TRANSFER_ERROR);
    }

    if (includePattern == null || includePattern.isEmpty()) {
      includePattern = new ArrayList<>();
      includePattern.add("*");
      includePattern.add("*/**");
    }

    long totalSize = 0L;
    HpcPaths paths = getFileList(directoryName, excludePattern, includePattern, depth);
    for (String filePath : paths) {
      if (depth > 0 && depth(Paths.get(directoryName), Paths.get(filePath)) != depth) {
        continue;
      }
      String fileName = filePath.replace("\\", File.separator).replace(
                                        "/", File.separator);
      logger.debug("Including: {}", fileName);
		File file = new File(fileName);
      totalSize += file.length();
      resultList.add(file);
    }
    logger.debug("\n{}", genFileSizeDisplayString(totalSize));

    return resultList;
  }

	private HpcPaths getFileList(String basePath, List<String> excludePatterns, List<String> includePatterns, int depth) {
		HpcPaths paths = new HpcPaths();
		if (includePatterns == null || includePatterns.isEmpty()) {
			includePatterns = new ArrayList<>();
			includePatterns.add("*");
		}

		List<String> patterns = new ArrayList<>();
		patterns.addAll(includePatterns);
		if (excludePatterns != null) {
			for (String pattern : excludePatterns)
				patterns.add("!" + pattern);
		}
		patterns.add("!**/hpc*.log/**");
		logger.debug("basePath {}", basePath);
		return paths.glob(basePath, depth, patterns);

	}
	
	//compute depth based on relative path to top directory
	 private int depth(Path top, Path file) {
	    Path rp = file.relativize(top);
	    return (rp.getFileName().toString().equals("")) ? 0 : rp.getNameCount();
	 }
	 
	
}
