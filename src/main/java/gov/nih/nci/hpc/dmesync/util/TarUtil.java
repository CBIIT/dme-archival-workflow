package gov.nih.nci.hpc.dmesync.util;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarFile;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Optional;
import java.util.regex.Pattern;

public class TarUtil {

  static final Logger logger = LoggerFactory.getLogger(TarUtil.class);

  private TarUtil() {}

  /**
   * Create a tar file
   * 
   * @param name the name of the tar file
   * @param excludeFolders list of folder names to exclude from tar
   * @param files the files to tar
 * @throws Exception 
   */
  public static void tar(String name, List<String> excludeFolders, boolean ignoreBrokenLinksInTar, File... files) throws Exception {
    try (TarArchiveOutputStream out = getTarArchiveOutputStream(name); ) {
      for (File file : files) {
        addToArchive(out, file, ".", excludeFolders, ignoreBrokenLinksInTar);
      }
    }
  }

  /**
   * Create a compressed tar file
   * 
   * @param name the name of the tar.gz file
   * @param excludeFolders list of folder names to exclude from tar
   * @param files files the files to tar and compress
 * @throws Exception 
   */
  public static void targz(String name, List<String> excludeFolders, boolean ignoreBrokenLinksInTar, File... files) throws Exception {
    try (TarArchiveOutputStream out = getTarGzArchiveOutputStream(name); ) {
      for (File file : files) {
        addToArchive(out, file, ".", excludeFolders, ignoreBrokenLinksInTar);
      }
    }
  }

  /**
   * Untar a tar file
   * @param name name of the tar file to untar
   * @param outFile the directory to untar to
   * @throws IOException on IO error
   */
  public static void untar(String name, File outFile) throws IOException {
    FileOutputStream out = null;
    try (TarArchiveInputStream in = getTarGzArchiveInputStream(name); ) {
      TarArchiveEntry entry;
      while ((entry = in.getNextTarEntry()) != null) {
        if (entry.isDirectory()) {
          continue;
        }
        
        File curfile = new File(outFile, entry.getName()).getCanonicalFile();
        if (!curfile.getPath().startsWith(outFile.getCanonicalPath())) {
            throw new IOException("Invalid tar entry: " + entry.getName());
          }
        File parent = curfile.getParentFile();
        if (!parent.exists()) {
          parent.mkdirs();
        }
        out = new FileOutputStream(curfile);
        IOUtils.copy(in, out);
        out.close();
      }
    } finally {
      try {
        if (out != null) out.close();
      } catch (IOException e) { //close quietly
      }
    }
  }

  /**
   * Delete the tar file from the work directory.
   * 
   * @param tarFile the tar file to be removed
   * @param workDir the work directory
   * @throws IOException on IO error
   */
  public static void deleteTarFile(String tarFile, String workDir , String doc) throws IOException {
    Path filePath = Paths.get(tarFile);
    Path basePath = Paths.get(workDir).toRealPath();

    // Make sure we are removing from the work directory ONLY
    if (!filePath.startsWith(basePath)) return;
    if (filePath == null || filePath.endsWith(basePath)) return;

    if (filePath.toFile().isFile()) {
      Files.deleteIfExists(filePath);
    }
    
  }
  
  public static void deleteTarAndParentsIfEmpty(String tarFile, String workDir , String doc) throws IOException {
	    Path filePath = Paths.get(tarFile);
	    Path basePath = Paths.get(workDir).toRealPath();

	    // Make sure we are removing from the work directory ONLY
	    if (!filePath.startsWith(basePath)) return;

	    //Delete the tar file and the parent dir until the work base directory if there are no other files
	    removeFileAndParentsIfEmpty(filePath, basePath , doc);
  }

  private static void removeFileAndParentsIfEmpty(Path path, Path basePath , String doc) throws IOException {
    if (path == null || path.endsWith(basePath)) return;

    if (path.toFile().isFile()) {
      Files.deleteIfExists(path);
    } else if (path.toFile().isDirectory()) {
      try {
        Files.delete(path);
        logger.info("{} Deleted the folder path", path);
      } catch (DirectoryNotEmptyException | NoSuchFileException e ) {
        //There could be common parent for the files being processed, the last one processing will remove the parent work folder.
        //Another thread might have removed it
        return;
      }
    }
    
      	removeFileAndParentsIfEmpty(path.getParent(), basePath , doc);

  }

  /**
   * List the immediate files under the tar folder.
   *
   * @param name the tar file name
   * @return list of path attributes
   * @throws IOException on IO error
   */
  public static List<HpcPathAttributes> listTar(String name) throws IOException {
    List<HpcPathAttributes> entries = new ArrayList<>();
    try (TarArchiveInputStream in = getTarGzArchiveInputStream(name)) {
      TarArchiveEntry entry;
      while (null != (entry = in.getNextTarEntry())) {
        if (!entry.isDirectory()) {
          Path entryPath = Paths.get(entry.getName());
          // Add entry
          HpcPathAttributes hpcEntry = new HpcPathAttributes();
          hpcEntry.setIsDirectory(false);
          hpcEntry.setIsFile(true);
          hpcEntry.setIsAccessible(true);
          hpcEntry.setExists(true);
          hpcEntry.setSize(entry.getSize());
          hpcEntry.setUpdatedDate(entry.getLastModifiedDate());
          hpcEntry.setTarEntry(entryPath.normalize().toString());
          Path path = Paths.get(name);
          hpcEntry.setAbsolutePath(name);
          hpcEntry.setName(path.getFileName().toString());
          entries.add(hpcEntry);
        }
      }
    }
    return entries;
  }
  
  /**
   * count the files  under the tar folder.
   *
   * @param path of the tar file name
   * @return number of files in tar
   * @throws IOException on IO error
   */
  public static int countFilesinTar(String name) throws IOException {
	  try (TarFile tarFile = new TarFile(new File(name))) {
		  
          return tarFile!=null?tarFile.getEntries().size():0;     
	  }catch (IOException e) {
	      logger.info("{} have issues or corrupted"
	      		+ "", name ,e.getMessage());

          return 0; // Indicate that the TAR file could not be processed
      }
  }
  
  

  /**
   * Compress a file
   * 
   * @param name the name of the compressed file
   * @param file the file to compress
   * @throws IOException on IO error
   */
  public static void compress(String name, File file) throws IOException {
    try (GzipCompressorOutputStream out = getGzipCompressorOutputStream(name); ) {
    	try (FileInputStream in = new FileInputStream(file)) {
            IOUtils.copy(in, out);
        }
    }
  }
  
  private static TarArchiveInputStream getTarGzArchiveInputStream(String name) throws IOException {
    FileInputStream fileInputStream = new FileInputStream(name);
    BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
    GzipCompressorInputStream gzipInputStream = new GzipCompressorInputStream(bufferedInputStream);
    return new TarArchiveInputStream(gzipInputStream);
  }

  private static TarArchiveOutputStream getTarGzArchiveOutputStream(String name)
      throws IOException {
    FileOutputStream fileOutputStream = new FileOutputStream(name);
    BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
    GzipCompressorOutputStream gzipOutputStream =
        new GzipCompressorOutputStream(bufferedOutputStream);
    TarArchiveOutputStream taos = new TarArchiveOutputStream(gzipOutputStream);
    // TAR has an 8 gig file limit by default, this gets around that
    taos.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);
    // TAR originally didn't support long file names, so enable the support for it
    taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
    taos.setAddPaxHeadersForNonAsciiNames(true);
    return taos;
  }

  private static TarArchiveOutputStream getTarArchiveOutputStream(String name) throws IOException {
    FileOutputStream fileOutputStream = new FileOutputStream(name);
    BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
    TarArchiveOutputStream taos = new TarArchiveOutputStream(bufferedOutputStream);
    // TAR has an 8 gig file limit by default, this gets around that
    taos.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);
    // TAR originally didn't support long file names, so enable the support for it
    taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
    taos.setAddPaxHeadersForNonAsciiNames(true);
    return taos;
  }
  
  private static GzipCompressorOutputStream getGzipCompressorOutputStream(String name) throws IOException {
	    FileOutputStream fileOutputStream = new FileOutputStream(name);
	    BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
	    return new GzipCompressorOutputStream(bufferedOutputStream);
	  }

  private static void addToArchive(TarArchiveOutputStream out, File file, String dir, List<String> excludeFolders, boolean ignoreBrokenLinksInTar)
      throws Exception {
    String entry = dir + File.separator + file.getName();
    Path path = Paths.get(file.getAbsolutePath());
    if (file.isFile()) {
      out.putArchiveEntry(new TarArchiveEntry(file, entry));
      try (FileInputStream in = new FileInputStream(file)) {
        IOUtils.copy(in, out);
      }
      out.closeArchiveEntry();
    } else if(file.isDirectory() && excludeFolders != null && !excludeFolders.isEmpty() && excludeFolders.contains(file.getName())) {
      logger.info("{} is excluded for tar", file.getName());
    } else if(file.isDirectory()) {
      if (!Files.isReadable(Paths.get(file.getAbsolutePath()))) {
        throw new Exception("No Read permission to " + file.getAbsolutePath());
      }
      File[] children = file.listFiles();
      if (children != null) {
        for (File child : children) {
          addToArchive(out, child, entry, excludeFolders, ignoreBrokenLinksInTar);
        }
      }
	} else if (!ignoreBrokenLinksInTar && Files.isSymbolicLink(path)) {
		/* When ignore Broken link is not set, workflow will throw the expection and error is recorded in DB and automated report
		 */
		Path target = Files.readSymbolicLink(path);
		Path resolved = path.getParent().resolve(target).normalize();
		if (!Files.exists(resolved))
			throw new Exception(
					"Broken symbolic link detected: " + resolved + " (target does not exist)");
		else if (!Files.isReadable(resolved))
			throw new Exception(
					"Broken symbolic link detected: " + resolved + " (target is inaccessible)");
	} else {
		/* When ignore Broken link is set , workflow ignores the broken links and if exclude contents file is also set then these broken links 
		 * will be added to excluded contents file and uploaded to DME
		 */
      logger.error("{} is not supported", file.getName());
    }
  }
  
  public static long getDirectorySize(Path dir, List<String> excludeFolders) throws IOException {
		final long[] size = { 0 };
		
		try {

		Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult preVisitDirectory(Path folder, BasicFileAttributes attrs) {
				// Exclude folders based on the names
				if (excludeFolders != null
						&& excludeFolders.stream().anyMatch(f -> folder.getFileName().toString().equals(f))) {
				      logger.info("{} is excluded for file size calculation", folder.getFileName().toString());
					return FileVisitResult.SKIP_SUBTREE;
				}
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
				
				 if (Files.isSymbolicLink(file)) {
                     try {
                         Path target = Files.readSymbolicLink(file);
                         Path resolved = file.getParent().resolve(target).normalize();
                         
                         if (Files.exists(resolved) && Files.isReadable(resolved)) {
                        	 size[0] += attrs.size();  // Valid symlink
                         } else {
                             logger.error("{} is not supported", file.toString());
                              // Broken or unreadable symlink
                         }
                     } catch (IOException e) {
                         logger.error("{} is not supported", file.toString());
                     	 // Couldn't resolve symlink
                     }
                 } else if (Files.isReadable(file)) {
                	 size[0] += attrs.size(); // Regular readable file
                 } else {
                     logger.error("{} is not readable", file.toString());
                 	// Not readable
                 }
				return FileVisitResult.CONTINUE;
			}

		});

		return size[0];
	} catch (IOException e) {
		logger.error("Failed to walk file tree for directory: {}", dir, e);
		throw e;
	}
  }
  
  public static boolean isSelectiveScanFileUpload(Path originalFilePath) {
	  return Files.isRegularFile(originalFilePath);
	  
  }
  
  /**
   * Filter out files with folder whose names start with any prefix in excludePrefixes.
   * @param File List of the folder/file in the Batch tarring folder 
   * @param multipleTarsExcludeFolderPrefixes 
   * @return List of files with excluded directory
   */
  public static File[] excludeBatchFoldersByPrefix(File[] files, String multipleTarsExcludeFolderPrefixes) {
	
	  List<String> excludePrefixes = multipleTarsExcludeFolderPrefixes == null || multipleTarsExcludeFolderPrefixes.isEmpty() ? null
				: new ArrayList<>(Arrays.asList(multipleTarsExcludeFolderPrefixes.split(",")));
		
	  return files = Arrays.stream(files).filter(f -> {
			if (!f.isDirectory())
				return true;

			String name = f.getName();
			return excludePrefixes.stream().filter(StringUtils::isNotBlank)
					.noneMatch(prefix -> name.startsWith(prefix));
		}).toArray(File[]::new);
  }
  
 	/**
 	 * Counts regular files under the provided list of File entries.
 	 * - If an entry is a file => counts 1
 	 * - If an entry is a directory => counts all nested regular files recursively
 	 *   while skipping any directory whose name matches exactly one of the
 	 *   provided excludeFolders (same semantics as tar creation).
 	 *
 	 * @param entries        List of files/folders
 	 * @param excludeFolders List of folder names to exclude from counting
 	 * @return total files in that list of passed files/folders, honoring exclusions
 	 */
	public static long countRegularFilesRecursively(List<File> entries, List<String> excludeFolders) throws Exception {
		if (entries == null || entries.isEmpty()) {
			return 0;
		}

		long count = 0;
		for (File entry : entries) {
			if (entry == null || !entry.exists()) {
				continue;
			}
			if (entry.isFile()) {
				count++;
			} else if (entry.isDirectory()) {
				// Skip entire directory if its name is in the exclude list.
				if (excludeFolders != null && excludeFolders.contains(entry.getName())) {
					continue;
				}
				final long[] dirCount = new long[1];
				Path p = entry.toPath();
				// Walk directory tree and count regular files, skipping excluded subtrees.
				Files.walkFileTree(p, new SimpleFileVisitor<Path>() {
					@Override
					public FileVisitResult preVisitDirectory(Path folder, BasicFileAttributes attrs)
							throws IOException {
						if (excludeFolders != null
								&& excludeFolders.stream().anyMatch(f -> folder.getFileName().toString().equals(f))) {
							logger.info("{} is excluded for files count calculation", folder.getFileName().toString());
							return FileVisitResult.SKIP_SUBTREE;
						}
						return FileVisitResult.CONTINUE;
					}

					@Override
					public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
						if (attrs.isRegularFile()) {
							dirCount[0]++;
						}
						return FileVisitResult.CONTINUE;
					}
				});
				count += dirCount[0];
			}
		}
		return count;
	}
  /**
   * Builds a batch/grouping key for a folder name by splitting it using a configurable delimiter
   * and joining the first {@code level} segments.
   * Example:
   * folderName = { "1_11_3"}, delimiter = { "_"}, level = { 2}  => { "1_11"}
   * folderName = { "a-b-c-d"}, delimiter = { "-"}, level = { 3} => { "a-b-c"}
   *
   * @param folderName The input folder name to be grouped (e.g., {@code "1_11_3"}). Must be non-blank.
   * @param delimiter  The delimiter used to split the folder name into segments (e.g., { "_"}, { "-"}, { "."})
   * @param level      Number of segments (from the start of {@ folderName}) to include in the group key.
   *                   Must be {@code >= 1}. If {@code level} is greater than the number of segments in the name,
   *                   the result is {@link Optional#empty()}.
   * @return An {@link Optional} containing the derived group key, or {@link Optional#empty()} if the inputs are invalid
   *         (blank folderName/delimiter, level &lt; 1) or the folder name does not contain enough segments.
   */
  public static Optional<String> buildBatchGroupKey(String folderName, String delimiter, int level) {
	    if (StringUtils.isBlank(folderName)) return Optional.empty();
	    if (StringUtils.isBlank(delimiter)) return Optional.empty();
	    if (level < 1) return Optional.empty();

	    String[] parts = folderName.split(Pattern.quote(delimiter));
	    if (parts.length < level) return Optional.empty();

	    String key = String.join(delimiter, Arrays.copyOfRange(parts, 0, level));
	    if (StringUtils.isBlank(key)) return Optional.empty();

	    return Optional.of(key);
	}
  
  /**
   * Checks whether the given sourceDirLeafNode matches any folder name or pattern
   * present in the multipleTarsFolders string.
   * Example input for multipleTarsFolders:
   * "abc,folderstart*,*folderend"
   * @param multipleTarsFolders comma-separated folder names or patterns
   * @param sourceDirLeafNode folder name to check
   * @return true if sourceDirLeafNode matches any folder pattern, else false
   */
  public static boolean matchesAnyMultipleTarFolder(String multipleTarsFolders, String sourceDirLeafNode) {
      if (multipleTarsFolders == null || sourceDirLeafNode == null) {
          return false;
      }

      return Arrays.stream(multipleTarsFolders.split(","))
              .map(String::trim)
              .filter(s -> !s.isEmpty())
              .anyMatch(pattern -> matchesPattern(pattern, sourceDirLeafNode));
  }
  /**
   * Converts a wildcard pattern into regex and checks whether it matches
   * the given sourceDirLeafNode.
   */
  private static boolean matchesPattern(String pattern, String sourceDirLeafNode) {
      String regex = pattern
              .replace(".", "\\.")
              .replace("*", ".*");
      // Check if the sourceDirLeafNode matches the generated regex
      return sourceDirLeafNode.matches(regex);
  }
}
