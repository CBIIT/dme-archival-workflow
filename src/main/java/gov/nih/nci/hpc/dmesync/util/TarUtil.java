package gov.nih.nci.hpc.dmesync.util;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

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
  public static void tar(String name, List<String> excludeFolders, File... files) throws Exception {
    try (TarArchiveOutputStream out = getTarArchiveOutputStream(name); ) {
      for (File file : files) {
        addToArchive(out, file, ".", excludeFolders);
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
  public static void targz(String name, List<String> excludeFolders, File... files) throws Exception {
    try (TarArchiveOutputStream out = getTarGzArchiveOutputStream(name); ) {
      for (File file : files) {
        addToArchive(out, file, ".", excludeFolders);
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
        File curfile = new File(outFile, entry.getName());
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
  public static void deleteTar(String tarFile, String workDir) throws IOException {
    Path filePath = Paths.get(tarFile);
    Path basePath = Paths.get(workDir).toRealPath();

    // Make sure we are removing from the work directory ONLY
    if (!filePath.startsWith(basePath)) return;

    //Delete the tar file and the parent dir until the work base directory if there are no other files
    removeFileAndParentsIfEmpty(filePath, basePath);
  }

  private static void removeFileAndParentsIfEmpty(Path path, Path basePath) throws IOException {
    if (path == null || path.endsWith(basePath)) return;

    if (path.toFile().isFile()) {
      Files.deleteIfExists(path);
    } else if (path.toFile().isDirectory()) {
      try {
        Files.delete(path);
      } catch (DirectoryNotEmptyException | NoSuchFileException e ) {
        //There could be common parent for the files being processed, the last one processing will remove the parent work folder.
        //Another thread might have removed it
        return;
      }
    }

    removeFileAndParentsIfEmpty(path.getParent(), basePath);
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
   * @param name the tar file name
   * @return number of files in tar
   * @throws IOException on IO error
   */
  public static int countFilesinTar(String name) throws IOException {
    List<HpcPathAttributes> entries = new ArrayList<>();
    int numFiles = 0;
    try (FileInputStream fis = new FileInputStream(name);
            TarArchiveInputStream tais = new TarArchiveInputStream(fis)) {
      // Iterate over each entry in the tar archive
      TarArchiveEntry entry;
      while ((entry = tais.getNextTarEntry()) != null) {
          if (!entry.isDirectory()) {
              numFiles++;
          }
      }
    }
    return numFiles;
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

  private static void addToArchive(TarArchiveOutputStream out, File file, String dir, List<String> excludeFolders)
      throws Exception {
    String entry = dir + File.separator + file.getName();
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
          addToArchive(out, child, entry, excludeFolders);
        }
      }
    } else {
      logger.error("{} is not supported", file.getName());
    }
  }
  
}
