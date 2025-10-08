package gov.nih.nci.hpc.dmesync.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import gov.nih.nci.hpc.dmesync.exception.DmeSyncMappingException;

public class TarContentsFileUtil {

	static final Logger logger = LoggerFactory.getLogger(TarContentsFileUtil.class);

	private TarContentsFileUtil() {
		
	}
   /* This method writes to contents file 
    *  notesWrtitee
    *  tarFileHeader: The title of the file
    *  List<File> list of files
    * 
    */
	public static boolean writeToTarContentsFile(BufferedWriter textWriter, String tarFileHeader, List<File> subList)
			throws IOException, DmeSyncMappingException {
		
		try {
			logger.info("Writing Files list to TarContents file Started {}", tarFileHeader);
			textWriter.write("Tar File: " + tarFileHeader + "\n");
			for (File fileName : subList) {
				if (fileName != null) {
					Path filePath = fileName.toPath();
					File sourceDir = new File(tarFileHeader);
					Path sourcePath = sourceDir.toPath();
					if (Files.isSymbolicLink(filePath)) {
						// file is a symlink
						Path target = Files.readSymbolicLink(filePath);
						// Get the relative path from sourceDir to the file
						Path relativePath = sourcePath.relativize(filePath);
						Path symlinkTarget = filePath.getParent().resolve(target).normalize();
						textWriter.write(relativePath + " ->  " + symlinkTarget + "\n");
					} else {

						// Get the relative path from sourceDir to the file
						Path relativePath = sourcePath.relativize(filePath);

						textWriter.write(relativePath + "\n");
					}
				}
			}
		textWriter.write("\n");
        logger.info("Writing Files list to TarContents file Completed {}" , tarFileHeader);
        
        textWriter.close();
        return true;

	}catch ( IOException e) {
        textWriter.close();
		logger.error("Error writing data to the contents file {}", tarFileHeader);
		throw new DmeSyncMappingException("Error writing data to the contents file ", e);
	}
	}

}
