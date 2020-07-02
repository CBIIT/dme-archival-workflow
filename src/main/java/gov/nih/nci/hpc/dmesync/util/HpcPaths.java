package gov.nih.nci.hpc.dmesync.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/** Collects filesystem paths using wildcards, preserving the directory structure. Copies, deletes, and zips paths. */
public class HpcPaths implements Iterable<String> {
  private static final Comparator<Path> LONGEST_TO_SHORTEST = new Comparator<Path>() {
		public int compare (Path s1, Path s2) {
			return s2.absolute().length() - s1.absolute().length();
		}
	};

	private static List<String> defaultGlobExcludes;

	final HashSet<Path> paths = new HashSet<>(32);

	/** Creates an empty Paths object. */
	public HpcPaths () {
	}

	/** Creates a Paths object and calls {@link #glob(String, int, String[])} with the specified arguments.
	 * @param dir the directory
	 * @param depth the depth
	 * @param patterns the patterns specified
	 */
	public HpcPaths (String dir, int depth, String... patterns) {
		glob(dir, depth, patterns);
	}

	/** Creates a Paths object and calls {@link #glob(String, int, List)} with the specified arguments.
	 * @param dir the directory
	 * @param depth the depth
	 * @param patterns the list of patterns specified
	 */
	public HpcPaths (String dir, int depth, List<String> patterns) {
		glob(dir, depth, patterns);
	}

	/**
	 * Creates a GlobScanner with the specified arguments.
	 * @param dir the directory
	 * @param ignoreCase flag to ignore case
	 * @param depth the depth
	 * @param patterns the patterns specified
	 * @return the paths
	 */
	private HpcPaths glob (String dir, boolean ignoreCase, int depth, String... patterns) {
		if (dir == null) dir = ".";
		if (patterns != null && patterns.length == 0) {
			String[] split = dir.split("\\|");
			if (split.length > 1) {
				dir = split[0];
				patterns = new String[split.length - 1];
				for (int i = 1, n = split.length; i < n; i++)
					patterns[i - 1] = split[i];
			}
		}
		File dirFile = new File(dir);
		if (!dirFile.exists()) return this;

		List<String> includes = new ArrayList<>();
		List<String> excludes = new ArrayList<>();
		if (patterns != null) {
			for (String pattern : patterns) {
				if (pattern.charAt(0) == '!')
					excludes.add(pattern.substring(1));
				else
					includes.add(pattern);
			}
		}
		if (includes.isEmpty()) includes.add("**");

		if (defaultGlobExcludes != null) excludes.addAll(defaultGlobExcludes);

		GlobScanner scanner = new GlobScanner(dirFile, includes, excludes, ignoreCase, depth);
		String rootDir = scanner.rootDir().getPath().replace('\\', '/');
		if (!rootDir.endsWith("/")) rootDir += '/';
		for (String filePath : scanner.matches())
			paths.add(new Path(rootDir, filePath));
		return this;
	}

	/** Collects all files and directories in the specified directory matching the wildcard patterns.
	 * @param dir The directory containing the paths to collect. If it does not exist, no paths are collected. If null, "." is
	 *           assumed.
	 * @param depth the depth
	 * @param patterns The wildcard patterns of the paths to collect or exclude. Patterns may optionally contain wildcards
	 *           represented by asterisks and question marks. If empty or omitted then the dir parameter is split on the "|"
	 *           character, the first element is used as the directory and remaining are used as the patterns. If null, ** is
	 *           assumed (collects all paths).<br>
	 * <br>
	 *           A single question mark (?) matches any single character. Eg, something? collects any path that is named
	 *           "something" plus any character.<br>
	 * <br>
	 *           A single asterisk (*) matches any characters up to the next slash (/). Eg, *\*\something* collects any path that
	 *           has two directories of any name, then a file or directory that starts with the name "something".<br>
	 * <br>
	 *           A double asterisk (**) matches any characters. Eg, **\something\** collects any path that contains a directory
	 *           named "something".<br>
	 * <br>
	 *           A pattern starting with an exclamation point (!) causes paths matched by the pattern to be excluded, even if other
	 *           patterns would select the paths.
	 * @return the paths */
	public HpcPaths glob (String dir, int depth, String... patterns) {
		return glob(dir, false, depth, patterns);
	}

	/** Case insensitive glob.
	 * @see #glob(String, int, String...)
	 * @param dir the directory
     * @param depth the depth
     * @param patterns the patterns specified
     * @return the paths
     */
	public HpcPaths globIgnoreCase (String dir, int depth, String... patterns) {
		return glob(dir, true, depth, patterns);
	}

	/** Case sensitive glob.
	 * @see #glob(String, int, String...)
	 * @param dir the directory
	 * @param depth the depth
	 * @param patterns the patterns specified
	 * @return the paths
	 */
	public HpcPaths glob (String dir, int depth, List<String> patterns) {
		if (patterns == null) throw new IllegalArgumentException("patterns cannot be null.");
		glob(dir, false, depth, patterns.toArray(new String[patterns.size()]));
		return this;
	}

	/** Case insensitive glob.
	 * @see #glob(String, int, String...)
	 * @param dir the directory
     * @param depth the depth
     * @param patterns the patterns specified
     * @return the paths
	 *  */
	public HpcPaths globIgnoreCase (String dir, int depth, List<String> patterns) {
		if (patterns == null) throw new IllegalArgumentException("patterns cannot be null.");
		glob(dir, true, depth, patterns.toArray(new String[patterns.size()]));
		return this;
	}

	/** Collects all files and directories in the specified directory matching the regular expression patterns. This method is much
	 * slower than {@link #glob(String, int, String...)} because every file and directory under the specified directory must be
	 * inspected.
	 * @param dir The directory containing the paths to collect. If it does not exist, no paths are collected.
	 * @param patterns The regular expression patterns of the paths to collect or exclude. If empty or omitted then the dir
	 *           parameter is split on the "|" character, the first element is used as the directory and remaining are used as the
	 *           patterns. If null, ** is assumed (collects all paths).<br>
	 * <br>
	 *           A pattern starting with an exclamation point (!) causes paths matched by the pattern to be excluded, even if other
	 *           patterns would select the paths. 
	 * @return the paths */
	public HpcPaths regex (String dir, String... patterns) {
		if (dir == null) dir = ".";
		if (patterns != null && patterns.length == 0) {
			String[] split = dir.split("\\|");
			if (split.length > 1) {
				dir = split[0];
				patterns = new String[split.length - 1];
				for (int i = 1, n = split.length; i < n; i++)
					patterns[i - 1] = split[i];
			}
		}
		File dirFile = new File(dir);
		if (!dirFile.exists()) return this;

		List<String> includes = new ArrayList<>();
		List<String> excludes = new ArrayList<>();
		if (patterns != null) {
			for (String pattern : patterns) {
				if (pattern.charAt(0) == '!')
					excludes.add(pattern.substring(1));
				else
					includes.add(pattern);
			}
		}
		if (includes.isEmpty()) includes.add(".*");

		RegexScanner scanner = new RegexScanner(dirFile, includes, excludes);
		String rootDir = scanner.rootDir().getPath().replace('\\', '/');
		if (!rootDir.endsWith("/")) rootDir += '/';
		for (String filePath : scanner.matches())
			paths.add(new Path(rootDir, filePath));
		return this;
	}

	/** Copies the files and directories to the specified directory.
	 * @param destDir the destination directory
	 * @return A paths object containing the paths of the new files.
	 * @throws IOException on IO error */
	public HpcPaths copyTo (String destDir) throws IOException {
		HpcPaths newPaths = new HpcPaths();
		for (Path path : paths) {
			File destFile = new File(destDir, path.name);
			File srcFile = path.file();
			if (srcFile.isDirectory()) {
				destFile.mkdirs();
			} else {
				destFile.getParentFile().mkdirs();
				copyFile(srcFile, destFile);
			}
			newPaths.paths.add(new Path(destDir, path.name));
		}
		return newPaths;
	}

	/** Deletes all the files, directories, and any files in the directories.
	 * @return False if any file could not be deleted. */
	public boolean delete () {
		boolean success = true;
		List<Path> pathsCopy = new ArrayList<>(paths);
		Collections.sort(pathsCopy, LONGEST_TO_SHORTEST);
		for (File file : getFiles(pathsCopy)) {
			if (file.isDirectory()) {
				if (!deleteDirectory(file)) success = false;
			} else {
				if (!file.delete()) success = false;
			}
		}
		return success;
	}

	/** Compresses the files and directories specified by the paths into a new zip file at the specified location. If there are no
	 * paths or all the paths are directories, no zip file will be created. 
	 * @param destFile the destination file
	 * @throws IOException on IO error
	 */
	public void zip (String destFile) throws IOException {
		HpcPaths zipPaths = filesOnly();
		if (zipPaths.paths.isEmpty()) return;
		byte[] buf = new byte[1024];
		ZipOutputStream out = new ZipOutputStream(new FileOutputStream(destFile));
		out.setLevel(Deflater.BEST_COMPRESSION);
		try {
			for (Path path : zipPaths.paths) {
				File file = path.file();
				out.putNextEntry(new ZipEntry(path.name.replace('\\', '/')));
				try (FileInputStream in = new FileInputStream(file)) {
				  int len;
				  while ((len = in.read(buf)) > 0)
				    out.write(buf, 0, len);
				}
				out.closeEntry();
			}
		} finally {
			out.close();
		}
	}

	public int count () {
		return paths.size();
	}

	public boolean isEmpty () {
		return paths.isEmpty();
	}

	/** Returns the absolute paths delimited by the specified character.
	 * @param delimiter the delimiter
	 * @return the absolute paths delimited by the specified character
	 */
	public String toString (String delimiter) {
		StringBuilder sb = new StringBuilder();
		for (String path : getPaths()) {
			if (sb.length() > 0) sb.append(delimiter);
			sb.append(path);
		}
		return sb.toString();
	}

	/** Returns the absolute paths delimited by commas. */
	public String toString () {
		return toString(", ");
	}

	/** Returns a Paths object containing the paths that are files, as if each file were selected from its parent directory. 
	 * @return the paths */
	public HpcPaths flatten () {
		HpcPaths newPaths = new HpcPaths();
		for (Path path : paths) {
			File file = path.file();
			if (file.isFile()) newPaths.paths.add(new Path(file.getParent(), file.getName()));
		}
		return newPaths;
	}

	/** Returns a Paths object containing the paths that are files.
	 * @return the paths
	 */
	public HpcPaths filesOnly () {
		HpcPaths newPaths = new HpcPaths();
		for (Path path : paths) {
			if (path.file().isFile()) newPaths.paths.add(path);
		}
		return newPaths;
	}

	/** Returns a Paths object containing the paths that are directories.
	 * @return the paths
	 */
	public HpcPaths dirsOnly () {
		HpcPaths newPaths = new HpcPaths();
		for (Path path : paths) {
			if (path.file().isDirectory()) newPaths.paths.add(path);
		}
		return newPaths;
	}

	/** Returns the paths as File objects.
	 * @return the list of files
	 */
	public List<File> getFiles () {
		return getFiles(new ArrayList<>(paths));
	}

	private ArrayList<File> getFiles (List<Path> paths) {
		ArrayList<File> files = new ArrayList<>(paths.size());
		for (Path path : paths)
			files.add(path.file());
		return files;
	}

	/** Returns the portion of the path after the root directory where the path was collected. 
	 * @return the portion of the path after the root directory where the path was collected */
	public List<String> getRelativePaths () {
		ArrayList<String> stringPaths = new ArrayList<>(paths.size());
		for (Path path : paths)
			stringPaths.add(path.name);
		return stringPaths;
	}

	/** Returns the full paths. 
	 * @return the full paths
	 */
	public List<String> getPaths () {
		ArrayList<String> stringPaths = new ArrayList<>(paths.size());
		for (File file : getFiles())
			stringPaths.add(file.getPath());
		return stringPaths;
	}

	/** Returns the paths' filenames. 
	 * @return the paths' filenames
	 */
	public List<String> getNames () {
		ArrayList<String> stringPaths = new ArrayList<>(paths.size());
		for (File file : getFiles())
			stringPaths.add(file.getName());
		return stringPaths;
	}

	/** Adds a single path to this Paths object.
	 * @param fullPath the full path
	 * @return a single path to this Paths object
	 */
	public HpcPaths addFile (String fullPath) {
		File file = new File(fullPath);
		String parent = file.getParent();
		paths.add(new Path(parent == null ? "" : parent, file.getName()));
		return this;
	}

	/** Adds a single path to this Paths object.
	 * @param dir the directory name
	 * @param name the name to add
	 * @return a single path to this Paths object
	 */
	public HpcPaths add (String dir, String name) {
		paths.add(new Path(dir, name));
		return this;
	}

	/** Adds all paths from the specified Paths object to this Paths object.
	 * @param paths the paths
	 */
	public void add (HpcPaths paths) {
		this.paths.addAll(paths.paths);
	}

	/** Iterates over the absolute paths. The iterator supports the remove method. */
	public Iterator<String> iterator () {
		return new Iterator<String>() {
			private Iterator<Path> iter = paths.iterator();

			@Override
			public void remove () {
				iter.remove();
			}

			public String next () {
				return iter.next().absolute();
			}

			public boolean hasNext () {
				return iter.hasNext();
			}
		};
	}

	/** Iterates over the paths as File objects. The iterator supports the remove method.
	 * @return the iterator for file object
	 */
	public Iterator<File> fileIterator () {
		return new Iterator<File>() {
			private Iterator<Path> iter = paths.iterator();

			@Override
			public void remove () {
				iter.remove();
			}

			public File next () {
				return iter.next().file();
			}

			public boolean hasNext () {
				return iter.hasNext();
			}
		};
	}

	private static final class Path {
		public final String dir;
		public final String name;

		public Path (String dir, String name) {
			if (dir.length() > 0 && !dir.endsWith("/")) dir += "/";
			this.dir = dir;
			this.name = name;
		}

		public String absolute () {
			return dir + name;
		}

		public File file () {
			return new File(dir, name);
		}

		public int hashCode () {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((dir == null) ? 0 : dir.hashCode());
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			return result;
		}

		public boolean equals (Object obj) {
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			Path other = (Path)obj;
			if (dir == null) {
				if (other.dir != null) return false;
			} else if (!dir.equals(other.dir)) return false;
			if (name == null) {
				if (other.name != null) return false;
			} else if (!name.equals(other.name)) return false;
			return true;
		}
	}

	/** Sets the exclude patterns that will be used in addition to the excludes specified for all glob searches.
	 * @param defaultGlobExcludes the glob exclude pattern
	 */
	public static void setDefaultGlobExcludes (String... defaultGlobExcludes) {
		HpcPaths.defaultGlobExcludes = Arrays.asList(defaultGlobExcludes);
	}

	/** Copies one file to another.
	 * @param in the file to copy from
	 * @param out the file to copy to
	 * @throws IOException on IO error
	 */
	private static void copyFile (File in, File out) throws IOException {
		try (FileChannel sourceChannel = new FileInputStream(in).getChannel()) {
		  try(FileChannel destinationChannel = new FileOutputStream(out).getChannel()) {
		    sourceChannel.transferTo(0, sourceChannel.size(), destinationChannel);
		  }
		}
	}

	/** Deletes a directory and all files and directories it contains.
	 * @param file the file
	 * @return true or false
	 */
	private static boolean deleteDirectory (File file) {
		if (file.exists()) {
			File[] files = file.listFiles();
			for (int i = 0, n = files.length; i < n; i++) {
				if (files[i].isDirectory())
					deleteDirectory(files[i]);
				else
					files[i].delete();
			}
		}
		return file.delete();
	}
}
