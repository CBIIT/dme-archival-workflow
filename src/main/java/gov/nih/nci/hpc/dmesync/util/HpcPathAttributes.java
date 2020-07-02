/*******************************************************************************
 * Copyright SVG, Inc.
 * Copyright Leidos Biomedical Research, Inc.
 *  
 * Distributed under the OSI-approved BSD 3-Clause License.
 * See https://github.com/CBIIT/HPC_DME_APIs/LICENSE.txt for details.
 ******************************************************************************/
package gov.nih.nci.hpc.dmesync.util;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Date;

public class HpcPathAttributes implements Serializable, Comparable<HpcPathAttributes> {

	private static final long serialVersionUID = 1L;
	protected boolean exists;
	protected boolean isFile;
	protected boolean isDirectory;
	protected long size;
	protected boolean isAccessible;
	protected Date updatedDate;
	protected String path;
	protected String absolutePath;
	protected String name;
	protected String tarEntry;

	public String getAbsolutePath() {
		return absolutePath;
	}

	public void setAbsolutePath(String absolutePath) {
		this.absolutePath = absolutePath;
	}

	public Date getUpdatedDate() {
		return updatedDate;
	}

	public void setUpdatedDate(Date updatedDate) {
		this.updatedDate = updatedDate;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Gets the value of the exists property. This getter has been renamed from
	 * isExists() to getExists() by cxf-xjc-boolean plugin.
	 * @return true of false
	 */
	public boolean getExists() {
		return exists;
	}

	/**
	 * Sets the value of the exists property.
	 * @param value The value to set
	 */
	public void setExists(boolean value) {
		this.exists = value;
	}

	/**
	 * Gets the value of the isFile property. This getter has been renamed from
	 * isIsFile() to getIsFile() by cxf-xjc-boolean plugin.
	 * @return true of false
	 */
	public boolean getIsFile() {
		return isFile;
	}

	/**
	 * Sets the value of the isFile property.
	 * @param value The value to set
	 */
	public void setIsFile(boolean value) {
		this.isFile = value;
	}

	/**
	 * Gets the value of the isDirectory property. This getter has been renamed
	 * from isIsDirectory() to getIsDirectory() by cxf-xjc-boolean plugin.
	 * @return true of false
	 */
	public boolean getIsDirectory() {
		return isDirectory;
	}

	/**
	 * Sets the value of the isDirectory property.
	 * @param value The value to set
	 */
	public void setIsDirectory(boolean value) {
		this.isDirectory = value;
	}

	/**
	 * Gets the value of the size property.
	 * @return the size
	 */
	public long getSize() {
		return size;
	}

	/**
	 * Sets the value of the size property.
	 * @param value The value to set
	 */
	public void setSize(long value) {
		this.size = value;
	}

	/**
	 * Gets the value of the isAccessible property. This getter has been renamed
	 * from isIsAccessible() to getIsAccessible() by cxf-xjc-boolean plugin.
	 * @return true of false
	 */
	public boolean getIsAccessible() {
		return isAccessible;
	}

	/**
	 * Sets the value of the isAccessible property.
	 * @param value The value to set
	 */
	public void setIsAccessible(boolean value) {
		this.isAccessible = value;
	}

	public String getTarEntry() {
  return tarEntry;}

  public void setTarEntry(String tarEntry) {
  this.tarEntry = tarEntry;}

  public static final Comparator<HpcPathAttributes> pathComparator = new Comparator<HpcPathAttributes>() {

		public int compare(HpcPathAttributes path1, HpcPathAttributes path2) {

			String absolutePath1 = path1.getAbsolutePath().toUpperCase();
			String absolutePath2 = path1.getAbsolutePath().toUpperCase();

			// ascending order
			return absolutePath1.compareTo(absolutePath2);
		}
	};

	@Override
	public int compareTo(HpcPathAttributes o) {
		return this.absolutePath.compareTo(o.getAbsolutePath());
	}

  @Override
  public String toString() {
    return "HpcPathAttributes [exists=" + exists + ", isFile=" + isFile + ", isDirectory="
        + isDirectory + ", size=" + size + ", isAccessible=" + isAccessible + ", updatedDate="
        + updatedDate + ", path=" + path + ", absolutePath=" + absolutePath + ", name=" + name
        + "]";
  }
	
	
}
