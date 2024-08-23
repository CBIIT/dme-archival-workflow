package gov.nih.nci.hpc.dmesync.dto;

import java.io.Serializable;

public class DmeSyncStats implements Serializable {

	private String date;
	private Long count;
	private Long size;

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}

	public Long getSize() {
		return size;
	}

	public void setSize(Long size) {
		this.size = size;
	}

}
