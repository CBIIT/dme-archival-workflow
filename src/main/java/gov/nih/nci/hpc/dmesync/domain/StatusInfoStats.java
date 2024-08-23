package gov.nih.nci.hpc.dmesync.domain;

import java.io.Serializable;

public class StatusInfoStats implements Serializable {

	private String label;
	private Long value;

	public StatusInfoStats(String label, Long value) {
		this.label = label;
		this.value = value;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public Long getValue() {
		return value;
	}

	public void setValue(Long value) {
		this.value = value;
	}

}
