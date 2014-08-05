package com.glassdoor.databean;

public class GlassdoorJobData {

	private Long jobListingId;
	private String jobTitle;
	private Employer employer;
	private String location;
	private String source;
	private String date;
	private Long hoursOld;
	private String jobViewUrl;
	private String descriptionFragment;
	
	public Long getJobListingId() {
		return jobListingId;
	}
	public void setJobListingId(Long jobListingId) {
		this.jobListingId = jobListingId;
	}
	public String getJobTitle() {
		return jobTitle;
	}
	public void setJobTitle(String jobTitle) {
		this.jobTitle = jobTitle;
	}
	public Employer getEmployer() {
		return employer;
	}
	public void setEmployer(Employer employer) {
		this.employer = employer;
	}
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public Long getHoursOld() {
		return hoursOld;
	}
	public void setHoursOld(Long hoursOld) {
		this.hoursOld = hoursOld;
	}
	public String getJobViewUrl() {
		return jobViewUrl;
	}
	public void setJobViewUrl(String jobViewUrl) {
		this.jobViewUrl = jobViewUrl;
	}
	public String getDescriptionFragment() {
		return descriptionFragment;
	}
	public void setDescriptionFragment(String descriptionFragment) {
		this.descriptionFragment = descriptionFragment;
	}
	
	
}
