package com.glassdoor.databean;

import java.util.List;

public class JobResponse {

	private List<GlassdoorJobData> jobListings;
	private Integer currentPageNumber;
	private Integer totalNumberOfPages;

	public List<GlassdoorJobData> getJobListings() {
		return jobListings;
	}

	public Integer getTotalNumberOfPages() {
		return totalNumberOfPages;
	}

	public void setTotalNumberOfPages(Integer totalNumberOfPages) {
		this.totalNumberOfPages = totalNumberOfPages;
	}

	public Integer getCurrentPageNumber() {
		return currentPageNumber;
	}

	public void setCurrentPageNumber(Integer currentPageNumber) {
		this.currentPageNumber = currentPageNumber;
	}

}
