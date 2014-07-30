package com.glassdoor.databean;


public class JobListing {
	
	private boolean success;
	private JobResponse response;

	public boolean isSuccess() {
		return success;
	}

	public void setSuccess(boolean success) {
		this.success = success;
	}

	public JobResponse getResponse() {
		return response;
	}

	public void setResponse(JobResponse response) {
		this.response = response;
	}
	
}
