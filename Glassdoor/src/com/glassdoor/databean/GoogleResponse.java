package com.glassdoor.databean;

public class GoogleResponse {
	
	private GoogleResult[] results ;
	 private String status ;
	 private String error_message;
	 
	 public GoogleResult[] getResults() {
	  return results;
	 }
	 public void setResults(GoogleResult[] results) {
	  this.results = results;
	 }
	 public String getStatus() {
	  return status;
	 }
	 public void setStatus(String status) {
	  this.status = status;
	 }
	public String getError_message() {
		return error_message;
	}
	public void setError_message(String error_message) {
		this.error_message = error_message;
	}
	 

}
