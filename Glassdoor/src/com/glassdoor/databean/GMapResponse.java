package com.glassdoor.databean;

public class GMapResponse {

	private String[] html_attributions;
	private GMapResult[] results ;
	 private String status ;
	 private String next_page_token;
	 private String error_message;
	 
	 public GMapResult[] getResults() {
	  return results;
	 }
	 public void setResults(GMapResult[] results) {
	  this.results = results;
	 }
	 public String getStatus() {
	  return status;
	 }
	 public void setStatus(String status) {
	  this.status = status;
	 }
	public String[] getHtml_attributions() {
		return html_attributions;
	}
	public void setHtml_attributions(String[] html_attributions) {
		this.html_attributions = html_attributions;
	}
	public String getNext_page_token() {
		return next_page_token;
	}
	public void setNext_page_token(String next_page_token) {
		this.next_page_token = next_page_token;
	}
	public String getError_message() {
		return error_message;
	}
	public void setError_message(String error_message) {
		this.error_message = error_message;
	}
	 
}
