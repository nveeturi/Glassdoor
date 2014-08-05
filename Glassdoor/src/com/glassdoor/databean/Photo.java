package com.glassdoor.databean;

public class Photo {

	private Long height;
	private String[] html_attributions;
	private String photo_reference;
	private Long width;
	public Long getHeight() {
		return height;
	}
	public void setHeight(Long height) {
		this.height = height;
	}
	public String[] getHtml_attributions() {
		return html_attributions;
	}
	public void setHtml_attributions(String[] html_attributions) {
		this.html_attributions = html_attributions;
	}
	public String getPhoto_reference() {
		return photo_reference;
	}
	public void setPhoto_reference(String photo_reference) {
		this.photo_reference = photo_reference;
	}
	public Long getWidth() {
		return width;
	}
	public void setWidth(Long width) {
		this.width = width;
	}
	
}
