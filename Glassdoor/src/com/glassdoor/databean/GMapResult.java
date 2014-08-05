package com.glassdoor.databean;

public class GMapResult {
	
	 private Geometry geometry;
	 private String name;
	 private String vicinity;
	 private String icon;
	 private String id;
	 private OpenHours opening_hours;
	 private Photo[] photos;
	 private String place_id;
	 private String scope;
	 private String reference;
	 private String[] types;
	 private Integer price_level;
	 private Double rating;
	 private String formatted_address;
	 

	public Geometry getGeometry() {
		return geometry;
	}

	public void setGeometry(Geometry geometry) {
		this.geometry = geometry;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getVicinity() {
		return vicinity;
	}

	public void setVicinity(String vicinity) {
		this.vicinity = vicinity;
	}

	public String getIcon() {
		return icon;
	}

	public void setIcon(String icon) {
		this.icon = icon;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}


	public String getPlace_id() {
		return place_id;
	}

	public void setPlace_id(String place_id) {
		this.place_id = place_id;
	}

	public String getScope() {
		return scope;
	}

	public void setScope(String scope) {
		this.scope = scope;
	}

	public String getReference() {
		return reference;
	}

	public void setReference(String reference) {
		this.reference = reference;
	}

	public String[] getTypes() {
		return types;
	}

	public void setTypes(String[] types) {
		this.types = types;
	}

	public OpenHours getOpening_hours() {
		return opening_hours;
	}

	public void setOpening_hours(OpenHours opening_hours) {
		this.opening_hours = opening_hours;
	}

	public Photo[] getPhotos() {
		return photos;
	}

	public void setPhotos(Photo[] photos) {
		this.photos = photos;
	}

	public Integer getPrice_level() {
		return price_level;
	}

	public void setPrice_level(Integer price_level) {
		this.price_level = price_level;
	}

	public Double getRating() {
		return rating;
	}

	public void setRating(Double rating) {
		this.rating = rating;
	}

	public String getFormatted_address() {
		return formatted_address;
	}

	public void setFormatted_address(String formatted_address) {
		this.formatted_address = formatted_address;
	}

}
