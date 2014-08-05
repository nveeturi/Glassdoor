package com.glassdoor.jobsearch;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLProtocolException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.http.client.utils.URLEncodedUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.glassdoor.dao.JobSearchDAO;
import com.glassdoor.databean.GMapResponse;
import com.glassdoor.databean.GMapResult;
import com.glassdoor.databean.GlassdoorJobData;
import com.glassdoor.databean.GoogleResponse;
import com.glassdoor.databean.GoogleResult;
import com.glassdoor.databean.JobDetails;
import com.glassdoor.databean.JobListing;
import com.glassdoor.databean.Location;
import com.google.gson.Gson;

public class JobSearchService {
	private static String passCode = "19754";
	private static String key = "IFuttOxoRG";
	private static String localIP = "127.0.0.1";
	private static String CB_API_KEY = "WDHV5C05WLC5PSQMN6TQ";
	private static final String MAPS_API_KEY = "AIzaSyCfvk1pQI4gVpVe3Q_PX9_6eKc6n8e4E1k";
	private static final String GEOCODE_URL = "http://maps.googleapis.com/maps/api/geocode/json";
	private static final String MAPS_NEARBY_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json";
	private static final String MAPS_TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json";
	private static String[] US_STATES = { "Alabama", "Alaska", "Arizona",
			"Arkansas", "California", "Colorado", "Connecticut", "Delaware",
			"District of Columbia", "Florida", "Georgia", "Hawaii", "Idaho",
			"Illinois", "Indiana", "Iowa",

			"Kansas", "Kentucky", "Louisiana", "Maine", "Maryland",
			"Massachusetts", "Michigan", "Minnesota",

			"Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
			"New Hampshire", "New Jersey", "New Mexico",

			"New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma",
			"Oregon", "Pennsylvania", "Rhode Island",

			"South Carolina", "South Dakota", "Tennessee", "Texas", "Utah",
			"Vermont", "Virginia", "Washington",

			"West Virginia", "Wisconsin", "Wyoming" };

	private static String[] StateCodes = { "AL", "AK", "AZ", "AR", "CA", "CO",
			"CT", "DC", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS",
			"KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE",
			"NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA",
			"RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI",
			"WY" };

	public List<JobDetails> getJobDataFromGlassdoor(String jobTitle, String city)
			throws IOException {
		int pageNum = 1;
		int totalpages = 1;
		Long prevJobId = 0L;
		List<JobDetails> jobResult = new ArrayList<JobDetails>();
		while (pageNum <= totalpages) {

			StringBuilder urlString = new StringBuilder(
					"http://api.glassdoor.com/api/api.htm?");
			urlString.append("t.p=" + passCode);
			urlString.append("&t.k=" + key);
			urlString.append("&userip=" + localIP);
			urlString.append("&useragent=");
			urlString.append("&format=json");
			urlString.append("&v=1.1");
			urlString.append("&action=jobs");
			urlString.append("&q=" + jobTitle);
			urlString.append("&l=" + city);
			urlString.append("&pn=" + pageNum);
			urlString.append("&ps=" + 50);
			URL url = new URL(urlString.toString());
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("GET");
			BufferedReader br = new BufferedReader(new InputStreamReader(
					(con.getInputStream())));
			StringBuilder response = new StringBuilder();
			String output;
			while ((output = br.readLine()) != null) {
				response.append(output);
			}

			Gson gson = new Gson();
			JobListing jobListings = gson.fromJson(response.toString(),
					JobListing.class);
			List<GlassdoorJobData> joblist = jobListings.getResponse()
					.getJobListings();
			totalpages = jobListings.getResponse().getTotalNumberOfPages();
			System.out.println(pageNum);

			pageNum++;

			for (GlassdoorJobData job : joblist) {
				JobDetails jobdetail = new JobDetails();
				if (prevJobId.longValue() == job.getJobListingId().longValue()) {
					continue;
				}
				jobdetail.setJobTitle(job.getJobTitle());
				String[] location = job.getLocation().split(",");
				jobdetail.setCity(location[0]);
				jobdetail.setState(location[1]);
				jobdetail.setCompanyName(job.getEmployer().getName());
				jobdetail.setJobId(job.getJobListingId());
				prevJobId = job.getJobListingId();
				jobdetail.setJobLink(job.getJobViewUrl());
				jobdetail.setSource(job.getSource());
				jobdetail.setCountry("USA");
				jobdetail.setJobDescription(job.getDescriptionFragment());
				jobResult.add(jobdetail);

			}

		}

		return jobResult;

	}

	public void saveJobDetails(List<JobDetails> jobdetails) {

		JobSearchDAO dao = new JobSearchDAO();
		System.out.println("Saving Job Details: " + jobdetails);
		dao.insertJobDetails(jobdetails);

	}

	public List<JobDetails> getCBJobData() {
		List<JobDetails> details = null;
		JobSearchDAO dao = new JobSearchDAO();
		details = dao.getCBJobDetails();
		return details;
	}

	public void updateLocation(String jobTitle, String city, JobDetails details)
			throws XPathExpressionException, SAXException, IOException,
			ParserConfigurationException {

		if (details.getJobRefID() != null && !details.getJobRefID().equals("")) {
			callCBJobRefURL(details);
		} else if (details.getLatitude() == null) {
			int pageNum = 1;
			int totalpages = 1;
			HttpURLConnection con = null;

			while (pageNum <= totalpages) {
				StringBuilder urlString = new StringBuilder(
						"http://api.careerbuilder.com/v1/jobsearch?");
				urlString.append("DeveloperKey=" + CB_API_KEY);
				urlString.append("&Keywords=" + jobTitle);
				urlString.append("&Location=" + city);
				urlString.append("&PageNumber=" + pageNum);
				URL url = new URL(urlString.toString());
				StringBuilder response = null;
				try {
					System.out.println("Processing page number: " + pageNum);
					con = (HttpURLConnection) url.openConnection();
					con.setRequestMethod("GET");
					BufferedReader br = new BufferedReader(
							new InputStreamReader((con.getInputStream())));
					response = new StringBuilder();
					pageNum++;
					String output;
					while ((output = br.readLine()) != null) {
						response.append(output);
					}
				} catch (IOException e) {
					System.out.println(e.getMessage());
					if (e.getMessage().contains("400")) {
						break;
					}

				}
				DocumentBuilderFactory builderFactory = DocumentBuilderFactory
						.newInstance();
				DocumentBuilder builder = builderFactory.newDocumentBuilder();
				Document document = builder.parse(new ByteArrayInputStream(
						response.toString().getBytes()));

				// parse xml with Xpath
				XPath xPath = XPathFactory.newInstance().newXPath();
				String expression = "/ResponseJobSearch/TotalPages";
				String totalPageStr = xPath.compile(expression).evaluate(
						document);
				System.out.println("totalPageStr" + totalPageStr);
				totalpages = Integer.parseInt(totalPageStr);
				if (totalpages == 0) {
					break;
				}
				expression = "/ResponseJobSearch/Results/JobSearchResult/JobServiceURL";

				// read a nodelist using xpath
				String cbServiceUrl = xPath.compile(expression).evaluate(
						document);
				expression = "/ResponseJobSearch/Results/JobSearchResult/DID";

				String jobDID = xPath.compile(expression).evaluate(document);

				if (callCBServiceURL(cbServiceUrl, con, response, builder,
						builderFactory, details, expression, document, xPath,
						jobDID)) {
					break;
				}

			}
		}

	}

	private boolean callCBServiceURL(String cbServiceUrl,
			HttpURLConnection con, StringBuilder response,
			DocumentBuilder builder, DocumentBuilderFactory builderFactory,
			JobDetails details, String expression, Document document,
			XPath xPath, String jobDID) throws IOException,
			ParserConfigurationException, XPathExpressionException,
			SAXException {

		URL serviceurl = new URL(cbServiceUrl);
		con = (HttpURLConnection) serviceurl.openConnection();
		con.setRequestMethod("GET");
		BufferedReader br = new BufferedReader(new InputStreamReader(
				(con.getInputStream())));
		response = new StringBuilder();
		String output;
		while ((output = br.readLine()) != null) {
			response.append(output);
		}

		builder = builderFactory.newDocumentBuilder();
		document = builder.parse(new ByteArrayInputStream(response.toString()
				.getBytes()));

		expression = "/ResponseJob/Job/DisplayJobID";
		String jobId = xPath.compile(expression).evaluate(document);
		System.out.println("Job Ref ID = " + details.getJobRefID()
				+ " parse Job ID = " + jobId + " parse DID  = " + jobDID);
		if (details.getJobId().equals(jobId)
				|| details.getJobId().equals(jobDID)) {
			expression = "/ResponseJobSearch/Results/JobSearchResult/LocationLatitude";
			String latitude = xPath.compile(expression).evaluate(document);
			details.setLatitude(Double.valueOf(latitude));
			expression = "/ResponseJobSearch/Results/JobSearchResult/LocationLongitude";
			String longitude = xPath.compile(expression).evaluate(document);
			details.setLongitude(Double.valueOf(longitude));
			return true;

		}
		return false;

	}

	public void callCBJobRefURL(JobDetails details) throws IOException,
			ParserConfigurationException, SAXException,
			XPathExpressionException {
		System.out.println(details.getJobRefID());
		StringBuilder urlString = new StringBuilder(
				"http://api.careerbuilder.com/v1/job?");
		urlString.append("DeveloperKey=" + CB_API_KEY);
		urlString.append("&DID=" + details.getJobRefID());
		urlString.append("&HostSite=US");
		URL url = new URL(urlString.toString());
		HttpURLConnection con = (HttpURLConnection) url.openConnection();
		con.setRequestMethod("GET");
		BufferedReader br = new BufferedReader(new InputStreamReader(
				(con.getInputStream())));
		StringBuilder response = new StringBuilder();
		String output;
		while ((output = br.readLine()) != null) {
			response.append(output);
		}
		DocumentBuilderFactory builderFactory = DocumentBuilderFactory
				.newInstance();
		DocumentBuilder builder = builderFactory.newDocumentBuilder();
		Document document = builder.parse(new ByteArrayInputStream(response
				.toString().getBytes()));
		XPath xPath = XPathFactory.newInstance().newXPath();
		String expression = "/ResponseJob/Errors";
		String error = xPath.compile(expression).evaluate(document);
		if (error.trim().equals("")) {
			expression = "/ResponseJob/Job/LocationLatitude";
			String latitude = xPath.compile(expression).evaluate(document);
			if (!latitude.equals("")) {
				details.setLatitude(Double.valueOf(latitude));
			}
			expression = "/ResponseJob/Job/LocationLongitude";
			String longitude = xPath.compile(expression).evaluate(document);
			if (!longitude.equals("")) {
				details.setLongitude(Double.valueOf(longitude));
			}

		}

	}

	public List<JobDetails> updateLocationFromCB() throws IOException,
			ParserConfigurationException, SAXException,
			XPathExpressionException {
		List<JobDetails> details = getCBJobData();
		for (JobDetails jobDetails : details) {
			System.out.println("Job ID processing: " + jobDetails.getJobId());

			String jobTitle = jobDetails.getJobTitle();
			String city = jobDetails.getCity();
			updateLocation(URLEncoder.encode(jobTitle, "UTF-8"),
					URLEncoder.encode(city, "UTF-8"), jobDetails);
			// Check if the lat-long maps to the correct address

			validateFromGoogleAPI(jobDetails);
			// if it does not match with the company's address or does not have
			// the company name, set it back to null

		}
		saveJobDetails(details);
		return details;

	}

	private void validateFromGoogleAPI(JobDetails jobDetails)
			throws IOException {

		if (jobDetails.getLatitude() == null
				|| jobDetails.getLongitude() == null) {
			return;
		}
		// Call Maps API to get the nearest company location from the lat/long
		// available

		String latlong = jobDetails.getLatitude().toString() + ","
				+ jobDetails.getLongitude().toString();

		setValidAddress(latlong, jobDetails);

	}

	private void setValidAddress(String latlong, JobDetails jobDetails)
			throws IOException {
		String address = "";
		GMapResponse grsp = getNearByLocation(latlong,
				jobDetails.getCompanyName());
		if (grsp != null) {
			if (grsp.getStatus().equals("OK")) {
				if (grsp.getResults().length == 0) {
					jobDetails.setLatitude(null);
					jobDetails.setLongitude(null);
					return;
				}
				int relevantCount = 0;
				for (GMapResult result : grsp.getResults()) {

					if (result.getVicinity().contains(jobDetails.getCity())
							|| result.getVicinity().contains(
									jobDetails.getCompanyName())) {
						address = result.getVicinity();
						latlong = result.getGeometry().getLocation().getLat()
								.toString()
								+ ","
								+ result.getGeometry().getLocation().getLng()
										.toString();
						relevantCount++;
						jobDetails.setLatitude(Double.valueOf(result
								.getGeometry().getLocation().getLat()));
						jobDetails.setLongitude(Double.valueOf(result
								.getGeometry().getLocation().getLng()));

						if (relevantCount > 1) {
							address = "";
							latlong = "";
							jobDetails.setLatitude(null);
							jobDetails.setLongitude(null);
							break;
						}
					}

					System.out.println("address is :" + address);

				}

				if (!address.equals("") && !latlong.equals("")) {
					if (address.split(",").length == 1) {
						jobDetails.setLatitude(null);
						jobDetails.setLongitude(null);
					} else if (address.split(",").length < 2) {
						GoogleResponse resp = convertFromLatLong(latlong);
						if (resp.getResults().length > 0) {
							GoogleResult result = resp.getResults()[0];
							setAddressDetails(result.getFormatted_address(),
									jobDetails);
							System.out.println(result.getFormatted_address());
						}
					} else {
						grsp = getTextLocation(address);
						if (grsp.getResults().length > 0) {
							GMapResult result = (GMapResult) grsp.getResults()[0];
							setAddressDetails(result.getFormatted_address(),
									jobDetails);
							System.out.println(result.getFormatted_address());
						}
					}
				}
			} else {
				System.out.println(grsp.getStatus());
				jobDetails.setLatitude(null);
				jobDetails.setLongitude(null);
			}

		}
	}

	private void setAddressDetails(String formattedAddress,
			JobDetails jobDetails) {
		System.out.println("formatted" + formattedAddress);
		if (!formattedAddress.equals("")) {
			String[] splitAddress = formattedAddress.split(",");
			jobDetails.setStreetName1(splitAddress[0].trim());
			jobDetails.setCity(splitAddress[1].trim());
			String[] stateZip = splitAddress[2].trim().split("\\s");
			System.out.println(stateZip);
			jobDetails.setState(stateZip[0]);
			if (stateZip.length > 1) {
				jobDetails.setZipCode(stateZip[1]);
			}
		}
	}

	public GMapResponse getNearByLocation(String latlongString,
			String companyName) throws IOException {
		if (companyName == null || companyName.equals("")) {
			return null;
		}

		URL url = new URL(MAPS_NEARBY_URL + "?location="
				+ URLEncoder.encode(latlongString, "UTF-8")
				+ "&rankby=distance&name="
				+ URLEncoder.encode(companyName, "UTF-8") + "&key="
				+ MAPS_API_KEY);
		// Open the Connection
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();

		InputStream in = conn.getInputStream();
		ObjectMapper mapper = new ObjectMapper();
		GMapResponse response = (GMapResponse) mapper.readValue(in,
				GMapResponse.class);
		in.close();
		return response;

	}

	public GMapResponse getTextLocation(String textSearch) throws IOException {

		URL url = new URL(MAPS_TEXT_SEARCH_URL + "?query="
				+ URLEncoder.encode(textSearch, "UTF-8") + "&key="
				+ MAPS_API_KEY);
		// Open the Connection
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		InputStream in = conn.getInputStream();
		ObjectMapper mapper = new ObjectMapper();
		GMapResponse response = (GMapResponse) mapper.readValue(in,
				GMapResponse.class);
		in.close();
		return response;

	}

	public GoogleResponse convertFromLatLong(String latlongString)
			throws IOException {

		URL url = new URL(GEOCODE_URL + "?latlng="
				+ URLEncoder.encode(latlongString, "UTF-8") + "&sensor=false");
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();

		InputStream in = conn.getInputStream();
		ObjectMapper mapper = new ObjectMapper();
		GoogleResponse response = (GoogleResponse) mapper.readValue(in,
				GoogleResponse.class);
		in.close();
		return response;

	}

	public GoogleResponse getLocationLatLong(String locationName)
			throws IOException {

		URL url = new URL(GEOCODE_URL + "?address="
				+ URLEncoder.encode(locationName, "UTF-8"));
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();

		InputStream in = conn.getInputStream();
		ObjectMapper mapper = new ObjectMapper();
		GoogleResponse response = (GoogleResponse) mapper.readValue(in,
				GoogleResponse.class);
		in.close();
		return response;

	}

	public void updateLocationInfo() throws IOException {

		// Traverse through all the records that do not have lat-long
		List<JobDetails> details = null;
		JobSearchDAO dao = new JobSearchDAO();
		details = dao.getJobDetailsWithNoLocation();
		for (JobDetails jobDetails : details) {

			String latlong = "";
			// Get the lat-long of the City
			GoogleResponse res = getLocationLatLong(jobDetails.getCity());
			if (res.getResults() != null && res.getResults().length > 0) {
				Location loc = res.getResults()[0].getGeometry().getLocation();
				latlong = loc.getLat() + "," + loc.getLng();
			}
			System.out.println(latlong);
			// Call the nearby search Google Places API with the company
			// name
			// and latlong

			setValidAddress(latlong, jobDetails);

			// If it is a single location single company, we will get one
			// record
			// If it is a single company in each location, we will get one
			// result

			// If many outlets in single city, we will get many results
			saveJobDetails(details);

			// For such records, we will call Career builder API if the source
			// is
			// Careerbuilder
			// For remaining records, we will try to scrape the company's
			// website
			// and get the location information
			// Save in the database

		}
	}

	public void updateLocationFromJobLink() {
		List<JobDetails> details = null;
		try {
			JobSearchDAO dao = new JobSearchDAO();
			// STEP 4: Execute a query
			details = dao.getJobDetailsWithNoLocation();
			org.jsoup.nodes.Document doc = null;
			boolean flag = false;
			// STEP 5: Extract data from result set
			String link = "";
			for (JobDetails jobDetails : details) {
				System.out.println(jobDetails.getJobLink());
				link = jobDetails.getJobLink();
				try {
					doc = Jsoup.connect(link).ignoreContentType(true)
							.timeout(0).get();
				} catch (HttpStatusException hp) {
					flag = true;
				} catch (SSLHandshakeException e) {
					flag = true;
				} catch (MalformedURLException e) {
					flag = true;
				} catch (SSLProtocolException e) {
					flag = true;
				} catch (Exception e) {
					flag = true;
				}
				if (flag) {
					flag = false;
					continue;
				}
				Elements elements = doc.body().select("*");

				for (Element x : elements) {
					// System.out.println(x.ownText());
					String text = x.ownText();
					String[] words = text.split("\\s+");
					for (String word : words) {
						if (Arrays.asList(US_STATES).contains(word)
								|| Arrays.asList(StateCodes).contains(word)) {
							System.out.println("Address found");
							System.out.println(text);
							return;
						} else {
							System.out
									.println("-------------Address not found");
						}
					}
				}

				// STEP 6: Clean-up environment

			}

		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
		}

		//saveJobDetails(details);

		System.out.println("Goodbye!");
	}// end main
}
