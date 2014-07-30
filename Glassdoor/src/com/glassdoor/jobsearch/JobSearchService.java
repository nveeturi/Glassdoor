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
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.http.client.utils.URLEncodedUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.glassdoor.dao.JobSearchDAO;
import com.glassdoor.databean.GlassdoorJobData;
import com.glassdoor.databean.GoogleResponse;
import com.glassdoor.databean.GoogleResult;
import com.glassdoor.databean.JobDetails;
import com.glassdoor.databean.JobListing;
import com.google.gson.Gson;

public class JobSearchService {
	private static String passCode = "pass";
	private static String key = "keyvalue";
	private static String localIP = "127.0.0.1";
	private static String CB_API_KEY = "cbi_key";
	private static final String GEOCODE_URL = "http://maps.googleapis.com/maps/api/geocode/json";

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

	public List<JobDetails> getAllJobData() {
		List<JobDetails> details = null;
		JobSearchDAO dao = new JobSearchDAO();
		details = dao.getAllJobDetails();
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
		List<JobDetails> details = getAllJobData();
		for (JobDetails jobDetails : details) {
			System.out.println("Job ID processing: " + jobDetails.getJobId());

			String jobTitle = jobDetails.getJobTitle();
			String city = jobDetails.getCity();
			updateLocation(URLEncoder.encode(jobTitle, "UTF-8"),
					URLEncoder.encode(city, "UTF-8"), jobDetails);
			// Check if the lat-long maps to the correct address

			validateLocation(jobDetails);
			// if it does not match with the company's address or does not have
			// the company name, set it back to null

		}
		saveJobDetails(details);
		return details;

	}

	private void validateLocation(JobDetails jobDetails) throws IOException {

		if (jobDetails.getLatitude() == null
				|| jobDetails.getLongitude() == null) {
			return;
		}
		// Call geocode api to get the address corresponding to the lat-long
		String latlong = jobDetails.getLatitude().toString() + ","
				+ jobDetails.getLongitude().toString();
		GoogleResponse grsp = convertFromLatLong(latlong);
		if (grsp.getStatus().equals("OK")) {
			for (GoogleResult result : grsp.getResults()) {
				System.out.println("address is :"
						+ result.getFormatted_address());
			}
		} else {
			System.out.println(grsp.getStatus());
		}

	}

	public GoogleResponse convertFromLatLong(String latlongString)
			throws IOException {

		URL url = new URL(GEOCODE_URL + "?latlng="
				+ URLEncoder.encode(latlongString, "UTF-8") + "&sensor=false");
		// Open the Connection
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();

		InputStream in = conn.getInputStream();
		ObjectMapper mapper = new ObjectMapper();
		GoogleResponse response = (GoogleResponse) mapper.readValue(in,
				GoogleResponse.class);
		in.close();
		return response;

	}

}
