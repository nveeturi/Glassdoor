package com.glassdoor.jobsearch;

import java.io.IOException;
import java.util.List;

import com.glassdoor.databean.JobDetails;

public class JobSearchTest {

	public static void main(String[] args) {	

		JobSearchService jobService = new JobSearchService();
		try {
			
			//List<JobDetails> jobdetails = jobService.getJobDataFromGlassdoor("", "Pittsburgh");
			//jobService.saveJobDetails(jobdetails);
			//jobService.updateLocationFromCB();
			//jobService.updateLocationInfo();
			jobService.updateLocationFromJobLink();
		
		} //catch (IOException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
	//	}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
