package com.glassdoor.dao;

import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.NonUniqueObjectException;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.criterion.Restrictions;

import com.glassdoor.databean.HibernateUtil;
import com.glassdoor.databean.JobDetails;

public class JobSearchDAO {

	public JobDetails getJobDetailsForSearch(String jobId) {
		HibernateUtil util = HibernateUtil.getInstance();
		JobDetails details = null;
		@SuppressWarnings("static-access")
		Session session = util.getSessionFactory().openSession();
		@SuppressWarnings("unchecked")
		List<JobDetails> results = session.createQuery(
				"from JobDetails where jobId =" + jobId).list();

		if (!(results == null || results.size() == 0)) {
			details = results.get(0);
		}
		session.close();
		return details;

	}

	public void insertJobDetails(List<JobDetails> jobdetails) {

		HibernateUtil util = HibernateUtil.getInstance();
		Session session = util.getSessionFactory().openSession();
		Transaction tx = session.beginTransaction();

		for (int i = 0; i < jobdetails.size(); i++) {

			try {

				JobDetails job = (JobDetails) jobdetails.get(i);
				/*System.out.println("object no. "+i +" "+ job.getJobId() + " "
						+ job.getCompanyName() + " " + job.getCity());*/
				session.saveOrUpdate(job);
				if (i % 20 == 0) { // 20, same as the JDBC batch size
					// flush a batch of inserts and release memory:
					session.flush();
					session.clear();
				}

			} catch (NonUniqueObjectException nue) {
				continue;
			}

		}
		tx.commit();
		session.close();
		System.out.println("Object saved");

	}

	public List<JobDetails> getAllJobDetails() {

		HibernateUtil util = HibernateUtil.getInstance();
		List<JobDetails> details = null;
		@SuppressWarnings("static-access")
		Session session = util.getSessionFactory().openSession();
		@SuppressWarnings("unchecked")
		List<JobDetails> results = session.createQuery("from JobDetails where source= 'CareerBuilder'")
				.list();

		if (!(results == null || results.size() == 0)) {
			details = results;
		}
		session.close();
		return details;
	}

}
