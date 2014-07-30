package com.glassdoor.databean;

import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;

public class HibernateUtil {

	private static HibernateUtil instance = null;

	   private static SessionFactory sessionFactory;
	   private static StandardServiceRegistry serviceRegistry;

	   private HibernateUtil(){
	        Configuration configuration = new Configuration();
	        configuration.configure();
	        serviceRegistry = new StandardServiceRegistryBuilder().applySettings(configuration.getProperties()).build();
	        sessionFactory = configuration.buildSessionFactory(serviceRegistry);
	   }

	   public static HibernateUtil getInstance(){
	        if(instance == null){
	            instance  = new HibernateUtil();
	        }
	        return instance;
	   }

	   public static SessionFactory getSessionFactory() {
	        return sessionFactory;
	   }

}
