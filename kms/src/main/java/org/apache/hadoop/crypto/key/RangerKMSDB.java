/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.crypto.key;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.ranger.kms.dao.DaoManager;

public class RangerKMSDB {
	
	static final Logger logger = Logger.getLogger(RangerKMSDB.class);
	
	private EntityManagerFactory entityManagerFactory;
	private DaoManager daoManager;
	
	private static Map<String, String> DB_PROPERTIES = null;
	
	private static final String PROPERTY_PREFIX = "ranger.ks.";
	private static final String DB_DIALECT = "jpa.jdbc.dialect";
	private static final String DB_DRIVER = "jpa.jdbc.driver";
	private static final String DB_URL = "jpa.jdbc.url";
	private static final String DB_USER = "jpa.jdbc.user";
	private static final String DB_PASSWORD = "jpa.jdbc.password";

    private static final String JPA_DB_DIALECT = "javax.persistence.jdbc.dialect";
    private static final String JPA_DB_DRIVER = "javax.persistence.jdbc.driver";
    private static final String JPA_DB_URL = "javax.persistence.jdbc.url";
    private static final String JPA_DB_USER = "javax.persistence.jdbc.user";
    private static final String JPA_DB_PASSWORD = "javax.persistence.jdbc.password";

	
	private final Configuration conf;
	
	public RangerKMSDB(){
		conf = new Configuration();
		//TODO: need to load kms db config file here ...
	}
	
	public RangerKMSDB(Configuration conf){		
		this.conf = conf;		
		initDBConnectivity();
	}
	
	public DaoManager getDaoManager(){
		return daoManager;
	}

	private void initDBConnectivity(){
		try {
			
			DB_PROPERTIES = new HashMap<String, String>();
			DB_PROPERTIES.put(JPA_DB_DIALECT, conf.get(PROPERTY_PREFIX+DB_DIALECT));
			DB_PROPERTIES.put(JPA_DB_DRIVER, conf.get(PROPERTY_PREFIX+DB_DRIVER));
			DB_PROPERTIES.put(JPA_DB_URL, conf.get(PROPERTY_PREFIX+DB_URL));
			DB_PROPERTIES.put(JPA_DB_USER, conf.get(PROPERTY_PREFIX+DB_USER));
			DB_PROPERTIES.put(JPA_DB_PASSWORD, conf.get(PROPERTY_PREFIX+DB_PASSWORD));

			//DB_PROPERTIES.list(System.out) ;

			/*
			Set keys = DB_PROPERTIES.keySet();

   			for (Iterator i = keys.iterator(); i.hasNext();) {
       				String key = (String) i.next();
       				String value = (String) DB_PROPERTIES.get(key);
       				System.out.println(key + " = " + value);
   			}
			*/
				
			entityManagerFactory = Persistence.createEntityManagerFactory("persistence_ranger_server", DB_PROPERTIES);
	   	    	daoManager = new DaoManager();
	   	    	daoManager.setEntityManagerFactory(entityManagerFactory);
	   	    	daoManager.getEntityManager(); // this forces the connection to be made to DB
	   	    	logger.info("Connected to DB : "+isDbConnected());	   	    
		} catch(Exception excp) {
			excp.printStackTrace();
		}
	}
	
	private boolean isDbConnected() {
		EntityManager em = getEntityManager();
		
		return em != null && em.isOpen();
	}
	
	private EntityManager getEntityManager() {
		DaoManager daoMgr = daoManager;

		if(daoMgr != null) {
			try {
				return daoMgr.getEntityManager();
			} catch(Exception excp) {
				excp.printStackTrace();
			}
		}

		return null;
	}
}
