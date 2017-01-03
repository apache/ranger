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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import org.apache.commons.lang.StringUtils;
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

	private static final String DB_SSL_ENABLED="db.ssl.enabled";
	private static final String DB_SSL_REQUIRED="db.ssl.required";
	private static final String DB_SSL_VerifyServerCertificate="db.ssl.verifyServerCertificate";
	private static final String DB_SSL_KEYSTORE="keystore.file";
	private static final String DB_SSL_KEYSTORE_PASSWORD="keystore.password";
	private static final String DB_SSL_TRUSTSTORE="truststore.file";
	private static final String DB_SSL_TRUSTSTORE_PASSWORD="truststore.password";

    public static final int DB_FLAVOR_UNKNOWN = 0;
	public static final int DB_FLAVOR_MYSQL = 1;
	public static final int DB_FLAVOR_ORACLE = 2;
	public static final int DB_FLAVOR_POSTGRES = 3;
	public static final int DB_FLAVOR_SQLSERVER = 4;
	public static final int DB_FLAVOR_SQLANYWHERE = 5;
	
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
			if(getDBFlavor(conf)==DB_FLAVOR_MYSQL){
				updateDBSSLURL();
			}

			//DB_PROPERTIES.list(System.out);

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

	public int getDBFlavor(Configuration newConfig) {
		String[] propertyNames = { PROPERTY_PREFIX+DB_DIALECT,PROPERTY_PREFIX+DB_DRIVER,PROPERTY_PREFIX+DB_URL};

		for(String propertyName : propertyNames) {
			String propertyValue = DB_PROPERTIES.get(propertyName);
			if(StringUtils.isBlank(propertyValue)) {
				continue;
			}
			if (StringUtils.containsIgnoreCase(propertyValue, "mysql")) {
				return DB_FLAVOR_MYSQL;
			} else if (StringUtils.containsIgnoreCase(propertyValue, "oracle")) {
				return DB_FLAVOR_ORACLE;
			} else if (StringUtils.containsIgnoreCase(propertyValue, "postgresql")) {
				return DB_FLAVOR_POSTGRES;
			} else if (StringUtils.containsIgnoreCase(propertyValue, "sqlserver")) {
				return DB_FLAVOR_SQLSERVER;
			} else if (StringUtils.containsIgnoreCase(propertyValue, "mssql")) {
				return DB_FLAVOR_SQLSERVER;
			} else if (StringUtils.containsIgnoreCase(propertyValue, "sqlanywhere")) {
				return DB_FLAVOR_SQLANYWHERE;
			} else if (StringUtils.containsIgnoreCase(propertyValue, "sqla")) {
				return DB_FLAVOR_SQLANYWHERE;
			}else {
				if(logger.isDebugEnabled()) {
					logger.debug("DB Flavor could not be determined from property - " + propertyName + "=" + propertyValue);
				}
			}
		}
		logger.error("DB Flavor could not be determined");
		return DB_FLAVOR_UNKNOWN;
	}

	private void updateDBSSLURL(){
		if(conf!=null && conf.get(PROPERTY_PREFIX+DB_SSL_ENABLED)!=null){
			String db_ssl_enabled=conf.get(PROPERTY_PREFIX+DB_SSL_ENABLED);
			if(StringUtils.isEmpty(db_ssl_enabled)|| !"true".equalsIgnoreCase(db_ssl_enabled)){
				db_ssl_enabled="false";
			}
			db_ssl_enabled=db_ssl_enabled.toLowerCase();
			if("true".equalsIgnoreCase(db_ssl_enabled)){
				String db_ssl_required=conf.get(PROPERTY_PREFIX+DB_SSL_REQUIRED);
				if(StringUtils.isEmpty(db_ssl_required)|| !"true".equalsIgnoreCase(db_ssl_required)){
					db_ssl_required="false";
				}
				db_ssl_required=db_ssl_required.toLowerCase();
				String db_ssl_verifyServerCertificate=conf.get(PROPERTY_PREFIX+DB_SSL_VerifyServerCertificate);
				if(StringUtils.isEmpty(db_ssl_verifyServerCertificate)|| !"true".equalsIgnoreCase(db_ssl_verifyServerCertificate)){
					db_ssl_verifyServerCertificate="false";
				}
				db_ssl_verifyServerCertificate=db_ssl_verifyServerCertificate.toLowerCase();
				conf.set(PROPERTY_PREFIX+DB_SSL_ENABLED, db_ssl_enabled);
				conf.set(PROPERTY_PREFIX+DB_SSL_REQUIRED, db_ssl_required);
				conf.set(PROPERTY_PREFIX+DB_SSL_VerifyServerCertificate, db_ssl_verifyServerCertificate);
				String ranger_jpa_jdbc_url=conf.get(PROPERTY_PREFIX+DB_URL);
				if(!StringUtils.isEmpty(ranger_jpa_jdbc_url)){
					StringBuffer ranger_jpa_jdbc_url_ssl=new StringBuffer(ranger_jpa_jdbc_url);
					ranger_jpa_jdbc_url_ssl.append("?useSSL="+db_ssl_enabled+"&requireSSL="+db_ssl_required+"&verifyServerCertificate="+db_ssl_verifyServerCertificate);
					conf.set(PROPERTY_PREFIX+DB_URL, ranger_jpa_jdbc_url_ssl.toString());
					DB_PROPERTIES.put(JPA_DB_URL, conf.get(PROPERTY_PREFIX+DB_URL));
					logger.info(PROPERTY_PREFIX+DB_URL+"="+ranger_jpa_jdbc_url_ssl.toString());
				}

				if("true".equalsIgnoreCase(db_ssl_verifyServerCertificate)){
					if (conf!=null) {
						// update system key store path with custom key store.
						String keystore=conf.get(PROPERTY_PREFIX+DB_SSL_KEYSTORE);
						if(!StringUtils.isEmpty(keystore)){
							Path path = Paths.get(keystore);
							if (Files.exists(path) && Files.isReadable(path)) {
								System.setProperty("javax.net.ssl.keyStore", conf.get(PROPERTY_PREFIX+DB_SSL_KEYSTORE));
								System.setProperty("javax.net.ssl.keyStorePassword", conf.get(PROPERTY_PREFIX+DB_SSL_KEYSTORE_PASSWORD));
								System.setProperty("javax.net.ssl.keyStoreType", KeyStore.getDefaultType());
							}else{
								logger.debug("Could not find or read keystore file '"+keystore+"'");
							}
						}else{
							logger.debug("keystore property '"+PROPERTY_PREFIX+DB_SSL_KEYSTORE+"' value not found!");
						}
						// update system trust store path with custom trust store.
						String truststore=conf.get(PROPERTY_PREFIX+DB_SSL_TRUSTSTORE);
						if(!StringUtils.isEmpty(truststore)){
							Path path = Paths.get(truststore);
							if (Files.exists(path) && Files.isReadable(path)) {
								System.setProperty("javax.net.ssl.trustStore", conf.get(PROPERTY_PREFIX+DB_SSL_TRUSTSTORE));
								System.setProperty("javax.net.ssl.trustStorePassword", conf.get(PROPERTY_PREFIX+DB_SSL_TRUSTSTORE_PASSWORD));
								System.setProperty("javax.net.ssl.trustStoreType", KeyStore.getDefaultType());
							}else{
								logger.debug("Could not find or read truststore file '"+truststore+"'");
							}
						}else{
							logger.debug("truststore property '"+PROPERTY_PREFIX+DB_SSL_TRUSTSTORE+"' value not found!");
						}
					}
				}
			}
		}
	}
}
