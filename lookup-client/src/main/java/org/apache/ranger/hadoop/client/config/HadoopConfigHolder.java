/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

 package org.apache.ranger.hadoop.client.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.hadoop.client.exceptions.HadoopException;

public class HadoopConfigHolder  {
	private static final Log LOG = LogFactory.getLog(HadoopConfigHolder.class) ;
	public static final String GLOBAL_LOGIN_PARAM_PROP_FILE = "hadoop-login.properties" ;
	public static final String DEFAULT_DATASOURCE_PARAM_PROP_FILE = "datasource.properties" ;
	public static final String RESOURCEMAP_PROP_FILE = "resourcenamemap.properties" ;
	public static final String DEFAULT_RESOURCE_NAME = "core-site.xml" ;
	public static final String RANGER_SECTION_NAME = "xalogin.xml" ;
	public static final String RANGER_LOGIN_USER_NAME_PROP = "username" ;
	public static final String RANGER_LOGIN_KEYTAB_FILE_PROP = "keytabfile" ;
	public static final String RANGER_LOGIN_PASSWORD = "password" ;
	public static final String HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";
	public static final String HADOOP_SECURITY_AUTHENTICATION_METHOD = "kerberos";
	public static final String HADOOP_RPC_PROTECTION = "hadoop.rpc.protection";
	

	private static boolean initialized = false ;
	private static HashMap<String,HashMap<String,Properties>> dataSource2ResourceListMap = new HashMap<String,HashMap<String,Properties>>() ;
	private static Properties globalLoginProp = new Properties() ;
	private static HashMap<String,HadoopConfigHolder> dataSource2HadoopConfigHolder = new HashMap<String,HadoopConfigHolder>() ;
	private static Properties resourcemapProperties = null ;
	
	
	private String datasourceName ;
	private String userName ;
	private String keyTabFile ;
	private String password ;
	private boolean isKerberosAuth ;
	
	private HadoopClassLoader classLoader ;
	private HashMap<String,String>  connectionProperties; 
	
	public static HadoopConfigHolder getInstance(String aDatasourceName) {
		HadoopConfigHolder ret = dataSource2HadoopConfigHolder.get(aDatasourceName) ;
		if (ret == null) {
			synchronized(HadoopConfigHolder.class) {
				HadoopConfigHolder temp = ret ;
				if (temp == null) {
					ret = new HadoopConfigHolder(aDatasourceName) ;
					dataSource2HadoopConfigHolder.put(aDatasourceName, ret) ;
				}
			}
		}
		return ret ;
	}
	
	public static HadoopConfigHolder getInstance(String aDatasourceName, HashMap<String,String> connectionProperties) {
		HadoopConfigHolder ret = dataSource2HadoopConfigHolder.get(aDatasourceName) ;
		if (ret == null) {
			synchronized(HadoopConfigHolder.class) {
				HadoopConfigHolder temp = ret ;
				if (temp == null) {
					ret = new HadoopConfigHolder(aDatasourceName,connectionProperties) ;
					dataSource2HadoopConfigHolder.put(aDatasourceName, ret) ;
				}
			}
		}
		else {
			if (connectionProperties !=null  &&  !connectionProperties.equals(ret.connectionProperties)) {
				ret = new HadoopConfigHolder(aDatasourceName,connectionProperties) ;
				dataSource2HadoopConfigHolder.remove(aDatasourceName) ;
				dataSource2HadoopConfigHolder.put(aDatasourceName, ret) ;
			}
		}
		return ret ;
	}
	
	

	private HadoopConfigHolder(String aDatasourceName) {
		datasourceName = aDatasourceName;
		if ( ! initialized ) {
			init() ;
		}
		initLoginInfo();
		initClassLoader() ;
	}
	
	private HadoopConfigHolder(String aDatasourceName, HashMap<String,String> connectionProperties) {
		datasourceName = aDatasourceName;
		this.connectionProperties = connectionProperties ;
		initConnectionProp() ;
		initLoginInfo();
		initClassLoader() ;
	}
	
	private void initConnectionProp() {
		for(String key : connectionProperties.keySet()) {
			
			String resourceName = getResourceName(key) ;
			
			if (resourceName == null) {
				resourceName = RANGER_SECTION_NAME ;
			}
			String val = connectionProperties.get(key) ;
			addConfiguration(datasourceName, resourceName, key, val );
		}
	}
	
	private String getResourceName(String key) {
		
		if (resourcemapProperties == null) {
			initResourceMap();
		}
		
		if (resourcemapProperties != null) {
			return resourcemapProperties.getProperty(key);
		}
		else {
			return null;
		}
	}

	public static void initResourceMap() {
		if (resourcemapProperties == null) {
			resourcemapProperties = new Properties() ;
			InputStream in = HadoopConfigHolder.class.getClassLoader().getResourceAsStream(RESOURCEMAP_PROP_FILE) ;
			if (in != null) {
				try {
					resourcemapProperties.load(in);
				} catch (IOException e) {
					throw new HadoopException("Unable to load resource map properties from [" + RESOURCEMAP_PROP_FILE + "]", e);
				}
			}
			else {
				throw new HadoopException("Unable to locate resource map properties from [" + RESOURCEMAP_PROP_FILE + "] in the class path.");
			}
		}
	}

	
	
	private static synchronized void init() {

		if (initialized) {
			return ;
		}

		try {
			InputStream in = HadoopConfigHolder.class.getClassLoader().getResourceAsStream(DEFAULT_DATASOURCE_PARAM_PROP_FILE) ;
			if (in != null) {
				Properties prop = new Properties() ;
				try {
					prop.load(in) ;
				} catch (IOException e) {
					throw new HadoopException("Unable to get configuration information for Hadoop environments", e);
				}
				finally {
					try {
						in.close();
					} catch (IOException e) {
						// Ignored exception when the stream is closed.
					} 
				}
	
				if (prop.size() == 0) 
					return ;
				
				for(Object keyobj : prop.keySet()) {
					String key = (String)keyobj;
					String val = prop.getProperty(key) ;
					
					int dotLocatedAt = key.indexOf(".") ;
					
					if (dotLocatedAt == -1) {
						continue ;
					}
					
					String dataSource = key.substring(0,dotLocatedAt) ;
					
					String propKey = key.substring(dotLocatedAt+1) ;
					int resourceFoundAt =  propKey.indexOf(".") ;
					if (resourceFoundAt > -1) {
						String resourceName = propKey.substring(0, resourceFoundAt) + ".xml" ; 
						propKey = propKey.substring(resourceFoundAt+1) ;
						addConfiguration(dataSource, resourceName, propKey, val) ;
					}
					
				}
			}
			
			in = HadoopConfigHolder.class.getClassLoader().getResourceAsStream(GLOBAL_LOGIN_PARAM_PROP_FILE) ;
			if (in != null) {
				Properties tempLoginProp = new Properties() ;
				try {
					tempLoginProp.load(in) ;
				} catch (IOException e) {
					throw new HadoopException("Unable to get login configuration information for Hadoop environments from file: [" + GLOBAL_LOGIN_PARAM_PROP_FILE + "]", e);
				}
				finally {
					try {
						in.close();
					} catch (IOException e) {
						// Ignored exception when the stream is closed.
					} 
				}
				globalLoginProp = tempLoginProp ;
			}
		}
		finally {
			initialized = true ;
		}
	}
	
	
	private void initLoginInfo() {
		Properties prop = this.getRangerSection() ;
		if (prop != null) {
			userName = prop.getProperty(RANGER_LOGIN_USER_NAME_PROP) ;
			keyTabFile = prop.getProperty(RANGER_LOGIN_KEYTAB_FILE_PROP) ;
			password = prop.getProperty(RANGER_LOGIN_PASSWORD) ;
		
			if ( getHadoopSecurityAuthentication() != null) {
				isKerberosAuth = ( getHadoopSecurityAuthentication().equalsIgnoreCase(HADOOP_SECURITY_AUTHENTICATION_METHOD));
			}
			else {
				isKerberosAuth = (userName != null) && (userName.indexOf("@") > -1) ;
			}
					
		}
	}
	
	private void initClassLoader() {
		classLoader = new HadoopClassLoader(this) ;
	}
	
	
	public Properties getRangerSection() {
		Properties prop = this.getProperties(RANGER_SECTION_NAME) ;
		if (prop == null) {
			prop = globalLoginProp ;
		}
		return prop ;
	}



	private static void addConfiguration(String dataSource, String resourceName, String propertyName, String value) {

		if (dataSource == null || dataSource.isEmpty()) {
			return ;
		}
		
		if (propertyName == null || propertyName.isEmpty()) {
			return ;
		}
		
		if (resourceName == null) {
			resourceName = DEFAULT_RESOURCE_NAME ;
		}
		
		
		HashMap<String,Properties> resourceName2PropertiesMap  = dataSource2ResourceListMap.get(dataSource) ;
		
		if (resourceName2PropertiesMap == null) {
			resourceName2PropertiesMap = new HashMap<String,Properties>() ;
			dataSource2ResourceListMap.put(dataSource, resourceName2PropertiesMap) ;
		}
		
		Properties prop = resourceName2PropertiesMap.get(resourceName) ;
		if (prop == null) {
			prop = new Properties() ;
			resourceName2PropertiesMap.put(resourceName, prop) ;
		}
		if (value == null) {
			prop.remove(propertyName) ;
		}
		else {
			prop.put(propertyName, value) ;
		}
	}
	
	
	public String getDatasourceName() {
		return datasourceName ;
	}
	
	public boolean hasResourceExists(String aResourceName) {
		HashMap<String,Properties> resourceName2PropertiesMap  = dataSource2ResourceListMap.get(datasourceName) ;
		return (resourceName2PropertiesMap != null && resourceName2PropertiesMap.containsKey(aResourceName)) ;
 	}

	public Properties getProperties(String aResourceName) {
		Properties ret = null ;
		HashMap<String,Properties> resourceName2PropertiesMap  = dataSource2ResourceListMap.get(datasourceName) ;
		if (resourceName2PropertiesMap != null) {
			ret =  resourceName2PropertiesMap.get(aResourceName) ;
		}
		return ret ;
 	}
	
	public String getHadoopSecurityAuthentication() {
		Properties repoParam = null ;
		String ret = null;
		
		HashMap<String,Properties> resourceName2PropertiesMap  = dataSource2ResourceListMap.get(this.getDatasourceName()) ;
		
		if ( resourceName2PropertiesMap != null) {
			repoParam=resourceName2PropertiesMap.get(DEFAULT_RESOURCE_NAME);
		}
		
		if ( repoParam != null ) {
			ret = (String)repoParam.get(HADOOP_SECURITY_AUTHENTICATION);
		}
		return ret;
 	}
	
	public String getUserName() {
		return userName;
	}

	public String getKeyTabFile() {
		return keyTabFile;
	}

	public String getPassword() {
		return password;
	}

	public HadoopClassLoader getClassLoader() {
		return classLoader;
	}

	public boolean isKerberosAuthentication() {
		return isKerberosAuth;
	}

  
	

}
