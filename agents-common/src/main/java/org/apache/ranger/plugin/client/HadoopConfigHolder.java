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

 package org.apache.ranger.plugin.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.SecureClientLogin;

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
	public static final String RANGER_LOOKUP_PRINCIPAL = "lookupprincipal";
	public static final String RANGER_LOOKUP_KEYTAB = "lookupkeytab";
	public static final String RANGER_PRINCIPAL = "rangerprincipal";
	public static final String RANGER_KEYTAB = "rangerkeytab";
	public static final String RANGER_NAME_RULES = "namerules";
	public static final String RANGER_AUTH_TYPE = "authtype";
	public static final String HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";
	public static final String HADOOP_NAME_RULES = "hadoop.security.auth_to_local";
	public static final String HADOOP_SECURITY_AUTHENTICATION_METHOD = "kerberos";
	public static final String HADOOP_RPC_PROTECTION = "hadoop.rpc.protection";
	

	private static boolean initialized = false ;
	private static Map<String,HashMap<String,Properties>> dataSource2ResourceListMap = new HashMap<String,HashMap<String,Properties>>() ;
	private static Properties globalLoginProp = new Properties() ;
	private static Map<String,HadoopConfigHolder> dataSource2HadoopConfigHolder = new HashMap<String,HadoopConfigHolder>() ;
	private static Properties resourcemapProperties = null ;
	
	
	private String datasourceName ;
	private String defaultConfigFile ;
	private String userName ;
	private String keyTabFile ;
	private String password ;
	private boolean isKerberosAuth ;
	private String lookupPrincipal;
	private String lookupKeytab;
	private String nameRules;
	private String authType;
	
	private Map<String,String>  connectionProperties;

  private static Set<String> rangerInternalPropertyKeys = new HashSet<String>();
	
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

  public static HadoopConfigHolder getInstance(String aDatasourceName, Map<String,String> connectionProperties) {
    return getInstance(aDatasourceName, connectionProperties, null);
  }

	public static HadoopConfigHolder getInstance(String aDatasourceName, Map<String,String> connectionProperties,
                                               String defaultConfigFile) {
		HadoopConfigHolder ret = dataSource2HadoopConfigHolder.get(aDatasourceName) ;
		if (ret == null) {
			synchronized(HadoopConfigHolder.class) {
				HadoopConfigHolder temp = ret ;
				if (temp == null) {
					ret = new HadoopConfigHolder(aDatasourceName,connectionProperties, defaultConfigFile) ;
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
	}

  private HadoopConfigHolder(String aDatasourceName, Map<String,String> connectionProperties) {
   this(aDatasourceName, connectionProperties, null);
  }

	private HadoopConfigHolder(String aDatasourceName, Map<String,String> connectionProperties,
                             String defaultConfigFile) {
		datasourceName = aDatasourceName;
		this.connectionProperties = connectionProperties ;
    this.defaultConfigFile = defaultConfigFile;
		initConnectionProp() ;
		initLoginInfo();
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
      String rn = resourcemapProperties.getProperty(key);
      return ( rn != null)  ? rn : defaultConfigFile;
		}
		else {
			return defaultConfigFile;
		}
	}

	public static void initResourceMap() {
		if (resourcemapProperties == null) {
			resourcemapProperties = new Properties() ;
			InputStream in = HadoopConfigHolder.class.getClassLoader().getResourceAsStream(RESOURCEMAP_PROP_FILE) ;
			if (in != null) {
				try {
					resourcemapProperties.load(in);
		          for (Map.Entry<Object, Object> entry : resourcemapProperties.entrySet() ) {
		            String key = (String)entry.getKey();
		            String value = (String)entry.getValue();
		            if (RANGER_SECTION_NAME.equals(value))  {
		              rangerInternalPropertyKeys.add(key);
		            }
		          }
				} catch (IOException e) {
					throw new HadoopException("Unable to load resource map properties from [" + RESOURCEMAP_PROP_FILE + "]", e);
				}
				finally {
					if (in != null) {
						try {
							in.close() ;
						}
						catch(IOException ioe) {
							// Ignore IOException during close of stream
						}
					}
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
			lookupPrincipal = prop.getProperty(RANGER_LOOKUP_PRINCIPAL);
			lookupKeytab = prop.getProperty(RANGER_LOOKUP_KEYTAB);
			nameRules = prop.getProperty(RANGER_NAME_RULES);
			authType = prop.getProperty(RANGER_AUTH_TYPE, "simple");
			
			String hadoopSecurityAuthenticationn =  getHadoopSecurityAuthentication();

			if ( hadoopSecurityAuthenticationn != null) {
				isKerberosAuth = ( hadoopSecurityAuthenticationn.equalsIgnoreCase(HADOOP_SECURITY_AUTHENTICATION_METHOD));
			}
			else {
				isKerberosAuth = (((userName != null) && (userName.indexOf("@") > -1)) || (SecureClientLogin.isKerberosCredentialExists(lookupPrincipal, lookupKeytab)));
			}
		}
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
	
	public boolean hasResourceExists(String aResourceName) {    // dilli
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
		String ret = null;
		String sectionName = RANGER_SECTION_NAME;

		if ( defaultConfigFile != null) {
			sectionName = defaultConfigFile;
		}

		if ( LOG.isDebugEnabled() ) {
			LOG.debug("==> HadoopConfigHolder.getHadoopSecurityAuthentication( " + " DataSource : " + sectionName + " Property : " +  HADOOP_SECURITY_AUTHENTICATION + ")" );
		}

		ret = getProperties(sectionName,HADOOP_SECURITY_AUTHENTICATION);
		
		if ( LOG.isDebugEnabled() ) {
			LOG.debug("<== HadoopConfigHolder.getHadoopSecurityAuthentication(" + " DataSource : " + sectionName + " Property : " +  HADOOP_SECURITY_AUTHENTICATION  + " Value : " + ret + ")" );
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

	public boolean isKerberosAuthentication() {
		return isKerberosAuth;
	}
	
	public String getLookupPrincipal(){
		return lookupPrincipal;
	}

	public String getLookupKeytab(){
		return lookupKeytab;
	}

	public String getNameRules(){
		return nameRules;
	}
	
	public String getAuthType(){
		return authType;
	}

  public Set<String> getRangerInternalPropertyKeys() {
    return rangerInternalPropertyKeys;

  }

	private String getProperties(String sectionName, String property) {

		if ( LOG.isDebugEnabled() ) {
			LOG.debug("==> HadoopConfigHolder.getProperties( " + " DataSource : " + sectionName + " Property : " +  property + ")" );
		}

		Properties repoParam = null ;
		String ret = null;

		HashMap<String,Properties> resourceName2PropertiesMap  = dataSource2ResourceListMap.get(this.getDatasourceName()) ;

		if ( resourceName2PropertiesMap != null) {
			repoParam=resourceName2PropertiesMap.get(sectionName);
		}

		if ( repoParam != null ) {
			ret = (String)repoParam.get(property);
		}

		if ( LOG.isDebugEnabled() ) {
			LOG.debug("<== HadoopConfigHolder.getProperties( " + " DataSource : " + sectionName + " Property : " +  property + " Value : " + ret);
		}

		return ret;
	}
	

}
