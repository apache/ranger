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

package org.apache.ranger.authorization.hadoop.config;

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;

public class RangerLegacyConfigBuilder {

	public static final Logger LOG = Logger.getLogger(RangerLegacyConfigBuilder.class);

	static String serviceType 	   = null;
	static String legacyResource  = null;


	public static Configuration getSecurityConfig(String serviceType) {

		RangerLegacyConfigBuilder.legacyResource = getPropertyName(RangerConfigConstants.XASECURE_SECURITY_FILE,serviceType);
		RangerLegacyConfigBuilder.serviceType	 = serviceType;

		Configuration	ret				= null;
		Configuration   legacyConfig	= new Configuration();
		URL 			legacyFileUrl	= getFileURL(legacyResource);

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> getSecurityConfig() " + legacyResource + " FileName: " + legacyFileUrl);
		}

		if ( legacyFileUrl  != null) {
			legacyConfig.addResource(legacyFileUrl);
			Configuration rangerDefaultProp =  buildRangerSecurityConf(serviceType);
					   ret	 			    =  mapLegacyConfigToRanger(rangerDefaultProp,legacyConfig);
		}
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== getSecurityConfig() " + legacyResource + " FileName: " + legacyFileUrl);
		}

		return ret;
	}

	public static URL getAuditConfig(String serviceType) throws Throwable {
		
		RangerLegacyConfigBuilder.legacyResource = getPropertyName(RangerConfigConstants.XASECURE_AUDIT_FILE,serviceType);
		RangerLegacyConfigBuilder.serviceType	 = serviceType;
		
		URL ret = null;
		try {
			ret = getAuditResource(legacyResource);
		} catch (Throwable t) {
		   throw t;
		}
		return ret;
	}

   private static  Configuration mapLegacyConfigToRanger(Configuration rangerInConf, Configuration legacyConf) {
	   
	    Configuration ret 				   = rangerInConf;
	   
	    HashMap<String,String>  chgMap     = getConfigChangeMap(serviceType);
	    if(LOG.isDebugEnabled()) {
			LOG.debug("<== mapLegacyConfigToRanger() MAP Size:  " + chgMap.size());
		} 
		for(Map.Entry<String, String> entry : chgMap.entrySet()) {
			String legacyKey 	 = entry.getKey();
			String rangerKey 	 = entry.getValue();
			
			String legacyConfVal = null;
			
			if ( rangerKey.equals(getPropertyName(RangerConfigConstants.RANGER_SERVICE_NAME,serviceType)) )  {
				//For getting the service
				String serviceURL = legacyConf.get(getPropertyName(RangerConfigConstants.XASECURE_POLICYMGR_URL,serviceType));
				legacyConfVal = fetchLegacyValue(serviceURL,rangerKey);
			} else if  ( rangerKey.equals(getPropertyName(RangerConfigConstants.RANGER_PLUGIN_POLICY_REST_URL,serviceType)) || 
					     rangerKey.equals(getPropertyName(RangerConfigConstants.RANGER_PLUGIN_POLICY_CACHE_DIR,serviceType)) ) {
				// For Getting Admin URL and CacheDir
				legacyConfVal = fetchLegacyValue(legacyConf.get(legacyKey),rangerKey); 
			} else {
				legacyConfVal = legacyConf.get(legacyKey);
			}
			
			if(LOG.isDebugEnabled()) {
				LOG.debug("<== mapLegacyConfigToRanger() Ranger Key: " + rangerKey + "Legacy Key:" + legacyKey +  "Legacy Value:" + legacyConfVal);
			}
			
			ret.set(rangerKey, legacyConfVal);	
		}
		return ret;
	} 
   
   
	public  static URL getAuditResource(String fName) throws Throwable {
		URL ret = null ;

		try {
			for(String  cfgFile : 	new String[] {  "hive-site.xml",  "hbase-site.xml",  "hdfs-site.xml" } ) {
				String loc = getFileLocation(cfgFile) ;
				if (loc != null) {
					File f = new File(loc);
					if ( f.exists() && f.canRead()) {
						File parentFile = new File(loc).getParentFile() ;
								   ret  = new File(parentFile, RangerConfigConstants.XASECURE_AUDIT_FILE).toURI().toURL();
						break ;
					}
				}
			}
		}
		catch(Throwable t) {
			LOG.error("Unable to locate audit file location." + fName + " " + t) ;
			throw t;
		}
		return ret ;
	}

	public static Configuration  buildRangerSecurityConf(String serviceType) {

		Configuration rangerConf = new Configuration();

		rangerConf.set(getPropertyName(RangerConfigConstants.RANGER_SERVICE_NAME,serviceType),"");
		if (serviceType.equals(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_KNOX_NAME) )  {
			rangerConf.set(getPropertyName(RangerConfigConstants.RANGER_PLUGIN_POLICY_SOURCE_IMPL,serviceType),RangerConfigConstants.RANGER_KNOX_PLUGIN_POLICY_SOURCE_IMPL_DEFAULT);
		} else {
			rangerConf.set(getPropertyName(RangerConfigConstants.RANGER_PLUGIN_POLICY_SOURCE_IMPL,serviceType),"");
		}
		rangerConf.set(getPropertyName(RangerConfigConstants.RANGER_PLUGIN_POLICY_REST_URL,serviceType),"");
		rangerConf.set(getPropertyName(RangerConfigConstants.RANGER_PLUGIN_REST_SSL_CONFIG_FILE,serviceType), getPropertyName(RangerConfigConstants.XASECURE_POLICYMGR_SSL_FILE,serviceType));
		rangerConf.set(getPropertyName(RangerConfigConstants.RANGER_PLUGIN_POLICY_POLLINVETERVALMS,serviceType), "");
		rangerConf.set(getPropertyName(RangerConfigConstants.RANGER_PLUGIN_POLICY_CACHE_DIR,serviceType), "");
		rangerConf.set(RangerConfigConstants.RANGER_PLUGIN_ADD_HADDOOP_AUTHORIZATION,"");

		return rangerConf;
	}


	public static HashMap<String, String> getConfigChangeMap(String serviceType) {
		// ConfigMap for moving legacy Configuration to Ranger Configuration
		HashMap<String, String> changeMap = new HashMap<String,String>();
		
		changeMap.put(serviceType,
					  getPropertyName(RangerConfigConstants.RANGER_SERVICE_NAME,serviceType));
		changeMap.put(getPropertyName(RangerConfigConstants.XASECURE_POLICYMGR_URL,serviceType),
					  getPropertyName(RangerConfigConstants.RANGER_PLUGIN_POLICY_REST_URL,serviceType));
		changeMap.put(getPropertyName(RangerConfigConstants.XASECURE_POLICYMGR_GRL_RELOADINTERVALINMILLIS,serviceType),
					  getPropertyName(RangerConfigConstants.RANGER_PLUGIN_POLICY_POLLINVETERVALMS,serviceType));
		changeMap.put(getPropertyName(RangerConfigConstants.XASECURE_POLICYMGR_URL_LASTSTOREDFILE,serviceType),
					  getPropertyName(RangerConfigConstants.RANGER_PLUGIN_POLICY_CACHE_DIR,serviceType));
		
		
		if (serviceType.equals(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_HDFS_NAME)) {
			changeMap.put(RangerConfigConstants.XASECURE_ADD_HADDOP_AUTHORZATION,
				      RangerConfigConstants.RANGER_PLUGIN_ADD_HADDOOP_AUTHORIZATION);
		}
		
		if (serviceType.equals(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_HBASE_NAME) ||
			serviceType.equals(EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_HIVE_NAME)) {
			changeMap.put(getPropertyName(RangerConfigConstants.XASECURE_UPDATE_XAPOLICIES_ON_GRANT,serviceType),
					getPropertyName(RangerConfigConstants.XASECURE_UPDATE_XAPOLICIES_ON_GRANT,serviceType));
		}
		
		if ( LOG.isDebugEnabled()) {
			for(Map.Entry<String, String> entry : changeMap.entrySet()) {
				String legacyKey 	 = entry.getKey();
				String rangerKey 	 = entry.getValue();
				LOG.debug("<== getConfigChangeMap() RangerConfig Key: " + rangerKey + " Legacy Key: " + legacyKey);
			}
		}
		
		return changeMap;
	}

	public static String getFileLocation(String fileName) {
		String ret = null ;
		
		URL lurl = RangerLegacyConfigBuilder.class.getClassLoader().getResource(fileName) ;
		if (lurl == null ) {
			lurl = RangerLegacyConfigBuilder.class.getClassLoader().getResource("/" + fileName) ;
		}
		if (lurl != null) {
			ret = lurl.getFile() ;
		}
		return ret ;
	}

	public static URL getFileURL(String fileName) {
		URL lurl = RangerLegacyConfigBuilder.class.getClassLoader().getResource(fileName) ;
		return lurl ;
	}

	public static String getPropertyName(String rangerProp, String serviceType) {
		
		String ret = rangerProp.replace("<ServiceType>", serviceType);
		
		return ret;
	}	

	public static String getPolicyMgrURL(String url) {
		String ret = null;
		
		int index = url.indexOf("/",url.lastIndexOf(":"));
		
		ret = url.substring(0,index);
		
		return ret;
	}

	public static String getServiceNameFromURL(String url) {
		String ret = null;
		
		int index = url.lastIndexOf("/");
		
		ret = url.substring(index+1);
		
		return ret;
	}

	
	public static String getCacheFileURL(String cacheFile) {
		String ret = null;
		
		int index = cacheFile.lastIndexOf("/");
		
		ret = cacheFile.substring(0,index);
		
		return ret;
	}

	public static String fetchLegacyValue(String legacyVal, String rangerKey) {
		String ret = null;
	
		if ( rangerKey.equals(getPropertyName(RangerConfigConstants.RANGER_SERVICE_NAME,serviceType)) ) {
			// To Fetch ServiceName
			ret = getServiceNameFromURL(legacyVal);
		} else if ( rangerKey.equals(getPropertyName(RangerConfigConstants.RANGER_PLUGIN_POLICY_REST_URL,serviceType)) ) {
		   // To Fetch PolicyMgr URL
		   ret = getPolicyMgrURL(legacyVal);		  
		} else if  ( rangerKey.equals(getPropertyName(RangerConfigConstants.RANGER_PLUGIN_POLICY_CACHE_DIR,serviceType)) ) {
			  ret = getCacheFileURL(legacyVal);  
	   }
	   
	   return ret;
	}
}
