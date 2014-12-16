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

 /**
 *
 */
package org.apache.ranger.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.ranger.credentialapi.CredentialReader;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;



public class PropertiesUtil extends PropertyPlaceholderConfigurer {
    private static Map<String, String> propertiesMap = new HashMap<String, String>();
    private static Logger logger = Logger.getLogger(PropertiesUtil.class);
    private PropertiesUtil() {

    }

    @Override
    protected void processProperties(
	    ConfigurableListableBeanFactory beanFactory, Properties props)
	    throws BeansException {
    	
    // First let's add the system properties
	Set<Object> keySet = System.getProperties().keySet();
	for (Object key : keySet) {
	    String keyStr = key.toString();
	    propertiesMap.put(keyStr, System.getProperties()
		    .getProperty(keyStr).trim());
	}
	
	// Let's add our properties now
	keySet = props.keySet();
	for (Object key : keySet) {
	    String keyStr = key.toString();
	    propertiesMap.put(keyStr, props.getProperty(keyStr).trim());
	}
	
	//update credential from keystore
	if(propertiesMap!=null && propertiesMap.containsKey("xaDB.jdbc.credential.provider.path") && propertiesMap.containsKey("xaDB.jdbc.credential.alias")){	
		String path=propertiesMap.get("xaDB.jdbc.credential.provider.path");
		String alias=propertiesMap.get("xaDB.jdbc.credential.alias");
		if(path!=null && alias!=null){
			String xaDBPassword=CredentialReader.getDecryptedString(path.trim(),alias.trim());		
			if(xaDBPassword!=null&& !xaDBPassword.trim().isEmpty() && 
					!xaDBPassword.trim().equalsIgnoreCase("none")){
				propertiesMap.put("jdbc.password", xaDBPassword);
				props.put("jdbc.password", xaDBPassword);
			}else{
				logger.info("Credential keystore password not applied for XA DB; clear text password shall be applicable");				
			}
		}
	}
	if(propertiesMap!=null && propertiesMap.containsKey("auditDB.jdbc.credential.provider.path") && propertiesMap.containsKey("auditDB.jdbc.credential.alias")){
		String path=propertiesMap.get("auditDB.jdbc.credential.provider.path");
		String alias=propertiesMap.get("auditDB.jdbc.credential.alias");
		if(path!=null && alias!=null){
			String auditDBPassword=CredentialReader.getDecryptedString(path.trim(), alias.trim());
			if(auditDBPassword!=null&& !auditDBPassword.trim().isEmpty() && 
					!auditDBPassword.trim().equalsIgnoreCase("none")){
				propertiesMap.put("auditDB.jdbc.password", auditDBPassword);
				props.put("auditDB.jdbc.password", auditDBPassword);
			}else{
				logger.info("Credential keystore password not applied for Audit DB; clear text password shall be applicable");
			}
		}		
	}	
	super.processProperties(beanFactory, props);
    }

    public static String getProperty(String key, String defaultValue) {
	if (key == null) {
	    return null;
	}
	String rtrnVal = propertiesMap.get(key);
	if (rtrnVal == null) {
	    rtrnVal = defaultValue;
	}
	return rtrnVal;
    }

    public static String getProperty(String key) {
	if (key == null) {
	    return null;
	}
	return propertiesMap.get(key);
    }

    public static String[] getPropertyStringList(String key) {
	if (key == null) {
	    return null;
	}
	String value = propertiesMap.get(key);
	if (value != null) {
	    String[] splitValues = value.split(",");
	    String[] returnValues = new String[splitValues.length];
	    for (int i = 0; i < splitValues.length; i++) {
		returnValues[i] = splitValues[i].trim();
	    }
	    return returnValues;
	} else {
	    return new String[0];
	}
    }

    public static Integer getIntProperty(String key, int defaultValue) {
	if (key == null) {
	    return null;
	}
	String rtrnVal = propertiesMap.get(key);
	if (rtrnVal == null) {
	    return defaultValue;
	}
	return Integer.valueOf(rtrnVal);
    }

    public static Integer getIntProperty(String key) {
	if (key == null) {
	    return null;
	}
	String rtrnVal = propertiesMap.get(key);
	if (rtrnVal == null) {
	    return null;
	}
	return Integer.valueOf(rtrnVal);
    }

    public static boolean getBooleanProperty(String key, boolean defaultValue) {
	if (key == null) {
	    return defaultValue;
	}
	String value = getProperty(key);
	if (value == null) {
	    return defaultValue;
	}
	return Boolean.parseBoolean(value);
    }
}
