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


package com.xasecure.authorization.hadoop.config;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.xasecure.audit.provider.AuditProviderFactory;
import com.xasecure.authorization.hadoop.constants.XaSecureHadoopConstants;

public class XaSecureConfiguration extends Configuration {
	
	private static final Logger LOG = Logger.getLogger(XaSecureConfiguration.class) ;
	
	private static XaSecureConfiguration config = null;
	
	private XaSecureConfiguration() {
		super(false) ;
		
		//
		// WorkAround for having all Hadoop Configuration in the CLASSPATH first, even if it is invoked by Hive Engine.
		// 
		//   So, we look for "hive-site.xml", if it is available, take the xasecure-audit.xml file from the same location.
		//   If we do not see "hive-site.xml", we look for "hbase-site.xml", if found, take the xasecure-audit.xml file from the same location.
		//   If we do not see "hbase-site.xml", we look for "hdfs-site.xml", if found, take the xasecure-audit.xml file from the same location.
		//   If we do not see, we let the CLASSPATH based search to find xasecure-audit.xml file.
		
		
		URL auditFileLocation = getXAAuditXMLFileLocation() ;
		
		if (auditFileLocation != null) {
			addResource(auditFileLocation) ;
		}
		else {
			addResourceIfReadable(XaSecureHadoopConstants.XASECURE_AUDIT_FILE) ;
		}
		addResourceIfReadable(XaSecureHadoopConstants.XASECURE_HDFS_SECURITY_FILE);
		addResourceIfReadable(XaSecureHadoopConstants.XASECURE_KNOX_SECURITY_FILE);
		addResourceIfReadable(XaSecureHadoopConstants.XASECURE_HBASE_SECURITY_FILE) ;
		addResourceIfReadable(XaSecureHadoopConstants.XASECURE_HIVE_SECURITY_FILE) ;
		addResourceIfReadable(XaSecureHadoopConstants.XASECURE_STORM_SECURITY_FILE);
		
	}
	
	@SuppressWarnings("deprecation")
	private void addResourceIfReadable(String aResourceName) {
		String fName = getFileLocation(aResourceName) ;
		if (fName != null) {
			File f = new File(fName) ;
			if (f.exists() && f.canRead()) {
				URL fUrl = null ;
				try {
					fUrl = f.toURL() ;
					addResource(fUrl) ;
				} catch (MalformedURLException e) {
					LOG.debug("Unable to find URL for the resource name [" + aResourceName +"]. Ignoring the resource:" + aResourceName);
				}
			}
		}
	}
	

	public static XaSecureConfiguration getInstance() {
		if (config == null) {
			synchronized (XaSecureConfiguration.class) {
				XaSecureConfiguration temp = config;
				if (temp == null) {
					config = new XaSecureConfiguration();
				}
			}
		}
		return config;
	}

	public void initAudit(AuditProviderFactory.ApplicationType appType) {
		AuditProviderFactory auditFactory = AuditProviderFactory.getInstance();

		if(auditFactory == null) {
			LOG.error("Unable to find the AuditProviderFactory. (null) found") ;
			return;
		}

		Properties props = getProps();

		if(props == null) {
			return;
		}

		if(! auditFactory.isInitDone()) {
			auditFactory.init(props, appType);
		}
	}

	public boolean isAuditInitDone() {
		AuditProviderFactory auditFactory = AuditProviderFactory.getInstance();

		return auditFactory != null && auditFactory.isInitDone();
	}

	
	@SuppressWarnings("deprecation")
	public  URL getXAAuditXMLFileLocation() {
		URL ret = null ;

		try {
			for(String  cfgFile : 	new String[] {  "hive-site.xml",  "hbase-site.xml",  "hdfs-site.xml" } ) {
				String loc = getFileLocation(cfgFile) ;
				if (loc != null) {
					if (new File(loc).canRead()) {
						File parentFile = new File(loc).getParentFile() ;
						ret = new File(parentFile, XaSecureHadoopConstants.XASECURE_AUDIT_FILE).toURL() ;
						break ;
					}
				}
			}
		}
		catch(Throwable t) {
			LOG.error("Unable to locate audit file location." , t) ;
			ret = null ;
		}
		
		return ret ;
	}
	
	private String getFileLocation(String fileName) {
		
		String ret = null ;
		
		URL lurl = XaSecureConfiguration.class.getClassLoader().getResource(fileName) ;
		
		if (lurl == null ) {
			lurl = XaSecureConfiguration.class.getClassLoader().getResource("/" + fileName) ;
		}
		
		if (lurl != null) {
			ret = lurl.getFile() ;
		}
		
		return ret ;
	}

}
