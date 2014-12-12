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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants;

public class RangerConfiguration extends Configuration {
	
	private static final Logger LOG = Logger.getLogger(RangerConfiguration.class) ;
	
	private static RangerConfiguration config = null;
	
	private RangerConfiguration() {
		super(false) ;
		
		//
		// WorkAround for having all Hadoop Configuration in the CLASSPATH first, even if it is invoked by Hive Engine.
		// 
		//   So, we look for "hive-site.xml", if it is available, take the xasecure-audit.xml file from the same location.
		//   If we do not see "hive-site.xml", we look for "hbase-site.xml", if found, take the xasecure-audit.xml file from the same location.
		//   If we do not see "hbase-site.xml", we look for "hdfs-site.xml", if found, take the xasecure-audit.xml file from the same location.
		//   If we do not see, we let the CLASSPATH based search to find xasecure-audit.xml file.
		
		
		URL auditFileLocation = getRangerAuditXMLFileLocation() ;
		
		if (auditFileLocation != null) {
			addResource(auditFileLocation) ;
		}
		else {
			addResourceIfReadable(RangerHadoopConstants.RANGER_AUDIT_FILE) ;
		}
		addResourceIfReadable(RangerHadoopConstants.RANGER_HDFS_SECURITY_FILE);
		addResourceIfReadable(RangerHadoopConstants.RANGER_KNOX_SECURITY_FILE);
		addResourceIfReadable(RangerHadoopConstants.RANGER_HBASE_SECURITY_FILE) ;
		addResourceIfReadable(RangerHadoopConstants.RANGER_HIVE_SECURITY_FILE) ;
		addResourceIfReadable(RangerHadoopConstants.RANGER_STORM_SECURITY_FILE);
		
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
	

	public static RangerConfiguration getInstance() {
		if (config == null) {
			synchronized (RangerConfiguration.class) {
				RangerConfiguration temp = config;
				if (temp == null) {
					config = new RangerConfiguration();
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
	public  URL getRangerAuditXMLFileLocation() {
		URL ret = null ;

		try {
			for(String  cfgFile : 	new String[] {  "hive-site.xml",  "hbase-site.xml",  "hdfs-site.xml" } ) {
				String loc = getFileLocation(cfgFile) ;
				if (loc != null) {
					if (new File(loc).canRead()) {
						File parentFile = new File(loc).getParentFile() ;
						ret = new File(parentFile, RangerHadoopConstants.RANGER_AUDIT_FILE).toURL() ;
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
		
		URL lurl = RangerConfiguration.class.getClassLoader().getResource(fileName) ;
		
		if (lurl == null ) {
			lurl = RangerConfiguration.class.getClassLoader().getResource("/" + fileName) ;
		}
		
		if (lurl != null) {
			ret = lurl.getFile() ;
		}
		
		return ret ;
	}

}
