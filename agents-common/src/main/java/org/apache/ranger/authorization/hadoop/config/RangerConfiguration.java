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

public class RangerConfiguration extends Configuration {
	
	private static final Logger LOG = Logger.getLogger(RangerConfiguration.class) ;
	
	private static RangerConfiguration config = null;
	
	private RangerConfiguration() {
		super(false) ;
	}

	public void addResourcesForServiceType(String serviceType) {
		String auditCfg    = "ranger-" + serviceType + "-audit.xml";
		String securityCfg = "ranger-" + serviceType + "-security.xml";
		
		addResourceIfReadable(auditCfg);
		addResourceIfReadable(securityCfg);
	}

	@SuppressWarnings("deprecation")
	private void addResourceIfReadable(String aResourceName) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> addResourceIfReadable(" + aResourceName + ")");
		}

		String fName = getFileLocation(aResourceName) ;
		if (fName != null) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("<== addResourceIfReadable(" + aResourceName + "): resource file is " + fName);
			}

			File f = new File(fName) ;
			if (f.exists() && f.canRead()) {
				URL fUrl = null ;
				try {
					fUrl = f.toURL() ;
					addResource(fUrl) ;
				} catch (MalformedURLException e) {
					if(LOG.isDebugEnabled()) {
						LOG.debug("Unable to find URL for the resource name [" + aResourceName +"]. Ignoring the resource:" + aResourceName);
					}
				}
			} else {
				if(LOG.isDebugEnabled()) {
					LOG.debug("<== addResourceIfReadable(" + aResourceName + "): resource not readable");
				}
			}
		} else {
			if(LOG.isDebugEnabled()) {
				LOG.debug("<== addResourceIfReadable(" + aResourceName + "): couldn't find resource file location");
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== addResourceIfReadable(" + aResourceName + ")");
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

	public void initAudit(String appType) {
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
