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
	
	private static volatile RangerConfiguration config = null;
	
	private RangerConfiguration() {
		super(false) ;
	}

	public void addResourcesForServiceType(String serviceType) {
		String auditCfg    = "ranger-" + serviceType + "-audit.xml";
		String securityCfg = "ranger-" + serviceType + "-security.xml";
		
		if ( !addResourceIfReadable(auditCfg)) {
			addAuditResource(serviceType);
		}

		if ( !addResourceIfReadable(securityCfg)) {
			addSecurityResource(serviceType);
		}
	}

	public boolean addAdminResources() {
		String defaultCfg = "ranger-admin-default-site.xml";
		String addlCfg = "ranger-admin-site.xml";
		String coreCfg = "core-site.xml";

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> addAdminResources()");
		}
		boolean ret = true;

		if (! addResourceIfReadable(defaultCfg)) {
			ret = false;
		}

		if (! addResourceIfReadable(addlCfg)) {
			ret = false;
		}
		
		if(! addResourceIfReadable(coreCfg)){
			ret = false;
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== addAdminResources(), result=" + ret);
		}
		return ret;
	}

	private boolean addResourceIfReadable(String aResourceName) {
		
		boolean ret = false;
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> addResourceIfReadable(" + aResourceName + ")");
		}

		String fName = getFileLocation(aResourceName) ;
		if (fName != null) {
			if(LOG.isInfoEnabled()) {
				LOG.info("addResourceIfReadable(" + aResourceName + "): resource file is " + fName);
			}

			File f = new File(fName) ;
			if (f.exists() && f.canRead()) {
				URL fUrl = null ;
				try {
					fUrl = f.toURI().toURL() ;
					addResource(fUrl) ;
					ret = true;
				} catch (MalformedURLException e) {
					LOG.error("Unable to find URL for the resource name [" + aResourceName + "]. Ignoring the resource:" + aResourceName);
				}
			} else {
				LOG.error("addResourceIfReadable(" + aResourceName + "): resource not readable");
			}
		} else {
			LOG.error("addResourceIfReadable(" + aResourceName + "): couldn't find resource file location");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== addResourceIfReadable(" + aResourceName + "), result=" + ret);
		}
		return ret;
	}
	

	public static RangerConfiguration getInstance() {
        RangerConfiguration result = config;
		if (result == null) {
			synchronized (RangerConfiguration.class) {
				result = config;
				if (result == null) {
					config = result = new RangerConfiguration();
				}
			}
		}
		return result;
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
	
	private void  addSecurityResource(String serviceType) {

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> addSecurityResource(Service Type: " + serviceType );
		}

		Configuration rangerConf = RangerLegacyConfigBuilder.getSecurityConfig(serviceType);

		if ( rangerConf != null ) {
			addResource(rangerConf);
		} else {
			if(LOG.isDebugEnabled()) {
				LOG.debug("Unable to add the Security Config for" + serviceType + "Pluing won't be enabled!");
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<= addSecurityResource(Service Type: " + serviceType );
		}
	}

	private void  addAuditResource(String serviceType) {

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> addAuditResource(Service Type: " + serviceType );
		}

		URL url = null;
		try {
			url = RangerLegacyConfigBuilder.getAuditConfig(serviceType);

			if( url != null) {
				addResource(url);
				
				if(LOG.isDebugEnabled()) {
					LOG.debug("==> addAuditResource() URL" + url.getPath());
				}
			}
				
		} catch (Throwable t) {
			LOG.warn(" Unable to find Audit Config for "  + serviceType + " Auditing not enabled !" );
			if(LOG.isDebugEnabled()) {
				LOG.debug(" Unable to find Audit Config for "  + serviceType + " Auditing not enabled !" + t);
			}
		}
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== addAuditResource(Service Type: " + serviceType + ")");
		}
	}

}
