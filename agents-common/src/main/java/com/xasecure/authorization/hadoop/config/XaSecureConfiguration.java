/**************************************************************************
 *                                                                        *
 * The information in this document is proprietary to XASecure Inc.,      *
 * It may not be used, reproduced or disclosed without the written        *
 * approval from the XASecure Inc.,                                       *
 *                                                                        *
 * PRIVILEGED AND CONFIDENTIAL XASECURE PROPRIETARY INFORMATION           *
 *                                                                        *
 * Copyright (c) 2013 XASecure, Inc.  All rights reserved.                *
 *                                                                        *
 *************************************************************************/

 /**
  *
  *	@version: 1.0.004
  *
  */

package com.xasecure.authorization.hadoop.config;

import java.io.File;
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
			addResource(XaSecureHadoopConstants.XASECURE_AUDIT_FILE) ;
		}
		addResource(XaSecureHadoopConstants.XASECURE_HDFS_SECURITY_FILE);
		addResource(XaSecureHadoopConstants.XASECURE_KNOX_SECURITY_FILE);
		addResource(XaSecureHadoopConstants.XASECURE_HBASE_SECURITY_FILE) ;
		addResource(XaSecureHadoopConstants.XASECURE_HIVE_SECURITY_FILE) ;
		addResource(XaSecureHadoopConstants.XASECURE_KEYMGR_FILE) ;
	}

	public static XaSecureConfiguration getInstance() {
		if (config == null) {
			synchronized (XaSecureConfiguration.class) {
				XaSecureConfiguration temp = config;
				if (temp == null) {
					config = new XaSecureConfiguration();
					
					onConfigInstantiation(config);
				}
			}
		}
		return config;
	}
	
	private static void onConfigInstantiation(XaSecureConfiguration config) {
		initAudit(config);
	}

	private static void initAudit(XaSecureConfiguration config) {
		AuditProviderFactory auditFactory = AuditProviderFactory.getInstance();
		
		if(auditFactory == null) {
			LOG.error("Unable to find the AuditProviderFactory. (null) found") ;
			return;
		}
		
		Properties props = config.getProps();
		
		if(props == null) {
			return;
		}
		
		auditFactory.init(props);
	}
	
	
	@SuppressWarnings("deprecation")
	public  URL getXAAuditXMLFileLocation() {
		URL ret = null ;

		try {
			for(String  cfgFile : 	new String[] {  "hive-site.xml",  "hbase-site.xml",  "hdfs-site.xml" } ) {
				String loc = getFileLocation(cfgFile) ;
				if (loc != null) {
					File parentFile = new File(loc).getParentFile() ;
					ret = new File(parentFile, XaSecureHadoopConstants.XASECURE_AUDIT_FILE).toURL() ;
					break ;
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
