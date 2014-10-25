/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.xasecure.audit.provider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xasecure.audit.provider.hdfs.HdfsAuditProvider;
import com.xasecure.authorization.hadoop.utils.XaSecureCredentialProvider;


/*
 * TODO:
 * 1) Flag to enable/disable audit logging
 * 2) Failed path to be recorded
 * 3) Repo name, repo type from configuration
 */

public class AuditProviderFactory {

	public enum ApplicationType { Unknown, Hdfs, HiveCLI, HiveServer2, HBaseMaster, HBaseRegionalServer, Knox, Storm };

	private static final Log LOG = LogFactory.getLog(AuditProviderFactory.class);

	private static final String AUDIT_IS_ENABLED_PROP               = "xasecure.audit.is.enabled" ;

	private static final String AUDIT_DB_IS_ENABLED_PROP            = "xasecure.audit.db.is.enabled" ;
	private static final String AUDIT_DB_IS_ASYNC_PROP              = "xasecure.audit.db.is.async";
	private static final String AUDIT_DB_MAX_QUEUE_SIZE_PROP        = "xasecure.audit.db.async.max.queue.size" ;
	private static final String AUDIT_DB_MAX_FLUSH_INTERVAL_PROP    = "xasecure.audit.db.async.max.flush.interval.ms";
	private static final String AUDIT_DB_BATCH_SIZE_PROP            = "xasecure.audit.db.batch.size" ;
	private static final String AUDIT_DB_RETRY_MIN_INTERVAL_PROP    = "xasecure.audit.db.config.retry.min.interval.ms";
	private static final String AUDIT_JPA_CONFIG_PROP_PREFIX        = "xasecure.audit.jpa.";
	private static final String AUDIT_DB_CREDENTIAL_PROVIDER_FILE   = "xasecure.audit.credential.provider.file";
	private static final String AUDIT_DB_CREDENTIAL_PROVIDER_ALIAS	= "auditDBCred";
	private static final String AUDIT_JPA_JDBC_PASSWORD  			= "javax.persistence.jdbc.password";

	private static final String AUDIT_HDFS_IS_ENABLED_PROP          = "xasecure.audit.hdfs.is.enabled";
	private static final String AUDIT_HDFS_IS_ASYNC_PROP            = "xasecure.audit.hdfs.is.async";
	private static final String AUDIT_HDFS_MAX_QUEUE_SIZE_PROP      = "xasecure.audit.hdfs.async.max.queue.size" ;
	private static final String AUDIT_HDFS_MAX_FLUSH_INTERVAL_PROP  = "xasecure.audit.hdfs.async.max.flush.interval.ms";
	private static final String AUDIT_HDFS_CONFIG_PREFIX_PROP       = "xasecure.audit.hdfs.config.";

	private static final String AUDIT_LOG4J_IS_ENABLED_PROP         = "xasecure.audit.log4j.is.enabled" ;
	private static final String AUDIT_LOG4J_IS_ASYNC_PROP           = "xasecure.audit.log4j.is.async";
	private static final String AUDIT_LOG4J_MAX_QUEUE_SIZE_PROP     = "xasecure.audit.log4j.async.max.queue.size" ;
	private static final String AUDIT_LOG4J_MAX_FLUSH_INTERVAL_PROP = "xasecure.audit.log4j.async.max.flush.interval.ms";

	private static AuditProviderFactory sFactory;

	private AuditProvider mProvider = null;
	private boolean       mInitDone = false;

	private AuditProviderFactory() {
		LOG.info("AuditProviderFactory: creating..");

		mProvider = getDefaultProvider();
	}

	public static AuditProviderFactory getInstance() {
		if(sFactory == null) {
			synchronized(AuditProviderFactory.class) {
				if(sFactory == null) {
					sFactory = new AuditProviderFactory();
				}
			}
		}

		return sFactory;
	}

	public static AuditProvider getAuditProvider() {
		return AuditProviderFactory.getInstance().getProvider();
	}
	
	public AuditProvider getProvider() {
		return mProvider;
	}

	public boolean isInitDone() {
		return mInitDone;
	}

	public synchronized void init(Properties props, ApplicationType appType) {
		LOG.info("AuditProviderFactory: initializing..");
		
		if(mInitDone) {
			LOG.warn("AuditProviderFactory.init(): already initialized!", new Exception());

			return;
		}
		mInitDone = true;
		
		setApplicationType(appType);

		boolean isEnabled             = getBooleanProperty(props, AUDIT_IS_ENABLED_PROP, false);
		boolean isAuditToDbEnabled    = getBooleanProperty(props, AUDIT_DB_IS_ENABLED_PROP, false);
		boolean isAuditToHdfsEnabled  = getBooleanProperty(props, AUDIT_HDFS_IS_ENABLED_PROP, false);
		boolean isAuditToLog4jEnabled = getBooleanProperty(props, AUDIT_LOG4J_IS_ENABLED_PROP, false);

		if(!isEnabled || !(isAuditToDbEnabled || isAuditToHdfsEnabled || isAuditToLog4jEnabled)) {
			LOG.info("AuditProviderFactory: Audit not enabled..");

			mProvider = getDefaultProvider();

			return;
		}

		List<AuditProvider> providers = new ArrayList<AuditProvider>();

		if(isAuditToDbEnabled) {
			Map<String, String> jpaInitProperties = getPropertiesWithPrefix(props, AUDIT_JPA_CONFIG_PROP_PREFIX);

			String jdbcPassword = getCredentialString(getStringProperty(props, AUDIT_DB_CREDENTIAL_PROVIDER_FILE), AUDIT_DB_CREDENTIAL_PROVIDER_ALIAS);

			if(jdbcPassword != null && !jdbcPassword.isEmpty()) {
				jpaInitProperties.put(AUDIT_JPA_JDBC_PASSWORD, jdbcPassword);
			}

			LOG.info("AuditProviderFactory: found " + jpaInitProperties.size() + " Audit JPA properties");
	
			int dbBatchSize          = getIntProperty(props, AUDIT_DB_BATCH_SIZE_PROP, 1000);
			int dbRetryMinIntervalMs = getIntProperty(props, AUDIT_DB_RETRY_MIN_INTERVAL_PROP, 15 * 1000);
			boolean isAuditToDbAsync = getBooleanProperty(props, AUDIT_DB_IS_ASYNC_PROP, false);
			
			if(! isAuditToDbAsync) {
				dbBatchSize = 1; // Batching not supported in sync mode; need to address multiple threads making audit calls
			}

			DbAuditProvider dbProvider = new DbAuditProvider(jpaInitProperties, dbBatchSize, dbRetryMinIntervalMs);
			
			if(isAuditToDbAsync) {
				int maxQueueSize     = getIntProperty(props, AUDIT_DB_MAX_QUEUE_SIZE_PROP, -1);
				int maxFlushInterval = getIntProperty(props, AUDIT_DB_MAX_FLUSH_INTERVAL_PROP, -1);

				AsyncAuditProvider asyncProvider = new AsyncAuditProvider("DbAuditProvider", maxQueueSize, maxFlushInterval, dbProvider);
				
				providers.add(asyncProvider);
			} else {
				providers.add(dbProvider);
			}
		}

		if(isAuditToHdfsEnabled) {
			Map<String, String> hdfsInitProperties = getPropertiesWithPrefix(props, AUDIT_HDFS_CONFIG_PREFIX_PROP);

			LOG.info("AuditProviderFactory: found " + hdfsInitProperties.size() + " Audit HDFS properties");
			
			HdfsAuditProvider hdfsProvider = new HdfsAuditProvider();
			
			hdfsProvider.init(hdfsInitProperties);

			boolean isAuditToHdfsAsync = getBooleanProperty(props, AUDIT_HDFS_IS_ASYNC_PROP, false);

			if(isAuditToHdfsAsync) {
				int maxQueueSize     = getIntProperty(props, AUDIT_HDFS_MAX_QUEUE_SIZE_PROP, -1);
				int maxFlushInterval = getIntProperty(props, AUDIT_HDFS_MAX_FLUSH_INTERVAL_PROP, -1);

				AsyncAuditProvider asyncProvider = new AsyncAuditProvider("HdfsAuditProvider", maxQueueSize, maxFlushInterval, hdfsProvider);
				
				providers.add(asyncProvider);
			} else {
				providers.add(hdfsProvider);
			}
		}

		if(isAuditToLog4jEnabled) {
			Log4jAuditProvider log4jProvider = new Log4jAuditProvider();

			boolean isAuditToLog4jAsync = getBooleanProperty(props, AUDIT_LOG4J_IS_ASYNC_PROP, false);
			
			if(isAuditToLog4jAsync) {
				int maxQueueSize     = getIntProperty(props, AUDIT_LOG4J_MAX_QUEUE_SIZE_PROP, -1);
				int maxFlushInterval = getIntProperty(props, AUDIT_LOG4J_MAX_FLUSH_INTERVAL_PROP, -1);

				AsyncAuditProvider asyncProvider = new AsyncAuditProvider("Log4jAuditProvider", maxQueueSize, maxFlushInterval, log4jProvider);
				
				providers.add(asyncProvider);
			} else {
				providers.add(log4jProvider);
			}
		}

		if(providers.size() == 0) {
			mProvider = getDefaultProvider();
		} else if(providers.size() == 1) {
			mProvider = providers.get(0);
		} else {
			MultiDestAuditProvider multiDestProvider = new MultiDestAuditProvider();
			
			multiDestProvider.addAuditProviders(providers);
			
			mProvider = multiDestProvider;
		}
		
		mProvider.start();

		JVMShutdownHook jvmShutdownHook = new JVMShutdownHook(mProvider);

	    Runtime.getRuntime().addShutdownHook(jvmShutdownHook);
	}

	private static void setApplicationType(ApplicationType appType) {
		String strAppType = null;

		switch(appType) {
			case Hdfs:
				strAppType = "hdfs";
			break;
	
			case HiveCLI:
				strAppType = "hiveCli";
			break;
	
			case HiveServer2:
				strAppType = "hiveServer2";
			break;
	
			case HBaseMaster:
				strAppType = "hbaseMaster";
			break;

			case HBaseRegionalServer:
				strAppType = "hbaseRegional";
			break;

			case Knox:
				strAppType = "knox";
			break;

			case Storm:
				strAppType = "storm";
			break;

			case Unknown:
				strAppType = "unknown";
			break;
		}

		MiscUtil.setApplicationType(strAppType);
	}
	
	private Map<String, String> getPropertiesWithPrefix(Properties props, String prefix) {
		Map<String, String> prefixedProperties = new HashMap<String, String>();
		
		for(String key : props.stringPropertyNames()) {
			if(key == null) {
				continue;
			}
			
			String val = props.getProperty(key);
			
			if(key.startsWith(prefix)) {
				key = key.substring(prefix.length());

				if(key == null) {
					continue;
				}
				
				prefixedProperties.put(key, val);
			}
		}

            
		return prefixedProperties;
	}
	
	private boolean getBooleanProperty(Properties props, String propName, boolean defValue) {
		boolean ret = defValue;

		if(props != null && propName != null) {
			String val = props.getProperty(propName);
			
			if(val != null) {
				ret = Boolean.valueOf(val);
			}
		}
		
		return ret;
	}
	
	private int getIntProperty(Properties props, String propName, int defValue) {
		int ret = defValue;

		if(props != null && propName != null) {
			String val = props.getProperty(propName);
			
			if(val != null) {
				ret = Integer.parseInt(val);
			}
		}
		
		return ret;
	}
	
	
	private String getStringProperty(Properties props, String propName) {
	
		String ret = null;
		if(props != null && propName != null) {
			String val = props.getProperty(propName);
			if ( val != null){
				ret = val;
			}
			
		}
		
		return ret;
	}
	
	private AuditProvider getDefaultProvider() {
		return new DummyAuditProvider();
	}

	private static class JVMShutdownHook extends Thread {
		AuditProvider mProvider;

		public JVMShutdownHook(AuditProvider provider) {
			mProvider = provider;
		}

		public void run() {
			mProvider.waitToComplete();
			mProvider.stop();
	    }
	  }

	
	private String getCredentialString(String url,String alias) {
		String ret = null;

		if(url != null && alias != null) {
			char[] cred = XaSecureCredentialProvider.getInstance().getCredentialString(url,alias);

			if ( cred != null ) {
				ret = new String(cred);	
			}
		}
		
		return ret;
	}
}
