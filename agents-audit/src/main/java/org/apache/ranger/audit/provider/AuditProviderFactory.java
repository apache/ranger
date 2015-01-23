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

package org.apache.ranger.audit.provider;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.audit.provider.hdfs.HdfsAuditProvider;


/*
 * TODO:
 * 1) Flag to enable/disable audit logging
 * 2) Failed path to be recorded
 * 3) Repo name, repo type from configuration
 */

public class AuditProviderFactory {
	private static final Log LOG = LogFactory.getLog(AuditProviderFactory.class);

	private static final String AUDIT_IS_ENABLED_PROP       = "xasecure.audit.is.enabled" ;
	private static final String AUDIT_DB_IS_ENABLED_PROP    = "xasecure.audit.db.is.enabled" ;
	private static final String AUDIT_HDFS_IS_ENABLED_PROP  = "xasecure.audit.hdfs.is.enabled";
	private static final String AUDIT_LOG4J_IS_ENABLED_PROP = "xasecure.audit.log4j.is.enabled" ;
	
	private static final int AUDIT_ASYNC_MAX_QUEUE_SIZE_DEFAULT     = 10 * 1024;
	private static final int AUDIT_ASYNC_MAX_FLUSH_INTERVAL_DEFAULT =  5 * 1000;
	
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

	public synchronized void init(Properties props, String appType) {
		LOG.info("AuditProviderFactory: initializing..");
		
		if(mInitDone) {
			LOG.warn("AuditProviderFactory.init(): already initialized!", new Exception());

			return;
		}
		mInitDone = true;
		
		MiscUtil.setApplicationType(appType);

		boolean isEnabled             = BaseAuditProvider.getBooleanProperty(props, AUDIT_IS_ENABLED_PROP, false);
		boolean isAuditToDbEnabled    = BaseAuditProvider.getBooleanProperty(props, AUDIT_DB_IS_ENABLED_PROP, false);
		boolean isAuditToHdfsEnabled  = BaseAuditProvider.getBooleanProperty(props, AUDIT_HDFS_IS_ENABLED_PROP, false);
		boolean isAuditToLog4jEnabled = BaseAuditProvider.getBooleanProperty(props, AUDIT_LOG4J_IS_ENABLED_PROP, false);

		if(!isEnabled || !(isAuditToDbEnabled || isAuditToHdfsEnabled || isAuditToLog4jEnabled)) {
			LOG.info("AuditProviderFactory: Audit not enabled..");

			mProvider = getDefaultProvider();

			return;
		}

		List<AuditProvider> providers = new ArrayList<AuditProvider>();

		if(isAuditToDbEnabled) {
			DbAuditProvider dbProvider = new DbAuditProvider();

			boolean isAuditToDbAsync = BaseAuditProvider.getBooleanProperty(props, DbAuditProvider.AUDIT_DB_IS_ASYNC_PROP, false);

			if(isAuditToDbAsync) {
				int maxQueueSize     = BaseAuditProvider.getIntProperty(props, DbAuditProvider.AUDIT_DB_MAX_QUEUE_SIZE_PROP, AUDIT_ASYNC_MAX_QUEUE_SIZE_DEFAULT);
				int maxFlushInterval = BaseAuditProvider.getIntProperty(props, DbAuditProvider.AUDIT_DB_MAX_FLUSH_INTERVAL_PROP, AUDIT_ASYNC_MAX_FLUSH_INTERVAL_DEFAULT);

				AsyncAuditProvider asyncProvider = new AsyncAuditProvider("DbAuditProvider", maxQueueSize, maxFlushInterval, dbProvider);

				providers.add(asyncProvider);
			} else {
				providers.add(dbProvider);
			}
		}

		if(isAuditToHdfsEnabled) {
			HdfsAuditProvider hdfsProvider = new HdfsAuditProvider();

			boolean isAuditToHdfsAsync = BaseAuditProvider.getBooleanProperty(props, HdfsAuditProvider.AUDIT_HDFS_IS_ASYNC_PROP, false);

			if(isAuditToHdfsAsync) {
				int maxQueueSize     = BaseAuditProvider.getIntProperty(props, HdfsAuditProvider.AUDIT_HDFS_MAX_QUEUE_SIZE_PROP, AUDIT_ASYNC_MAX_QUEUE_SIZE_DEFAULT);
				int maxFlushInterval = BaseAuditProvider.getIntProperty(props, HdfsAuditProvider.AUDIT_HDFS_MAX_FLUSH_INTERVAL_PROP, AUDIT_ASYNC_MAX_FLUSH_INTERVAL_DEFAULT);

				AsyncAuditProvider asyncProvider = new AsyncAuditProvider("HdfsAuditProvider", maxQueueSize, maxFlushInterval, hdfsProvider);

				providers.add(asyncProvider);
			} else {
				providers.add(hdfsProvider);
			}
		}

		if(isAuditToLog4jEnabled) {
			Log4jAuditProvider log4jProvider = new Log4jAuditProvider();

			boolean isAuditToLog4jAsync = BaseAuditProvider.getBooleanProperty(props, Log4jAuditProvider.AUDIT_LOG4J_IS_ASYNC_PROP, false);

			if(isAuditToLog4jAsync) {
				int maxQueueSize     = BaseAuditProvider.getIntProperty(props, Log4jAuditProvider.AUDIT_LOG4J_MAX_QUEUE_SIZE_PROP, AUDIT_ASYNC_MAX_QUEUE_SIZE_DEFAULT);
				int maxFlushInterval = BaseAuditProvider.getIntProperty(props, Log4jAuditProvider.AUDIT_LOG4J_MAX_FLUSH_INTERVAL_PROP, AUDIT_ASYNC_MAX_FLUSH_INTERVAL_DEFAULT);

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
		
		mProvider.init(props);
		mProvider.start();

		JVMShutdownHook jvmShutdownHook = new JVMShutdownHook(mProvider);

	    Runtime.getRuntime().addShutdownHook(jvmShutdownHook);
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
}
