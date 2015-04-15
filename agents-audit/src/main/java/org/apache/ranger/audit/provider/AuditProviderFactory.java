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
import org.apache.ranger.audit.provider.kafka.KafkaAuditProvider;
import org.apache.ranger.audit.provider.solr.SolrAuditProvider;

/*
 * TODO:
 * 1) Flag to enable/disable audit logging
 * 2) Failed path to be recorded
 * 3) Repo name, repo type from configuration
 */

public class AuditProviderFactory {
	private static final Log LOG = LogFactory
			.getLog(AuditProviderFactory.class);

	public static final String AUDIT_IS_ENABLED_PROP = "xasecure.audit.is.enabled";
	public static final String AUDIT_DB_IS_ENABLED_PROP = "xasecure.audit.db.is.enabled";
	public static final String AUDIT_HDFS_IS_ENABLED_PROP = "xasecure.audit.hdfs.is.enabled";
	public static final String AUDIT_LOG4J_IS_ENABLED_PROP = "xasecure.audit.log4j.is.enabled";
	public static final String AUDIT_KAFKA_IS_ENABLED_PROP = "xasecure.audit.kafka.is.enabled";
	public static final String AUDIT_SOLR_IS_ENABLED_PROP = "xasecure.audit.solr.is.enabled";

	public static final String AUDIT_DEST_BASE = "xasecure.audit.destination";

	public static final int AUDIT_ASYNC_MAX_QUEUE_SIZE_DEFAULT = 10 * 1024;
	public static final int AUDIT_ASYNC_MAX_FLUSH_INTERVAL_DEFAULT = 5 * 1000;

	private static AuditProviderFactory sFactory;

	private AuditProvider mProvider = null;
	private boolean mInitDone = false;

	private AuditProviderFactory() {
		LOG.info("AuditProviderFactory: creating..");

		mProvider = getDefaultProvider();
	}

	public static AuditProviderFactory getInstance() {
		if (sFactory == null) {
			synchronized (AuditProviderFactory.class) {
				if (sFactory == null) {
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

		if (mInitDone) {
			LOG.warn("AuditProviderFactory.init(): already initialized!",
					new Exception());

			return;
		}
		mInitDone = true;

		MiscUtil.setApplicationType(appType);

		boolean isEnabled = MiscUtil.getBooleanProperty(props,
				AUDIT_IS_ENABLED_PROP, false);
		boolean isAuditToDbEnabled = MiscUtil.getBooleanProperty(props,
				AUDIT_DB_IS_ENABLED_PROP, false);
		boolean isAuditToHdfsEnabled = MiscUtil.getBooleanProperty(props,
				AUDIT_HDFS_IS_ENABLED_PROP, false);
		boolean isAuditToLog4jEnabled = MiscUtil.getBooleanProperty(props,
				AUDIT_LOG4J_IS_ENABLED_PROP, false);
		boolean isAuditToKafkaEnabled = MiscUtil.getBooleanProperty(props,
				AUDIT_KAFKA_IS_ENABLED_PROP, false);
		boolean isAuditToSolrEnabled = MiscUtil.getBooleanProperty(props,
				AUDIT_SOLR_IS_ENABLED_PROP, false);

		List<AuditProvider> providers = new ArrayList<AuditProvider>();

		// TODO: Delete me
		for (Object propNameObj : props.keySet()) {
			LOG.info("DELETE ME: " + propNameObj.toString() + "="
					+ props.getProperty(propNameObj.toString()));
		}

		// Process new audit configurations
		List<String> destNameList = new ArrayList<String>();

		for (Object propNameObj : props.keySet()) {
			String propName = propNameObj.toString();
			if (propName.length() <= AUDIT_DEST_BASE.length() + 1) {
				continue;
			}
			String destName = propName.substring(AUDIT_DEST_BASE.length() + 1);
			List<String> splits = MiscUtil.toArray(destName, ".");
			if (splits.size() > 1) {
				continue;
			}
			String value = props.getProperty(propName);
			if (value.equalsIgnoreCase("enable")
					|| value.equalsIgnoreCase("true")) {
				destNameList.add(destName);
				LOG.info("Audit destination " + propName + " is set to "
						+ value);
			}
		}

		for (String destName : destNameList) {
			String destPropPrefix = AUDIT_DEST_BASE + "." + destName;
			AuditProvider destProvider = getProviderFromConfig(props,
					destPropPrefix, destName);

			if (destProvider != null) {
				destProvider.init(props, destPropPrefix);

				String queueName = MiscUtil.getStringProperty(props,
						destPropPrefix + "." + BaseAuditProvider.PROP_QUEUE);
				if( queueName == null || queueName.isEmpty()) {
					queueName = "batch";
				}
				if (queueName != null && !queueName.isEmpty()
						&& !queueName.equalsIgnoreCase("none")) {
					String queuePropPrefix = destPropPrefix + "." + queueName;
					AuditProvider queueProvider = getProviderFromConfig(props,
							queuePropPrefix, queueName);
					if (queueProvider != null) {
						if (queueProvider instanceof BaseAuditProvider) {
							BaseAuditProvider qProvider = (BaseAuditProvider) queueProvider;
							qProvider.setConsumer(destProvider);
							qProvider.init(props, queuePropPrefix);
							providers.add(queueProvider);
						} else {
							LOG.fatal("Provider queue doesn't extend BaseAuditProvider destination "
									+ destName
									+ " can't be created. queueName="
									+ queueName);
						}
					} else {
						LOG.fatal("Queue provider for destination " + destName
								+ " can't be created. queueName=" + queueName);
					}
				} else {
					LOG.info("Audit destination " + destProvider.getName()
							+ " added to provider list");
					providers.add(destProvider);
				}
			}
		}
		if (providers.size() > 0) {
			LOG.info("Using v2 audit configuration");
			AuditAsyncQueue asyncQueue = new AuditAsyncQueue();
			String propPrefix = BaseAuditProvider.PROP_DEFAULT_PREFIX + "." + "async";
			asyncQueue.init(props, propPrefix);

			if (providers.size() == 1) {
				asyncQueue.setConsumer(providers.get(0));
			} else {
				MultiDestAuditProvider multiDestProvider = new MultiDestAuditProvider();
				multiDestProvider.init(props);
				multiDestProvider.addAuditProviders(providers);
				asyncQueue.setConsumer(multiDestProvider);
			}

			mProvider = asyncQueue;
			mProvider.start();
		} else {
			LOG.info("No v2 audit configuration found. Trying v1 audit configurations");
			if (!isEnabled
					|| !(isAuditToDbEnabled || isAuditToHdfsEnabled
							|| isAuditToKafkaEnabled || isAuditToLog4jEnabled
							|| isAuditToSolrEnabled || providers.size() == 0)) {
				LOG.info("AuditProviderFactory: Audit not enabled..");

				mProvider = getDefaultProvider();

				return;
			}

			if (isAuditToDbEnabled) {
				LOG.info("DbAuditProvider is enabled");
				DbAuditProvider dbProvider = new DbAuditProvider();

				boolean isAuditToDbAsync = MiscUtil.getBooleanProperty(props,
						DbAuditProvider.AUDIT_DB_IS_ASYNC_PROP, false);

				if (isAuditToDbAsync) {
					int maxQueueSize = MiscUtil.getIntProperty(props,
							DbAuditProvider.AUDIT_DB_MAX_QUEUE_SIZE_PROP,
							AUDIT_ASYNC_MAX_QUEUE_SIZE_DEFAULT);
					int maxFlushInterval = MiscUtil.getIntProperty(props,
							DbAuditProvider.AUDIT_DB_MAX_FLUSH_INTERVAL_PROP,
							AUDIT_ASYNC_MAX_FLUSH_INTERVAL_DEFAULT);

					AsyncAuditProvider asyncProvider = new AsyncAuditProvider(
							"DbAuditProvider", maxQueueSize, maxFlushInterval,
							dbProvider);

					providers.add(asyncProvider);
				} else {
					providers.add(dbProvider);
				}
			}

			if (isAuditToHdfsEnabled) {
				LOG.info("HdfsAuditProvider is enabled");

				HdfsAuditProvider hdfsProvider = new HdfsAuditProvider();

				boolean isAuditToHdfsAsync = MiscUtil.getBooleanProperty(props,
						HdfsAuditProvider.AUDIT_HDFS_IS_ASYNC_PROP, false);

				if (isAuditToHdfsAsync) {
					int maxQueueSize = MiscUtil.getIntProperty(props,
							HdfsAuditProvider.AUDIT_HDFS_MAX_QUEUE_SIZE_PROP,
							AUDIT_ASYNC_MAX_QUEUE_SIZE_DEFAULT);
					int maxFlushInterval = MiscUtil
							.getIntProperty(
									props,
									HdfsAuditProvider.AUDIT_HDFS_MAX_FLUSH_INTERVAL_PROP,
									AUDIT_ASYNC_MAX_FLUSH_INTERVAL_DEFAULT);

					AsyncAuditProvider asyncProvider = new AsyncAuditProvider(
							"HdfsAuditProvider", maxQueueSize,
							maxFlushInterval, hdfsProvider);

					providers.add(asyncProvider);
				} else {
					providers.add(hdfsProvider);
				}
			}

			if (isAuditToKafkaEnabled) {
				LOG.info("KafkaAuditProvider is enabled");
				KafkaAuditProvider kafkaProvider = new KafkaAuditProvider();
				kafkaProvider.init(props);

				if (kafkaProvider.isAsync()) {
					AsyncAuditProvider asyncProvider = new AsyncAuditProvider(
							"MyKafkaAuditProvider",
							kafkaProvider.getMaxQueueSize(),
							kafkaProvider.getMaxBatchInterval(), kafkaProvider);
					providers.add(asyncProvider);
				} else {
					providers.add(kafkaProvider);
				}
			}

			if (isAuditToSolrEnabled) {
				LOG.info("SolrAuditProvider is enabled");
				SolrAuditProvider solrProvider = new SolrAuditProvider();
				solrProvider.init(props);

				if (solrProvider.isAsync()) {
					AsyncAuditProvider asyncProvider = new AsyncAuditProvider(
							"MySolrAuditProvider",
							solrProvider.getMaxQueueSize(),
							solrProvider.getMaxBatchInterval(), solrProvider);
					providers.add(asyncProvider);
				} else {
					providers.add(solrProvider);
				}
			}

			if (isAuditToLog4jEnabled) {
				Log4jAuditProvider log4jProvider = new Log4jAuditProvider();

				boolean isAuditToLog4jAsync = MiscUtil.getBooleanProperty(
						props, Log4jAuditProvider.AUDIT_LOG4J_IS_ASYNC_PROP,
						false);

				if (isAuditToLog4jAsync) {
					int maxQueueSize = MiscUtil.getIntProperty(props,
							Log4jAuditProvider.AUDIT_LOG4J_MAX_QUEUE_SIZE_PROP,
							AUDIT_ASYNC_MAX_QUEUE_SIZE_DEFAULT);
					int maxFlushInterval = MiscUtil
							.getIntProperty(
									props,
									Log4jAuditProvider.AUDIT_LOG4J_MAX_FLUSH_INTERVAL_PROP,
									AUDIT_ASYNC_MAX_FLUSH_INTERVAL_DEFAULT);

					AsyncAuditProvider asyncProvider = new AsyncAuditProvider(
							"Log4jAuditProvider", maxQueueSize,
							maxFlushInterval, log4jProvider);

					providers.add(asyncProvider);
				} else {
					providers.add(log4jProvider);
				}
			}
			if (providers.size() == 0) {
				mProvider = getDefaultProvider();
			} else if (providers.size() == 1) {
				mProvider = providers.get(0);
			} else {
				MultiDestAuditProvider multiDestProvider = new MultiDestAuditProvider();

				multiDestProvider.addAuditProviders(providers);

				mProvider = multiDestProvider;
			}

			mProvider.init(props);
			mProvider.start();
		}

		JVMShutdownHook jvmShutdownHook = new JVMShutdownHook(mProvider);

		Runtime.getRuntime().addShutdownHook(jvmShutdownHook);
	}

	private AuditProvider getProviderFromConfig(Properties props,
			String propPrefix, String providerName) {
		AuditProvider provider = null;
		String className = MiscUtil.getStringProperty(props, propPrefix + "."
				+ BaseAuditProvider.PROP_CLASS_NAME);
		if (className != null && !className.isEmpty()) {
			try {
				provider = (AuditProvider) Class.forName(className)
						.newInstance();
			} catch (Exception e) {
				LOG.fatal("Can't instantiate audit class for providerName="
						+ providerName + ", className=" + className);
			}
		} else {
			if (providerName.equals("file")) {
				provider = new FileAuditDestination();
			} else if (providerName.equalsIgnoreCase("hdfs")) {
				provider = new HDFSAuditDestination();
			} else if (providerName.equals("solr")) {
				provider = new SolrAuditProvider();
			} else if (providerName.equals("kafka")) {
				provider = new KafkaAuditProvider();
			} else if (providerName.equals("db")) {
				provider = new DbAuditProvider();
			} else if (providerName.equals("log4j")) {
				provider = new Log4jAuditProvider();
			} else if (providerName.equals("batch")) {
				provider = new AuditBatchProcessor();
			} else if (providerName.equals("async")) {
				provider = new AuditAsyncQueue();
			} else {
				LOG.error("Provider name doesn't have any class associated with it. providerName="
						+ providerName);
			}
		}
		return provider;
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
