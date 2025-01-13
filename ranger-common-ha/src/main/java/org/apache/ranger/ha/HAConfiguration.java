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

package org.apache.ranger.ha;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HAConfiguration {

	private static final Logger LOG = LoggerFactory.getLogger(HAConfiguration.class);

	private HAConfiguration() {
	}

	public static final String RANGER_SERVICE_ACTIVE_SERVER_INFO = "/active_server_info";
	public static final  String RANGER_SERVICE_NAME 				= "ranger.service.name";
	public static final  String RANGER_SERVER_ZK_ROOT_DEFAULT 		= "/apache" + RANGER_SERVICE_NAME + "_zkroot";
	private static final String RANGER_SERVER_HA_PREFIX 			= ".server.ha.";
	private static final String ZOOKEEPER_PREFIX 					= "zookeeper.";
	public static final  String RANGER_SERVER_HA_ADDRESS_PREFIX 	= RANGER_SERVER_HA_PREFIX + "address.";
	private static final String RANGER_SERVER_HA_ZK_ROOT_KEY 		= RANGER_SERVER_HA_PREFIX + ZOOKEEPER_PREFIX + "zkroot";
	private static final String RANGER_SERVER_HA_ENABLED_KEY 		= RANGER_SERVER_HA_PREFIX + "enabled";
	public static final  String RANGER_SERVER_HA_IDS 				= RANGER_SERVER_HA_PREFIX + "ids";
	private static final String HA_ZOOKEEPER_CONNECT 				= RANGER_SERVER_HA_PREFIX + ZOOKEEPER_PREFIX + "connect";
	private static final int DEFAULT_ZOOKEEPER_CONNECT_SLEEPTIME_MILLIS = 1000;
	private static final String HA_ZOOKEEPER_RETRY_SLEEPTIME_MILLIS = RANGER_SERVER_HA_PREFIX + ZOOKEEPER_PREFIX + "retry.sleeptime.ms";
	private static final String HA_ZOOKEEPER_NUM_RETRIES 			= RANGER_SERVER_HA_PREFIX + ZOOKEEPER_PREFIX + "num.retries";
	private static final int DEFAULT_ZOOKEEPER_CONNECT_NUM_RETRIES 	= 3;
	private static final String HA_ZOOKEEPER_SESSION_TIMEOUT_MS 	= RANGER_SERVER_HA_PREFIX + ZOOKEEPER_PREFIX + "session.timeout.ms";
	private static final int DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_MILLIS = 20000;
	private static final String HA_ZOOKEEPER_ACL 					= RANGER_SERVER_HA_PREFIX + ZOOKEEPER_PREFIX + "acl";
	private static final String HA_ZOOKEEPER_AUTH 					= RANGER_SERVER_HA_PREFIX + ZOOKEEPER_PREFIX + "auth";
	public static final String RANGER_HA_SERVICE_HTTPS_PORT 		= ".service.https.port";
	public static final String RANGER_HA_SERVICE_HTTP_PORT 			= ".service.http.port";
	public static final String RANGER_SERVICE_SSL_ENABLED 			= RANGER_SERVER_HA_PREFIX + "ssl.enabled";

	/**
	 * Return whether HA is enabled or not.
	 *
	 * @param configuration underlying configuration instance
	 * @return true if more than 1 ids, false otherwise
	 */
	public static boolean isHAEnabled(Configuration configuration) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("HAConfiguration.isHAEnabled() RANGER_SERVER_HA_ENABLED_KEY :  "
					+ getStringConfig(configuration, HAConfiguration.RANGER_SERVER_HA_ENABLED_KEY, null));
		}
		final boolean ret;

		if (getStringConfig(configuration, HAConfiguration.RANGER_SERVER_HA_ENABLED_KEY, null) != null) {
			ret = getBooleanConfig(configuration, RANGER_SERVER_HA_ENABLED_KEY, false);
		} else {
			String[] ids = getStringsConfig(configuration, HAConfiguration.RANGER_SERVER_HA_IDS, null);

			ret = ids != null && ids.length > 1;
		}

		LOG.info("<== HAConfiguration.isHAEnabled() ret :" + ret);

		return ret;
	}

	/**
	 * Get the web server address that a server instance with the passed ID is bound
	 * to.
	 *
	 *
	 * @param configuration underlying configuration
	 * @param serverId      serverId whose host:port property is picked to build the
	 *                      web server address.
	 * @return address string
	 */
	public static String getBoundAddressForId(Configuration configuration, String serverId) {

		String hostPort = getStringConfig(configuration, RANGER_SERVER_HA_ADDRESS_PREFIX + serverId, null);
		boolean isSecure = getBooleanConfig(configuration, RANGER_SERVICE_SSL_ENABLED, false);
		String protocol = (isSecure) ? "https://" : "http://";

		return protocol + hostPort;
	}

	public static List<String> getServerInstances(Configuration configuration) {
		String[] serverIds = getStringsConfig(configuration, RANGER_SERVER_HA_IDS, null);
		List<String> serverInstances = new ArrayList<>(serverIds.length);

		for (String serverId : serverIds) {
			serverInstances.add(getBoundAddressForId(configuration, serverId));
		}

		return serverInstances;
	}

	/**
	 * A collection of Zookeeper specific configuration that is used by High
	 * Availability code.
	 */
	public static class ZookeeperProperties {
		private String connectString;
		private String zkRoot;
		private int retriesSleepTimeMillis;
		private int numRetries;
		private int sessionTimeout;
		private String acl;
		private String auth;

		public ZookeeperProperties(String connectString, String zkRoot, int retriesSleepTimeMillis, int numRetries,
				int sessionTimeout, String acl, String auth) {
			this.connectString = connectString;
			this.zkRoot = zkRoot;
			this.retriesSleepTimeMillis = retriesSleepTimeMillis;
			this.numRetries = numRetries;
			this.sessionTimeout = sessionTimeout;
			this.acl = acl;
			this.auth = auth;
		}

		public String getConnectString() {
			return connectString;
		}

		public int getRetriesSleepTimeMillis() {
			return retriesSleepTimeMillis;
		}

		public int getNumRetries() {
			return numRetries;
		}

		public int getSessionTimeout() {
			return sessionTimeout;
		}

		public String getAcl() {
			return acl;
		}

		public String getAuth() {
			return auth;
		}

		public String getZkRoot() {
			return zkRoot;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;

			ZookeeperProperties that = (ZookeeperProperties) o;

			return retriesSleepTimeMillis == that.retriesSleepTimeMillis && numRetries == that.numRetries
					&& sessionTimeout == that.sessionTimeout && Objects.equals(connectString, that.connectString)
					&& Objects.equals(zkRoot, that.zkRoot) && Objects.equals(acl, that.acl)
					&& Objects.equals(auth, that.auth);
		}

		@Override
		public int hashCode() {
			return Objects.hash(connectString, zkRoot, retriesSleepTimeMillis, numRetries, sessionTimeout, acl, auth);
		}

		public boolean hasAcl() {
			return StringUtils.isNotBlank(getAcl());
		}

		public boolean hasAuth() {
			return StringUtils.isNotBlank(getAuth());
		}
	}

	public static ZookeeperProperties getZookeeperProperties(Configuration configuration) {
		String[] zkServers = null;

		if (getStringConfig(configuration, HA_ZOOKEEPER_CONNECT, null) != null) {
			zkServers = getStringsConfig(configuration, HA_ZOOKEEPER_CONNECT, null);
		}

		String zkRoot 				= getStringConfig(configuration, RANGER_SERVER_HA_ZK_ROOT_KEY, RANGER_SERVER_ZK_ROOT_DEFAULT);
		int retriesSleepTimeMillis 	= getIntConfig(configuration, HA_ZOOKEEPER_RETRY_SLEEPTIME_MILLIS, DEFAULT_ZOOKEEPER_CONNECT_SLEEPTIME_MILLIS);
		int numRetries 				= getIntConfig(configuration, HA_ZOOKEEPER_NUM_RETRIES, DEFAULT_ZOOKEEPER_CONNECT_NUM_RETRIES);
		int sessionTimeout 			= getIntConfig(configuration, HA_ZOOKEEPER_SESSION_TIMEOUT_MS, DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_MILLIS);
		String acl 					= getStringConfig(configuration, HA_ZOOKEEPER_ACL, null);
		String auth 				= getStringConfig(configuration, HA_ZOOKEEPER_AUTH, null);

		LOG.info(" ==> HAConfiguration.ZookeeperProperties zkServers: " + Arrays.toString(zkServers) + " zkRoot:{" + zkRoot
				+ "} retriesSleepTimeMillis:{" + retriesSleepTimeMillis + "} numRetries:{" + numRetries
				+ "} sessionTimeout:{" + sessionTimeout + "} acl:{" + acl + "} auth:{" + auth+"}");

		return new ZookeeperProperties(StringUtils.join(zkServers, ','), zkRoot, retriesSleepTimeMillis, numRetries,
				sessionTimeout, acl, auth);
	}

	public static String getPrefix(Configuration configuration) {

		String serverPrefix = null;
		serverPrefix = configuration.get(RANGER_SERVICE_NAME, "ranger.service.name");
		return serverPrefix;
	}

	public static String getStringConfig(Configuration configuration, String confStr, String confDefaultValue) {
		String value = null;
		String key = getPrefix(configuration) + confStr;
		value = configuration.get(key, confDefaultValue);
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== HAConfiguration.getConfigString() key :{" + key + "} value :{" + value + "} confDefaultValue :{"
					+ confDefaultValue+"}");
		}
		return value;
	}

	public static int getIntConfig(Configuration configuration, String confStr, int confDefaultValue) {
		int value;
		String key = getPrefix(configuration) + confStr;
		value = configuration.getInt(key, confDefaultValue);
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== HAConfiguration.getConfigString() key :{" + key + "} value :{" + value + "} confDefaultValue :{"
					+ confDefaultValue+"}");
		}
		return value;
	}

	public static String[] getStringsConfig(Configuration configuration, String confStr, String confDefaultValue) {
		String[] value = null;
		String key = getPrefix(configuration) + confStr;
		value = configuration.getStrings(key, confDefaultValue);
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== HAConfiguration getConfigString() key :{" + key + "} value :{" + value + "} confDefaultValue :{"
					+ confDefaultValue+"}");
		}
		return value;
	}

	public static boolean getBooleanConfig(Configuration configuration, String confStr, boolean confDefaultValue) {
		boolean value;
		String key = getPrefix(configuration) + confStr;
		value = configuration.getBoolean(key, confDefaultValue);
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== HAConfiguration getConfigString() key :{" + key + "} value :{" + value + "} confDefaultValue :{"
					+ confDefaultValue+"}");
		}
		return value;
	}
}
