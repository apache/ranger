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

import java.net.InetSocketAddress;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangerServiceServerIdSelector {

	private static final Logger LOG = LoggerFactory.getLogger(RangerServiceServerIdSelector.class);
	/**
	 * Return the ID corresponding to this RANGER Service instance.
	 *
	 * The match is done by looking for an ID configured in
	 * {@link HAConfiguration#RANGER_SERVER_HA_IDS} key that has a
	 * host:port entry for the key
	 * {@link HAConfiguration#RANGER_SERVER_HA_ADDRESS_PREFIX}+ID
	 * where the host is a local IP address and port is set in the system property
	 * {@link HAConfiguration#RANGER_HA_SERVICE_HTTPS_PORT}.
	 *
	 * @param configuration
	 * @return
	 * @throws Exception if no ID is found that maps to a local IP Address or port
	 */
	public static String selectServerId(Configuration configuration) throws Exception {
		// ids are already trimmed by this method
		String[] ids              = HAConfiguration.getStringsConfig(configuration,HAConfiguration.RANGER_SERVER_HA_IDS, null);
		String   matchingServerId = null;
		boolean  isSecure         = HAConfiguration.getBooleanConfig(configuration, HAConfiguration.RANGER_SERVICE_SSL_ENABLED, false);

		int appPort = isSecure ? HAConfiguration.getIntConfig(configuration, HAConfiguration.RANGER_HA_SERVICE_HTTPS_PORT, -1) : HAConfiguration.getIntConfig(configuration, HAConfiguration.RANGER_HA_SERVICE_HTTP_PORT, -1);

		if (appPort < 1) {
			LOG.warn("Service HTTP/HTTPS port is not configured correctly. Please configure properties {}{} and {}{}", HAConfiguration.getPrefix(configuration), HAConfiguration.RANGER_HA_SERVICE_HTTPS_PORT, HAConfiguration.getPrefix(configuration), HAConfiguration.RANGER_HA_SERVICE_HTTP_PORT);
		}

		for (String id : ids) {
			String hostPort = HAConfiguration.getStringConfig(configuration, HAConfiguration.RANGER_SERVER_HA_ADDRESS_PREFIX + id, null);
			LOG.info("==> RangerServiceServerIdSelector.selectServerId() id["+id + "] hostPort["+hostPort+"]");
			if (!StringUtils.isEmpty(hostPort)) {
				InetSocketAddress socketAddress;
				try {
					socketAddress = NetUtils.createSocketAddr(hostPort, appPort);
					LOG.info("==> RangerServiceServerIdSelector.selectServerId() socketAddress["+socketAddress + "]");
				} catch (Exception e) {
					LOG.error("Exception while trying to get socket address for {}"+ hostPort, e);
					continue;
				}

				if (!socketAddress.isUnresolved() && NetUtils.isLocalAddress(socketAddress.getAddress())
						&& appPort == socketAddress.getPort()) {
					LOG.info("Found matched server id {} with host port: {}",id , hostPort);
					matchingServerId = id;
					break;
				}
			} else {
				LOG.info("Could not find matching address entry for id: {}", id);
			}
		}

		if (matchingServerId == null) {
			String msg = String.format("Could not find server id for this instance. Unable to find IDs matching any local host and port binding among %s", StringUtils.join(ids, ","));
			throw new Exception(msg);
		}
		return matchingServerId;
	}
}
