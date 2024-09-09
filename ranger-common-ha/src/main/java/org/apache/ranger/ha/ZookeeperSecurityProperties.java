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

import org.apache.hadoop.thirdparty.com.google.common.base.Charsets;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.zookeeper.data.ACL;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.AuthInfo;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Id;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperSecurityProperties {

	private static final Logger LOG = LoggerFactory.getLogger(ServiceState.class);

	public static ACL parseAcl(String aclString, ACL defaultAcl) {
		if (StringUtils.isEmpty(aclString)) {
			return defaultAcl;
		}
		return parseAcl(aclString);
	}

	/**
	 * Get an {@link ACL} by parsing input string.
	 *
	 * @param aclString A string of the form scheme:id
	 * @return {@link ACL} with the perms set to {@link ZooDefs.Perms#ALL} and
	 *         scheme and id taken from configuration values.
	 */
	public static ACL parseAcl(String aclString) {
		LOG.info(" inside parseAcl ");
		String[] aclComponents = getComponents(aclString, "acl", "scheme:id");
		return new ACL(ZooDefs.Perms.ALL, new Id(aclComponents[0], aclComponents[1]));
	}

	private static String[] getComponents(String securityString, String variableName, String formatExample) {
		Preconditions.checkArgument(!StringUtils.isEmpty(securityString),
				String.format("%s cannot be null or empty. " + "Needs to be of form %s", variableName, formatExample));
		String[] aclComponents = securityString.split(":", 2);
		if (aclComponents.length != 2) {
			throw new IllegalArgumentException(
					String.format("Invalid %s string. " + "Needs to be of form %s", variableName, formatExample));
		}
		return aclComponents;
	}

	/**
	 * Get an {@link AuthInfo} by parsing input string.
	 *
	 * @param authString A string of the form scheme:authString
	 * @return {@link AuthInfo} with the scheme and auth taken from configuration
	 *         values.
	 */
	public static AuthInfo parseAuth(String authString) {
		LOG.info(" inside parseAuth ");
		String[] authComponents = getComponents(authString, "authString", "scheme:authString");
		return new AuthInfo(authComponents[0], authComponents[1].getBytes(Charsets.UTF_8));
	}

}
