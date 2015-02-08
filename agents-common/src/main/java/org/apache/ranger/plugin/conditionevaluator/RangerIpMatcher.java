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


package org.apache.ranger.plugin.conditionevaluator;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;

/**
 * Credits: Large parts of this file have been lifted as is from org.apache.ranger.pdp.knox.URLBasedAuthDB.  Credits for those are due to Dilli Arumugam. 
 * @author alal
 *
 */
public class RangerIpMatcher implements RangerConditionEvaluator {

	private static final Log LOG = LogFactory.getLog(RangerIpMatcher.class);
	private List<String> _ipAddresses = new ArrayList<String>();
	private boolean _allowAny = false;

	@Override
	public void init(RangerPolicyItemCondition condition) {
		if (condition != null) {
			List<String> ipAddresses = condition.getValues();
			if (CollectionUtils.isNotEmpty(ipAddresses)) {
				_ipAddresses.addAll(ipAddresses);
				// do this once, contains on a list is O(n) operation!
				if (_ipAddresses.contains("*")) {
					_allowAny = true;
				}
			}
		}
	}

	@Override
	public boolean isMatched(String requestIp) {
		boolean ipMatched = false;
		if (LOG.isDebugEnabled()) {
			LOG.debug("Checking ipMatch for rolePermissionIpList: " + _ipAddresses +
					", requestIP: " + requestIp);
		}
		if (CollectionUtils.isEmpty(_ipAddresses)) {
			LOG.debug("RolePermission does not require IP Matching");
			ipMatched = true;
		} else if (_allowAny) {
			LOG.debug("RolePermission allows any IP: *");
			ipMatched = true;
		} else {
			for (String ip : _ipAddresses) {
				if (ipMatches(ip, requestIp)) {
					LOG.debug("RolePermission IP matches request IP");
					ipMatched = true;
					break;// break out of ipList
				}
			}
		}
		return ipMatched;
	}
	
	private boolean ipMatches(String policyIp, String requestIp) {
		if (policyIp == null) {
			return false;
		}
		policyIp = policyIp.trim();
		if (policyIp.isEmpty()) {
			return false;
		}
		boolean ipMatched = false;
		boolean wildEnd = false;
		if (policyIp.contains(".")) {
			while (policyIp.endsWith(".*")) {
				wildEnd = true;
				policyIp = policyIp.substring(0, policyIp.lastIndexOf(".*"));
			}
			if (wildEnd) {
				policyIp = policyIp + ".";
			}
		} else if (policyIp.contains(":")) {
			while (policyIp.endsWith(":*")) {
				wildEnd = true;
				policyIp = policyIp.substring(0, policyIp.lastIndexOf(":*"));
			}
			if (wildEnd) {
				policyIp = policyIp + ":";
			}
		}
		if (wildEnd && requestIp.toLowerCase().startsWith(policyIp.toLowerCase())) {
			ipMatched = true;
		} else if (policyIp.equalsIgnoreCase(requestIp)) {
			ipMatched = true;
		}
		return ipMatched;
	}


}
