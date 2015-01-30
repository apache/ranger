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

package org.apache.ranger.plugin.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;


public class SearchFilter {
	public static final String LOGIN_USER      = "loginUser";
	public static final String SERVICE_TYPE    = "serviceType";
	public static final String SERVICE_TYPE_ID = "serviceTypeId";
	public static final String SERVICE_NAME    = "serviceName";
	public static final String SERVICE_ID      = "serviceId";
	public static final String POLICY_NAME     = "policyName";
	public static final String POLICY_ID       = "policyId";
	public static final String RESOURCE_PREFIX = "resource:";
	public static final String STATUS          = "status";
	public static final String USER            = "user";
	public static final String GROUP           = "group";
	public static final String START_INDEX     = "startIndex";
	public static final String PAGE_SIZE       = "pageSize";
	public static final String SORT_BY         = "sortBy";

	private Map<String, String> params = null;

	public SearchFilter() {
		this(null);
	}

	public SearchFilter(String name, String value) {
		setParam(name, value);
	}

	public SearchFilter(Map<String, String> values) {
		setParams(values);
	}

	public Map<String, String> getParams() {
		return params;
	}

	public void setParams(Map<String, String> params) {
		this.params = params;
	}

	public String getParam(String name) {
		return params == null ? null : params.get(name);
	}

	public void setParam(String name, String value) {
		if(StringUtils.isEmpty(name) || StringUtils.isEmpty(value)) {
			return;
		}

		if(params == null) {
			params = new HashMap<String, String>();
		}

		params.put(name, value);
	}

	public Map<String, String> getParamsWithPrefix(String prefix, boolean stripPrefix) {
		Map<String, String> ret = null;

		if(prefix == null) {
			prefix = StringUtils.EMPTY;
		}

		if(params != null) {
			for(Map.Entry<String, String> e : params.entrySet()) {
				String name = e.getKey();

				if(name.startsWith(prefix)) {
					if(ret == null) {
						ret = new HashMap<String, String>();
					}

					if(stripPrefix) {
						name = name.substring(prefix.length());
					}

					ret.put(name, e.getValue());
				}
			}
		}

		return ret;
	}

	public boolean isEmpty() {
		return MapUtils.isEmpty(params);
	}
}
