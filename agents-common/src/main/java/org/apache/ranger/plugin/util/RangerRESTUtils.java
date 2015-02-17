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


import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;

/**
 * Since this class does not retain any state.  It isn't a singleton for testability. 
 *
 */
public class RangerRESTUtils {

	private static final Log LOG = LogFactory.getLog(RangerRESTUtils.class);
	static final String REST_URL_POLICY_GET_FOR_SERVICE_IF_UPDATED = "/service/plugins/policies/download/";
	static final String REST_URL_SERVICE_GRANT_ACCESS              = "/service/plugins/services/grant/";
	static final String REST_URL_SERVICE_REVOKE_ACCESS             = "/service/plugins/services/revoke/";
	
	public String getPolicyRestUrl(String propertyPrefix) {
		String url = RangerConfiguration.getInstance().get(propertyPrefix + ".policy.rest.url");
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerRESTUtils.getPolicyRestUrl(" + url + ")");
		}

		return url;
	}
	
	public String getSsslConfigFileName(String propertyPrefix) {
		String sslConfigFileName = RangerConfiguration.getInstance().get(propertyPrefix + ".policy.rest.ssl.config.file");

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerRESTUtils.getSsslConfigFileName(" + sslConfigFileName + ")");
		}

		return sslConfigFileName;
	}
	
	public String getUrlForPolicyUpdate(String baseUrl, String serviceName, long lastKnownVersion) {
		String url = baseUrl + REST_URL_POLICY_GET_FOR_SERVICE_IF_UPDATED + serviceName + "/" + lastKnownVersion;
		
		return url;
	}

	public boolean isSsl(String _baseUrl) {
		return StringUtils.isEmpty(_baseUrl) ? false : _baseUrl.toLowerCase().startsWith("https");
	}

	public String getUrlForGrantAccess(String baseUrl, String serviceName) {
		String url = baseUrl + REST_URL_SERVICE_GRANT_ACCESS + serviceName;
		
		return url;
	}

	public String getUrlForRevokeAccess(String baseUrl, String serviceName) {
		String url = baseUrl + REST_URL_SERVICE_REVOKE_ACCESS + serviceName;
		
		return url;
	}
}
