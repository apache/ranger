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


import java.net.InetAddress;
import java.net.UnknownHostException;

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

	public static final String REST_URL_POLICY_GET_FOR_SERVICE_IF_UPDATED = "/service/plugins/policies/download/";
	public static final String REST_URL_SERVICE_GRANT_ACCESS              = "/service/plugins/services/grant/";
	public static final String REST_URL_SERVICE_REVOKE_ACCESS             = "/service/plugins/services/revoke/";
	
	public static final String REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED = "/service/plugins/secure/policies/download/";
	public static final String REST_URL_SECURE_SERVICE_GRANT_ACCESS              = "/service/plugins/secure/services/grant/";
	public static final String REST_URL_SECURE_SERVICE_REVOKE_ACCESS             = "/service/plugins/secure/services/revoke/";

	public static final String REST_URL_GET_SERVICE_TAGS_IF_UPDATED = "/service/tags/download/";
	public static final String REST_URL_GET_SECURE_SERVICE_TAGS_IF_UPDATED = "/service/tags/secure/download/";
	public static final String SERVICE_NAME_PARAM = "serviceName";
	public static final String LAST_KNOWN_TAG_VERSION_PARAM = "lastKnownVersion";
	public static final String PATTERN_PARAM = "pattern";

	public static final String REST_URL_LOOKUP_TAG_NAMES = "/service/tags/lookup";

	public static final String REST_EXPECTED_MIME_TYPE = "application/json";
	public static final String REST_MIME_TYPE_JSON     = "application/json";

	public static final String REST_PARAM_LAST_KNOWN_POLICY_VERSION = "lastKnownVersion";
	public static final String REST_PARAM_LAST_ACTIVATION_TIME = "lastActivationTime";
	public static final String REST_PARAM_PLUGIN_ID                 = "pluginId";

	private static final int MAX_PLUGIN_ID_LEN = 255;
	
	public static final String REST_PARAM_CLUSTER_NAME   = "clusterName";

	public static final String REST_PARAM_ZONE_NAME		 = "zoneName";

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
	
	public String getUrlForPolicyUpdate(String baseUrl, String serviceName) {
		String url = baseUrl + REST_URL_POLICY_GET_FOR_SERVICE_IF_UPDATED + serviceName;
		
		return url;
	}

	public String getSecureUrlForPolicyUpdate(String baseUrl, String serviceName) {
		String url = baseUrl + REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED + serviceName;
		return url;
	}

	public String getUrlForTagUpdate(String baseUrl, String serviceName) {
		String url = baseUrl + REST_URL_GET_SERVICE_TAGS_IF_UPDATED + serviceName;

		return url;
	}

	public String getSecureUrlForTagUpdate(String baseUrl, String serviceName) {
		String url = baseUrl + REST_URL_GET_SECURE_SERVICE_TAGS_IF_UPDATED + serviceName;
		return url;
	}

	public boolean isSsl(String _baseUrl) {
		return !StringUtils.isEmpty(_baseUrl) && _baseUrl.toLowerCase().startsWith("https");
	}

	public String getUrlForGrantAccess(String baseUrl, String serviceName) {
		String url = baseUrl + REST_URL_SERVICE_GRANT_ACCESS + serviceName;
		
		return url;
	}

	public String getUrlForRevokeAccess(String baseUrl, String serviceName) {
		String url = baseUrl + REST_URL_SERVICE_REVOKE_ACCESS + serviceName;
		
		return url;
	}

    public String getPluginId(String serviceName, String appId) {
        String hostName = null;

        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOG.error("ERROR: Unable to find hostname for the agent ", e);
            hostName = "unknownHost";
        }

        String ret  = hostName + "-" + serviceName;

        if(! StringUtils.isEmpty(appId)) {
        	ret = appId + "@" + ret;
        }

        if (ret.length() > MAX_PLUGIN_ID_LEN ) {
        	ret = ret.substring(0,MAX_PLUGIN_ID_LEN);
        }

        return ret ;
    }

    public String getHostnameFromPluginId(String pluginId, String serviceName) {
    	String ret = "";

    	if (StringUtils.isNotBlank(pluginId)) {
			int lastIndex;
			String[] parts = pluginId.split("@");
			int index = parts.length > 1 ? 1 : 0;
			if (StringUtils.isNotBlank(serviceName)) {
				lastIndex = StringUtils.lastIndexOf(parts[index], serviceName);
				if (lastIndex > 1) {
					ret = parts[index].substring(0, lastIndex-1);
				}
			} else {
				lastIndex = StringUtils.lastIndexOf(parts[index], "-");
				if (lastIndex > 0) {
					ret = parts[index].substring(0, lastIndex);
				}
			}
		}

		return ret;
	}
	public String getAppIdFromPluginId(String pluginId) {
		String ret = "**Unknown**";

		if (StringUtils.isNotBlank(pluginId)) {
			String[] parts = pluginId.split("@");
			ret = parts[0];
		}

		return ret;
	}
}
