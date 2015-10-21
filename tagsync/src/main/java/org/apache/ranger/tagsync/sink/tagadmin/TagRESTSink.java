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

package org.apache.ranger.tagsync.sink.tagadmin;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.admin.client.datatype.RESTResponse;
import org.apache.ranger.tagsync.model.TagSink;
import org.apache.ranger.plugin.util.RangerRESTClient;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServiceTags;
import org.apache.ranger.tagsync.process.TagSyncConfig;

import java.util.Map;
import java.util.Properties;

public class TagRESTSink implements TagSink {
	private static final Log LOG = LogFactory.getLog(TagRESTSink.class);

	private static final String REST_PREFIX = "/service";
	private static final String MODULE_PREFIX = "/tags";

	private static final String REST_MIME_TYPE_JSON = "application/json" ;

	private static final String REST_URL_IMPORT_SERVICETAGS_RESOURCE = REST_PREFIX + MODULE_PREFIX + "/importservicetags/";

	private RangerRESTClient tagRESTClient = null;

	@Override
	public boolean initialize(Properties properties) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> TagRESTSink.initialize()");
		}

		boolean ret = true;

		String restUrl       = TagSyncConfig.getTagAdminRESTUrl(properties);
		String sslConfigFile = TagSyncConfig.getTagAdminRESTSslConfigFile(properties);
		String userName = TagSyncConfig.getTagAdminUserName(properties);
		String password = TagSyncConfig.getTagAdminPassword(properties);

		if (LOG.isDebugEnabled()) {
			LOG.debug("restUrl=" + restUrl);
			LOG.debug("sslConfigFile=" + sslConfigFile);
			LOG.debug("userName=" + userName);
		}

		if (StringUtils.isBlank(restUrl)) {
			ret = false;
			LOG.error("No value specified for property 'ranger.tagsync.tagadmin.rest.url'!");
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("ranger.tagsync.tagadmin.rest.url:" + restUrl);
			}
		}

		if (ret) {
			tagRESTClient = new RangerRESTClient(restUrl, sslConfigFile);
			tagRESTClient.setBasicAuthInfo(userName, password);

			ret = testConnection();
		}

		if (!ret) {
			LOG.error("Cannot connect to Tag Admin. Please recheck configuration properties and/or check if Tag Admin is running and responsive");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== TagRESTSink.initialize(), result=" + ret);
		}

		return ret;
	}

	public boolean testConnection() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagRESTSink.testConnection()");
		}

		boolean ret = true;

		try {
			// build a dummy serviceTags structure and upload it to test connectivity
			ServiceTags serviceTags = new ServiceTags();
			serviceTags.setOp(ServiceTags.OP_ADD_OR_UPDATE);
			uploadServiceTags(serviceTags);
		} catch (Exception exception) {
			LOG.error("test-upload of serviceTags failed.", exception);
			ret = false;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagRESTSink.testConnection(), result=" + ret);
		}
		return ret;
	}

	@Override
	public void uploadServiceTags(ServiceTags serviceTags) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> uploadServiceTags()");
		}
		WebResource webResource = createWebResource(REST_URL_IMPORT_SERVICETAGS_RESOURCE);

		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).type(REST_MIME_TYPE_JSON).put(ClientResponse.class, tagRESTClient.toJson(serviceTags));

		if(response == null || response.getStatus() != 204) {
			LOG.error("RangerAdmin REST call returned with response={" + response + "}");

			RESTResponse resp = RESTResponse.fromClientResponse(response);

			LOG.error("Upload of service-tags failed with message " + resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== uploadServiceTags()");
		}
	}

	private WebResource createWebResource(String url) {
		return createWebResource(url, null);
	}

	private WebResource createWebResource(String url, SearchFilter filter) {
		WebResource ret = tagRESTClient.getResource(url);

		if(filter != null && !MapUtils.isEmpty(filter.getParams())) {
			for(Map.Entry<String, String> e : filter.getParams().entrySet()) {
				String name  = e.getKey();
				String value = e.getValue();

				ret.queryParam(name, value);
			}
		}

		return ret;
	}
}
