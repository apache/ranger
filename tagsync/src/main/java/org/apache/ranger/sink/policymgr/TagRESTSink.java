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

package org.apache.ranger.sink.policymgr;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.admin.client.datatype.RESTResponse;
import org.apache.ranger.model.TagSink;
import org.apache.ranger.plugin.model.*;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.util.RangerRESTClient;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServiceTags;
import org.apache.ranger.process.TagSyncConfig;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by akulkarni on 9/11/15.
 */
public class TagRESTSink implements TagSink {
	private static final Log LOG = LogFactory.getLog(TagRESTSink.class);

	private static final String REST_PREFIX = "/service";
	private static final String MODULE_PREFIX = "/tags";

	private static final String REST_MIME_TYPE_JSON = "application/json" ;
	private static final String REST_URL_TAGDEFS_RESOURCE = REST_PREFIX + MODULE_PREFIX + "/tagdefs/" ;
	private static final String REST_URL_TAGDEF_RESOURCE = REST_PREFIX + MODULE_PREFIX + "/tagdef/" ;
	private static final String REST_URL_SERVICERESOURCES_RESOURCE = REST_PREFIX + MODULE_PREFIX + "resources/" ;
	private static final String REST_URL_SERVICERESOURCE_RESOURCE = REST_PREFIX + MODULE_PREFIX + "resource/" ;
	private static final String REST_URL_TAGS_RESOURCE = REST_PREFIX + MODULE_PREFIX + "/tags/" ;
	private static final String REST_URL_TAG_RESOURCE = REST_PREFIX + MODULE_PREFIX + "/tag/" ;
	private static final String REST_URL_TAGRESOURCEMAP_IDS_RESOURCE = REST_PREFIX + MODULE_PREFIX + "/tagresourcemapids/";
	private static final String REST_URL_IMPORT_SERVICETAGS_RESOURCE = REST_PREFIX + MODULE_PREFIX + "/importservicetags/";
	public static final String REST_URL_IMPORT_SERVICETAGS_PARAM = "op";


	private RangerRESTClient tagRESTClient = null;

	@Override
	public void init() {}

	@Override
	public boolean initialize(Properties properties) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> TagRESTSink.initialize()");
		}

		boolean ret = false;

		String restUrl       = TagSyncConfig.getPolicyMgrUrl(properties);
		String sslConfigFile = TagSyncConfig.getPolicyMgrSslConfigFile(properties);
		String userName = TagSyncConfig.getPolicyMgrUserName(properties);
		String password = TagSyncConfig.getPolicyMgrPassword(properties);

		if (LOG.isDebugEnabled()) {
			LOG.debug("restUrl=" + restUrl);
			LOG.debug("sslConfigFile=" + sslConfigFile);
			LOG.debug("userName=" + userName);
			LOG.debug("password=" + password);
		}
		tagRESTClient = new RangerRESTClient(restUrl, sslConfigFile);
		if (tagRESTClient != null) {
			tagRESTClient.setBasicAuthInfo(userName, password);
			ret = true;
		} else {
			LOG.error("Could not create RangerRESTClient");
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== TagRESTSink.initialize(), result=" + ret);
		}
		return ret;
	}

	@Override
	public void setServiceStore(ServiceStore svcStore) {

	}

	@Override
	public RangerTagDef createTagDef(RangerTagDef tagDef) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> createTagDef(" + tagDef + ")");
		}

		RangerTagDef ret = null;

		WebResource webResource = createWebResource(REST_URL_TAGDEFS_RESOURCE);
		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).type(REST_MIME_TYPE_JSON).post(ClientResponse.class, tagRESTClient.toJson(tagDef));

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(RangerTagDef.class);
		} else {
			LOG.error("RangerAdmin REST call returned with response={" + response +"}");
			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== createTagDef(" + tagDef + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerTagDef updateTagDef(RangerTagDef TagDef) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public void deleteTagDefByName(String name) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public void deleteTagDef(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> deleteTagDef(" + id  + ")");
		}
		WebResource webResource = createWebResource(REST_URL_TAGDEF_RESOURCE + Long.toString(id));

		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).type(REST_MIME_TYPE_JSON).delete(ClientResponse.class);

		if(response != null && response.getStatus() == 204) {
		} else {
			LOG.error("RangerAdmin REST call returned with response={" + response + "}");

			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== deleteTagDef(" + id + ")");
		}
	}

	@Override
	public RangerTagDef getTagDef(Long id) throws Exception {
		throw new Exception("Not implemented");

	}

	@Override
	public RangerTagDef getTagDefByGuid(String guid) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public RangerTagDef getTagDefByName(String name) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public List<RangerTagDef> getTagDefs(SearchFilter filter) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public PList<RangerTagDef> getPaginatedTagDefs(SearchFilter filter) throws Exception {
		throw new Exception("Not implemented");
	}


	@Override
	public RangerTag createTag(RangerTag tag) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> createTag(" + tag + ")");
		}

		RangerTag ret = null;

		WebResource webResource = createWebResource(REST_URL_TAGS_RESOURCE);
		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).type(REST_MIME_TYPE_JSON).post(ClientResponse.class, tagRESTClient.toJson(tag));

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(RangerTag.class);
		} else {
			LOG.error("RangerAdmin REST call returned with response={" + response +"}");
			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== createTag(" + tag + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerTag updateTag(RangerTag tag) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public void deleteTag(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> deleteTag(" + id  + ")");
		}
		WebResource webResource = createWebResource(REST_URL_TAG_RESOURCE + Long.toString(id));

		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).type(REST_MIME_TYPE_JSON).delete(ClientResponse.class);

		if(response != null && response.getStatus() == 204) {
		} else {
			LOG.error("RangerAdmin REST call returned with response={" + response + "}");

			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== deleteTag(" + id + ")");
		}
	}

	@Override
	public RangerTag getTag(Long id) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public RangerTag getTagByGuid(String guid) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public List<RangerTag> getTagsByType(String name) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public List<RangerTag> getTagsForResourceId(Long resourceId) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public List<RangerTag> getTagsForResourceGuid(String resourceGuid) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public List<RangerTag> getTags(SearchFilter filter) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public PList<RangerTag> getPaginatedTags(SearchFilter filter) throws Exception {
		throw new Exception("Not implemented");
	}


	@Override
	public RangerServiceResource createServiceResource(RangerServiceResource resource) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> createServiceResource(" + resource + ")");
		}

		RangerServiceResource ret = null;

		WebResource webResource = createWebResource(REST_URL_SERVICERESOURCES_RESOURCE);
		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).type(REST_MIME_TYPE_JSON).post(ClientResponse.class, tagRESTClient.toJson(resource));

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(RangerServiceResource.class);
		} else {
			LOG.error("RangerAdmin REST call returned with response={" + response +"}");

			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== createServiceResource(" + resource + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerServiceResource updateServiceResource(RangerServiceResource resource) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public void deleteServiceResource(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> deleteServiceResource(" + id  + ")");
		}
		WebResource webResource = createWebResource(REST_URL_SERVICERESOURCE_RESOURCE + Long.toString(id));

		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).type(REST_MIME_TYPE_JSON).delete(ClientResponse.class);

		if(response != null && response.getStatus() == 204) {
		} else {
			LOG.error("RangerAdmin REST call returned with response={" + response + "}");

			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== deleteServiceResource(" + id + ")");
		}
	}

	@Override
	public RangerServiceResource getServiceResource(Long id) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public RangerServiceResource getServiceResourceByGuid(String guid) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public List<RangerServiceResource> getServiceResourcesByService(String serviceName) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public RangerServiceResource getServiceResourceByResourceSignature(String resourceSignature) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public List<RangerServiceResource> getServiceResources(SearchFilter filter) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public PList<RangerServiceResource> getPaginatedServiceResources(SearchFilter filter) throws Exception {
		throw new Exception("Not implemented");
	}


	@Override
	public RangerTagResourceMap createTagResourceMap(RangerTagResourceMap tagResourceMap) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> createTagResourceMap(" + tagResourceMap + ")");
		}

		RangerTagResourceMap ret = null;

		WebResource webResource = createWebResource(REST_URL_TAGRESOURCEMAP_IDS_RESOURCE)
				.queryParam("tag-id", Long.toString(tagResourceMap.getTagId()))
				.queryParam("resource-id", Long.toString(tagResourceMap.getResourceId()));

		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).type(REST_MIME_TYPE_JSON).post(ClientResponse.class);

		if(response != null && response.getStatus() == 200) {
			ret = response.getEntity(RangerTagResourceMap.class);
		} else {
			LOG.error("RangerAdmin REST call returned with response={" + response +"}");

			RESTResponse resp = RESTResponse.fromClientResponse(response);

			throw new Exception(resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== createTagResourceMap(" + tagResourceMap + "): " + ret);
		}

		return ret;
	}

	@Override
	public void deleteTagResourceMap(Long id) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public void uploadServiceTags(ServiceTags serviceTags) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> uploadServiceTags()");
		}
		WebResource webResource = createWebResource(REST_URL_IMPORT_SERVICETAGS_RESOURCE);

		ClientResponse response    = webResource.accept(REST_MIME_TYPE_JSON).type(REST_MIME_TYPE_JSON).put(ClientResponse.class, tagRESTClient.toJson(serviceTags));

		if(response != null && response.getStatus() == 204) {
		} else {
			LOG.error("RangerAdmin REST call returned with response={" + response + "}");

			RESTResponse resp = RESTResponse.fromClientResponse(response);

			LOG.error("Upload of service-tags failed with message " + resp.getMessage());
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== uploadServiceTags()");
		}
	}

	@Override
	public RangerTagResourceMap getTagResourceMap(Long id) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public RangerTagResourceMap getTagResourceMapByGuid(String guid) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public List<RangerTagResourceMap> getTagResourceMapsForTagId(Long tagId) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public List<RangerTagResourceMap> getTagResourceMapsForTagGuid(String tagGuid) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public List<RangerTagResourceMap> getTagResourceMapsForResourceId(Long resourceId) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public List<RangerTagResourceMap> getTagResourceMapsForResourceGuid(String resourceGuid) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public RangerTagResourceMap getTagResourceMapForTagAndResourceId(Long tagId, Long resourceId) throws Exception {
		throw new Exception("Not implemented");
	}


	@Override
	public RangerTagResourceMap getTagResourceMapForTagAndResourceGuid(String tagGuid, String resourceGuid) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public List<RangerTagResourceMap> getTagResourceMaps(SearchFilter filter) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public PList<RangerTagResourceMap> getPaginatedTagResourceMaps(SearchFilter filter) throws Exception {
		throw new Exception("Not implemented");
	}


	@Override
	public ServiceTags getServiceTagsIfUpdated(String serviceName, Long lastKnownVersion) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public List<String> getTagTypes(String serviceName) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public List<String> lookupTagTypes(String serviceName, String pattern) throws Exception {
		throw new Exception("Not implemented");
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
