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

package org.apache.ranger.plugin.store;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.*;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServiceTags;

import java.util.*;

public abstract class AbstractTagStore implements TagStore {
	private static final Log LOG = LogFactory.getLog(AbstractTagStore.class);


	protected ServiceStore svcStore;

	@Override
	public void init() throws Exception {
		// Empty
	}

	@Override
	final public void setServiceStore(ServiceStore svcStore) {
		this.svcStore = svcStore;
	}

	protected void preCreate(RangerBaseModelObject obj) throws Exception {
		obj.setId(0L);
		if(obj.getGuid() == null) {
			obj.setGuid(UUID.randomUUID().toString());
		}
		obj.setCreateTime(new Date());
		obj.setUpdateTime(obj.getCreateTime());
		obj.setVersion(1L);
	}

	protected void postCreate(RangerBaseModelObject obj) throws Exception {
	}

	protected void preUpdate(RangerBaseModelObject obj) throws Exception {
		if(obj.getId() == null) {
			obj.setId(0L);
		}

		if(obj.getGuid() == null) {
			obj.setGuid(UUID.randomUUID().toString());
		}

		if(obj.getCreateTime() == null) {
			obj.setCreateTime(new Date());
		}

		Long version = obj.getVersion();

		if(version == null) {
			version = 1L;
		} else {
			version =  version + 1;
		}

		obj.setVersion(version);
		obj.setUpdateTime(new Date());
	}

	protected void postUpdate(RangerBaseModelObject obj) throws Exception {
	}

	protected void preDelete(RangerBaseModelObject obj) throws Exception {
		// TODO:
	}

	protected void postDelete(RangerBaseModelObject obj) throws Exception {
	}

	protected long getMaxId(List<? extends RangerBaseModelObject> objs) {
		long ret = -1;

		if (objs != null) {
			for (RangerBaseModelObject obj : objs) {
				if (obj.getId() > ret) {
					ret = obj.getId();
				}
			}
		}
		return ret;
	}

	@Override
	public PList<RangerTagDef> getPaginatedTagDefs(SearchFilter filter) throws Exception {
		List<RangerTagDef> list = getTagDefs(filter);

		return new PList<RangerTagDef>(list, 0, list.size(),
				(long)list.size(), list.size(), filter.getSortType(), filter.getSortBy());
	}

	@Override
	public PList<RangerTagResourceMap> getPaginatedTagResourceMaps(SearchFilter filter) throws Exception {
		List<RangerTagResourceMap> list = getTagResourceMaps(filter);

		return new PList<RangerTagResourceMap>(list, 0, list.size(),
				(long)list.size(), list.size(), filter.getSortType(), filter.getSortBy());
	}


	@Override
	public List<RangerTagDef> getTagDefsByExternalId(String externalId) throws Exception {

		List<RangerTagDef> ret;

		if (StringUtils.isNotBlank(externalId)) {
			SearchFilter filter = new SearchFilter(SearchFilter.TAG_DEF_EXTERNAL_ID, externalId);

			ret = getTagDefs(filter);

		} else {
			ret = null;
		}

		return ret;
	}

	@Override
	public RangerTag getTagById(Long id) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> AbstractTagStore.getTagById(" + id + ")");
		}

		RangerTag ret = null;

		if (id != null) {
			SearchFilter filter = new SearchFilter(SearchFilter.TAG_ID, id.toString());

			List<RangerTag> tags = getTags(filter);

			if (CollectionUtils.isNotEmpty(tags) && CollectionUtils.size(tags) == 1) {
				ret = tags.get(0);
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== AbstractTagStore.getTagDefById(" + id + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerTag> getTagsByName(String name) throws Exception {
		SearchFilter filter = new SearchFilter(SearchFilter.TAG_NAME, name);

		return getTags(filter);
	}

	@Override
	public List<RangerTag> getTagsByExternalId(String externalId) throws Exception {
		SearchFilter filter = new SearchFilter(SearchFilter.TAG_EXTERNAL_ID, externalId);

		return getTags(filter);
	}


	@Override
	public RangerServiceResource getServiceResourceById(Long id) throws Exception {
		SearchFilter filter = new SearchFilter(SearchFilter.TAG_RESOURCE_ID, id.toString());

		List<RangerServiceResource> resources = getServiceResources(filter);
		if (CollectionUtils.isEmpty(resources) || resources.size() > 1) {
			throw new Exception("Not exactly one resource found with id=" + id);
		}

		return resources.get(0);
	}

	@Override
	public List<RangerServiceResource> getServiceResourcesByExternalId(String externalId) throws Exception {
		SearchFilter filter = new SearchFilter(SearchFilter.TAG_RESOURCE_EXTERNAL_ID, externalId);

		return getServiceResources(filter);
	}

	@Override
	public List<RangerServiceResource> getServiceResourcesByServiceAndResourceSpec(String serviceName, Map<String, RangerPolicy.RangerPolicyResource> resourceSpec) throws Exception {
		List<RangerServiceResource> ret = null;

		RangerService service;
		try {
			service = svcStore.getServiceByName(serviceName);
		} catch (Exception excp) {
			LOG.error("AbstractTagStore.getTaggedResource - failed to get service " + serviceName);
			throw new Exception("Invalid service: " + serviceName);
		}

		if (MapUtils.isNotEmpty(resourceSpec)) {

			RangerServiceResource resource = new RangerServiceResource(serviceName, resourceSpec);
			ret = getServiceResources(resource);
		}

		return ret;
	}

	private List<RangerServiceResource> getServiceResources(RangerServiceResource resource) throws Exception {

		List<RangerServiceResource> ret = null;

		RangerServiceResourceSignature serializer = new RangerServiceResourceSignature(resource);
		String signature = serializer.getSignature();

		SearchFilter filter = new SearchFilter(SearchFilter.TAG_RESOURCE_SIGNATURE, signature);

		ret = getServiceResources(filter);

		return ret;
	}

	@Override
	public List<RangerTagResourceMap> getTagResourceMap(String externalResourceId, String externalTagId) throws Exception {
		List<RangerTagResourceMap> ret = null;

		SearchFilter serviceResourceFilter = new SearchFilter();
		SearchFilter tagFilter = new SearchFilter();

		serviceResourceFilter.setParam(SearchFilter.TAG_RESOURCE_EXTERNAL_ID, externalResourceId);
		List<RangerServiceResource> serviceResources = getServiceResources(serviceResourceFilter);

		tagFilter.setParam(SearchFilter.TAG_EXTERNAL_ID, externalTagId);
		List<RangerTag> tags = getTags(tagFilter);

		if (CollectionUtils.isNotEmpty(serviceResources) && CollectionUtils.isNotEmpty(tags)) {

			for (RangerServiceResource serviceResource : serviceResources) {

				Long resourceId = serviceResource.getId();

				for (RangerTag tag : tags) {

					Long tagId = tag.getId();

					SearchFilter mapFilter = new SearchFilter();

					mapFilter.setParam(SearchFilter.TAG_MAP_TAG_ID, tagId.toString());

					mapFilter.setParam(SearchFilter.TAG_MAP_RESOURCE_ID, resourceId.toString());

					ret = getTagResourceMaps(mapFilter);

					if (CollectionUtils.isNotEmpty(ret)) {
						break;
					}
				}
			}
		}

		return ret;
	}

	@Override
	public RangerTagResourceMap getTagResourceMapById(Long id) throws Exception {
		SearchFilter filter = new SearchFilter();

		filter.setParam(SearchFilter.TAG_MAP_ID, id.toString());

		List<RangerTagResourceMap> list = getTagResourceMaps(filter);

		if (CollectionUtils.isEmpty(list) || CollectionUtils.size(list) != 1)  {
			throw new Exception("Cannot find unique tagResourceMap object with id=" + id);
		}
		return list.get(0);
	}

	@Override
	public ServiceTags getServiceTagsIfUpdated(String serviceName, Long lastKnownVersion) throws Exception {

		ServiceTags ret = new ServiceTags();

		boolean tagsChanged = true;

		RangerService service = null;

		try {
			service = svcStore.getServiceByName(serviceName);
			ret.setServiceName(serviceName);
		} catch (Exception exception) {
			LOG.error("Cannot find service for serviceName=" + serviceName);
			tagsChanged = false;
		}

		if (lastKnownVersion != null
				&& service != null && service.getTagVersion() != null
				&& lastKnownVersion.compareTo(service.getTagVersion()) >= 0 ) {
			tagsChanged = false;
		}

		if (tagsChanged) {
			SearchFilter filter = new SearchFilter();

			filter.setParam(SearchFilter.TAG_RESOURCE_SERVICE_NAME, serviceName);

			List<RangerServiceResource> serviceResources = getServiceResources(filter);

			Map<Long, RangerTag> tagsMap = new HashMap<Long, RangerTag>();
			Map<Long, List<Long>> resourceToTagIdsMap = new HashMap<Long, List<Long>>();

			for (RangerServiceResource serviceResource : serviceResources) {
				List<RangerTag> tagList = getTagsForServiceResourceObject(serviceResource);

				if (CollectionUtils.isNotEmpty(tagList)) {
					List<Long> tagIdList = new ArrayList<Long>();
					for (RangerTag tag : tagList) {
						tagsMap.put(tag.getId(), tag);
						tagIdList.add(tag.getId());
					}
					resourceToTagIdsMap.put(serviceResource.getId(), tagIdList);
				}
			}

			if (MapUtils.isEmpty(resourceToTagIdsMap)) {
				serviceResources.clear();
			}

			ret.setServiceResources(serviceResources);
			ret.setResourceToTagIds(resourceToTagIdsMap);
			ret.setTags(tagsMap);

			if (service != null && service.getTagVersion() != null) {
				ret.setTagVersion(service.getTagVersion());
			}
			if (service != null && service.getTagUpdateTime() != null) {
				ret.setTagUpdateTime(service.getTagUpdateTime());
			}
			if (LOG.isDebugEnabled()) {
				LOG.debug("Changes to tagVersion detected, tagVersion in service=" + (service == null ? null : service.getTagVersion())
						+ ", Plugin-provided lastKnownVersion=" + lastKnownVersion);
			}
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("No changes to tagVersion detected, tagVersion in service=" + (service == null ? null : service.getTagVersion())
				+ ", Plugin-provided lastKnownVersion=" + lastKnownVersion);
			}
			ret.setTagVersion(lastKnownVersion);
		}

		return ret;

	}

	@Override
	public List<RangerTag> getTagsForServiceResource(Long resourceId) throws Exception {
		RangerServiceResource serviceResource = getServiceResourceById(resourceId);

		List<RangerTag> tagList = getTagsForServiceResourceObject(serviceResource);

		return tagList;
	}

	@Override
	public List<RangerTag> getTagsForServiceResourceByExtId(String resourceExtId) throws Exception {
		List<RangerTag> tagList = new ArrayList<RangerTag>();

		List<RangerServiceResource> serviceResources = getServiceResourcesByExternalId(resourceExtId);
		for (RangerServiceResource serviceResource : serviceResources) {
			List<RangerTag> tmp = getTagsForServiceResourceObject(serviceResource);
			tagList.addAll(tmp);
		}
		return tagList;
	}

	private List<RangerTag> getTagsForServiceResourceObject(RangerServiceResource serviceResource) throws Exception {

		List<RangerTag> tagList = new ArrayList<RangerTag>();

		if (serviceResource != null) {
			SearchFilter mapFilter = new SearchFilter();
			mapFilter.setParam(SearchFilter.TAG_MAP_RESOURCE_ID, serviceResource.getId().toString());

			List<RangerTagResourceMap> associations = getTagResourceMaps(mapFilter);
			if (CollectionUtils.isNotEmpty(associations)) {

				for (RangerTagResourceMap association : associations) {
					RangerTag tag = getTagById(association.getTagId());
					if (tag != null) {
						tagList.add(tag);
					}
				}
			}
		}
		return tagList;
	}

	@Override
	public List<RangerTagResourceMap> getTagResourceMapsByResourceId(Long tagId) throws Exception {
		SearchFilter filter = new SearchFilter(SearchFilter.TAG_MAP_RESOURCE_ID, tagId.toString());
		List<RangerTagResourceMap> associations = getTagResourceMaps(filter);
		return associations;
	}

	@Override
	public List<RangerTagResourceMap> getTagResourceMapsByTagId(Long tagId) throws Exception {
		SearchFilter filter = new SearchFilter(SearchFilter.TAG_MAP_TAG_ID, tagId.toString());
		List<RangerTagResourceMap> associations = getTagResourceMaps(filter);
		return associations;
	}
}


