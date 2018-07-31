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

package org.apache.ranger.biz;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.common.GUIDUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerAdminTagEnricher;
import org.apache.ranger.common.RangerServiceTagsCache;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXDBBase;
import org.apache.ranger.entity.XXResourceDef;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.entity.XXServiceResource;
import org.apache.ranger.entity.XXServiceVersionInfo;
import org.apache.ranger.entity.XXTag;
import org.apache.ranger.entity.XXTagAttribute;
import org.apache.ranger.entity.XXTagAttributeDef;
import org.apache.ranger.entity.XXServiceResourceElement;
import org.apache.ranger.entity.XXServiceResourceElementValue;
import org.apache.ranger.entity.XXTagResourceMap;
import org.apache.ranger.plugin.model.*;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerTagDef.RangerTagAttributeDef;
import org.apache.ranger.plugin.store.AbstractTagStore;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.store.RangerServiceResourceSignature;
import org.apache.ranger.plugin.util.RangerServiceNotFoundException;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServiceTags;
import org.apache.ranger.service.RangerAuditFields;
import org.apache.ranger.service.RangerTagDefService;
import org.apache.ranger.service.RangerTagResourceMapService;
import org.apache.ranger.service.RangerTagService;
import org.apache.ranger.service.RangerServiceResourceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletResponse;

@Component
public class TagDBStore extends AbstractTagStore {
	private static final Log LOG = LogFactory.getLog(TagDBStore.class);

	@Autowired
	RangerTagDefService rangerTagDefService;

	@Autowired
	RangerTagService rangerTagService;

	@Autowired
	RangerServiceResourceService rangerServiceResourceService;

	@Autowired
	RangerTagResourceMapService rangerTagResourceMapService;

	@Autowired
	RangerDaoManager daoManager;

	@Autowired
	@Qualifier(value = "transactionManager")
	PlatformTransactionManager txManager;

	@Autowired
	RESTErrorUtil errorUtil;

	@Autowired
	RangerAuditFields<XXDBBase> rangerAuditFields;

	@Autowired
	GUIDUtil guidUtil;

	@Autowired
	RESTErrorUtil restErrorUtil;

	@PostConstruct
	public void initStore() {
		RangerAdminTagEnricher.setTagStore(this);
	}

	@Override
	public RangerTagDef createTagDef(RangerTagDef tagDef) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.createTagDef(" + tagDef + ")");
		}

		RangerTagDef ret = rangerTagDefService.create(tagDef);

		createTagAttributeDefs(ret.getId(), tagDef.getAttributeDefs());

		ret = rangerTagDefService.read(ret.getId());

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.createTagDef(" + tagDef + "): id=" + (ret == null ? null : ret.getId()));
		}

		return ret;
	}

	@Override
	public RangerTagDef updateTagDef(RangerTagDef tagDef) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.updateTagDef(" + tagDef + ")");
		}

		RangerTagDef existing = rangerTagDefService.read(tagDef.getId());

		if (existing == null) {
			throw errorUtil.createRESTException("failed to update tag-def [" + tagDef.getName() + "], Reason: No TagDef found with id: [" + tagDef.getId() + "]", MessageEnums.DATA_NOT_UPDATABLE);
		}

		tagDef.setCreatedBy(existing.getCreatedBy());
		tagDef.setCreateTime(existing.getCreateTime());
		tagDef.setGuid(existing.getGuid());
		tagDef.setVersion(existing.getVersion());

		RangerTagDef ret = rangerTagDefService.update(tagDef);

		// TODO: delete attributes might fail; so instead of delete+create, following should be updated to deal with only attributes that changed
		deleteTagAttributeDefs(ret.getId());
		createTagAttributeDefs(ret.getId(), tagDef.getAttributeDefs());

		ret = rangerTagDefService.read(ret.getId());

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.updateTagDef(" + tagDef + "): " + ret);
		}

		return ret;
	}

	@Override
	public void deleteTagDefByName(String name) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.deleteTagDef(" + name + ")");
		}

		if (StringUtils.isNotBlank(name)) {
			RangerTagDef tagDef = getTagDefByName(name);

			if(tagDef != null) {
				if(LOG.isDebugEnabled()) {
					LOG.debug("Deleting tag-def [name=" + name + "; id=" + tagDef.getId() + "]");
				}

				deleteTagAttributeDefs(tagDef.getId());
				rangerTagDefService.delete(tagDef);
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.deleteTagDef(" + name + ")");
		}
	}

	@Override
	public void deleteTagDef(Long id) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.deleteTagDef(" + id + ")");
		}

		if(id != null) {
			RangerTagDef tagDef = rangerTagDefService.read(id);

			if(tagDef != null) {
				deleteTagAttributeDefs(tagDef.getId());
				rangerTagDefService.delete(tagDef);
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.deleteTagDef(" + id + ")");
		}
	}

	@Override
	public RangerTagDef getTagDef(Long id) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getTagDef(" + id + ")");
		}

		RangerTagDef ret = rangerTagDefService.read(id);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getTagDef(" + id + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerTagDef getTagDefByGuid(String guid) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getTagDefByGuid(" + guid + ")");
		}

		RangerTagDef ret = rangerTagDefService.getTagDefByGuid(guid);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getTagDefByGuid(" + guid + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerTagDef getTagDefByName(String name) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getTagDefByName(" + name + ")");
		}

		RangerTagDef ret = null;

		if (StringUtils.isNotBlank(name)) {
			ret = rangerTagDefService.getTagDefByName(name);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getTagDefByName(" + name + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerTagDef> getTagDefs(SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getTagDefs(" + filter + ")");
		}

		List<RangerTagDef> ret = getPaginatedTagDefs(filter).getList();

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getTagDefs(" + filter + "): " + ret);
		}

		return ret;
	}

	@Override
	public PList<RangerTagDef> getPaginatedTagDefs(SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getPaginatedTagDefs(" + filter + ")");
		}

		PList<RangerTagDef> ret = rangerTagDefService.searchRangerTagDefs(filter);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getPaginatedTagDefs(" + filter + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<String> getTagTypes() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getTagTypes()");
		}

		List<String> ret = daoManager.getXXTagDef().getAllNames();

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getTagTypes(): count=" + (ret != null ? ret.size() : 0));
		}

		return ret;
	}


	@Override
	public RangerTag createTag(RangerTag tag) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.createTag(" + tag + ")");
		}

		RangerTag ret = rangerTagService.create(tag);

		createTagAttributes(ret.getId(), tag.getAttributes());

		ret = rangerTagService.read(ret.getId());

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.createTag(" + tag + "): " + ret);
		}

		return ret;
	}
	
	@Override
	public RangerTag updateTag(RangerTag tag) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.updateTag(" + tag + ")");
		}

		RangerTag existing = rangerTagService.read(tag.getId());

		if (existing == null) {
			throw errorUtil.createRESTException("failed to update tag [" + tag.getType() + "], Reason: No Tag found with id: [" + tag.getId() + "]", MessageEnums.DATA_NOT_UPDATABLE);
		}

		tag.setCreatedBy(existing.getCreatedBy());
		tag.setCreateTime(existing.getCreateTime());
		tag.setGuid(existing.getGuid());
		tag.setVersion(existing.getVersion());

		RangerTag ret = rangerTagService.update(tag);

		deleteTagAttributes(existing.getId());
		createTagAttributes(existing.getId(), tag.getAttributes());

		ret = rangerTagService.read(ret.getId());

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.updateTag(" + tag + ") : " + ret);
		}

		return ret;
	}

	@Override
	public void deleteTag(Long id) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.deleteTag(" + id + ")");
		}

		RangerTag tag = rangerTagService.read(id);

		deleteTagAttributes(id);

		rangerTagService.delete(tag);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.deleteTag(" + id + ")");
		}
	}

	@Override
	public RangerTag getTag(Long id) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getTag(" + id + ")");
		}

		RangerTag ret = rangerTagService.read(id);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getTag(" + id + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerTag getTagByGuid(String guid) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getTagByGuid(" + guid + ")");
		}

		RangerTag ret = rangerTagService.getTagByGuid(guid);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getTagByGuid(" + guid + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerTag> getTagsByType(String type) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getTagsByType(" + type + ")");
		}

		List<RangerTag> ret = null;

		if (StringUtils.isNotBlank(type)) {
			ret = rangerTagService.getTagsByType(type);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getTagsByType(" + type + "): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public List<RangerTag> getTagsForResourceId(Long resourceId) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getTagsForResourceId(" + resourceId + ")");
		}

		List<RangerTag> ret = null;

		if (resourceId != null) {
			ret = rangerTagService.getTagsForResourceId(resourceId);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getTagsForResourceId(" + resourceId + "): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public List<RangerTag> getTagsForResourceGuid(String resourceGuid) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getTagsForResourceGuid(" + resourceGuid + ")");
		}

		List<RangerTag> ret = null;

		if (resourceGuid != null) {
			ret = rangerTagService.getTagsForResourceGuid(resourceGuid);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getTagsForResourceGuid(" + resourceGuid + "): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public List<RangerTag> getTags(SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getTags(" + filter + ")");
		}

		List<RangerTag> ret = rangerTagService.searchRangerTags(filter).getList();

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getTags(" + filter + "): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public PList<RangerTag> getPaginatedTags(SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getPaginatedTags(" + filter + ")");
		}

		PList<RangerTag> ret = rangerTagService.searchRangerTags(filter);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getPaginatedTags(" + filter + "): count=" + (ret == null ? 0 : ret.getPageSize()));
		}

		return ret;
	}


	@Override
	public RangerServiceResource createServiceResource(RangerServiceResource resource) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.createServiceResource(" + resource + ")");
		}

		if (StringUtils.isEmpty(resource.getResourceSignature())) {
			RangerServiceResourceSignature serializer = new RangerServiceResourceSignature(resource);

			resource.setResourceSignature(serializer.getSignature());
		}

		RangerServiceResource ret = rangerServiceResourceService.create(resource);

		createResourceForServiceResource(ret.getId(), resource);

		ret = rangerServiceResourceService.read(ret.getId());

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.createServiceResource(" + resource + ")");
		}

		return ret;
	}

	@Override
	public RangerServiceResource updateServiceResource(RangerServiceResource resource) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.updateResource(" + resource + ")");
		}

		RangerServiceResource existing = rangerServiceResourceService.read(resource.getId());

		if (existing == null) {
			throw errorUtil.createRESTException("failed to update tag [" + resource.getId() + "], Reason: No resource found with id: [" + resource.getId() + "]", MessageEnums.DATA_NOT_UPDATABLE);
		}

		if (StringUtils.isEmpty(resource.getResourceSignature())) {
			RangerServiceResourceSignature serializer = new RangerServiceResourceSignature(resource);

			resource.setResourceSignature(serializer.getSignature());
		}

		boolean serviceResourceElementUpdateNeeded =
				!StringUtils.equals(existing.getResourceSignature(), resource.getResourceSignature());

		resource.setCreatedBy(existing.getCreatedBy());
		resource.setCreateTime(existing.getCreateTime());
		resource.setGuid(existing.getGuid());
		resource.setVersion(existing.getVersion());

		rangerServiceResourceService.update(resource);

		if (serviceResourceElementUpdateNeeded) {
			deleteResourceForServiceResource(existing.getId());
			createResourceForServiceResource(existing.getId(), resource);
		}

		RangerServiceResource ret = rangerServiceResourceService.read(existing.getId());

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.updateResource(" + resource + ") : " + ret);
		}

		return ret;
	}

	@Override
	public void deleteServiceResource(Long id) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.deleteServiceResource(" + id + ")");
		}

		RangerServiceResource resource = getServiceResource(id);

		if(resource != null) {
			deleteResourceForServiceResource(resource.getId());
			rangerServiceResourceService.delete(resource);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.deleteServiceResource(" + id + ")");
		}
	}

	@Override
	public void deleteServiceResourceByGuid(String guid) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.deleteServiceResourceByGuid(" + guid + ")");
		}

		RangerServiceResource resource = getServiceResourceByGuid(guid);

		if(resource != null) {
			deleteResourceForServiceResource(resource.getId());
			rangerServiceResourceService.delete(resource);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.deleteServiceResourceByGuid(" + guid + ")");
		}
	}

	@Override
	public RangerServiceResource getServiceResource(Long id) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getServiceResource(" + id + ")");
		}

		RangerServiceResource ret = rangerServiceResourceService.read(id);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getServiceResource(" + id + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerServiceResource getServiceResourceByGuid(String guid) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getServiceResourceByGuid(" + guid + ")");
		}

		RangerServiceResource ret = rangerServiceResourceService.getServiceResourceByGuid(guid);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getServiceResourceByGuid(" + guid + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerServiceResource> getServiceResourcesByService(String serviceName) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getServiceResourcesByService(" + serviceName + ")");
		}

		List<RangerServiceResource> ret = null;

		XXService service = daoManager.getXXService().findByName(serviceName);

		if (service != null) {
			ret = rangerServiceResourceService.getByServiceId(service.getId());
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getServiceResourcesByService(" + serviceName + "): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public List<String> getServiceResourceGuidsByService(String serviceName) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getServiceResourceGuidsByService(" + serviceName + ")");
		}

		List<String> ret = null;

		XXService service = daoManager.getXXService().findByName(serviceName);

		if (service != null) {
			ret = daoManager.getXXServiceResource().findServiceResourceGuidsInServiceId(service.getId());
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getServiceResourceGuidsByService(" + serviceName + "): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public RangerServiceResource getServiceResourceByServiceAndResourceSignature(String serviceName, String resourceSignature) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getServiceResourceByServiceAndResourceSignature(" + serviceName + ", " + resourceSignature + ")");
		}

		RangerServiceResource ret = null;

		XXService service = daoManager.getXXService().findByName(serviceName);

		if (service != null) {
			ret = rangerServiceResourceService.getByServiceAndResourceSignature(service.getId(), resourceSignature);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getServiceResourceByServiceAndResourceSignature(" + serviceName + ", " + resourceSignature + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerServiceResource> getServiceResources(SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getServiceResources(" + filter + ")");
		}

		List<RangerServiceResource> ret = rangerServiceResourceService.searchServiceResources(filter).getList();

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getServiceResources(" + filter + "): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public PList<RangerServiceResource> getPaginatedServiceResources(SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getPaginatedServiceResources(" + filter + ")");
		}

		PList<RangerServiceResource> ret = rangerServiceResourceService.searchServiceResources(filter);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getPaginatedServiceResources(" + filter + "): count=" + (ret == null ? 0 : ret.getPageSize()));
		}

		return ret;
	}


	@Override
	public RangerTagResourceMap createTagResourceMap(RangerTagResourceMap tagResourceMap) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.createTagResourceMap(" + tagResourceMap + ")");
		}

		RangerTagResourceMap ret = rangerTagResourceMapService.create(tagResourceMap);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.createTagResourceMap(" + tagResourceMap + "): " + ret);
		}

		return ret;
	}

	@Override
	public void deleteTagResourceMap(Long id) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.deleteTagResourceMap(" + id + ")");
		}

		RangerTagResourceMap tagResourceMap = rangerTagResourceMapService.read(id);
		Long tagId = tagResourceMap.getTagId();
		RangerTag tag = getTag(tagId);

		rangerTagResourceMapService.delete(tagResourceMap);

		if (tag.getOwner() == RangerTag.OWNER_SERVICERESOURCE) {
			deleteTag(tagId);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.deleteTagResourceMap(" + id + ")");
		}
	}

	@Override
	public RangerTagResourceMap getTagResourceMap(Long id) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getTagResourceMap(" + id + ")");
		}

		RangerTagResourceMap ret = rangerTagResourceMapService.read(id);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getTagResourceMap(" + id + ")");
		}

		return ret;
	}

	@Override
	public RangerTagResourceMap getTagResourceMapByGuid(String guid) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getTagResourceMapByGuid(" + guid + ")");
		}

		RangerTagResourceMap ret = rangerTagResourceMapService.getByGuid(guid);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getTagResourceMapByGuid(" + guid + ")");
		}

		return ret;
	}

	@Override
	public List<RangerTagResourceMap> getTagResourceMapsForTagId(Long tagId) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getTagResourceMapsForTagId(" + tagId + ")");
		}

		List<RangerTagResourceMap> ret = rangerTagResourceMapService.getByTagId(tagId);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getTagResourceMapsForTagId(" + tagId + "): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public List<RangerTagResourceMap> getTagResourceMapsForTagGuid(String tagGuid) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getTagResourceMapsForTagGuid(" + tagGuid + ")");
		}

		List<RangerTagResourceMap> ret = rangerTagResourceMapService.getByTagGuid(tagGuid);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getTagResourceMapsForTagGuid(" + tagGuid + "): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public List<Long> getTagIdsForResourceId(Long resourceId) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getTagIdsForResourceId(" + resourceId + ")");
		}

		List<Long> ret = rangerTagResourceMapService.getTagIdsForResourceId(resourceId);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getTagIdsForResourceId(" + resourceId + "): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public List<RangerTagResourceMap> getTagResourceMapsForResourceId(Long resourceId) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getTagResourceMapsForResourceId(" + resourceId + ")");
		}

		List<RangerTagResourceMap> ret = rangerTagResourceMapService.getByResourceId(resourceId);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getTagResourceMapsForResourceId(" + resourceId + "): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public List<RangerTagResourceMap> getTagResourceMapsForResourceGuid(String resourceGuid) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getTagResourceMapsForResourceGuid(" + resourceGuid + ")");
		}

		List<RangerTagResourceMap> ret = rangerTagResourceMapService.getByResourceGuid(resourceGuid);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getTagResourceMapsForResourceGuid(" + resourceGuid + "): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public RangerTagResourceMap getTagResourceMapForTagAndResourceId(Long tagId, Long resourceId) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getTagResourceMapsForTagAndResourceId(" + tagId + ", " + resourceId + ")");
		}

		RangerTagResourceMap ret = rangerTagResourceMapService.getByTagAndResourceId(tagId, resourceId);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getTagResourceMapsForTagAndResourceId(" + tagId + ", " + resourceId + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerTagResourceMap getTagResourceMapForTagAndResourceGuid(String tagGuid, String resourceGuid) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getTagResourceMapForTagAndResourceGuid(" + tagGuid + ", " + resourceGuid + ")");
		}

		RangerTagResourceMap ret = rangerTagResourceMapService.getByTagAndResourceGuid(tagGuid, resourceGuid);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getTagResourceMapForTagAndResourceGuid(" + tagGuid + ", " + resourceGuid + "): " + ret);
		}

		return ret;
	}


	@Override
	public List<RangerTagResourceMap> getTagResourceMaps(SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getTagResourceMaps(" + filter+ ")");
		}

		List<RangerTagResourceMap> ret = rangerTagResourceMapService.searchRangerTaggedResources(filter).getList();

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getTagResourceMaps(" + filter + "): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public PList<RangerTagResourceMap> getPaginatedTagResourceMaps(SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getPaginatedTagResourceMaps(" + filter+ ")");
		}

		PList<RangerTagResourceMap> ret = rangerTagResourceMapService.searchRangerTaggedResources(filter);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getPaginatedTagResourceMaps(" + filter + "): count=" + (ret == null ? 0 : ret.getPageSize()));
		}

		return ret;
	}


	@Override
	public ServiceTags getServiceTagsIfUpdated(String serviceName, Long lastKnownVersion) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getServiceTagsIfUpdated(" + serviceName + ", " + lastKnownVersion + ")");
		}

		ServiceTags ret = null;

		XXService xxService = daoManager.getXXService().findByName(serviceName);

		if (xxService == null) {
			LOG.error("Requested Service not found. serviceName=" + serviceName);
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, RangerServiceNotFoundException.buildExceptionMsg(serviceName),
					false);
		}

		XXServiceVersionInfo serviceVersionInfoDbObj = daoManager.getXXServiceVersionInfo().findByServiceName(serviceName);

		if (serviceVersionInfoDbObj == null) {
			LOG.warn("serviceVersionInfo does not exist. name=" + serviceName);
		}

		if (lastKnownVersion == null || serviceVersionInfoDbObj == null || serviceVersionInfoDbObj.getTagVersion() == null || !lastKnownVersion.equals(serviceVersionInfoDbObj.getTagVersion())) {
			ret = RangerServiceTagsCache.getInstance().getServiceTags(serviceName, xxService.getId(), this);
		}

		if (ret != null && lastKnownVersion != null && lastKnownVersion.equals(ret.getTagVersion())) {
			// ServiceTags are not changed
			ret = null;
		}

		if (LOG.isDebugEnabled()) {
			RangerServiceTagsCache.getInstance().dump();
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getServiceTagsIfUpdated(" + serviceName + ", " + lastKnownVersion + "): count=" + ((ret == null || ret.getTags() == null) ? 0 : ret.getTags().size()));
		}

		return ret;
	}

	@Override
	public Long getTagVersion(String serviceName) {

		XXServiceVersionInfo serviceVersionInfoDbObj = daoManager.getXXServiceVersionInfo().findByServiceName(serviceName);

		return serviceVersionInfoDbObj != null ? serviceVersionInfoDbObj.getTagVersion() : null;
	}

	@Override
	public ServiceTags getServiceTags(String serviceName) throws Exception {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.getServiceTags(" + serviceName + ")");
		}

		ServiceTags ret = null;

		XXService xxService = daoManager.getXXService().findByName(serviceName);

		if (xxService == null) {
			throw new Exception("service does not exist. name=" + serviceName);
		}

		XXServiceVersionInfo serviceVersionInfoDbObj = daoManager.getXXServiceVersionInfo().findByServiceName(serviceName);

		if (serviceVersionInfoDbObj == null) {
			LOG.warn("serviceVersionInfo does not exist for service [" + serviceName + "]");
		}

		RangerServiceDef serviceDef = svcStore.getServiceDef(xxService.getType());

		if (serviceDef == null) {
			throw new Exception("service-def does not exist. id=" + xxService.getType());
		}

		RangerTagDBRetriever tagDBRetriever = new RangerTagDBRetriever(daoManager, txManager, xxService);

		Map<Long, RangerTagDef> tagDefMap = tagDBRetriever.getTagDefs();
		Map<Long, RangerTag> tagMap = tagDBRetriever.getTags();
		List<RangerServiceResource> resources = tagDBRetriever.getServiceResources();
		List<RangerTagResourceMap> tagResourceMaps = tagDBRetriever.getTagResourceMaps();

		Map<Long, List<Long>> resourceToTagIds = new HashMap<Long, List<Long>>();

		if (CollectionUtils.isNotEmpty(tagResourceMaps)) {
			Long resourceId = null;
			List<Long> tagIds = null;

			for (RangerTagResourceMap tagResourceMap : tagResourceMaps) {
				if (!tagResourceMap.getResourceId().equals(resourceId)) {
					if (resourceId != null) {
						resourceToTagIds.put(resourceId, tagIds);
					}

					resourceId = tagResourceMap.getResourceId();
					tagIds = new ArrayList<Long>();
				}

				tagIds.add(tagResourceMap.getTagId());
			}

			if (resourceId != null) {
				resourceToTagIds.put(resourceId, tagIds);
			}
		}

		ret = new ServiceTags();

		ret.setServiceName(xxService.getName());
		ret.setTagVersion(serviceVersionInfoDbObj == null ? null : serviceVersionInfoDbObj.getTagVersion());
		ret.setTagUpdateTime(serviceVersionInfoDbObj == null ? null : serviceVersionInfoDbObj.getTagUpdateTime());
		ret.setTagDefinitions(tagDefMap);
		ret.setTags(tagMap);
		ret.setServiceResources(resources);
		ret.setResourceToTagIds(resourceToTagIds);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.getServiceTags(" + serviceName + ")");
		}
		return ret;

	}

	private List<XXTagAttributeDef> createTagAttributeDefs(Long tagDefId, List<RangerTagAttributeDef> tagAttrDefList) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.createTagAttributeDefs(" + tagDefId + ", attributeDefCount=" + (tagAttrDefList == null ? 0 : tagAttrDefList.size()) + ")");
		}

		if (tagDefId == null) {
			throw errorUtil.createRESTException("TagDBStore.createTagAttributeDefs(): Error creating tag-attr def. tagDefId can not be null.", MessageEnums.ERROR_CREATING_OBJECT);
		}

		List<XXTagAttributeDef> ret = new ArrayList<XXTagAttributeDef>();

		if (CollectionUtils.isNotEmpty(tagAttrDefList)) {
			for (RangerTagDef.RangerTagAttributeDef attrDef : tagAttrDefList) {
				XXTagAttributeDef xAttrDef = new XXTagAttributeDef();

				xAttrDef.setTagDefId(tagDefId);
				xAttrDef.setName(attrDef.getName());
				xAttrDef.setType(attrDef.getType());
				xAttrDef = (XXTagAttributeDef) rangerAuditFields.populateAuditFieldsForCreate(xAttrDef);

				xAttrDef = daoManager.getXXTagAttributeDef().create(xAttrDef);

				ret.add(xAttrDef);
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.createTagAttributeDefs(" + tagDefId + ", attributeDefCount=" + (tagAttrDefList == null ? 0 : tagAttrDefList.size()) + "): retCount=" + ret.size());
		}

		return ret;
	}

	private void deleteTagAttributeDefs(Long tagDefId) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.deleteTagAttributeDefs(" + tagDefId + ")");
		}

		if (tagDefId != null) {
			List<XXTagAttributeDef> tagAttrDefList = daoManager.getXXTagAttributeDef().findByTagDefId(tagDefId);

			if (CollectionUtils.isNotEmpty(tagAttrDefList)) {
				for (XXTagAttributeDef xAttrDef : tagAttrDefList) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Deleting tag-attribute def [name=" + xAttrDef.getName() + "; id=" + xAttrDef.getId() + "]");
					}
					daoManager.getXXTagAttributeDef().remove(xAttrDef);
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.deleteTagAttributeDefs(" + tagDefId + ")");
		}
	}

	private List<XXTagAttribute> createTagAttributes(Long tagId, Map<String, String> attributes) {
		List<XXTagAttribute> ret = new ArrayList<XXTagAttribute>();

		if(MapUtils.isNotEmpty(attributes)) {
			for (Map.Entry<String, String> attr : attributes.entrySet()) {
				XXTagAttribute xTagAttr = new XXTagAttribute();

				xTagAttr.setTagId(tagId);
				xTagAttr.setName(attr.getKey());
				xTagAttr.setValue(attr.getValue());
				xTagAttr = (XXTagAttribute) rangerAuditFields.populateAuditFieldsForCreate(xTagAttr);

				xTagAttr = daoManager.getXXTagAttribute().create(xTagAttr);

				ret.add(xTagAttr);
			}
		}

		return ret;
	}

	private void deleteTagAttributes(Long tagId) {
		List<XXTagAttribute> tagAttrList = daoManager.getXXTagAttribute().findByTagId(tagId);
		for (XXTagAttribute tagAttr : tagAttrList) {
			daoManager.getXXTagAttribute().remove(tagAttr);
		}
	}

	private void deleteResourceForServiceResource(Long resourceId) {
		List<XXServiceResourceElement> resElements = daoManager.getXXServiceResourceElement().findByResourceId(resourceId);
		
		if(CollectionUtils.isNotEmpty(resElements)) {
			for(XXServiceResourceElement resElement : resElements) {
				List<XXServiceResourceElementValue> elementValues = daoManager.getXXServiceResourceElementValue().findByResValueId(resElement.getId());
				
				if(CollectionUtils.isNotEmpty(elementValues)) {
					for(XXServiceResourceElementValue elementValue : elementValues) {
						daoManager.getXXServiceResourceElementValue().remove(elementValue.getId());
					}
				}
				
				daoManager.getXXServiceResourceElement().remove(resElement.getId());
			}
		}
	}

	private void createResourceForServiceResource(Long resourceId, RangerServiceResource serviceResource) {
		String serviceName = serviceResource.getServiceName();

		XXService xService = daoManager.getXXService().findByName(serviceName);

		if (xService == null) {
			throw errorUtil.createRESTException("No Service found with name: " + serviceName, MessageEnums.ERROR_CREATING_OBJECT);
		}

		XXServiceDef xServiceDef = daoManager.getXXServiceDef().getById(xService.getType());

		if (xServiceDef == null) {
			throw errorUtil.createRESTException("No Service-Def found with ID: " + xService.getType(), MessageEnums.ERROR_CREATING_OBJECT);
		}

		Map<String, RangerPolicy.RangerPolicyResource> resElements = serviceResource.getResourceElements();

		for (Map.Entry<String, RangerPolicyResource> resElement : resElements.entrySet()) {
			XXResourceDef xResDef = daoManager.getXXResourceDef().findByNameAndServiceDefId(resElement.getKey(), xServiceDef.getId());

			if (xResDef == null) {
				LOG.error("TagDBStore.createResource: ResourceType is not valid [" + resElement.getKey() + "]");
				throw errorUtil.createRESTException("Resource Type is not valid [" + resElement.getKey() + "]", MessageEnums.DATA_NOT_FOUND);
			}

			RangerPolicyResource policyRes = resElement.getValue();

			XXServiceResourceElement resourceElement = new XXServiceResourceElement();
			resourceElement.setIsExcludes(policyRes.getIsExcludes());
			resourceElement.setIsRecursive(policyRes.getIsRecursive());
			resourceElement.setResDefId(xResDef.getId());
			resourceElement.setResourceId(resourceId);

			resourceElement = (XXServiceResourceElement) rangerAuditFields.populateAuditFieldsForCreate(resourceElement);

			resourceElement = daoManager.getXXServiceResourceElement().create(resourceElement);

			int sortOrder = 1;
			for (String resVal : policyRes.getValues()) {
				XXServiceResourceElementValue resourceElementValue = new XXServiceResourceElementValue();
				resourceElementValue.setResElementId(resourceElement.getId());
				resourceElementValue.setValue(resVal);
				resourceElementValue.setSortOrder(sortOrder);
				resourceElementValue = (XXServiceResourceElementValue) rangerAuditFields.populateAuditFieldsForCreate(resourceElementValue);

				resourceElementValue = daoManager.getXXServiceResourceElementValue().create(resourceElementValue);
				sortOrder++;
			}
		}
	}

	@Override
	public void deleteAllTagObjectsForService(String serviceName) throws Exception {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.deleteAllTagObjectsForService(" + serviceName + ")");
		}

		XXService service = daoManager.getXXService().findByName(serviceName);

		if (service != null) {
			Long serviceId = service.getId();

			List<XXTagAttribute> xxTagAttributes = daoManager.getXXTagAttribute().findByServiceIdAndOwner(serviceId, RangerTag.OWNER_SERVICERESOURCE);

			List<XXTag> xxTags = daoManager.getXXTag().findByServiceIdAndOwner(serviceId, RangerTag.OWNER_SERVICERESOURCE);

			List<XXTagResourceMap> xxTagResourceMaps = daoManager.getXXTagResourceMap().findByServiceId(serviceId);

			if (CollectionUtils.isNotEmpty(xxTagResourceMaps)) {
				for (XXTagResourceMap xxTagResourceMap : xxTagResourceMaps) {
					try {
						daoManager.getXXTagResourceMap().remove(xxTagResourceMap);
					} catch (Exception e) {
						LOG.error("Error deleting RangerTagResourceMap with id=" + xxTagResourceMap.getId(), e);
						throw e;
					}
				}
			}

			if (CollectionUtils.isNotEmpty(xxTagAttributes)) {
				for (XXTagAttribute xxTagAttribute : xxTagAttributes) {
					try {
						daoManager.getXXTagAttribute().remove(xxTagAttribute);
					} catch (Exception e) {
						LOG.error("Error deleting RangerTagAttribute with id=" + xxTagAttribute.getId(), e);
						throw e;
					}
				}
			}

			if (CollectionUtils.isNotEmpty(xxTags)) {
				for (XXTag xxTag : xxTags) {
					try {
						daoManager.getXXTag().remove(xxTag);
					} catch (Exception e) {
						LOG.error("Error deleting RangerTag with id=" + xxTag.getId(), e);
						throw e;
					}
				}
			}

			List<XXServiceResourceElementValue> xxServiceResourceElementValues = daoManager.getXXServiceResourceElementValue().findByServiceId(serviceId);

			if (CollectionUtils.isNotEmpty(xxServiceResourceElementValues)) {
				for (XXServiceResourceElementValue xxServiceResourceElementValue : xxServiceResourceElementValues) {
					try {
						daoManager.getXXServiceResourceElementValue().remove(xxServiceResourceElementValue);
					} catch (Exception e) {
						LOG.error("Error deleting ServiceResourceElementValue with id=" + xxServiceResourceElementValue.getId(), e);
						throw e;
					}
				}
			}

			List<XXServiceResourceElement> xxServiceResourceElements = daoManager.getXXServiceResourceElement().findByServiceId(serviceId);

			if (CollectionUtils.isNotEmpty(xxServiceResourceElements)) {
				for (XXServiceResourceElement xxServiceResourceElement : xxServiceResourceElements) {
					try {
						daoManager.getXXServiceResourceElement().remove(xxServiceResourceElement);
					} catch (Exception e) {
						LOG.error("Error deleting ServiceResourceElement with id=" + xxServiceResourceElement.getId(), e);
						throw e;
					}
				}
			}

			List<XXServiceResource> xxServiceResources = daoManager.getXXServiceResource().findByServiceId(serviceId);

			if (CollectionUtils.isNotEmpty(xxServiceResources)) {
				for (XXServiceResource xxServiceResource : xxServiceResources) {
					try {
						daoManager.getXXServiceResource().remove(xxServiceResource);
					} catch (Exception e) {
						LOG.error("Error deleting RangerServiceResource with id=" + xxServiceResource.getId(), e);
						throw e;
					}
				}
			}

		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.deleteAllTagObjectsForService(" + serviceName + ")");
		}
	}
}
