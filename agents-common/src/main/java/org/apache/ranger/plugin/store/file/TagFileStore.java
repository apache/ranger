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

package org.apache.ranger.plugin.store.file;

import java.util.*;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.model.*;
import org.apache.ranger.plugin.store.AbstractTagStore;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.store.RangerServiceResourceSignature;
import org.apache.ranger.plugin.store.TagPredicateUtil;
import org.apache.ranger.plugin.store.TagStore;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServiceTags;


public class TagFileStore extends AbstractTagStore {
	private static final Log LOG = LogFactory.getLog(TagFileStore.class);

	public static final String PROPERTY_TAG_FILE_STORE_DIR = "ranger.tag.store.file.dir";


	protected static final String FILE_PREFIX_TAG_DEF          = "ranger-tagdef-";
	protected static final String FILE_PREFIX_TAG              = "ranger-tag-";
	protected static final String FILE_PREFIX_RESOURCE         = "ranger-serviceresource-";
	protected static final String FILE_PREFIX_TAG_RESOURCE_MAP = "ranger-tagresourcemap-";

	private String tagDataDir            = null;
	private long   nextTagDefId          = 0;
	private long   nextTagId             = 0;
	private long   nextServiceResourceId = 0;
	private long   nextTagResourceMapId  = 0;

	private TagPredicateUtil predicateUtil = null;
	private FileStoreUtil    fileStoreUtil = null;

	private volatile static TagFileStore instance = null;

	public static TagStore getInstance() {
		if (instance == null) {
			synchronized (TagFileStore.class) {
				if (instance == null) {
					instance = new TagFileStore();
					instance.initStore();
				}
			}
		}
		return instance;
	}

	TagFileStore() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.TagFileStore()");
		}

		tagDataDir = RangerConfiguration.getInstance().get(PROPERTY_TAG_FILE_STORE_DIR, "file:///etc/ranger/data");
		fileStoreUtil = new FileStoreUtil();
		predicateUtil = new TagPredicateUtil();

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.TagFileStore()");
		}
	}

	@Override
	public void init() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.init()");
		}

		super.init();
		fileStoreUtil.initStore(tagDataDir);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.init()");
		}
	}

	protected void initStore() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.initStore()");
		}

		fileStoreUtil.initStore(tagDataDir);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.initStore()");
		}
	}

	@Override
	public RangerTagDef createTagDef(RangerTagDef tagDef) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.createTagDef(" + tagDef + ")");
		}

		RangerTagDef ret = null;

		try {
			preCreate(tagDef);

			tagDef.setId(nextTagDefId);

			ret = fileStoreUtil.saveToFile(tagDef, new Path(fileStoreUtil.getDataFile(FILE_PREFIX_TAG_DEF, nextTagDefId++)), false);

			postCreate(ret);
		} catch (Exception excp) {
			LOG.warn("TagFileStore.createTagDef(): failed to save tag-def '" + tagDef.getName() + "'", excp);

			throw new Exception("failed to save tag-def '" + tagDef.getName() + "'", excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.createTagDef(" + tagDef + ")");
		}

		return ret;
	}

	@Override
	public RangerTagDef updateTagDef(RangerTagDef tagDef) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.updateTagDef(" + tagDef + ")");
		}

		RangerTagDef existing = null;

		if(tagDef.getId() == null) {
			existing = getTagDefByName(tagDef.getName());

			if (existing == null) {
				throw new Exception("tag-def does not exist: name=" + tagDef.getName());
			}
		} else {
			existing = getTagDef(tagDef.getId());

			if (existing == null) {
				throw new Exception("tag-def does not exist: id=" + tagDef.getId());
			}
		}

		RangerTagDef ret = null;

		try {
			preUpdate(existing);

			existing.setSource(tagDef.getSource());
			existing.setAttributeDefs(tagDef.getAttributeDefs());

			ret = fileStoreUtil.saveToFile(existing, new Path(fileStoreUtil.getDataFile(FILE_PREFIX_TAG_DEF, existing.getId())), true);

			postUpdate(existing);
		} catch (Exception excp) {
			LOG.warn("TagFileStore.updateTagDef(): failed to save tag-def '" + tagDef.getName() + "'", excp);

			throw new Exception("failed to save tag-def '" + tagDef.getName() + "'", excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.updateTagDef(" + tagDef + ")");
		}

		return ret;
	}

	@Override
	public void deleteTagDefByName(String name) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.deleteTagDef(" + name + ")");
		}

		RangerTagDef existing = getTagDefByName(name);

		if (existing != null) {
			try {
				deleteTagDef(existing);
			} catch (Exception excp) {
				throw new Exception("failed to delete tag-def with ID=" + name, excp);
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.deleteTagDef(" + name + ")");
		}
	}

	@Override
	public void deleteTagDef(Long id) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.deleteTagDef(" + id + ")");
		}

		RangerTagDef existing = getTagDef(id);

		if(existing != null) {
			deleteTagDef(existing);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.deleteTagDef(" + id + ")");
		}
	}
	
	@Override
	public RangerTagDef getTagDef(Long id) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getTagDef(" + id + ")");
		}

		RangerTagDef ret = null;

		if (id != null) {
			SearchFilter filter = new SearchFilter(SearchFilter.TAG_DEF_ID, id.toString());

			List<RangerTagDef> tagDefs = getTagDefs(filter);

			ret = CollectionUtils.isEmpty(tagDefs) ? null : tagDefs.get(0);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getTagDef(" + id + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerTagDef getTagDefByGuid(String guid) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getTagDefByGuid(" + guid + ")");
		}

		RangerTagDef ret = null;

		if (StringUtils.isNotBlank(guid)) {
			SearchFilter filter = new SearchFilter(SearchFilter.TAG_DEF_GUID, guid);

			List<RangerTagDef> tagDefs = getTagDefs(filter);

			if(CollectionUtils.isNotEmpty(tagDefs)) {
				ret = tagDefs.get(0);
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getTagDefByGuid(" + guid + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerTagDef getTagDefByName(String name) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getTagDefByName(" + name + ")");
		}

		RangerTagDef ret = null;

		if (StringUtils.isNotBlank(name)) {
			SearchFilter filter = new SearchFilter(SearchFilter.TAG_TYPE, name);

			List<RangerTagDef> tagDefs = getTagDefs(filter);

			ret = CollectionUtils.isEmpty(tagDefs) ? null : tagDefs.get(0);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getTagDefByName(" + name + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerTagDef> getTagDefs(SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getTagDefs()");
		}

		List<RangerTagDef> ret = getAllTagDefs();

		if (CollectionUtils.isNotEmpty(ret) && filter != null && !filter.isEmpty()) {
			CollectionUtils.filter(ret, predicateUtil.getPredicate(filter));
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getTagDefs(): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public PList<RangerTagDef> getPaginatedTagDefs(SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getPaginatedTagDefs()");
		}

		PList<RangerTagDef> ret = null;

		List<RangerTagDef> list = getTagDefs(filter);

		if(list != null) {
			ret = new PList<RangerTagDef>(list, 0, list.size(), list.size(), list.size(), filter.getSortType(), filter.getSortBy());
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getPaginatedTagDefs(): count=" + (ret == null ? 0 : ret.getPageSize()));
		}

		return ret;
	}

	@Override
	public List<String> getTagTypes() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getTagTypes()");
		}

		List<String> ret = new ArrayList<String>();

		List<RangerTag> allTags = getAllTags();

		for (RangerTag tag : allTags) {
			ret.add(tag.getType());
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getTagTypes(): count=" + ret.size());
		}

		return ret;
	}

	@Override
	public RangerTag createTag(RangerTag tag) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.createTag(" + tag + ")");
		}

		RangerTag ret = null;

		try {
			preCreate(tag);

			tag.setId(nextTagId);

			ret = fileStoreUtil.saveToFile(tag, new Path(fileStoreUtil.getDataFile(FILE_PREFIX_TAG, nextTagId++)), false);

			postCreate(ret);
		} catch (Exception excp) {
			LOG.warn("TagFileStore.createTag(): failed to save tag '" + tag.getType() + "'", excp);

			throw new Exception("failed to save tag '" + tag.getType() + "'", excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.createTag(" + tag + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerTag updateTag(RangerTag tag) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.updateTag(" + tag + ")");
		}

		RangerTag ret = null;

		try {
			preUpdate(tag);

			ret = fileStoreUtil.saveToFile(tag, new Path(fileStoreUtil.getDataFile(FILE_PREFIX_TAG, tag.getId())), true);

			postUpdate(tag);
		} catch (Exception excp) {
			LOG.warn("TagFileStore.updateTag(): failed to save tag '" + tag.getType() + "'", excp);

			throw new Exception("failed to save tag '" + tag.getType() + "'", excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.updateTag(" + tag + "): " + ret);
		}

		return ret;
	}

	@Override
	public void deleteTag(Long id) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.deleteTag(" + id + ")");
		}

		try {
			RangerTag tag = getTag(id);

			if (tag != null) {
				deleteTag(tag);
			}
		} catch (Exception excp) {
			throw new Exception("failed to delete tag with ID=" + id, excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.deleteTag(" + id + ")");
		}
	}

	@Override
	public RangerTag getTag(Long id) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getTag(" + id + ")");
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
			LOG.debug("<== TagFileStore.getTagDef(" + id + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerTag getTagByGuid(String guid) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getTagByGuid(" + guid + ")");
		}

		RangerTag ret = null;

		if (guid != null) {
			SearchFilter filter = new SearchFilter(SearchFilter.TAG_GUID, guid);

			List<RangerTag> tags = getTags(filter);

			if (CollectionUtils.isNotEmpty(tags) && CollectionUtils.size(tags) == 1) {
				ret = tags.get(0);
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getTagByGuid(" + guid + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerTag> getTagsByType(String type) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getTagsByType(" + type + ")");
		}

		SearchFilter filter = new SearchFilter(SearchFilter.TAG_TYPE, type);

		List<RangerTag> ret = getTags(filter);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getTagsByType(" + type + "): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public List<Long> getTagIdsForResourceId(Long resourceId) throws Exception {
		List<Long> ret = new ArrayList<Long>();

		List<RangerTag> tags = getTagsForResourceId(resourceId);

		if(CollectionUtils.isNotEmpty(tags)) {
			for(RangerTag tag : tags) {
				ret.add(tag.getId());
			}
		}

		return ret;
	}

	@Override
	public List<RangerTag> getTagsForResourceId(Long resourceId) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getTagsForResourceId(" + resourceId + ")");
		}

		SearchFilter filter = new SearchFilter(SearchFilter.TAG_RESOURCE_ID, resourceId.toString());

		List<RangerTag> ret = getTags(filter);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getTagsForResourceId(" + resourceId + "): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public List<RangerTag> getTagsForResourceGuid(String resourceGuid) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getTagsForResourceGuid(" + resourceGuid + ")");
		}

		SearchFilter filter = new SearchFilter(SearchFilter.TAG_RESOURCE_GUID, resourceGuid);

		List<RangerTag> ret = getTags(filter);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getTagsForResourceGuid(" + resourceGuid + "): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public List<RangerTag> getTags(SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getTags()");
		}

		List<RangerTag> ret = getAllTags();

		if (CollectionUtils.isNotEmpty(ret) && filter != null && !filter.isEmpty()) {
			CollectionUtils.filter(ret, predicateUtil.getPredicate(filter));
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getTags(): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public PList<RangerTag> getPaginatedTags(SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getPaginatedTags()");
		}

		PList<RangerTag> ret = null;

		List<RangerTag> list = getTags(filter);

		if(list != null) {
			ret = new PList<RangerTag>(list, 0, list.size(), list.size(), list.size(), filter.getSortType(), filter.getSortBy());
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getPaginatedTags(): count=" + (ret == null ? 0 : ret.getPageSize()));
		}

		return ret;
	}


	@Override
	public RangerServiceResource createServiceResource(RangerServiceResource resource) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.createServiceResource(" + resource + ")");
		}

		RangerServiceResource ret = null;

		try {
			preCreate(resource);

			if (StringUtils.isEmpty(resource.getResourceSignature())) {
				RangerServiceResourceSignature serializer = new RangerServiceResourceSignature(resource);

				resource.setResourceSignature(serializer.getSignature());
			}

			resource.setId(nextServiceResourceId);

			ret = fileStoreUtil.saveToFile(resource, new Path(fileStoreUtil.getDataFile(FILE_PREFIX_RESOURCE, nextServiceResourceId++)), false);

			postCreate(ret);
		} catch (Exception excp) {
			LOG.warn("TagFileStore.createServiceResource(): failed to save resource '" + resource.getId() + "'", excp);

			throw new Exception("failed to save service-resource '" + resource.getId() + "'", excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.createServiceResource(" + resource + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerServiceResource updateServiceResource(RangerServiceResource resource) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.updateServiceResource(" + resource + ")");
		}

		RangerServiceResource ret = null;

		try {
			preUpdate(resource);

			if (StringUtils.isEmpty(resource.getResourceSignature())) {
				RangerServiceResourceSignature serializer = new RangerServiceResourceSignature(resource);

				resource.setResourceSignature(serializer.getSignature());
			}

			ret = fileStoreUtil.saveToFile(resource, new Path(fileStoreUtil.getDataFile(FILE_PREFIX_RESOURCE, resource.getId())), true);

			postUpdate(resource);
		} catch (Exception excp) {
			LOG.warn("TagFileStore.updateServiceResource(): failed to save resource '" + resource.getId() + "'", excp);

			throw new Exception("failed to save service-resource '" + resource.getId() + "'", excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.updateServiceResource(" + resource + "): " + ret);
		}

		return ret;

	}

	@Override
	public void deleteServiceResource(Long id) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.deleteServiceResource(" + id + ")");
		}

		try {
			RangerServiceResource resource = getServiceResource(id);

			if (resource != null) {
				deleteServiceResource(resource);
			}
		} catch (Exception excp) {
			throw new Exception("failed to delete service-resource with ID=" + id, excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.deleteServiceResource(" + id + ")");
		}
	}

	@Override
	public void deleteServiceResourceByGuid(String guid) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.deleteServiceResourceByGuid(" + guid + ")");
		}

		try {
			RangerServiceResource resource = getServiceResourceByGuid(guid);

			if (resource != null) {
				deleteServiceResource(resource);
			}
		} catch (Exception excp) {
			throw new Exception("failed to delete service-resource with GUID=" + guid, excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.deleteServiceResourceByGuid(" + guid + ")");
		}

	}

	@Override
	public RangerServiceResource getServiceResource(Long id) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getServiceResource(" + id + ")");
		}

		RangerServiceResource ret = null;

		SearchFilter filter = new SearchFilter(SearchFilter.TAG_RESOURCE_ID, id.toString());

		List<RangerServiceResource> resources = getServiceResources(filter);

		if (CollectionUtils.isNotEmpty(resources)) {
			ret = resources.get(0);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getServiceResource(" + id + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerServiceResource getServiceResourceByGuid(String guid) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getServiceResourcesByGuid(" + guid + ")");
		}

		RangerServiceResource ret = null;

		SearchFilter filter = new SearchFilter(SearchFilter.TAG_RESOURCE_GUID, guid);

		List<RangerServiceResource> resources = getServiceResources(filter);

		if (CollectionUtils.isNotEmpty(resources)) {
			ret = resources.get(0);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getServiceResourcesByGuid(" + guid + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerServiceResource> getServiceResourcesByService(String serviceName) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getServiceResourcesByService(" + serviceName + ")");
		}

		List<RangerServiceResource> ret = null;

		if (StringUtils.isNotBlank(serviceName)) {
			SearchFilter filter = new SearchFilter(SearchFilter.SERVICE_NAME, serviceName);

			ret = getServiceResources(filter);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getServiceResourcesByService(" + serviceName + "): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public List<String> getServiceResourceGuidsByService(String serviceName) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getServiceResourceGuidsByService(" + serviceName + ")");
		}

		List<String> ret = null;

		if (StringUtils.isNotBlank(serviceName)) {
			List<RangerServiceResource> serviceResources = this.getServiceResourcesByService(serviceName);

			if(CollectionUtils.isNotEmpty(serviceResources)) {
				ret = new ArrayList<String>(serviceResources.size());

				for(RangerServiceResource serviceResource : serviceResources) {
					ret.add(serviceResource.getGuid());
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getServiceResourceGuidsByService(" + serviceName + "): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public RangerServiceResource getServiceResourceByServiceAndResourceSignature(String serviceName, String resourceSignature) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getServiceResourceByServiceAndResourceSignature(" + serviceName + ", " + resourceSignature + ")");
		}

		RangerServiceResource ret = null;

		if (StringUtils.isNotBlank(resourceSignature)) {
			SearchFilter filter = new SearchFilter();
			filter.setParam(SearchFilter.TAG_RESOURCE_SERVICE_NAME, serviceName);
			filter.setParam(SearchFilter.TAG_RESOURCE_SIGNATURE, resourceSignature);

			List<RangerServiceResource> resources = getServiceResources(filter);

			ret = CollectionUtils.isNotEmpty(resources) ? resources.get(0) : null;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getServiceResourceByServiceAndResourceSignature(" + serviceName + ", " + resourceSignature + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerServiceResource> getServiceResources(SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getServiceResources()");
		}

		List<RangerServiceResource> ret = getAllResources();

		if (CollectionUtils.isNotEmpty(ret) && filter != null && !filter.isEmpty()) {
			CollectionUtils.filter(ret, predicateUtil.getPredicate(filter));
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getServicesResources(): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public PList<RangerServiceResource> getPaginatedServiceResources(SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getPaginatedServiceResources()");
		}

		PList<RangerServiceResource> ret = null;

		List<RangerServiceResource> list = getServiceResources(filter);

		if(list != null) {
			ret = new PList<RangerServiceResource>(list, 0, list.size(), list.size(), list.size(), filter.getSortType(), filter.getSortBy());
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getPaginatedServiceResources(): count=" + (ret == null ? 0 : ret.getPageSize()));
		}

		return ret;
	}


	@Override
	public RangerTagResourceMap createTagResourceMap(RangerTagResourceMap tagResourceMap) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.createTagResourceMap(" + tagResourceMap + ")");
		}

		preCreate(tagResourceMap);

		tagResourceMap.setId(nextTagResourceMapId);

		RangerTagResourceMap ret = fileStoreUtil.saveToFile(tagResourceMap, new Path(fileStoreUtil.getDataFile(FILE_PREFIX_TAG_RESOURCE_MAP, nextTagResourceMapId++)), false);

		postCreate(ret);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.createTagResourceMap(" + tagResourceMap + "): " + ret);
		}

		return ret;
	}

	@Override
	public void deleteTagResourceMap(Long id) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.deleteTagResourceMap(" + id + ")");
		}

		try {
			RangerTagResourceMap tagResourceMap = getTagResourceMap(id);
			if (tagResourceMap != null) {
				Long tagId = tagResourceMap.getTagId();
				RangerTag tag = getTag(tagId);

				deleteTagResourceMap(tagResourceMap);
				if (tag != null && tag.getOwner() == RangerTag.OWNER_SERVICERESOURCE) {
					deleteTag(tagId);
				}
			}
		} catch (Exception excp) {
			throw new Exception("failed to delete tagResourceMap with ID=" + id, excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.deleteTagResourceMap(" + id + ")");
		}
	}

	@Override
	public RangerTagResourceMap getTagResourceMap(Long id) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getTagResourceMap(" + id + ")");
		}

		RangerTagResourceMap ret = null;

		SearchFilter filter = new SearchFilter();

		filter.setParam(SearchFilter.TAG_MAP_ID, id.toString());

		List<RangerTagResourceMap> list = getTagResourceMaps(filter);

		if (CollectionUtils.isNotEmpty(list))  {
			ret = list.get(0);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getTagResourceMap(" + id + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerTagResourceMap getTagResourceMapByGuid(String guid) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getTagResourceMapByGuid(" + guid + ")");
		}

		RangerTagResourceMap ret = null;

		SearchFilter filter = new SearchFilter();

		filter.setParam(SearchFilter.TAG_MAP_GUID, guid.toString());

		List<RangerTagResourceMap> list = getTagResourceMaps(filter);

		if (CollectionUtils.isNotEmpty(list))  {
			ret = list.get(0);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getTagResourceMapByGuid(" + guid + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerTagResourceMap> getTagResourceMapsForTagId(Long tagId) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getTagResourceMapsForTagId(" + tagId + ")");
		}

		SearchFilter filter = new SearchFilter();

		filter.setParam(SearchFilter.TAG_ID, tagId.toString());

		List<RangerTagResourceMap> ret = getTagResourceMaps(filter);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getTagResourceMapsForTagId(" + tagId + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerTagResourceMap> getTagResourceMapsForTagGuid(String tagGuid) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getTagResourceMapsForTagGuid(" + tagGuid + ")");
		}

		List<RangerTagResourceMap> ret = null;

		RangerTag tag = getTagByGuid(tagGuid);

		if(tag != null) {
			SearchFilter filter = new SearchFilter();
			filter.setParam(SearchFilter.TAG_ID, tag.getId().toString());

			ret = getTagResourceMaps(filter);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getTagResourceMapsForTagGuid(" + tagGuid + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerTagResourceMap> getTagResourceMapsForResourceId(Long resourceId) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getTagResourceMap(" + resourceId + ")");
		}

		SearchFilter filter = new SearchFilter();

		filter.setParam(SearchFilter.TAG_RESOURCE_ID, resourceId.toString());

		List<RangerTagResourceMap> ret = getTagResourceMaps(filter);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getTagResourceMap(" + resourceId + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerTagResourceMap> getTagResourceMapsForResourceGuid(String resourceGuid) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getTagResourceMapsForResourceGuid(" + resourceGuid + ")");
		}

		List<RangerTagResourceMap> ret = null;

		RangerServiceResource resource = getServiceResourceByGuid(resourceGuid);

		if (resource != null) {
			SearchFilter filter = new SearchFilter();
			filter.setParam(SearchFilter.TAG_RESOURCE_ID, resource.getId().toString());
			ret = getTagResourceMaps(filter);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getTagResourceMapsForResourceGuid(" + resourceGuid + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerTagResourceMap getTagResourceMapForTagAndResourceId(Long tagId, Long resourceId) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getTagResourceMapForTagAndResourceId(" + tagId + ", " + resourceId + ")");
		}

		RangerTagResourceMap ret = null;

		SearchFilter filter = new SearchFilter();

		filter.setParam(SearchFilter.TAG_ID, tagId.toString());
		filter.setParam(SearchFilter.TAG_RESOURCE_ID, resourceId.toString());

		List<RangerTagResourceMap> tagResourceMaps = getTagResourceMaps(filter);

		if(CollectionUtils.isNotEmpty(tagResourceMaps)) {
			ret = tagResourceMaps.get(0);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getTagResourceMapForTagAndResourceId(" + tagId + ", " + resourceId + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerTagResourceMap getTagResourceMapForTagAndResourceGuid(String tagGuid, String resrouceGuid) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getTagResourceMapForTagAndResourceGuid(" + tagGuid + ", " + resrouceGuid + ")");
		}

		RangerTagResourceMap ret = null;

		SearchFilter filter = new SearchFilter();
		
		RangerTag             tag      = getTagByGuid(tagGuid);
		RangerServiceResource resource = getServiceResourceByGuid(resrouceGuid);
		
		if(tag != null && resource != null) {
			filter.setParam(SearchFilter.TAG_ID, tag.getId().toString());
			filter.setParam(SearchFilter.TAG_RESOURCE_ID, resource.getId().toString());

			List<RangerTagResourceMap> tagResourceMaps = getTagResourceMaps(filter);

			if(CollectionUtils.isNotEmpty(tagResourceMaps)) {
				ret = tagResourceMaps.get(0);
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getTagResourceMapForTagAndResourceGuid(" + tagGuid + ", " + resrouceGuid + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerTagResourceMap> getTagResourceMaps(SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getTagResourceMaps()");
		}

		List<RangerTagResourceMap> ret = getAllTaggedResources();

		if (CollectionUtils.isNotEmpty(ret) && filter != null && !filter.isEmpty()) {
			CollectionUtils.filter(ret, predicateUtil.getPredicate(filter));
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getTagResourceMaps(): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;

	}

	@Override
	public PList<RangerTagResourceMap> getPaginatedTagResourceMaps(SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getPaginatedTagResourceMaps()");
		}

		PList<RangerTagResourceMap> ret = null;

		List<RangerTagResourceMap> list = getTagResourceMaps(filter);
		
		if(CollectionUtils.isNotEmpty(list)) {
			ret = new PList<RangerTagResourceMap>(list, 0, list.size(), list.size(), list.size(), filter.getSortType(), filter.getSortBy());
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getPaginatedTagResourceMaps(): count=" + (ret == null ? 0 : ret.getPageSize()));
		}

		return ret;
	}


	@Override
	public ServiceTags getServiceTagsIfUpdated(String serviceName, Long lastKnownVersion) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getServiceTagsIfUpdated(" + serviceName + ", " + lastKnownVersion + ")");
		}

		ServiceTags ret = null;

		boolean tagsChanged = true;

		RangerService service = null;

		try {
			service = svcStore.getServiceByName(serviceName);
		} catch (Exception exception) {
			LOG.error("Cannot find service for serviceName=" + serviceName);
			tagsChanged = false;
		}

		if (lastKnownVersion != null
				&& service != null && service.getTagVersion() != null
				&& lastKnownVersion.equals(service.getTagVersion())) {
			tagsChanged = false;
		}

		if (tagsChanged) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Changes to tagVersion detected, tagVersion in service=" + (service == null ? null : service.getTagVersion())
						+ ", Plugin-provided lastKnownVersion=" + lastKnownVersion);
			}
			ret = getServiceTags(serviceName);
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug("No changes to tagVersion detected, tagVersion in service=" + (service == null ? null : service.getTagVersion())
						+ ", Plugin-provided lastKnownVersion=" + lastKnownVersion);
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getServiceTagsIfUpdated(" + serviceName + ", " + lastKnownVersion + "): " + ret);
		}

		return ret;
	}

	@Override
	public ServiceTags getServiceTags(String serviceName) throws Exception {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getServiceTags(" + serviceName  + ")");
		}

		ServiceTags ret = new ServiceTags();

		RangerService service = null;

		try {
			service = svcStore.getServiceByName(serviceName);
		} catch (Exception exception) {
			LOG.error("Cannot find service for serviceName=" + serviceName);
		}

		SearchFilter filter = new SearchFilter();

		filter.setParam(SearchFilter.TAG_RESOURCE_SERVICE_NAME, serviceName);

		List<RangerServiceResource> serviceResources = getServiceResources(filter);
		List<RangerServiceResource> filteredServiceResources = new ArrayList<RangerServiceResource>();

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
				filteredServiceResources.add(serviceResource);
			}
		}

		ret.setServiceName(serviceName);
		ret.setServiceResources(filteredServiceResources);
		ret.setResourceToTagIds(resourceToTagIdsMap);
		ret.setTags(tagsMap);

		if (service != null && service.getTagVersion() != null) {
			ret.setTagVersion(service.getTagVersion());
		}
		if (service != null && service.getTagUpdateTime() != null) {
			ret.setTagUpdateTime(service.getTagUpdateTime());
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getServiceTags(" + serviceName  + "): " + ret);
		}

		return ret;

	}

	@Override
	public Long getTagVersion(String serviceName) {

		RangerService service = null;

		try {
			service = svcStore.getServiceByName(serviceName);
		} catch (Exception exception) {
			LOG.error("Cannot find service for serviceName=" + serviceName);
		}

		return service != null ? service.getTagVersion() : null;
	}

	private List<RangerTag> getTagsForServiceResourceObject(RangerServiceResource serviceResource) throws Exception {

		List<RangerTag> tagList = new ArrayList<RangerTag>();

		if (serviceResource != null) {
			SearchFilter mapFilter = new SearchFilter();
			mapFilter.setParam(SearchFilter.TAG_RESOURCE_ID, serviceResource.getId().toString());

			List<RangerTagResourceMap> associations = getTagResourceMaps(mapFilter);
			if (CollectionUtils.isNotEmpty(associations)) {

				for (RangerTagResourceMap association : associations) {
					RangerTag tag = getTag(association.getTagId());
					if (tag != null) {
						tagList.add(tag);
					}
				}
			}
		}
		return tagList;
	}

	private List<RangerTagDef> getAllTagDefs() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getAllTagDefs()");
		}

		List<RangerTagDef> ret = new ArrayList<RangerTagDef>();

		try {
			// load Tag definitions from file system
			List<RangerTagDef> sds = fileStoreUtil.loadFromDir(new Path(fileStoreUtil.getDataDir()), FILE_PREFIX_TAG_DEF, RangerTagDef.class);

			if (CollectionUtils.isNotEmpty(sds)) {
				for (RangerTagDef sd : sds) {
					if (sd != null) {
						// if the TagDef is already found, remove the earlier definition
						for (int i = 0; i < ret.size(); i++) {
							RangerTagDef currSd = ret.get(i);

							if (StringUtils.equals(currSd.getName(), sd.getName()) ||
									ObjectUtils.equals(currSd.getId(), sd.getId())) {
								ret.remove(i);
							}
						}

						ret.add(sd);
					}
				}
			}
			nextTagDefId = getMaxId(ret) + 1;
		} catch (Exception excp) {
			LOG.error("TagFileStore.getAllTagDefs(): failed to read Tag-defs", excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getAllTagDefs(): count=" + ret.size());
		}

		return ret;
	}

	private List<RangerTag> getAllTags() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getAllTags()");
		}

		List<RangerTag> ret = new ArrayList<RangerTag>();

		try {
			List<RangerTag> sds = fileStoreUtil.loadFromDir(new Path(fileStoreUtil.getDataDir()), FILE_PREFIX_TAG, RangerTag.class);

			if (CollectionUtils.isNotEmpty(sds)) {
				for (RangerTag sd : sds) {
					if (sd != null) {
						// if the Tag is already found, remove the earlier one
						for (int i = 0; i < ret.size(); i++) {
							RangerTag currSd = ret.get(i);

							if (StringUtils.equals(currSd.getType(), sd.getType()) ||
									ObjectUtils.equals(currSd.getId(), sd.getId())) {
								ret.remove(i);
							}
						}

						ret.add(sd);
					}
				}
			}
			nextTagId = getMaxId(ret) + 1;
		} catch (Exception excp) {
			LOG.error("TagFileStore.getAllTags(): failed to read Tags", excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getAllTags(): count=" + ret.size());
		}

		return ret;
	}

	private List<RangerServiceResource> getAllResources() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getAllResources()");
		}

		List<RangerServiceResource> ret = new ArrayList<RangerServiceResource>();

		try {
			List<RangerServiceResource> sds = fileStoreUtil.loadFromDir(new Path(fileStoreUtil.getDataDir()), FILE_PREFIX_RESOURCE, RangerServiceResource.class);

			if (CollectionUtils.isNotEmpty(sds)) {
				for (RangerServiceResource sd : sds) {
					if (sd != null) {
						// if the resource is already found, remove the earlier one
						for (int i = 0; i < ret.size(); i++) {
							RangerServiceResource currSd = ret.get(i);

							if (ObjectUtils.equals(currSd.getId(), sd.getId())) {
								ret.remove(i);
							}
						}

						ret.add(sd);
					}
				}
			}
			nextServiceResourceId = getMaxId(ret) + 1;
		} catch (Exception excp) {
			LOG.error("TagFileStore.getAllResources(): failed to read Resources", excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getAllResourcess(): count=" + ret.size());
		}

		return ret;
	}

	private List<RangerTagResourceMap> getAllTaggedResources() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagFileStore.getAllTaggedResources()");
		}

		List<RangerTagResourceMap> ret = new ArrayList<RangerTagResourceMap>();

		try {
			// load resource definitions from file system
			List<RangerTagResourceMap> resources = fileStoreUtil.loadFromDir(new Path(fileStoreUtil.getDataDir()), FILE_PREFIX_TAG_RESOURCE_MAP, RangerTagResourceMap.class);

			if (CollectionUtils.isNotEmpty(resources)) {
				for (RangerTagResourceMap resource : resources) {
					if (resource != null) {
						// if the RangerTagResourceMap is already found, remove the earlier definition
						for (int i = 0; i < ret.size(); i++) {
							RangerTagResourceMap currResource = ret.get(i);

							if (ObjectUtils.equals(currResource.getId(), resource.getId())) {
								ret.remove(i);
							}
						}

						ret.add(resource);
					}
				}
			}
			nextTagResourceMapId = getMaxId(ret) + 1;
		} catch (Exception excp) {
			LOG.error("TagFileStore.getAllTaggedResources(): failed to read tagged resources", excp);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagFileStore.getAllTaggedResources(): count=" + ret.size());
		}

		return ret;
	}

	private void deleteTagDef(RangerTagDef tagDef) throws Exception {
		Path filePath = new Path(fileStoreUtil.getDataFile(FILE_PREFIX_TAG_DEF, tagDef.getId()));

		preDelete(tagDef);

		fileStoreUtil.deleteFile(filePath);

		postDelete(tagDef);
	}

	private void deleteTag(RangerTag tag) throws Exception {
		Path filePath = new Path(fileStoreUtil.getDataFile(FILE_PREFIX_TAG, tag.getId()));

		preDelete(tag);

		fileStoreUtil.deleteFile(filePath);

		postDelete(tag);
	}

	private void deleteServiceResource(RangerServiceResource resource) throws Exception {
		Path filePath = new Path(fileStoreUtil.getDataFile(FILE_PREFIX_RESOURCE, resource.getId()));

		preDelete(resource);

		fileStoreUtil.deleteFile(filePath);

		postDelete(resource);
	}

	private void deleteTagResourceMap(RangerTagResourceMap tagResourceMap) throws Exception {
		Path filePath = new Path(fileStoreUtil.getDataFile(FILE_PREFIX_TAG_RESOURCE_MAP, tagResourceMap.getId()));

		preDelete(tagResourceMap);

		fileStoreUtil.deleteFile(filePath);

		postDelete(tagResourceMap);
	}
}

