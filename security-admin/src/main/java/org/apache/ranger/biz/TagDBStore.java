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
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.common.GUIDUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXDBBase;
import org.apache.ranger.entity.XXResourceDef;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.entity.XXTag;
import org.apache.ranger.entity.XXTagAttribute;
import org.apache.ranger.entity.XXTagAttributeDef;
import org.apache.ranger.entity.XXTagResourceMap;
import org.apache.ranger.entity.XXTaggedResource;
import org.apache.ranger.entity.XXTaggedResourceValue;
import org.apache.ranger.entity.XXTaggedResourceValueMap;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerTagDef;
import org.apache.ranger.plugin.model.RangerTaggedResource;
import org.apache.ranger.plugin.model.RangerTagDef.RangerTagAttributeDef;
import org.apache.ranger.plugin.model.RangerTaggedResource.RangerResourceTag;
import org.apache.ranger.plugin.model.RangerTaggedResourceKey;
import org.apache.ranger.plugin.policyresourcematcher.RangerDefaultPolicyResourceMatcher;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.store.TagStore;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.TagServiceResources;
import org.apache.ranger.service.RangerAuditFields;
import org.apache.ranger.service.RangerTagDefService;
import org.apache.ranger.service.RangerTaggedResourceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TagDBStore implements TagStore {
	private static final Log LOG = LogFactory.getLog(TagDBStore.class);

	@Autowired
	RangerTagDefService rangerTagDefService;

	@Autowired
	RangerTaggedResourceService rangerTaggedResourceService;

	@Autowired
	RangerDaoManager daoManager;

	@Autowired
	RESTErrorUtil errorUtil;

	@Autowired
	RangerAuditFields<XXDBBase> rangerAuditFields;

	@Autowired
	GUIDUtil guidUtil;

	@Autowired
	ServiceDBStore serviceDBStore;

	@Override
	public void init() throws Exception {

	}

	@Override
	public void setServiceStore(ServiceStore svcStore) {

	}

	@Override
	public RangerTagDef createTagDef(RangerTagDef tagDef) throws Exception {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.createTagDef(" + tagDef + ")");
		}

		RangerTagDef ret;

		try {
			ret = rangerTagDefService.create(tagDef);

			createTagAttributeDefs(ret.getId(), tagDef.getAttributeDefs());

			ret = rangerTagDefService.read(ret.getId());

		} catch (Exception e) {
			throw errorUtil.createRESTException("failed to save tag-def [" + tagDef.getName() + "]", MessageEnums.ERROR_CREATING_OBJECT);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.createTagDef(" + tagDef + ")");
		}

		return ret;
	}

	@Override
	public RangerTagDef updateTagDef(RangerTagDef tagDef) throws Exception {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.updateTagDef(" + tagDef + ")");
		}

		RangerTagDef existing = rangerTagDefService.read(tagDef.getId());
		RangerTagDef ret = null;
		if (existing == null) {
			throw errorUtil.createRESTException("failed to update tag-def [" + tagDef.getName() + "], Reason: No TagDef found with id: [" + tagDef.getId() + "]",
					MessageEnums.DATA_NOT_UPDATABLE);
		}

		if (StringUtils.isEmpty(tagDef.getCreatedBy())) {
			tagDef.setCreatedBy(existing.getCreatedBy());
		}
		if (tagDef.getCreateTime() == null) {
			tagDef.setCreateTime(existing.getCreateTime());
		}
		if (StringUtils.isEmpty(tagDef.getGuid())) {
			tagDef.setGuid(existing.getGuid());
		}

		ret = rangerTagDefService.update(tagDef);

		deleteTagAttributeDefs(ret.getId());

		createTagAttributeDefs(ret.getId(), tagDef.getAttributeDefs());

		return rangerTagDefService.read(ret.getId());
	}

	private List<XXTagAttributeDef> createTagAttributeDefs(Long tagDefId, List<RangerTagAttributeDef> tagAttrDefList) {

		if (tagDefId == null) {
			throw errorUtil.createRESTException("TagDBStore.createTagAttributeDefs(): Error creating tag-attr def. tagDefId can not be null.", MessageEnums.ERROR_CREATING_OBJECT);
		}

		if (CollectionUtils.isEmpty(tagAttrDefList)) {
			return null;
		}

		List<XXTagAttributeDef> xTagAttrDefList = new ArrayList<XXTagAttributeDef>();
		for (RangerTagDef.RangerTagAttributeDef attrDef : tagAttrDefList) {
			XXTagAttributeDef xAttrDef = new XXTagAttributeDef();

			xAttrDef.setGuid(guidUtil.genGUID());
			xAttrDef.setTagDefId(tagDefId);
			xAttrDef.setName(attrDef.getName());
			xAttrDef.setType(attrDef.getType());
			xAttrDef = (XXTagAttributeDef) rangerAuditFields.populateAuditFieldsForCreate(xAttrDef);

			xAttrDef = daoManager.getXXTagAttributeDef().create(xAttrDef);

			xTagAttrDefList.add(xAttrDef);
		}
		return xTagAttrDefList;
	}

	private void deleteTagAttributeDefs(Long tagDefId) {
		if (tagDefId == null) {
			return;
		}
		List<XXTagAttributeDef> tagAttrDefList = daoManager.getXXTagAttributeDef().findByTagDefId(tagDefId);

		if (CollectionUtils.isEmpty(tagAttrDefList)) {
			return;
		}

		for (XXTagAttributeDef xAttrDef : tagAttrDefList) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Deleting tag-attribute def [" + xAttrDef.getName() + "]");
			}
			daoManager.getXXTagAttributeDef().remove(xAttrDef);
		}
	}

	@Override
	public void deleteTagDef(String name) throws Exception {

		if (StringUtils.isNotBlank(name)) {
			return;
		}

		List<RangerTagDef> ret;

		if (LOG.isDebugEnabled()) {
			LOG.debug("Deleting all tag-defs with name [" + name + "]");
		}

		SearchFilter filter = new SearchFilter(SearchFilter.TAG_DEF_NAME, name);
		ret = getTagDefs(filter);

		for (RangerTagDef tagDef : ret) {
			LOG.info("Deleting tag-def with name [" + name + "]");
			rangerTagDefService.delete(tagDef);
		}
	}

	@Override
	public void deleteTagDefById(Long id) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Deleting tag-def [" + id + "]");
		}

		RangerTagDef tagDef = rangerTagDefService.read(id);

		rangerTagDefService.delete(tagDef);
	}

	@Override
	public List<RangerTagDef> getTagDef(String name) throws Exception {

		List<RangerTagDef> ret;
		if (StringUtils.isNotBlank(name)) {
			SearchFilter filter = new SearchFilter(SearchFilter.TAG_DEF_NAME, name);
			ret = getTagDefs(filter);
		} else {
			ret = null;
		}
		return ret;
	}

	@Override
	public RangerTagDef getTagDefById(Long id) throws Exception {
		return rangerTagDefService.read(id);
	}

	@Override
	public List<RangerTagDef> getTagDefs(SearchFilter filter) throws Exception {
		return getPaginatedTagDefs(filter).getList();
	}

	@Override
	public PList<RangerTagDef> getPaginatedTagDefs(SearchFilter filter) throws Exception {
		return rangerTagDefService.searchRangerTagDefs(filter);
	}

	@Override
	public RangerTaggedResource createTaggedResource(RangerTaggedResource resource, boolean createOrUpdate) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.createTaggedResource(" + resource + ")");
		}

		RangerTaggedResource ret = null;
		RangerTaggedResource existing = null;
		boolean updateResource = false;

		existing = getResource(resource.getKey());

		if (existing != null) {
			if (!createOrUpdate) {
				throw errorUtil.createRESTException("resource(s) with same specification already exists", MessageEnums.ERROR_DUPLICATE_OBJECT);
			} else {
				updateResource = true;
			}
		}

		if (!updateResource) {
			if (resource.getId() != null) {
				existing = getResource(resource.getId());
			}

			if (existing != null) {
				if (!createOrUpdate) {
					throw errorUtil.createRESTException(resource.getId() + ": resource already exists (id=" + existing.getId() + ")", MessageEnums.ERROR_DUPLICATE_OBJECT);
				} else {
					updateResource = true;
				}
			}
		}

		try {
			if (updateResource) {
				ret = updateTaggedResource(resource);
			} else {
				ret = rangerTaggedResourceService.create(resource);

				ret.setKey(resource.getKey());
				ret.setTags(resource.getTags());
				RangerTaggedResourceKey resKey = createResourceSpecForTaggedResource(ret);
				List<RangerResourceTag> tags = createTagsForTaggedResource(ret);

				if (resKey == null || tags == null) {
					throw errorUtil.createRESTException("failed to save resource '" + resource.getId() + "'", MessageEnums.ERROR_CREATING_OBJECT);
				}
			}
		} catch (Exception excp) {
			LOG.warn("TagDBStore.createTaggedResource: failed to save resource '" + resource.getId() + "'", excp);
			throw errorUtil.createRESTException("failed to save resource '" + resource.getId() + "'", MessageEnums.ERROR_CREATING_OBJECT);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.createTaggedResource(" + resource + ")");
		}
		return ret;
	}

	private List<RangerResourceTag> createTagsForTaggedResource(RangerTaggedResource resource) {

		List<RangerResourceTag> tags = resource.getTags();

		if (tags == null) {
			return null;
		}

		for (RangerResourceTag tag : tags) {
			XXTag xTag = new XXTag();

			xTag.setExternalId(tag.getExternalId());
			xTag.setName(tag.getName());
			xTag.setGuid(guidUtil.genGUID());
			xTag = (XXTag) rangerAuditFields.populateAuditFieldsForCreate(xTag);

			xTag = daoManager.getXXTag().create(xTag);

			for (Entry<String, String> attr : tag.getAttributeValues().entrySet()) {
				XXTagAttribute xTagAttr = new XXTagAttribute();

				xTagAttr.setTagId(xTag.getId());
				xTagAttr.setName(attr.getKey());
				xTagAttr.setValue(attr.getValue());
				xTagAttr.setGuid(guidUtil.genGUID());
				xTagAttr = (XXTagAttribute) rangerAuditFields.populateAuditFieldsForCreate(xTagAttr);

				xTagAttr = daoManager.getXXTagAttribute().create(xTagAttr);
			}

			XXTagResourceMap tagResMap = new XXTagResourceMap();
			tagResMap.setTaggedResId(resource.getId());
			tagResMap.setTagId(xTag.getId());
			tagResMap.setGuid(guidUtil.genGUID());
			tagResMap = (XXTagResourceMap) rangerAuditFields.populateAuditFieldsForCreate(tagResMap);

			tagResMap = daoManager.getXXTagResourceMap().create(tagResMap);
		}

		return tags;
	}

	private RangerTaggedResourceKey createResourceSpecForTaggedResource(RangerTaggedResource resource) {

		if (resource.getKey() == null) {
			return null;
		}

		String serviceName = resource.getKey().getServiceName();

		XXService xService = daoManager.getXXService().findByName(serviceName);
		if (xService == null) {
			throw errorUtil.createRESTException("No Service found with name: " + serviceName, MessageEnums.ERROR_CREATING_OBJECT);
		}

		XXServiceDef xServiceDef = daoManager.getXXServiceDef().getById(xService.getType());
		if (xServiceDef == null) {
			throw errorUtil.createRESTException("No Service-Def found with ID: " + xService.getType(), MessageEnums.ERROR_CREATING_OBJECT);
		}

		RangerTaggedResourceKey resKey = resource.getKey();
		Map<String, RangerPolicy.RangerPolicyResource> resourceSpec = resKey.getResourceSpec();

		for (Entry<String, RangerPolicyResource> resSpec : resourceSpec.entrySet()) {
			XXResourceDef xResDef = daoManager.getXXResourceDef().findByNameAndServiceDefId(resSpec.getKey(), xServiceDef.getId());

			if (xResDef == null) {
				LOG.error("TagDBStore.createTaggedResource: ResourceType is not valid [" + resSpec.getKey() + "]");
				throw errorUtil.createRESTException("Resource Type is not valid [" + resSpec.getKey() + "]", MessageEnums.DATA_NOT_FOUND);
			}

			RangerPolicyResource policyRes = resSpec.getValue();

			XXTaggedResourceValue taggedResValue = new XXTaggedResourceValue();
			taggedResValue.setIsExcludes(policyRes.getIsExcludes());
			taggedResValue.setIsRecursive(policyRes.getIsRecursive());
			taggedResValue.setResDefId(xResDef.getId());
			taggedResValue.setTaggedResourceId(resource.getId());
			taggedResValue.setGuid(guidUtil.genGUID());

			taggedResValue = (XXTaggedResourceValue) rangerAuditFields.populateAuditFieldsForCreate(taggedResValue);

			taggedResValue = daoManager.getXXTaggedResourceValue().create(taggedResValue);

			int sortOrder = 1;
			for (String resVal : policyRes.getValues()) {
				XXTaggedResourceValueMap taggedResValueMap = new XXTaggedResourceValueMap();
				taggedResValueMap.setResValueId(taggedResValue.getId());
				taggedResValueMap.setValue(resVal);
				taggedResValueMap.setSortOrder(sortOrder);
				taggedResValueMap.setGuid(guidUtil.genGUID());
				taggedResValueMap = (XXTaggedResourceValueMap) rangerAuditFields.populateAuditFieldsForCreate(taggedResValueMap);

				taggedResValueMap = daoManager.getXXTaggedResourceValueMap().create(taggedResValueMap);
				sortOrder++;
			}
		}
		return resKey;
	}

	@Override
	public RangerTaggedResource updateTaggedResource(RangerTaggedResource resource) throws Exception {

		RangerTaggedResource existing = getResource(resource.getId());
		if (existing == null) {
			throw errorUtil.createRESTException(resource.getId() + ": resource does not exist (id=" + resource.getId() + ")", MessageEnums.DATA_NOT_FOUND);
		}

		RangerTaggedResource ret = null;

		try {
			if (StringUtils.isEmpty(resource.getCreatedBy())) {
				resource.setCreatedBy(existing.getCreatedBy());
			}
			if (resource.getCreateTime() == null) {
				resource.setCreateTime(existing.getCreateTime());
			}
			if (StringUtils.isEmpty(resource.getGuid())) {
				resource.setGuid(existing.getGuid());
			}

			ret = rangerTaggedResourceService.update(resource);
			ret.setTags(resource.getTags());
			ret.setKey(resource.getKey());

			RangerTaggedResourceKey updKey = updateResourceSpecForTaggedResource(ret);
			List<RangerResourceTag> updTags = updateTagsForTaggedResource(ret);

			ret.setKey(updKey);
			ret.setTags(updTags);

		} catch (Exception excp) {
			LOG.warn("TagDBStore.updateTagDef(): failed to save resource '" + resource.getId() + "'", excp);

			throw new Exception("failed to save resource '" + resource.getId() + "'", excp);
		}

		return ret;
	}

	private RangerTaggedResourceKey updateResourceSpecForTaggedResource(RangerTaggedResource updResource) {

		if (updResource == null) {
			return null;
		}

		deleteTaggedResourceValue(updResource.getId());

		return createResourceSpecForTaggedResource(updResource);
	}

	private List<RangerResourceTag> updateTagsForTaggedResource(RangerTaggedResource updResource) {

		if (updResource == null) {
			return null;
		}

		deleteTagsForTaggedResource(updResource.getId());

		return createTagsForTaggedResource(updResource);
	}

	private void deleteTaggedResourceValue(Long resourceId) {
		List<XXTaggedResourceValue> taggedResValueList = daoManager.getXXTaggedResourceValue().findByTaggedResId(resourceId);
		for (XXTaggedResourceValue taggedResValue : taggedResValueList) {
			List<XXTaggedResourceValueMap> taggedResValueMapList = daoManager.getXXTaggedResourceValueMap().findByResValueId(taggedResValue.getId());
			for (XXTaggedResourceValueMap taggedResValueMap : taggedResValueMapList) {
				daoManager.getXXTaggedResourceValueMap().remove(taggedResValueMap);
			}
			daoManager.getXXTaggedResourceValue().remove(taggedResValue);
		}
	}

	private void deleteTagsForTaggedResource(Long resourceId) {
		List<XXTagResourceMap> oldTagResMapList = daoManager.getXXTagResourceMap().findByTaggedResourceId(resourceId);
		for (XXTagResourceMap oldTagResMap : oldTagResMapList) {
			daoManager.getXXTagResourceMap().remove(oldTagResMap);

			List<XXTagAttribute> tagAttrList = daoManager.getXXTagAttribute().findByTagId(oldTagResMap.getTagId());
			for (XXTagAttribute tagAttr : tagAttrList) {
				daoManager.getXXTagAttribute().remove(tagAttr);
			}
			daoManager.getXXTag().remove(oldTagResMap.getTagId());
		}
	}

	@Override
	public void deleteResource(Long taggedResId) throws Exception {

		XXTaggedResource taggedRes = daoManager.getXXTaggedResource().getById(taggedResId);
		if (taggedRes == null) {
			throw errorUtil.createRESTException("No Resource exists with Id: " + taggedResId, MessageEnums.DATA_NOT_FOUND);
		}

		// Remove tags associated with resource
		deleteTagsForTaggedResource(taggedResId);

		// Remove taggedResourceValue
		deleteTaggedResourceValue(taggedResId);

		// Remove taggedResource
		daoManager.getXXTaggedResource().remove(taggedRes);
	}

	@Override
	public RangerTaggedResource getResource(Long id) throws Exception {
		return rangerTaggedResourceService.read(id);
	}

	@Override
	public TagServiceResources getResources(String serviceName, Long lastTimestamp) throws Exception {

		List<RangerTaggedResource> taggedResources;

		SearchFilter filter = new SearchFilter();

		if (StringUtils.isNotBlank(serviceName)) {
			filter.setParam(SearchFilter.TAG_RESOURCE_SERVICE_NAME, serviceName);
		}

		if (lastTimestamp != null) {
			filter.setParam(SearchFilter.TAG_RESOURCE_TIMESTAMP, Long.toString(lastTimestamp.longValue()));
		}

		taggedResources = getResources(filter);

		TagServiceResources ret = new TagServiceResources();
		ret.setTaggedResources(taggedResources);
		// TBD
		ret.setLastUpdateTime(new Date());
		ret.setVersion(1L);

		return ret;
	}

	@Override
	public List<RangerTaggedResource> getResources(SearchFilter filter) throws Exception {
		return getPaginatedResources(filter).getList();
	}

	@Override
	public PList<RangerTaggedResource> getPaginatedResources(SearchFilter filter) throws Exception {
		return rangerTaggedResourceService.searchRangerTaggedResources(filter);
	}

	@Override
	public List<String> getTags(String serviceName) throws Exception {

		XXService xService = daoManager.getXXService().findByName(serviceName);
		if (xService == null) {
			throw errorUtil.createRESTException("No Service found with name [" + serviceName + "]", MessageEnums.DATA_NOT_FOUND);
		}

		List<String> tagList = daoManager.getXXTag().findTagNamesByServiceId(xService.getId());

		Collections.sort(tagList, new Comparator<String>() {
			@Override
			public int compare(String s1, String s2) {
				return s1.compareToIgnoreCase(s2);
			}
		});

		return tagList;
	}

	@Override
	public List<String> lookupTags(String serviceName, String tagNamePattern) throws Exception {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.lookupTags(" + serviceName + ", " + tagNamePattern + ")");
		}

		List<String> tagNameList = getTags(serviceName);
		List<String> matchedTagList = new ArrayList<String>();

		if (CollectionUtils.isNotEmpty(tagNameList)) {
			Pattern p = Pattern.compile(tagNamePattern);
			for (String tagName : tagNameList) {
				Matcher m = p.matcher(tagName);
				if (LOG.isDebugEnabled()) {
					LOG.debug("TagDBStore.lookupTags) - Trying to match .... tagNamePattern=" + tagNamePattern + ", tagName=" + tagName);
				}
				if (m.matches()) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("TagDBStore.lookupTags) - Match found.... tagNamePattern=" + tagNamePattern + ", tagName=" + tagName);
					}
					matchedTagList.add(tagName);
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.lookupTags(" + serviceName + ", " + tagNamePattern + ")");
		}

		return matchedTagList;
	}

	@Override
	public RangerTaggedResource getResource(RangerTaggedResourceKey key) throws Exception {
		if (key == null) {
			LOG.error("TagDBStore.getResources() - parameter 'key' is null.");
			throw errorUtil.createRESTException("TagFileStore.getResources() - parameter 'key' is null.", MessageEnums.INVALID_INPUT_DATA);
		}

		XXService xService = daoManager.getXXService().findByName(key.getServiceName());
		if (xService == null) {
			LOG.error("TagDBStore.getResources() - No Service found with name [" + key.getServiceName() + "]");
			throw errorUtil.createRESTException("TagDBStore.getResources() - No Service found with name [" + key.getServiceName() + "]", MessageEnums.INVALID_INPUT_DATA);
		}

		RangerServiceDef serviceDef = serviceDBStore.getServiceDef(xService.getType());

		Long serviceId = xService.getId();

		RangerTaggedResource ret = null;

		List<XXTaggedResource> taggedResList = daoManager.getXXTaggedResource().findByServiceId(serviceId);

		if (CollectionUtils.isEmpty(taggedResList)) {
			return null;
		}

		if (taggedResList.size() == 1) {
			ret = rangerTaggedResourceService.getPopulatedViewObjject(taggedResList.get(0));
			return ret;
		} else {
			for (XXTaggedResource xTaggedRes : taggedResList) {
				RangerTaggedResource taggedRes = rangerTaggedResourceService.getPopulatedViewObjject(xTaggedRes);

				RangerDefaultPolicyResourceMatcher policyResourceMatcher = new RangerDefaultPolicyResourceMatcher();

				policyResourceMatcher.setPolicyResources(taggedRes.getKey().getResourceSpec());

				policyResourceMatcher.setServiceDef(serviceDef);
				policyResourceMatcher.init();
				boolean isMatch = policyResourceMatcher.isExactMatch(key.getResourceSpec());

				if (isMatch) {
					return taggedRes;
				}
			}
		}
		return ret;
	}

}
