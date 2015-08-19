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
import org.apache.ranger.plugin.model.*;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerTagDef.RangerTagAttributeDef;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.policyresourcematcher.RangerDefaultPolicyResourceMatcher;
import org.apache.ranger.plugin.store.AbstractTagStore;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.store.TagStore;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServiceTags;
import org.apache.ranger.service.RangerAuditFields;
import org.apache.ranger.service.RangerTagDefService;
import org.apache.ranger.service.RangerTagService;
import org.apache.ranger.service.RangerTaggedResourceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TagDBStore implements TagStore {
	private static final Log LOG = LogFactory.getLog(TagDBStore.class);

	@Autowired
	RangerTagDefService rangerTagDefService;

	@Autowired
	RangerTagService rangerTagService;

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

	/*
	private XXTag createTagAttributes(RangerTag tag) {
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

		return xTag;
	}

	private void deleteTagAttributes(Long tagId) {
		List<XXTagAttribute> tagAttrList = daoManager.getXXTagAttribute().findByTagId(tagId);
		for (XXTagAttribute tagAttr : tagAttrList) {
			daoManager.getXXTagAttribute().remove(tagAttr);
		}
	}

	private void createResourceSpecForResource(RangerServiceResource resource) {

		String serviceName = resource.getServiceName();

		XXService xService = daoManager.getXXService().findByName(serviceName);
		if (xService == null) {
			throw errorUtil.createRESTException("No Service found with name: " + serviceName, MessageEnums.ERROR_CREATING_OBJECT);
		}

		XXServiceDef xServiceDef = daoManager.getXXServiceDef().getById(xService.getType());
		if (xServiceDef == null) {
			throw errorUtil.createRESTException("No Service-Def found with ID: " + xService.getType(), MessageEnums.ERROR_CREATING_OBJECT);
		}

		Map<String, RangerPolicy.RangerPolicyResource> resourceSpec = resource.getResourceSpec();

		for (Entry<String, RangerPolicyResource> resSpec : resourceSpec.entrySet()) {
			XXResourceDef xResDef = daoManager.getXXResourceDef().findByNameAndServiceDefId(resSpec.getKey(), xServiceDef.getId());

			if (xResDef == null) {
				LOG.error("TagDBStore.createResource: ResourceType is not valid [" + resSpec.getKey() + "]");
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
	}

	private void deleteResourceValue(Long resourceId) {
		List<XXTaggedResourceValue> taggedResValueList = daoManager.getXXTaggedResourceValue().findByTaggedResId(resourceId);
		for (XXTaggedResourceValue taggedResValue : taggedResValueList) {
			List<XXTaggedResourceValueMap> taggedResValueMapList = daoManager.getXXTaggedResourceValueMap().findByResValueId(taggedResValue.getId());
			for (XXTaggedResourceValueMap taggedResValueMap : taggedResValueMapList) {
				daoManager.getXXTaggedResourceValueMap().remove(taggedResValueMap);
			}
			daoManager.getXXTaggedResourceValue().remove(taggedResValue);
		}
	}

	private void updateResourceSpecForResource(RangerServiceResource updResource) {

		if (updResource != null) {
			deleteResourceValue(updResource.getId());
		}

		createResourceSpecForResource(updResource);
	}
	*/

	@Override
	public RangerTag createTag(RangerTag tag) throws Exception
	{
		throw new Exception("Not implemented");

		/*
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.createTag(" + tag + ")");
		}

		throw new Exception("Not implemented");



		RangerTag ret = null;


		try {

			ret = rangerTagService.getPopulatedViewObject(createTagAttributes(tag));

		} catch (Exception e) {
			throw errorUtil.createRESTException("failed to save tag [" + tag.getName() + "]", MessageEnums.ERROR_CREATING_OBJECT);
		}


		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.createTag(" + tag + ")");
		}

		return ret;
		*/
	}

	@Override
	public RangerTag updateTag(RangerTag tag) throws Exception
	{

		throw new Exception("Not implemented");

		/*
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.updateTag(" + tag + ")");
		}

		throw new Exception("Not implemented");

		RangerTag ret = null;


		RangerTag existing = rangerTagService.read(tag.getId());

		if (existing == null) {
			throw errorUtil.createRESTException("failed to update tag [" + tag.getName() + "], Reason: No Tag found with id: [" + tag.getId() + "]",
					MessageEnums.DATA_NOT_UPDATABLE);
		}

		if (StringUtils.isEmpty(tag.getCreatedBy())) {
			tag.setCreatedBy(existing.getCreatedBy());
		}
		if (tag.getCreateTime() == null) {
			tag.setCreateTime(existing.getCreateTime());
		}
		if (StringUtils.isEmpty(tag.getGuid())) {
			tag.setGuid(existing.getGuid());
		}

		deleteTagAttributes(existing.getId());

		createTagAttributes(tag);

		ret = rangerTagService.update(tag);

		ret = rangerTagService.read(ret.getId());



		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.updateTag(" + tag + ") : " + ret);
		}

		return ret;
		*/
	}

	@Override
	public void deleteTagById(Long id) throws Exception {

		throw new Exception("Not implemented");

		/*
		RangerTag tag = rangerTagService.read(id);
		deleteTagAttributes(id);
		rangerTagService.delete(tag);
		*/
	}

	@Override
	public RangerTag getTagById(Long id) throws Exception {
		throw new Exception("Not implemented");

		/*
		RangerTag ret = null;

		ret = rangerTagService.read(id);

		return ret;
		*/
	}

	@Override
	public List<RangerTag> getTagsByName(String name) throws Exception {
		throw new Exception("Not implemented");

		/*
		List<RangerTag> ret = null;

		if (StringUtils.isNotBlank(name)) {
			SearchFilter filter = new SearchFilter(SearchFilter.TAG_DEF_NAME, name);
			ret = getTags(filter);
		} else {
			ret = null;
		}

		return ret;
		*/
	}

	@Override
	public List<RangerTag> getTagsByExternalId(String externalId) throws Exception {
		throw new Exception("Not implemented");

		/*
		List<RangerTag> ret = null;

		if (StringUtils.isNotBlank(externalId)) {
			SearchFilter filter = new SearchFilter(SearchFilter.TAG_EXTERNAL_ID, externalId);
			ret = getTags(filter);
		} else {
			ret = null;
		}

		return ret;
		*/
	}

	@Override
	public List<RangerTag> getTags(SearchFilter filter) throws Exception {
		throw new Exception("Not implemented");

		/*
		List<RangerTag> ret = null;

		ret = rangerTagService.searchRangerTags(filter).getList();

		return ret;
		*/
	}


	@Override
	public RangerServiceResource createServiceResource(RangerServiceResource resource) throws Exception {
		throw new Exception("Not implemented");

		/*
		if (LOG.isDebugEnabled()) {

			LOG.debug("==> TagDBStore.createResource(" + resource + ")");
		}
		throw new Exception("Not implemented");

		RangerServiceResource ret = null;

		try {
			ret = rangerTaggedResourceService.create(resource);

			ret = rangerTaggedResourceService.read(ret.getId());

			createResourceSpecForResource(ret);

		} catch (Exception e) {
			throw errorUtil.createRESTException("failed to save resource [" + resource.getId() + "]", MessageEnums.ERROR_CREATING_OBJECT);
		}


		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.createResource(" + resource + ")");
		}

		return ret;
		*/
	}

	@Override
	public RangerServiceResource updateServiceResource(RangerServiceResource resource) throws Exception {
		throw new Exception("Not implemented");

		/*

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> TagDBStore.updateResource(" + resource + ")");
		}

		throw new Exception("Not implemented");

		RangerServiceResource ret = null;

		RangerServiceResource existing = rangerTaggedResourceService.read(resource.getId());


		if (existing == null) {
			throw errorUtil.createRESTException("failed to update tag [" + resource.getId() + "], Reason: No resource found with id: [" + resource.getId() + "]",
					MessageEnums.DATA_NOT_UPDATABLE);
		}

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

		ret = rangerTaggedResourceService.read(ret.getId());

		updateResourceSpecForResource(ret);


		if (LOG.isDebugEnabled()) {
			LOG.debug("<== TagDBStore.updateResource(" + resource + ") : " + ret);
		}

		return ret;
		*/
	}

	@Override
	public void deleteServiceResourceById(Long id) throws Exception {

		throw new Exception("Not implemented");

		/*
		XXTaggedResource taggedRes = daoManager.getXXTaggedResource().getById(id);
		if (taggedRes == null) {
			throw errorUtil.createRESTException("No Resource exists with Id: " + id, MessageEnums.DATA_NOT_FOUND);
		}

		// Remove taggedResourceValue
		deleteResourceValue(id);

		// Remove taggedResource
		daoManager.getXXTaggedResource().remove(id);
		*/
	}

	@Override
	public List<RangerServiceResource> getServiceResourcesByExternalId(String externalId) throws Exception {

		throw new Exception("Not implemented");

		/*
		List<RangerServiceResource> ret = null;


		if (StringUtils.isNotBlank(externalId)) {
			SearchFilter filter = new SearchFilter(SearchFilter.TAG_EXTERNAL_ID, externalId);
			ret = getServiceResources(filter);
		} else {
			ret = null;
		}

		return ret;
		*/
	}

	@Override
	public RangerServiceResource getServiceResourceById(Long id) throws Exception {

		throw new Exception("Not implemented");

		/*
		RangerServiceResource ret = null;
		ret = rangerTaggedResourceService.read(id);
		return ret;
		*/
	}


	@Override
	public List<RangerServiceResource> getServiceResourcesByServiceAndResourceSpec(String serviceName, Map<String, RangerPolicy.RangerPolicyResource> resourceSpec) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public List<RangerServiceResource> getServiceResources(SearchFilter filter) throws Exception{

		throw new Exception("Not implemented");

		/*
		List<RangerServiceResource> ret = null;

		ret = rangerTaggedResourceService.searchRangerTaggedResources(filter).getList();
		return ret;
		*/
	}

	@Override
	public RangerTagResourceMap createTagResourceMap(RangerTagResourceMap tagResourceMap) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public void deleteTagResourceMapById(Long id) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public List<RangerTagResourceMap> getTagResourceMap(String externalResourceId, String externalTagId) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public RangerTagResourceMap getTagResourceMapById(Long id) throws Exception {
		throw new Exception("Not implemented");
	}


	@Override
	public List<RangerTagResourceMap> getTagResourceMaps(SearchFilter filter) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public ServiceTags getServiceTagsIfUpdated(String serviceName, Long lastKnownVersion) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public PList<RangerTagResourceMap> getPaginatedTagResourceMaps(SearchFilter filter) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public List<String> getTags(String serviceName) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public List<String> lookupTags(String serviceName, String tagNamePattern) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public List<RangerTag> getTagsForServiceResource(Long resourceId) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public List<RangerTag> getTagsForServiceResourceByExtId(String resourceExtId) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public List<RangerTagDef> getTagDefsByExternalId(String extId) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public List<RangerTagResourceMap> getTagResourceMapsByTagId(Long tagId) throws Exception {
		throw new Exception("Not implemented");
	}

	@Override
	public List<RangerTagResourceMap> getTagResourceMapsByResourceId(Long resourceId) throws Exception {
		throw new Exception("Not implemented");
	}
}
