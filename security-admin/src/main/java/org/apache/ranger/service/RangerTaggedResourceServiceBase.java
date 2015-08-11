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

package org.apache.ranger.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.common.GUIDUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.entity.XXResourceDef;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXTag;
import org.apache.ranger.entity.XXTagAttribute;
import org.apache.ranger.entity.XXTaggedResource;
import org.apache.ranger.entity.XXTaggedResourceValue;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerTaggedResource;
import org.apache.ranger.plugin.model.RangerTaggedResourceKey;
import org.apache.ranger.plugin.model.RangerTaggedResource.RangerResourceTag;
import org.springframework.beans.factory.annotation.Autowired;

public abstract class RangerTaggedResourceServiceBase<T extends XXTaggedResource, V extends RangerTaggedResource> extends RangerBaseModelService<T, V> {

	@Autowired
	GUIDUtil guidUtil;

	@Override
	@SuppressWarnings("unchecked")
	protected XXTaggedResource mapViewToEntityBean(RangerTaggedResource vObj, XXTaggedResource xObj, int operationContext) {
		String guid = (StringUtils.isEmpty(vObj.getGuid())) ? guidUtil.genGUID() : vObj.getGuid();

		xObj.setGuid(guid);
		xObj.setVersion(vObj.getVersion());
		xObj.setIsEnabled(vObj.getIsEnabled());
		xObj.setExternalId(vObj.getExternalId());

		RangerTaggedResourceKey key = vObj.getKey();
		if (key == null) {
			throw restErrorUtil.createRESTException("Error Populating XXTaggedResource. Key cannot be null in RangerTaggedResource.", MessageEnums.INVALID_INPUT_DATA);
		}
		XXService xService = daoMgr.getXXService().findByName(key.getServiceName());
		if (xService == null) {
			throw restErrorUtil.createRESTException("Error Populating XXTaggedResource. No Service found with name: " + key.getServiceName(), MessageEnums.INVALID_INPUT_DATA);
		}

		xObj.setServiceId(xService.getId());

		return xObj;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected RangerTaggedResource mapEntityToViewBean(RangerTaggedResource vObj, XXTaggedResource xObj) {
		vObj.setGuid(xObj.getGuid());
		vObj.setVersion(xObj.getVersion());
		vObj.setIsEnabled(xObj.getIsEnabled());
		vObj.setExternalId(xObj.getExternalId());

		RangerTaggedResourceKey key = populateRangerTaggedResourceKey(xObj);
		vObj.setKey(key);

		List<RangerResourceTag> tags = populateRangerResourceTags(xObj);
		vObj.setTags(tags);

		return vObj;
	}

	private List<RangerResourceTag> populateRangerResourceTags(XXTaggedResource xTaggedRes) {
		if (xTaggedRes == null) {
			return null;
		}

		List<RangerResourceTag> tags = new ArrayList<RangerTaggedResource.RangerResourceTag>();

		List<XXTag> xTagList = daoMgr.getXXTag().findByTaggedResource(xTaggedRes.getId());

		for (XXTag xTag : xTagList) {
			RangerResourceTag tag = new RangerResourceTag();

			List<XXTagAttribute> tagAttrList = daoMgr.getXXTagAttribute().findByTagId(xTag.getId());
			Map<String, String> attrList = new HashMap<String, String>();

			for (XXTagAttribute tagAttr : tagAttrList) {
				attrList.put(tagAttr.getName(), tagAttr.getValue());
			}

			tag.setAttributeValues(attrList);
			tag.setName(xTag.getName());
			tag.setExternalId(xTag.getExternalId());
			
			tags.add(tag);
		}

		return tags;
	}

	private RangerTaggedResourceKey populateRangerTaggedResourceKey(XXTaggedResource xTaggedRes) {
		if (xTaggedRes == null) {
			return null;
		}

		RangerTaggedResourceKey key = new RangerTaggedResourceKey();

		XXService xService = daoMgr.getXXService().getById(xTaggedRes.getServiceId());
		key.setServiceName(xService.getName());

		List<XXTaggedResourceValue> resValueList = daoMgr.getXXTaggedResourceValue().findByTaggedResId(xTaggedRes.getId());
		Map<String, RangerPolicy.RangerPolicyResource> resourceSpec = new HashMap<String, RangerPolicy.RangerPolicyResource>();

		for (XXTaggedResourceValue resValue : resValueList) {
			List<String> resValueMapList = daoMgr.getXXTaggedResourceValueMap().findValuesByResValueId(resValue.getId());

			XXResourceDef xResDef = daoMgr.getXXResourceDef().getById(resValue.getResDefId());

			RangerPolicyResource policyRes = new RangerPolicyResource();
			policyRes.setIsExcludes(resValue.getIsExcludes());
			policyRes.setIsRecursive(resValue.getIsRecursive());
			policyRes.setValues(resValueMapList);

			resourceSpec.put(xResDef.getName(), policyRes);
		}
		key.setResourceSpec(resourceSpec);
		return key;
	}

}
