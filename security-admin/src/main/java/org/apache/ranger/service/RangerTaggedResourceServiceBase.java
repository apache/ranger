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
import org.apache.ranger.entity.XXTaggedResource;
import org.apache.ranger.entity.XXTaggedResourceValue;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.util.SearchFilter;
import org.springframework.beans.factory.annotation.Autowired;

public abstract class RangerTaggedResourceServiceBase<T extends XXTaggedResource, V extends RangerServiceResource> extends RangerBaseModelService<T, V> {

	@Autowired
	GUIDUtil guidUtil;

	@Override
	@SuppressWarnings("unchecked")
	protected XXTaggedResource mapViewToEntityBean(RangerServiceResource vObj, XXTaggedResource xObj, int operationContext) {
		String guid = (StringUtils.isEmpty(vObj.getGuid())) ? guidUtil.genGUID() : vObj.getGuid();

		xObj.setGuid(guid);
		xObj.setVersion(vObj.getVersion());
		xObj.setIsEnabled(vObj.getIsEnabled());

		XXService xService = daoMgr.getXXService().findByName(vObj.getServiceName());
		if (xService == null) {
			throw restErrorUtil.createRESTException("Error Populating XXTaggedResource. No Service found with name: " + vObj.getServiceName(), MessageEnums.INVALID_INPUT_DATA);
		}

		xObj.setServiceId(xService.getId());

		return xObj;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected RangerServiceResource mapEntityToViewBean(RangerServiceResource vObj, XXTaggedResource xObj) {
		vObj.setGuid(xObj.getGuid());
		vObj.setVersion(xObj.getVersion());
		vObj.setIsEnabled(xObj.getIsEnabled());

		XXService xService = daoMgr.getXXService().getById(xObj.getServiceId());

		vObj.setServiceName(xService.getName());

		List<XXTaggedResourceValue> resValueList = daoMgr.getXXTaggedResourceValue().findByTaggedResId(xObj.getId());
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

		vObj.setResourceSpec(resourceSpec);

		return vObj;
	}

	@SuppressWarnings("unchecked")
	public PList<RangerServiceResource> searchRangerTaggedResources(SearchFilter searchFilter) {
		PList<RangerServiceResource> retList = new PList<RangerServiceResource>();
		List<RangerServiceResource> taggedResList = new ArrayList<RangerServiceResource>();

		List<XXTaggedResource> xTaggedResList = (List<XXTaggedResource>) searchRangerObjects(searchFilter, searchFields, sortFields, (PList<V>) retList);

		for (XXTaggedResource xTaggedRes : xTaggedResList) {
			RangerServiceResource taggedRes = populateViewBean((T) xTaggedRes);
			taggedResList.add(taggedRes);
		}
		retList.setList(taggedResList);
		return retList;
	}

}
