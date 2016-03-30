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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.common.GUIDUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RangerConfigUtil;
import org.apache.ranger.entity.XXDBBase;
import org.apache.ranger.entity.XXTagAttribute;
import org.apache.ranger.entity.XXTag;
import org.apache.ranger.entity.XXTagDef;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.util.SearchFilter;
import org.springframework.beans.factory.annotation.Autowired;

public abstract class RangerTagServiceBase<T extends XXTag, V extends RangerTag> extends
		RangerBaseModelService<T, V> {

	@Autowired
	GUIDUtil guidUtil;

	@Autowired
	RangerAuditFields<XXDBBase> rangerAuditFields;
	
	@Autowired
	RangerConfigUtil configUtil;

	@Override
	@SuppressWarnings("unchecked")
	protected XXTag mapViewToEntityBean(RangerTag vObj, XXTag xObj, int OPERATION_CONTEXT) {
		String guid = (StringUtils.isEmpty(vObj.getGuid())) ? guidUtil.genGUID() : vObj.getGuid();

		XXTagDef xTagDef = daoMgr.getXXTagDef().findByName(vObj.getType());
		if(xTagDef == null) {
			throw restErrorUtil.createRESTException(
					"No TagDefinition found with name :" + vObj.getType(),
					MessageEnums.INVALID_INPUT_DATA);
		}

		xObj.setGuid(guid);
		xObj.setType(xTagDef.getId());
		xObj.setOwner(vObj.getOwner());
		return xObj;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected RangerTag mapEntityToViewBean(RangerTag vObj, XXTag xObj) {
		XXTagDef xTagDef = daoMgr.getXXTagDef().getById(xObj.getType());
		if(xTagDef == null) {
			throw restErrorUtil.createRESTException(
					"No TagDefinition found with name :" + xObj.getType(),
					MessageEnums.INVALID_INPUT_DATA);
		}

		vObj.setGuid(xObj.getGuid());
		vObj.setType(xTagDef.getName());
		vObj.setOwner(xObj.getOwner());

		Map<String, String> attributes = getAttributesForTag(xObj);
		vObj.setAttributes(attributes);

		return vObj;
	}

	public Map<String, String> getAttributesForTag(XXTag xtag) {
		List<XXTagAttribute> tagAttrList = daoMgr.getXXTagAttribute().findByTagId(xtag.getId());
		Map<String, String>  ret         = new HashMap<String, String>();

		if(CollectionUtils.isNotEmpty(tagAttrList)) {
			for (XXTagAttribute tagAttr : tagAttrList) {
				ret.put(tagAttr.getName(), tagAttr.getValue());
			}
		}

		return ret;
	}

	@SuppressWarnings("unchecked")
	public PList<RangerTag> searchRangerTags(SearchFilter searchFilter) {
		PList<RangerTag> retList = new PList<RangerTag>();
		List<RangerTag> tagList = new ArrayList<RangerTag>();

		List<XXTag> xTagList = (List<XXTag>) searchRangerObjects(searchFilter, searchFields, sortFields, (PList<V>) retList);

		for (XXTag xTag : xTagList) {
			RangerTag tag = populateViewBean((T) xTag);
			tagList.add(tag);
		}

		retList.setList(tagList);

		return retList;
	}
}
