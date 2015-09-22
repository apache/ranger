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
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.common.GUIDUtil;
import org.apache.ranger.common.RangerConfigUtil;
import org.apache.ranger.entity.XXDBBase;
import org.apache.ranger.entity.XXTagAttributeDef;
import org.apache.ranger.entity.XXTagDef;
import org.apache.ranger.plugin.model.RangerTagDef;
import org.apache.ranger.plugin.model.RangerTagDef.RangerTagAttributeDef;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.util.SearchFilter;
import org.springframework.beans.factory.annotation.Autowired;

public abstract class RangerTagDefServiceBase<T extends XXTagDef, V extends RangerTagDef> extends
		RangerBaseModelService<T, V> {

	@Autowired
	GUIDUtil guidUtil;

	@Autowired
	RangerAuditFields<XXDBBase> rangerAuditFields;
	
	@Autowired
	RangerConfigUtil configUtil;

	@Override
	@SuppressWarnings("unchecked")
	protected XXTagDef mapViewToEntityBean(RangerTagDef vObj, XXTagDef xObj, int OPERATION_CONTEXT) {
		String guid = (StringUtils.isEmpty(vObj.getGuid())) ? guidUtil.genGUID() : vObj.getGuid();

		xObj.setGuid(guid);
		xObj.setVersion(vObj.getVersion());
		xObj.setIsEnabled(vObj.getIsEnabled());
		xObj.setName(vObj.getName());
		xObj.setSource(vObj.getSource());
		return xObj;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected RangerTagDef mapEntityToViewBean(RangerTagDef vObj, XXTagDef xObj) {

		vObj.setGuid(xObj.getGuid());
		vObj.setVersion(xObj.getVersion());
		vObj.setIsEnabled(xObj.getIsEnabled());
		vObj.setName(xObj.getName());
		vObj.setSource(xObj.getSource());

		List<RangerTagAttributeDef> attributeDefs = getAttributeDefForTagDef(xObj);
		vObj.setAttributeDefs(attributeDefs);

		return vObj;
	}

	public List<RangerTagAttributeDef> getAttributeDefForTagDef(XXTagDef xtagDef) {
		List<XXTagAttributeDef> tagAttrDefList = daoMgr.getXXTagAttributeDef().findByTagDefId(xtagDef.getId());
		List<RangerTagDef.RangerTagAttributeDef> attributeDefList = new ArrayList<RangerTagDef.RangerTagAttributeDef>();

		for (XXTagAttributeDef xAttrTag : tagAttrDefList) {
			RangerTagAttributeDef attrDef = populateRangerTagAttributeDef(xAttrTag);
			attributeDefList.add(attrDef);
		}
		return attributeDefList;
	}

	/**
	 * @param xObj
	 * @return
	 */
	public RangerTagAttributeDef populateRangerTagAttributeDef(XXTagAttributeDef xObj) {
		RangerTagAttributeDef attrDef = new RangerTagAttributeDef();
		attrDef.setName(xObj.getName());
		attrDef.setType(xObj.getType());
		return attrDef;
	}

	/**
	 * @param attrDef
	 * @param xTagAttrDef
	 * @param parentObj
	 * @return
	 */
	public XXTagAttributeDef populateXXTagAttributeDef(RangerTagAttributeDef attrDef, XXTagAttributeDef xTagAttrDef,
			XXTagDef parentObj) {

		if (xTagAttrDef == null) {
			xTagAttrDef = new XXTagAttributeDef();
		}

		xTagAttrDef = (XXTagAttributeDef) rangerAuditFields.populateAuditFields(xTagAttrDef, parentObj);

		xTagAttrDef.setTagDefId(parentObj.getId());
		xTagAttrDef.setName(attrDef.getName());
		xTagAttrDef.setType(attrDef.getType());
		return xTagAttrDef;
	}

	@SuppressWarnings("unchecked")
	public PList<RangerTagDef> searchRangerTagDefs(SearchFilter searchFilter) {
		PList<RangerTagDef> retList = new PList<RangerTagDef>();
		List<RangerTagDef> tagDefList = new ArrayList<RangerTagDef>();

		List<XXTagDef> xTagDefList = (List<XXTagDef>) searchRangerObjects(searchFilter, searchFields, sortFields, (PList<V>) retList);

		for (XXTagDef xTagDef : xTagDefList) {
			RangerTagDef tagDef = populateViewBean((T) xTagDef);
			tagDefList.add(tagDef);
		}

		retList.setList(tagDefList);

		return retList;
	}
}
