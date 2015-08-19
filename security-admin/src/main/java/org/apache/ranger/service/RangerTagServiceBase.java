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
import org.apache.ranger.entity.XXTag;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.model.RangerTagDef;
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

		xObj.setGuid(guid);
		xObj.setVersion(vObj.getVersion());
		xObj.setIsEnabled(vObj.getIsEnabled());
		xObj.setName(vObj.getName());
		return xObj;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected RangerTag mapEntityToViewBean(RangerTag vObj, XXTag xObj) {

		vObj.setGuid(xObj.getGuid());
		vObj.setVersion(xObj.getVersion());
		vObj.setIsEnabled(xObj.getIsEnabled());
		vObj.setName(xObj.getName());

		return vObj;
	}

	/**
	 * @param xObj
	 * @return
	 */
	public RangerTagDef.RangerTagAttributeDef populateRangerTagAttributeDef(XXTagAttributeDef xObj) {
		RangerTagDef.RangerTagAttributeDef attrDef = new RangerTagDef.RangerTagAttributeDef();
		attrDef.setName(xObj.getName());
		attrDef.setType(xObj.getType());
		return attrDef;
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
