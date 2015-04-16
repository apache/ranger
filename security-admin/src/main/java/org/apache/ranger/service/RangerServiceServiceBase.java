/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.common.GUIDUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.SortField;
import org.apache.ranger.common.SearchField.DATA_TYPE;
import org.apache.ranger.common.SearchField.SEARCH_TYPE;
import org.apache.ranger.common.SortField.SORT_ORDER;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceBase;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.view.RangerServiceList;

public abstract class RangerServiceServiceBase<T extends XXServiceBase, V extends RangerService> extends RangerBaseModelService<T, V> {
	
	public RangerServiceServiceBase() {
		super();
		
		searchFields.add(new SearchField(SearchFilter.SERVICE_TYPE, "xSvcDef.name", DATA_TYPE.STRING, 
				SEARCH_TYPE.FULL, "XXServiceDef xSvcDef", "obj.type = xSvcDef.id"));
		searchFields.add(new SearchField(SearchFilter.SERVICE_TYPE_ID, "obj.type", DATA_TYPE.INTEGER, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.SERVICE_NAME, "obj.name", DATA_TYPE.STRING, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.SERVICE_ID, "obj.id", DATA_TYPE.INTEGER, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.IS_ENABLED, "obj.isEnabled", DATA_TYPE.BOOLEAN, SEARCH_TYPE.FULL));
		
		sortFields.add(new SortField(SearchFilter.CREATE_TIME, "obj.createTime"));
		sortFields.add(new SortField(SearchFilter.UPDATE_TIME, "obj.updateTime"));
		sortFields.add(new SortField(SearchFilter.SERVICE_ID, "obj.id", true, SORT_ORDER.ASC));
		sortFields.add(new SortField(SearchFilter.SERVICE_NAME, "obj.name"));
		
	}
	
	@Override
	@SuppressWarnings("unchecked")
	protected XXServiceBase mapViewToEntityBean(RangerService vObj, XXServiceBase xObj, int OPERATION_CONTEXT) {
		String guid = (StringUtils.isEmpty(vObj.getGuid())) ? GUIDUtil.genGUI() : vObj.getGuid();
		
		xObj.setGuid(guid);
		xObj.setVersion(vObj.getVersion());
		
		XXServiceDef xServiceDef = daoMgr.getXXServiceDef().findByName(vObj.getType());
		if(xServiceDef == null) {
			throw restErrorUtil.createRESTException(
					"No ServiceDefinition found with name :" + vObj.getType(),
					MessageEnums.INVALID_INPUT_DATA);
		}
		xObj.setType(xServiceDef.getId());
		xObj.setName(vObj.getName());
		xObj.setPolicyVersion(vObj.getPolicyVersion());
		xObj.setPolicyUpdateTime(vObj.getPolicyUpdateTime());
		xObj.setDescription(vObj.getDescription());
		xObj.setIsEnabled(vObj.getIsEnabled());
		return xObj;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected RangerService mapEntityToViewBean(RangerService vObj, XXServiceBase xObj) {
		XXServiceDef xServiceDef = daoMgr.getXXServiceDef().getById(xObj.getType());
		vObj.setType(xServiceDef.getName());
		vObj.setGuid(xObj.getGuid());
		vObj.setVersion(xObj.getVersion());
		vObj.setName(xObj.getName());
		vObj.setDescription(xObj.getDescription());
		vObj.setPolicyVersion(xObj.getPolicyVersion());
		vObj.setPolicyUpdateTime(xObj.getPolicyUpdateTime());
		return vObj;
	}

	@SuppressWarnings("unchecked")
	public RangerServiceList searchRangerServices(SearchFilter searchFilter) {
		List<RangerService> serviceList = new ArrayList<RangerService>();
		RangerServiceList retList = new RangerServiceList();

		List<XXService> xSvcList = (List<XXService>) searchResources(searchFilter, searchFields, sortFields, retList);
		for (XXService xSvc : xSvcList) {
			serviceList.add(populateViewBean((T) xSvc));
		}
		retList.setServices(serviceList);
		return retList;
	}

}
