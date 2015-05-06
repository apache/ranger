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

import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.SortField;
import org.apache.ranger.common.SearchField.DATA_TYPE;
import org.apache.ranger.common.SearchField.SEARCH_TYPE;
import org.apache.ranger.entity.XXContextEnricherDef;
import org.apache.ranger.entity.XXAccessTypeDef;
import org.apache.ranger.entity.XXEnumDef;
import org.apache.ranger.entity.XXPolicyConditionDef;
import org.apache.ranger.entity.XXResourceDef;
import org.apache.ranger.entity.XXServiceConfigDef;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerContextEnricherDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerPolicyConditionDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerServiceConfigDef;
import org.apache.ranger.plugin.util.SearchFilter;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope("singleton")
public class RangerServiceDefService extends RangerServiceDefServiceBase<XXServiceDef, RangerServiceDef> {

	public RangerServiceDefService() {
		super();

		searchFields.add(new SearchField(SearchFilter.SERVICE_TYPE, "obj.name", DATA_TYPE.STRING, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.SERVICE_TYPE_ID, "obj.id", DATA_TYPE.INTEGER, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.IS_ENABLED, "obj.isEnabled", DATA_TYPE.BOOLEAN, SEARCH_TYPE.FULL));
		
		sortFields.add(new SortField(SearchFilter.CREATE_TIME, "obj.createTime"));
		sortFields.add(new SortField(SearchFilter.UPDATE_TIME, "obj.updateTime"));
		sortFields.add(new SortField(SearchFilter.SERVICE_TYPE_ID, "obj.id"));
		sortFields.add(new SortField(SearchFilter.SERVICE_TYPE, "obj.name"));
	}

	@Override
	protected void validateForCreate(RangerServiceDef vObj) {
		// TODO Auto-generated method stub

	}

	@Override
	protected void validateForUpdate(RangerServiceDef vObj,
			XXServiceDef entityObj) {
		// TODO Auto-generated method stub

	}
	

	@Override
	protected RangerServiceDef populateViewBean(XXServiceDef xServiceDef) {
		RangerServiceDef serviceDef = super.populateViewBean(xServiceDef);
		Long serviceDefId = xServiceDef.getId();
		
		List<XXServiceConfigDef> xConfigs = daoMgr.getXXServiceConfigDef().findByServiceDefId(serviceDefId);
		if (!stringUtil.isEmpty(xConfigs)) {
			List<RangerServiceConfigDef> configs = new ArrayList<RangerServiceConfigDef>();
			for (XXServiceConfigDef xConfig : xConfigs) {
				RangerServiceConfigDef config = populateXXToRangerServiceConfigDef(xConfig);
				configs.add(config);
			}
			serviceDef.setConfigs(configs);
		}
		
		List<XXResourceDef> xResources = daoMgr.getXXResourceDef().findByServiceDefId(serviceDefId);
		if(!stringUtil.isEmpty(xResources)) {
			List<RangerResourceDef> resources = new ArrayList<RangerResourceDef>();
			for(XXResourceDef xResource : xResources) {
				RangerResourceDef resource = populateXXToRangerResourceDef(xResource);
				resources.add(resource);
			}
			serviceDef.setResources(resources);
		}
		
		List<XXAccessTypeDef> xAccessTypes = daoMgr.getXXAccessTypeDef().findByServiceDefId(serviceDefId);
		if(!stringUtil.isEmpty(xAccessTypes)) {
			List<RangerAccessTypeDef> accessTypes = new ArrayList<RangerAccessTypeDef>();
			for(XXAccessTypeDef xAtd : xAccessTypes) {
				RangerAccessTypeDef accessType = populateXXToRangerAccessTypeDef(xAtd);
				accessTypes.add(accessType);
			}
			serviceDef.setAccessTypes(accessTypes);
		}
		
		List<XXPolicyConditionDef> xPolicyConditions = daoMgr.getXXPolicyConditionDef().findByServiceDefId(serviceDefId);
		if(!stringUtil.isEmpty(xPolicyConditions)) {
			List<RangerPolicyConditionDef> policyConditions = new ArrayList<RangerServiceDef.RangerPolicyConditionDef>();
			for(XXPolicyConditionDef xPolicyCondDef : xPolicyConditions) {
				RangerPolicyConditionDef policyCondition = populateXXToRangerPolicyConditionDef(xPolicyCondDef);
				policyConditions.add(policyCondition);
			}
			serviceDef.setPolicyConditions(policyConditions);
		}
		
		List<XXContextEnricherDef> xContextEnrichers = daoMgr.getXXContextEnricherDef().findByServiceDefId(serviceDefId);
		if(!stringUtil.isEmpty(xContextEnrichers)) {
			List<RangerContextEnricherDef> contextEnrichers = new ArrayList<RangerServiceDef.RangerContextEnricherDef>();
			for(XXContextEnricherDef xContextEnricherDef : xContextEnrichers) {
				RangerContextEnricherDef contextEnricher = populateXXToRangerContextEnricherDef(xContextEnricherDef);
				contextEnrichers.add(contextEnricher);
			}
			serviceDef.setContextEnrichers(contextEnrichers);
		}
		
		List<XXEnumDef> xEnumList = daoMgr.getXXEnumDef().findByServiceDefId(serviceDefId);
		if(!stringUtil.isEmpty(xEnumList)) {
			List<RangerEnumDef> enums = new ArrayList<RangerEnumDef>();
			for(XXEnumDef xEnum : xEnumList) {
				RangerEnumDef vEnum = populateXXToRangerEnumDef(xEnum);
				enums.add(vEnum);
			}
			serviceDef.setEnums(enums);
		}
		return serviceDef;
	}
	
	public List<RangerServiceDef> getAllServiceDefs() {
		List<XXServiceDef> xxServiceDefList = daoMgr.getXXServiceDef().getAll();
		List<RangerServiceDef> serviceDefList = new ArrayList<RangerServiceDef>();
		
		for(XXServiceDef xxServiceDef : xxServiceDefList) {
			RangerServiceDef serviceDef = populateViewBean(xxServiceDef);
			serviceDefList.add(serviceDef);
		}
		return serviceDefList;
	}
	
	public RangerServiceDef getPopulatedViewObject(XXServiceDef xServiceDef) {
		return this.populateViewBean(xServiceDef);
	}

}
