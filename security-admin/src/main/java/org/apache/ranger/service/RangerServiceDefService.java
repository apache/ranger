package org.apache.ranger.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.ranger.entity.XXAccessTypeDef;
import org.apache.ranger.entity.XXEnumDef;
import org.apache.ranger.entity.XXPolicyConditionDef;
import org.apache.ranger.entity.XXResourceDef;
import org.apache.ranger.entity.XXServiceConfigDef;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
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
	
	public List<RangerServiceDef> getServiceDefs(SearchFilter filter) {
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
