package org.apache.ranger.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ranger.db.XXAccessTypeDefDao;
import org.apache.ranger.db.XXPolicyResourceDao;
import org.apache.ranger.entity.XXAccessTypeDef;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXPolicyConditionDef;
import org.apache.ranger.entity.XXPolicyItem;
import org.apache.ranger.entity.XXPolicyItemAccess;
import org.apache.ranger.entity.XXPolicyItemCondition;
import org.apache.ranger.entity.XXPolicyResource;
import org.apache.ranger.entity.XXPolicyResourceMap;
import org.apache.ranger.entity.XXResourceDef;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope("singleton")
public class RangerPolicyService extends RangerPolicyServiceBase<XXPolicy, RangerPolicy> {

	@Override
	protected void validateForCreate(RangerPolicy vObj) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void validateForUpdate(RangerPolicy vObj, XXPolicy entityObj) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	protected RangerPolicy populateViewBean(XXPolicy xPolicy) {
		RangerPolicy vPolicy = super.populateViewBean(xPolicy);
		
		Map<String, RangerPolicyResource> resources = getResourcesForXXPolicy(xPolicy);
		vPolicy.setResources(resources);
		
		List<RangerPolicyItem> policyItems = getPolicyItemListForXXPolicy(xPolicy);
		vPolicy.setPolicyItems(policyItems);
		
		return vPolicy;
	}
	
	public List<RangerPolicyItem> getPolicyItemListForXXPolicy(XXPolicy xPolicy) {
		
		List<RangerPolicyItem> policyItems = new ArrayList<RangerPolicyItem>();
		List<XXPolicyItem> xPolicyItemList = daoMgr.getXXPolicyItem().findByPolicyId(xPolicy.getId());
		
		for(XXPolicyItem xPolItem : xPolicyItemList) {
			RangerPolicyItem policyItem = populateXXToRangerPolicyItem(xPolItem);
			policyItems.add(policyItem);
		}
		return policyItems;
	}

	public RangerPolicyItem populateXXToRangerPolicyItem(XXPolicyItem xPolItem) {
		
		RangerPolicyItem rangerPolItem = new RangerPolicyItem();
		
		List<XXPolicyItemAccess> xPolItemAccList = daoMgr
				.getXXPolicyItemAccess().findByPolicyItemId(xPolItem.getId());
		List<RangerPolicyItemAccess> accesses = new ArrayList<RangerPolicyItemAccess>();
		
		XXAccessTypeDefDao xAccDefDao = daoMgr.getXXAccessTypeDef();
		for(XXPolicyItemAccess xPolAccess : xPolItemAccList) {
			RangerPolicyItemAccess access = new RangerPolicyItemAccess();
			access.setIsAllowed(xPolAccess.getIsallowed());
			XXAccessTypeDef xAccessType = xAccDefDao.getById(xPolAccess.getType());
			access.setType(xAccessType.getName());
			
			accesses.add(access);
		}
		rangerPolItem.setAccesses(accesses);
		
		List<RangerPolicyItemCondition> conditions = new ArrayList<RangerPolicyItemCondition>();
		List<XXPolicyConditionDef> xConditionDefList = daoMgr
				.getXXPolicyConditionDef()
				.findByPolicyItemId(xPolItem.getId());
		for(XXPolicyConditionDef xCondDef : xConditionDefList) {
			
			List<XXPolicyItemCondition> xPolCondItemList = daoMgr
					.getXXPolicyItemCondition().findByPolicyItemAndDefId(
							xPolItem.getId(), xCondDef.getId());
			List<String> values = new ArrayList<String>();
			
			for(XXPolicyItemCondition polCond : xPolCondItemList) {
				values.add(polCond.getValue());
			}
			
			RangerPolicyItemCondition condition = new RangerPolicyItemCondition();
			condition.setType(xCondDef.getName());
			condition.setValues(values);
			
			conditions.add(condition);
		}
		rangerPolItem.setConditions(conditions);
		
		List<String> userList = daoMgr.getXXUser().findByPolicyItemId(xPolItem.getId());
		List<String> grpList = daoMgr.getXXGroup().findByPolicyItemId(xPolItem.getId());
		
		rangerPolItem.setUsers(userList);
		rangerPolItem.setGroups(grpList);
		
		rangerPolItem.setDelegateAdmin(xPolItem.getDelegateAdmin());
		return rangerPolItem;
	}

	public Map<String, RangerPolicyResource> getResourcesForXXPolicy(XXPolicy xPolicy) {
		List<XXResourceDef> resDefList = daoMgr.getXXResourceDef().findByPolicyId(xPolicy.getId());
		Map<String, RangerPolicyResource> resources = new HashMap<String, RangerPolicyResource>();
		
		XXPolicyResourceDao xPolResDao = daoMgr.getXXPolicyResource();
		for(XXResourceDef xResDef : resDefList) {
			XXPolicyResource xPolRes = xPolResDao.findByResDefIdAndPolicyId(
					xResDef.getId(), xPolicy.getId());
			if(xPolRes == null) {
				continue;
			}
			List<String> values = new ArrayList<>();
			List<XXPolicyResourceMap> xPolResMapList = daoMgr
					.getXXPolicyResourceMap()
					.findByPolicyResId(xPolRes.getId());
			for(XXPolicyResourceMap xPolResMap : xPolResMapList) {
				values.add(xPolResMap.getValue());
			}
			RangerPolicyResource resource = new RangerPolicyResource();
			resource.setValues(values);
			resource.setIsExcludes(xPolRes.getIsexcludes());
			resource.setIsRecursive(xPolRes.getIsrecursive());
			
			resources.put(xResDef.getName(), resource);
		}
		return resources;
	}
	
	public RangerPolicy getPopulatedViewObject(XXPolicy xPolicy) {
		return this.populateViewBean(xPolicy);
	}

}
