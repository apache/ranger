package org.apache.ranger.service;

import java.util.HashMap;
import java.util.List;

import org.apache.ranger.common.JSONUtil;
import org.apache.ranger.entity.XXServiceBase;
import org.apache.ranger.entity.XXServiceWithAssignedId;
import org.apache.ranger.entity.XXServiceConfigMap;
import org.apache.ranger.plugin.model.RangerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class RangerServiceWithAssignedIdService extends RangerServiceServiceBase<XXServiceWithAssignedId, RangerService> {

	@Autowired
	JSONUtil jsonUtil;

	@Override
	protected XXServiceWithAssignedId mapViewToEntityBean(RangerService vObj, XXServiceWithAssignedId xObj, int OPERATION_CONTEXT) {
		return (XXServiceWithAssignedId)super.mapViewToEntityBean(vObj, (XXServiceBase)xObj, OPERATION_CONTEXT);
	}

	@Override
	protected RangerService mapEntityToViewBean(RangerService vObj, XXServiceWithAssignedId xObj) {
		return super.mapEntityToViewBean(vObj, (XXServiceBase)xObj);
	}
	
	@Override
	protected void validateForCreate(RangerService vObj) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void validateForUpdate(RangerService vService, XXServiceWithAssignedId xService) {
		
	}
	
	@Override
	protected RangerService populateViewBean(XXServiceWithAssignedId xService) {
		RangerService vService = super.populateViewBean(xService);
		
		HashMap<String, String> configs = new HashMap<String, String>();
		List<XXServiceConfigMap> svcConfigMapList = daoMgr.getXXServiceConfigMap()
				.findByServiceId(xService.getId());
		for(XXServiceConfigMap svcConfMap : svcConfigMapList) {
			configs.put(svcConfMap.getConfigkey(), svcConfMap.getConfigvalue());
		}
		vService.setConfigs(configs);
		
		return vService;
	}
	
	public RangerService getPopulatedViewObject(XXServiceWithAssignedId xService) {
		return this.populateViewBean(xService);
	}

}
