package org.apache.ranger.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceConfigMap;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.util.SearchFilter;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope("singleton")
public class RangerServiceService extends RangerServiceServiceBase<XXService, RangerService> {

	@Override
	protected void validateForCreate(RangerService vObj) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void validateForUpdate(RangerService vService, XXService xService) {
		
	}
	
	@Override
	protected RangerService populateViewBean(XXService xService) {
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
	
	public RangerService getPopulatedViewObject(XXService xService) {
		return this.populateViewBean(xService);
	}
	
	public List<RangerService> getServices(SearchFilter filter) {
		List<XXService> xxServiceList = daoMgr.getXXService().getAll();
		List<RangerService> serviceList = new ArrayList<RangerService>();
		
		for(XXService xxService : xxServiceList) {
			RangerService service = populateViewBean(xxService);
			serviceList.add(service);
		}
		return serviceList;
	}

}
