package org.apache.ranger.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.common.GUIDUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.util.SearchFilter;

public abstract class RangerServiceServiceBase<T extends XXService, V extends RangerService> extends RangerBaseModelService<T, V> {
	
	@Override
	@SuppressWarnings("unchecked")
	protected XXService mapViewToEntityBean(RangerService vObj, XXService xObj, int OPERATION_CONTEXT) {
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
	protected RangerService mapEntityToViewBean(RangerService vObj, XXService xObj) {
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
	public List<RangerService> searchRangerPolicies(SearchFilter searchFilter) {
		List<RangerService> serviceList = new ArrayList<RangerService>();
		
		List<XXService> xSvcList = (List<XXService>) searchResources(searchFilter, searchFields, sortFields, null);
		for(XXService xSvc : xSvcList) {
			serviceList.add(populateViewBean((T) xSvc));
		}
		
		return serviceList;
	}
	
}
