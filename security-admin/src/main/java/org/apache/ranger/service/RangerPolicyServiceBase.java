package org.apache.ranger.service;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.common.GUIDUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.plugin.model.RangerPolicy;

public abstract class RangerPolicyServiceBase<T extends XXPolicy, V extends RangerPolicy> extends RangerBaseModelService<T, V> {
	
	@Override
	@SuppressWarnings("unchecked")
	protected XXPolicy mapViewToEntityBean(RangerPolicy vObj, XXPolicy xObj, int OPERATION_CONTEXT) {
		String guid = (StringUtils.isEmpty(vObj.getGuid())) ? GUIDUtil.genGUI() : vObj.getGuid();
		
		xObj.setGuid(guid);
		xObj.setVersion(vObj.getVersion());
		
		XXService xService = daoMgr.getXXService().findByName(vObj.getService());
		if(xService == null) {
			throw restErrorUtil.createRESTException(
					"No corresponding service found for policyName: "
							+ vObj.getName() + "Service Not Found : "
							+ vObj.getName(), MessageEnums.INVALID_INPUT_DATA);
		}
		xObj.setService(xService.getId());
		xObj.setName(vObj.getName());
		xObj.setDescription(vObj.getDescription());
		xObj.setIsAuditEnabled(vObj.getIsAuditEnabled());
		xObj.setIsEnabled(vObj.getIsEnabled());
		
		return xObj;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected RangerPolicy mapEntityToViewBean(RangerPolicy vObj, XXPolicy xObj) {
		XXService xService = daoMgr.getXXService().getById(xObj.getService());
		vObj.setGuid(xObj.getGuid());
		vObj.setVersion(xObj.getVersion());
		vObj.setService(xService.getName());
		vObj.setName(xObj.getName());
		vObj.setDescription(xObj.getDescription());
		vObj.setIsEnabled(xObj.getIsEnabled());
		vObj.setIsAuditEnabled(xObj.getIsAuditEnabled());
		return vObj;
	}
}
