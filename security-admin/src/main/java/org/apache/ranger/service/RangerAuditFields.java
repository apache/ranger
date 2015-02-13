package org.apache.ranger.service;

import org.apache.ranger.entity.XXDBBase;
import org.springframework.stereotype.Component;

@Component
public class RangerAuditFields<T extends XXDBBase> {
	
	public T populateAuditFields(T xObj, T parentObj) {
		xObj.setCreateTime(parentObj.getCreateTime());
		xObj.setUpdateTime(parentObj.getUpdateTime());
		xObj.setAddedByUserId(parentObj.getAddedByUserId());
		xObj.setUpdatedByUserId(parentObj.getUpdatedByUserId());
		return xObj;
	}

}
